import base64
import json
import os
import tempfile
from collections.abc import MutableMapping
from copy import deepcopy
from os.path import expanduser as user
from typing import Dict, List, Optional

import boto3
import hubble
from docarray import Document, DocumentArray
from jina import __version__ as jina_version

from now.apps.base.app import JinaNOWApp
from now.constants import (
    NOW_ANNLITE_INDEXER_VERSION,
    NOW_PREPROCESSOR_VERSION,
    PREFETCH_NR,
    DatasetTypes,
)
from now.finetuning.run_finetuning import finetune
from now.finetuning.settings import FinetuneSettings, parse_finetune_settings
from now.now_dataclasses import UserInput


def common_get_flow_env_dict(
    finetune_settings: FinetuneSettings,
    encoder_uses: str,
    encoder_with: Dict,
    encoder_uses_with: Dict,
    pre_trained_embedding_size: int,
    indexer_uses: str,
    indexer_resources: Dict,
    user_input: UserInput,
    tags: List,
):
    """Returns dictionary for the environments variables for the clip & music flow.yml files."""
    if (
        finetune_settings.perform_finetuning and finetune_settings.bi_modal
    ) or user_input.app_instance.app_name == 'music_to_music':
        pre_trained_embedding_size = pre_trained_embedding_size * 2

    config = {
        'JINA_VERSION': jina_version,
        'ENCODER_NAME': f'jinahub+docker://{encoder_uses}',
        'N_DIM': finetune_settings.finetune_layer_size
        if finetune_settings.perform_finetuning
        else pre_trained_embedding_size,
        'PRE_TRAINED_EMBEDDINGS_SIZE': pre_trained_embedding_size,
        'INDEXER_NAME': f'jinahub+docker://{indexer_uses}',
        'PREFETCH': PREFETCH_NR,
        'PREPROCESSOR_NAME': f'jinahub+docker://NOWPreprocessor/v{NOW_PREPROCESSOR_VERSION}',
        'APP': user_input.app_instance.app_name,
        'COLUMNS': tags,
        'ADMIN_EMAILS': user_input.admin_emails or [] if user_input.secured else [],
        'USER_EMAILS': user_input.user_emails or [] if user_input.secured else [],
        **encoder_with,
        **indexer_resources,
    }
    if encoder_uses_with.get('pretrained_model_name_or_path'):
        config['PRE_TRAINED_MODEL_NAME'] = encoder_uses_with[
            "pretrained_model_name_or_path"
        ]
    if finetune_settings.perform_finetuning:
        config['FINETUNE_ARTIFACT'] = finetune_settings.finetuned_model_artifact
        config['JINA_TOKEN'] = finetune_settings.token

    # retention days
    if 'NOW_CI_RUN' in os.environ:
        config[
            'RETENTION_DAYS'
        ] = 0  # JCloud will delete after 24hrs of being idle if not deleted in CI
    else:
        config['RETENTION_DAYS'] = 7  # for user deployment set it to 30 days

    return config


def common_setup(
    app_instance: JinaNOWApp,
    user_input: UserInput,
    dataset: DocumentArray,
    encoder_uses: str,
    encoder_uses_with: Dict,
    indexer_uses: str,
    pre_trained_embedding_size: int,
    kubectl_path: str,
    encoder_with: Optional[Dict] = {},
    indexer_resources: Optional[Dict] = {},
) -> Dict:
    # should receive pre embedding size
    finetune_settings = parse_finetune_settings(
        pre_trained_embedding_size=pre_trained_embedding_size,
        user_input=user_input,
        dataset=dataset,
        finetune_datasets=app_instance.finetune_datasets,
        model_name='mlp',
        add_embeddings=True,
        loss='TripletMarginLoss',
    )
    tags = _extract_tags_annlite(deepcopy(dataset[0]), user_input)
    env_dict = common_get_flow_env_dict(
        finetune_settings=finetune_settings,
        encoder_uses=encoder_uses,
        encoder_with=encoder_with,
        encoder_uses_with=encoder_uses_with,
        pre_trained_embedding_size=pre_trained_embedding_size,
        indexer_uses=indexer_uses,
        indexer_resources=indexer_resources,
        user_input=user_input,
        tags=tags,
    )

    if finetune_settings.perform_finetuning:
        try:
            artifact_id, token = finetune(
                finetune_settings=finetune_settings,
                app_instance=app_instance,
                dataset=dataset,
                user_input=user_input,
                env_dict=env_dict,
                kubectl_path=kubectl_path,
            )

            finetune_settings.finetuned_model_artifact = artifact_id
            finetune_settings.token = token

            env_dict['FINETUNE_ARTIFACT'] = finetune_settings.finetuned_model_artifact
            env_dict['JINA_TOKEN'] = finetune_settings.token
        except Exception as e:
            print(
                'Finetuning is currently offline. The programm execution still continues without finetuning. Please report the following exception to us:'
            )
            import traceback

            traceback.print_exc()
            finetune_settings.perform_finetuning = False

    app_instance.set_flow_yaml(
        finetuning=finetune_settings.perform_finetuning, dataset_len=len(dataset)
    )

    return env_dict


def _get_email():
    try:
        with open(user('~/.jina/config.json')) as fp:
            config_val = json.load(fp)
            user_token = config_val['auth_token']
            client = hubble.Client(token=user_token, max_retries=None, jsonify=True)
            response = client.get_user_info()
        if 'email' in response['data']:
            return response['data']['email']
        return ''
    except FileNotFoundError:
        return ''


def get_indexer_config(num_indexed_samples: int) -> Dict:
    """Depending on the number of samples, which will be indexed, indexer and its resources are determined.

    :param num_indexed_samples: number of samples which will be indexed; should incl. chunks for e.g. text-to-video app
    """
    config = {'indexer_uses': f'NOWAnnLiteIndexer/v{NOW_ANNLITE_INDEXER_VERSION}'}
    threshold1 = 250_000
    if num_indexed_samples <= threshold1:
        config['indexer_resources'] = {'INDEXER_CPU': 0.1, 'INDEXER_MEM': '2G'}
    else:
        config['indexer_resources'] = {'INDEXER_CPU': 1.0, 'INDEXER_MEM': '4G'}

    return config


def _extract_tags_annlite(d: Document, user_input):
    print(
        'We assume all tags follow the same structure, only first json file will be used to determine structure'
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        if user_input and user_input.custom_dataset_type == DatasetTypes.S3_BUCKET:
            _maybe_download_from_s3(doc=d, tmpdir=tmpdir, user_input=user_input)
    tags = set()
    for tag, _ in d.tags.items():
        tags.add((tag, str(tag.__class__.__name__)))
    final_tags = [list(tag) for tag in tags]
    return final_tags


def download(bucket, uri, tmpdir):
    path_s3 = '/'.join(uri.split('/')[3:])
    path_local = os.path.join(
        str(tmpdir),
        base64.b64encode(bytes(path_s3, "utf-8")).decode("utf-8"),
    )
    bucket.download_file(
        path_s3,
        path_local,
    )
    return path_local


def convert_fn(d: Document, user_input, tmpdir) -> Document:
    """Downloads files and tags from S3 bucket and updates the content uri and the tags uri to the local path"""
    d.tags['uri'] = d.uri
    session = boto3.session.Session(
        aws_access_key_id=user_input.aws_access_key_id,
        aws_secret_access_key=user_input.aws_secret_access_key,
        region_name=user_input.aws_region_name,
    )
    bucket = session.resource('s3').Bucket(d.uri.split('/')[2])
    d.uri = download(bucket=bucket, uri=d.uri, tmpdir=tmpdir)
    if 'tag_uri' in d.tags:
        d.tags['tag_uri'] = download(bucket, d.tags['tag_uri'], tmpdir)
        with open(d.tags['tag_uri'], 'r') as fp:
            tags = json.load(fp)
            tags = flatten_dict(tags)
            d.tags.update(tags)
        del d.tags['tag_uri']
    return d


def _maybe_download_from_s3(
    doc: Document, tmpdir: tempfile.TemporaryDirectory, user_input
):
    """Downloads file to local temporary dictionary, saves S3 URI to `tags['uri']` and modifies `uri` attribute of
    document to local path in-place.

    :param doc: document containing URI pointing to the location on S3 bucket
    :param tmpdir: temporary directory in which files will be saved
    """

    if doc.uri.startswith('s3://'):
        doc = convert_fn(doc, user_input, tmpdir)


def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
