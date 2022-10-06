import json
import os
import tempfile
from copy import deepcopy
from os.path import expanduser as user
from typing import Dict, List, Optional

import hubble
from docarray import Document, DocumentArray
from jina import __version__ as jina_version

from now.apps.base.app import JinaNOWApp
from now.constants import (
    DEFAULT_EXAMPLE_HOSTED,
    NOW_ANNLITE_INDEXER_VERSION,
    NOW_PREPROCESSOR_VERSION,
    PREFETCH_NR,
    DatasetTypes,
)
from now.finetuning.run_finetuning import finetune
from now.finetuning.settings import FinetuneSettings, parse_finetune_settings
from now.now_dataclasses import UserInput
from now.utils import _maybe_download_from_s3


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
        or user_input.app_instance.app_name == 'music_to_music'
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

    if 'NOW_EXAMPLES' in os.environ:
        valid_app = DEFAULT_EXAMPLE_HOSTED.get(user_input.app_instance.app_name, {})
        is_demo_ds = user_input.data in valid_app
        if is_demo_ds:
            config[
                'CUSTOM_DNS'
            ] = f'now-example-{user_input.app_instance.app_name}-{user_input.data}.dev.jina.ai'
            config['CUSTOM_DNS'] = config['CUSTOM_DNS'].replace('_', '-')
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
            _maybe_download_from_s3(
                docs=DocumentArray([d]),
                tmpdir=tmpdir,
                user_input=user_input,
                max_workers=1,
            )
    tags = set()
    for tag, _ in d.tags.items():
        tags.add((tag, str(tag.__class__.__name__)))
    final_tags = [list(tag) for tag in tags]
    return final_tags
