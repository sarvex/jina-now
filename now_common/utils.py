import json
import os
from os.path import expanduser as user
from typing import Dict, Union

import hubble
from docarray import Document, DocumentArray

from now.apps.base.app import JinaNOWApp
from now.constants import NOW_PREPROCESSOR_VERSION, PREFETCH_NR
from now.data_loading.convert_datasets_to_jpeg import to_thumbnail_jpg
from now.finetuning.run_finetuning import finetune
from now.finetuning.settings import FinetuneSettings, parse_finetune_settings
from now.now_dataclasses import UserInput


def get_clip_music_flow_env_dict(
    finetune_settings: FinetuneSettings,
    encoder_uses: str,
    encoder_uses_with: Dict,
    indexer_uses: str,
    indexer_resources: Dict,
    user_input: UserInput,
    device: str,
    gpu: str,
):
    """Returns dictionary for the environments variables for the clip & music flow.yml files."""
    if finetune_settings.perform_finetuning and finetune_settings.bi_modal:
        pre_trained_embedding_size = finetune_settings.pre_trained_embedding_size * 2
    else:
        pre_trained_embedding_size = finetune_settings.pre_trained_embedding_size

    config = {
        'ENCODER_NAME': f'jinahub+docker://{encoder_uses}',
        'FINETUNE_LAYER_SIZE': finetune_settings.finetune_layer_size,
        'PRE_TRAINED_EMBEDDINGS_SIZE': pre_trained_embedding_size,
        'INDEXER_NAME': f'jinahub+docker://{indexer_uses}',
        'PREFETCH': PREFETCH_NR,
        'PREPROCESSOR_NAME': f'jinahub+docker://NOWPreprocessor/v{NOW_PREPROCESSOR_VERSION}',
        'APP': user_input.app,
        'DEVICE': device,
        'GPU': gpu,
        **indexer_resources,
    }
    if encoder_uses_with.get('pretrained_model_name_or_path'):
        config['PRE_TRAINED_MODEL_NAME'] = encoder_uses_with[
            "pretrained_model_name_or_path"
        ]
    if finetune_settings.perform_finetuning:
        config['FINETUNE_ARTIFACT'] = finetune_settings.finetuned_model_artifact
        config['JINA_TOKEN'] = finetune_settings.token

    return config


def setup_clip_music_apps(
    app_instance: JinaNOWApp,
    user_input: UserInput,
    dataset: DocumentArray,
    encoder_uses: Union[str, Dict],
    encoder_uses_with: Dict,
    indexer_uses: str,
    indexer_resources: Dict,
    kubectl_path: str,
) -> Dict:
    finetune_settings = parse_finetune_settings(
        app_instance=app_instance,
        user_input=user_input,
        dataset=dataset,
        finetune_datasets=app_instance.finetune_datasets,
    )

    gpu = '0'
    device = 'cpu'
    gpu_threshold = 250000
    if 'NOW_CI_RUN' in os.environ:  # test gpu in CI if len(dataset) > 1000
        gpu_threshold = 1000

    user_email = _get_email()

    if (len(dataset) > gpu_threshold) and user_email.split('@')[
        -1
    ] == 'jina.ai':  # uses GPU if dataset contains over 250000 documents and user is from jina team
        gpu = 'shared'
        device = 'cuda'

    if isinstance(encoder_uses, dict):
        key = 'gpu' if device == 'cuda' else 'cpu'
        encoder_ = encoder_uses[key]
    else:
        encoder_ = encoder_uses

    env_dict = get_clip_music_flow_env_dict(
        finetune_settings=finetune_settings,
        encoder_uses=encoder_,
        encoder_uses_with=encoder_uses_with,
        indexer_uses=indexer_uses,
        indexer_resources=indexer_resources,
        user_input=user_input,
        device=device,
        gpu=gpu,
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


def preprocess_images(da: DocumentArray) -> DocumentArray:
    """Loads all documents into memory to thumbnail them."""

    def convert_fn(d: Document):
        try:
            if d.tensor is None:
                if d.blob != b'':
                    d.convert_blob_to_image_tensor()
                elif d.uri:
                    d.load_uri_to_image_tensor()
            return to_thumbnail_jpg(d)
        except:
            return d

    da.apply(convert_fn)
    return DocumentArray(d for d in da if d.blob != b'')


def preprocess_text(da: DocumentArray, split_by_sentences=False) -> DocumentArray:
    """If necessary, loads text for all documents. If asked for, splits documents by sentences."""
    import nltk

    nltk.download('punkt', quiet=True)
    from nltk.tokenize import sent_tokenize

    def convert_fn(d: Document):
        try:
            if not d.text:
                if d.uri:
                    d.load_uri_to_text()
                    d.tags['additional_info'] = d.uri
            return d
        except:
            return d

    def gen_split_by_sentences():
        def _get_sentence_docs(batch):
            ret = []
            for d in batch:
                try:
                    ret += [
                        Document(
                            mime_type='text',
                            text=sentence,
                            tags=d.tags,
                        )
                        for sentence in set(sent_tokenize(d.text.replace('\n', ' ')))
                    ]
                except:
                    pass
            return ret

        for batch in da.map_batch(_get_sentence_docs, backend='process', batch_size=64):
            for d in batch:
                yield d

    da.apply(convert_fn)

    if split_by_sentences:
        da = DocumentArray(d for d in gen_split_by_sentences())

    return DocumentArray(d for d in da if d.text and d.text != '')


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
    config = {'indexer_uses': 'AnnLiteIndexer/0.3.0'}
    threshold1 = 50_000
    threshold2 = 250_000
    if 'NOW_CI_RUN' in os.environ:
        threshold1 = 1_500
    if num_indexed_samples <= threshold1:
        config['indexer_uses'] = 'DocarrayIndexerV2'
        config['indexer_resources'] = {'INDEXER_CPU': 0.1, 'INDEXER_MEM': '2G'}
    elif num_indexed_samples <= threshold2:
        config['indexer_resources'] = {'INDEXER_CPU': 0.1, 'INDEXER_MEM': '2G'}
    else:
        config['indexer_resources'] = {'INDEXER_CPU': 1.0, 'INDEXER_MEM': '4G'}

    return config
