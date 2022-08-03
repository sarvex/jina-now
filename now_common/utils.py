from typing import Dict, Optional, Tuple

from docarray import Document, DocumentArray

from now.apps.base.app import JinaNOWApp
from now.constants import PREFETCH_NR
from now.data_loading.convert_datasets_to_jpeg import to_thumbnail_jpg
from now.finetuning.run_finetuning import finetune
from now.finetuning.settings import FinetuneSettings, parse_finetune_settings
from now.now_dataclasses import UserInput


def get_clip_music_flow_env_dict(
    finetune_settings: FinetuneSettings,
    encoder_uses: str,
    encoder_uses_with: Dict,
    indexer_uses: str,
    owner_id: str,
    email_ids: str,
    secured: bool,
):
    """Returns dictionary for the environments variables for the clip & music flow.yml files."""
    indexer_name = f'jinahub+docker://' + indexer_uses
    encoder_name = f'jinahub+docker://' + encoder_uses

    if finetune_settings.bi_modal:
        pre_trained_embedding_size = finetune_settings.pre_trained_embedding_size * 2
    else:
        pre_trained_embedding_size = finetune_settings.pre_trained_embedding_size
    config = {
        'ENCODER_NAME': encoder_name,
        'FINETUNE_LAYER_SIZE': finetune_settings.finetune_layer_size,
        'PRE_TRAINED_EMBEDDINGS_SIZE': pre_trained_embedding_size,
        'INDEXER_NAME': indexer_name,
        'PREFETCH': PREFETCH_NR,
    }
    if encoder_uses_with.get('pretrained_model_name_or_path'):
        config['PRE_TRAINED_MODEL_NAME'] = encoder_uses_with[
            "pretrained_model_name_or_path"
        ]
    if finetune_settings.perform_finetuning:
        config['FINETUNE_ARTIFACT'] = finetune_settings.finetuned_model_artifact
        config['JINA_TOKEN'] = finetune_settings.token

    if secured:
        config['OWNER_ID'] = owner_id
        if email_ids:
            config['EMAIL_IDS'] = email_ids

    return config


def setup_clip_music_apps(
    app_instance: JinaNOWApp,
    user_input: UserInput,
    dataset: DocumentArray,
    encoder_uses: str,
    encoder_uses_with: Dict,
    indexer_uses: str,
    kubectl_path: str,
    finetune_datasets: Optional[Tuple] = (),
) -> Dict:
    finetune_settings = parse_finetune_settings(
        app_instance=app_instance,
        user_input=user_input,
        dataset=dataset,
        finetune_datasets=finetune_datasets,
    )

    env_dict = get_clip_music_flow_env_dict(
        finetune_settings=finetune_settings,
        encoder_uses=encoder_uses,
        encoder_uses_with=encoder_uses_with,
        indexer_uses=indexer_uses,
        owner_id=user_input.owner_id,
        email_ids=user_input.email_ids,
        secured=user_input.secured,
    )

    if finetune_settings.perform_finetuning:
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

    app_instance.set_flow_yaml(finetuning=finetune_settings.perform_finetuning)

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

    return DocumentArray(d for d in da if d.text)
