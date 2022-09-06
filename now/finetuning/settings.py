""" This module contains pre-configurations for finetuning on the demo datasets. """
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any

from docarray import DocumentArray

from now.apps.base.app import JinaNOWApp
from now.constants import Apps
from now.now_dataclasses import UserInput

DEFAULT_EPOCHS = 5
DEFAULT_HIDDEN_SIZES = (128,)
DEFAULT_NUM_VAL_QUERIES = 50
DEFAULT_FINETUNED_EMBEDDING_SIZE = 128
DEFAULT_BATCH_SIZE = 128
DEFAULT_TRAIN_VAL_SPLIT_RATIO = 0.9
DEFAULT_EVAL_MATCH_LIMIT = 20
DEFAULT_NUM_ITEMS_PER_CLASS = 4
DEFAULT_LEARNING_RATE = 5e-4
DEFAULT_EARLY_STOPPING_PATIENCE = 5
DEFAULT_POS_MINING_START = 'hard'
DEFAULT_NEG_MINING_START = 'hard'


@dataclass
class FinetuneSettings:
    perform_finetuning: bool
    model_name: str
    add_embeddings: bool
    bi_modal: bool  # atm, bi-modal means text and some blob value
    loss: str
    pre_trained_embedding_size: Optional[int] = None
    finetuned_model_artifact: Optional[str] = None
    token: Optional[str] = None

    hidden_sizes: Tuple[int] = DEFAULT_HIDDEN_SIZES
    batch_size: int = DEFAULT_BATCH_SIZE
    epochs: int = DEFAULT_EPOCHS
    finetune_layer_size: int = DEFAULT_FINETUNED_EMBEDDING_SIZE
    train_val_split_ration: int = DEFAULT_TRAIN_VAL_SPLIT_RATIO
    num_val_queries: int = DEFAULT_NUM_VAL_QUERIES
    eval_match_limit: int = DEFAULT_EVAL_MATCH_LIMIT
    num_items_per_class: int = DEFAULT_NUM_ITEMS_PER_CLASS
    learning_rate: int = DEFAULT_LEARNING_RATE
    pos_mining_strat: str = DEFAULT_POS_MINING_START
    neg_mining_strat: str = DEFAULT_NEG_MINING_START
    early_stopping_patience: int = DEFAULT_EARLY_STOPPING_PATIENCE


def _is_finetuning(
    user_input: UserInput, dataset: DocumentArray, finetuneable_datasets: Tuple
) -> bool:
    if (
        user_input.data in finetuneable_datasets
        or user_input.app == Apps.TEXT_TO_TEXT_AND_IMAGE
    ):
        return True
    elif user_input.is_custom_dataset and all(
        ['finetuner_label' in d.tags for d in dataset]
    ):
        return True
    else:
        return False


def _is_bi_modal(user_input: UserInput, dataset: DocumentArray) -> bool:
    if user_input.app == Apps.TEXT_TO_TEXT_AND_IMAGE:
        return False
    elif user_input.is_custom_dataset:
        has_blob = any([d.blob != b'' for d in dataset])
        has_text = any([d.text != '' for d in dataset])
        return has_text and has_blob
    else:
        return True  # right now all demo cases are bi-modal


def _get_model_name(app: Apps, metadata: Optional[dict] = None) -> str:
    """
    Get the name of the model to be fine-tuned. `TextToTextAndImage` needs to fine-tune
    both sbert and clip model depending on the encoder. Other apps only fine-tune an
    additional linear layer.

    :param app: Name of the app.
    :param metadata: Additional info on model, app or dataset.
    :return: Name of the model.
    """
    if app == Apps.TEXT_TO_TEXT_AND_IMAGE:
        if metadata['encoder_type'] == 'text_to_text':
            return 'sentence-transformers/msmarco-distilbert-base-v3'
        elif metadata['encoder_type'] == 'text_to_image':
            return 'openai/clip-vit-base-patch32'
    else:
        return 'mlp'


def _get_loss(app: Apps, metadata: Optional[dict] = None) -> str:
    """
    Get loss function based on the app and encoder type.

    :param app: Name of the app.
    :param metadata: Additional info on model, app or dataset.
    :return: Name of the loss function.
    """
    if app == Apps.TEXT_TO_TEXT_AND_IMAGE:
        if metadata['encoder_type'] == 'text_to_image':
            return 'CLIPLoss'
    return 'TripletMarginLoss'


def _add_embeddings(app: Apps) -> bool:
    """
    Determines whether we need to add embeddings to the dataset before fine-tuning.
    (Currently, this is `True` for every app except `TextToTextAndImage`).

    :param app: Name of the app.
    :return: `True` if embeddings need to be calculated beforehand, `False` otherwise.
    """
    return app != Apps.TEXT_TO_TEXT_AND_IMAGE


def parse_finetune_settings(
    user_input: UserInput,
    dataset: DocumentArray,
    finetune_datasets: Tuple = (),
    pre_trained_embedding_size: Optional[int] = None,
    metadata: Optional[dict] = None,
) -> FinetuneSettings:
    """This function parses the user input configuration into the finetune settings"""
    return FinetuneSettings(
        perform_finetuning=_is_finetuning(user_input, dataset, finetune_datasets),
        bi_modal=_is_bi_modal(user_input, dataset),
        model_name=_get_model_name(user_input.app, metadata),
        loss=_get_loss(user_input.app, metadata),
        add_embeddings=_add_embeddings(user_input.app),
        pre_trained_embedding_size=pre_trained_embedding_size,
    )
