""" This module contains pre-configurations for finetuning on the demo datasets. """
from dataclasses import dataclass
from typing import Optional, Tuple

from docarray import DocumentArray

from now.constants import DatasetTypes
from now.now_dataclasses import UserInput

DEFAULT_EPOCHS = 50
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
    return False
    # if (
    #     (
    #         user_input.dataset_type == DatasetTypes.DEMO
    #         and user_input.dataset_name in finetuneable_datasets
    #     )
    #     or (
    #         user_input.dataset_type != DatasetTypes.DEMO
    #         and all(['finetuner_label' in d.tags for d in dataset])
    #     )
    #     # TODO: enable finetuning again when FinetunerExecutor has been updated
    #     # or user_input.app_instance.app_name == Apps.TEXT_TO_TEXT_AND_IMAGE
    # ):
    #     return True
    # else:
    #     return False


def _is_bi_modal(user_input: UserInput, dataset: DocumentArray) -> bool:
    if user_input.dataset_type != DatasetTypes.DEMO:
        has_blob = any([d.blob != b'' for d in dataset])
        has_text = any([d.text != '' for d in dataset])
        return has_text and has_blob
    else:
        return True  # right now all demo cases are bi-modal


def parse_finetune_settings(
    user_input: UserInput,
    dataset: DocumentArray,
    model_name: str,
    loss: str,
    finetune_datasets: Tuple = (),
    add_embeddings: bool = True,
    pre_trained_embedding_size: Optional[int] = None,
) -> FinetuneSettings:
    """This function parses the user input configuration into the finetune settings"""
    return FinetuneSettings(
        perform_finetuning=_is_finetuning(user_input, dataset, finetune_datasets),
        bi_modal=_is_bi_modal(user_input, dataset),
        pre_trained_embedding_size=pre_trained_embedding_size,
        model_name=model_name,
        loss=loss,
        add_embeddings=add_embeddings,
    )
