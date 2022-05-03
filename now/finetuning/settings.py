""" This module contains pre-configurations for finetuning on the demo datasets. """
from dataclasses import dataclass

from now.constants import Modalities, Qualities
from now.dialog import UserInput

TUNEABLE_DEMO_DATASETS = {
    Modalities.IMAGE: ['deepfasion', 'bird-species'],
    Modalities.TEXT: [],
    Modalities.MUSIC: ['music-genres-small', 'music-genres-large'],
}

DEFAULT_EPOCHS = 50
DEFAULT_NUM_VAL_QUERIES = 10
DEFAULT_FINETUNED_EMBEDDING_SIZE = 128
DEFAULT_BATCH_SIZE = 128
DEFAULT_TRAIN_VAL_SPLIT_RATIO = 0.9

PRE_TRAINED_EMBEDDING_SIZE = {
    Modalities.IMAGE: {
        Qualities.MEDIUM: 512,
        Qualities.GOOD: 512,
        Qualities.EXCELLENT: 768,
    },
    Modalities.TEXT: {
        Qualities.MEDIUM: 512,
        Qualities.GOOD: 512,
        Qualities.EXCELLENT: 768,
    },
    Modalities.MUSIC: 512,
}


@dataclass
class FinetuneSetting:
    pre_trained_embedding_size: int

    batch_size: int = DEFAULT_BATCH_SIZE
    epochs: int = DEFAULT_EPOCHS
    finetune_layer_size: int = DEFAULT_FINETUNED_EMBEDDING_SIZE
    train_val_split_ration: int = DEFAULT_TRAIN_VAL_SPLIT_RATIO
    num_val_queries: int = DEFAULT_NUM_VAL_QUERIES


def _get_embedding_size(user_input: UserInput) -> int:
    """Returns the dimension of embeddings given the configured user input object."""
    if user_input.output_modality == Modalities.MUSIC:
        assert user_input.quality is None, 'Music modality has no quality'
        return PRE_TRAINED_EMBEDDING_SIZE[Modalities.MUSIC]
    else:
        assert user_input.quality is not None, (
            f'Missing quality ' f'for modality {user_input.output_modality}.'
        )
        return PRE_TRAINED_EMBEDDING_SIZE[user_input.quality][user_input.quality]


def parse_finetune_settings(user_input: UserInput) -> FinetuneSetting:
    """This function parse the user input configuration into the finetune settings"""
    return FinetuneSetting(pre_trained_embedding_size=_get_embedding_size(user_input))
