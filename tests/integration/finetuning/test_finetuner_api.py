""" This module implements integrations tests against the external finetune-api """
import finetuner
import numpy as np
import pytest
from docarray import Document, DocumentArray
from pytest_mock import MockerFixture

from now.constants import (
    CLIP_USES,
    IMAGE_MODEL_QUALITY_MAP,
    Apps,
    Modalities,
    Qualities,
)
from now.dialog import UserInput
from now.finetuning.dataset import FinetuneDataset
from now.finetuning.run_finetuning import finetune_now
from now.finetuning.settings import FinetuneSettings, parse_finetune_settings


@pytest.fixture()
def random_dataset() -> DocumentArray:
    num_classes = 32
    num_images_per_class = 100
    image_shape = (28, 28, 3)

    train_data = DocumentArray()
    for class_id in range(num_classes):
        for _ in range(num_images_per_class):
            doc = Document(
                tensor=np.random.rand(
                    *image_shape
                ),  # are the embeddings computed behind the api?
                tags={'finetuner_label': str(class_id)},
            )
            train_data.append(doc)
    return train_data


@pytest.fixture()
def random_embeddings() -> DocumentArray:
    num_classes = 10
    num_embeddings_per_class = 100
    embeddings_shape = (512,)

    train_data = DocumentArray()
    for class_id in range(num_classes):
        for _ in range(num_embeddings_per_class):
            doc = Document(
                tensor=np.random.rand(
                    *embeddings_shape
                ),  # are the embeddings computed behind the api?
                tags={'finetuner_label': str(class_id)},
            )
            train_data.append(doc)
    return train_data


@pytest.fixture()
def user_input() -> UserInput:
    return UserInput(
        output_modality=Modalities.IMAGE,
        is_custom_dataset=True,
        quality=Qualities.MEDIUM,
        data='test-dataset',
        app=Apps.TEXT_TO_IMAGE,
    )


@pytest.fixture()
def finetune_setting(
    user_input: UserInput, random_dataset: DocumentArray
) -> FinetuneSettings:
    return parse_finetune_settings(
        user_input, dataset=random_dataset, finetuneable_datasets=(user_input.data,)
    )


def patch_finetune_layer(
    finetune_ds: FinetuneDataset, finetune_settings: FinetuneSettings, save_dir: str
) -> str:
    finetuner.login()
    finetuner.fit(
        model='mlp',
        model_options={'input_size': 512, 'hidden_size': [128], 'l2': True},
        train_data=finetune_ds.train,
    )
    # run = finetuner.fit(
    #     model='resnet50', train_data=train_data, batch_size=64, epochs=2
    # )
    # print(f'Run name: {run.name}')

    return 'null'


def test_end2end(
    random_dataset: DocumentArray,
    user_input: UserInput,
    finetune_setting: FinetuneSettings,
    mocker: MockerFixture,
):
    mocker.patch('now.finetuning.run_finetuning._finetune_layer', patch_finetune_layer)
    finetune_setting.bi_modal = False

    finetune_now(
        user_input,
        random_dataset,
        finetune_setting,
        pre_trained_head_map={},
        kubectl_path='unused',
        encoder_uses=CLIP_USES,
        encoder_uses_with={
            'pretrained_model_name_or_path': IMAGE_MODEL_QUALITY_MAP[
                user_input.quality
            ][1]
        },
    )
