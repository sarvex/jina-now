""" This module implements integrations tests against the external finetune-api """
import numpy as np
import pytest
from docarray import Document, DocumentArray
from pytest_mock import MockerFixture

from now.constants import IMAGE_MODEL_QUALITY_MAP, Modalities, Qualities
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
def user_input() -> UserInput:
    return UserInput(
        output_modality=Modalities.IMAGE,
        is_custom_dataset=True,
        quality=Qualities.MEDIUM,
        model_variant=IMAGE_MODEL_QUALITY_MAP[Qualities.MEDIUM],
    )


@pytest.fixture()
def finetune_setting(
    user_input: UserInput, random_dataset: DocumentArray
) -> FinetuneSettings:
    return parse_finetune_settings(user_input, dataset=random_dataset)


def patch_finetune_layer(
    finetune_ds: FinetuneDataset, finetune_settings: FinetuneSettings, save_dir: str
) -> str:
    return 'null'


def test_end2end(
    random_dataset: DocumentArray,
    user_input: UserInput,
    finetune_setting: FinetuneSettings,
    mocker: MockerFixture,
):
    mocker.patch('now.finetuning.run_finetuning._finetune_layer', patch_finetune_layer)
    finetune_setting.bi_modal = False

    finetune_now(user_input, random_dataset, finetune_setting, kubectl_path='unused')

    # finetuner.login()
    # run = finetuner.fit(model='resnet50', train_data=train_data, batch_size=64, epochs=2)
    # print(f'Run name: {run.name}')
