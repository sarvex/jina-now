""" This suite tests the data_loading.py module """
import os
from typing import Tuple

import pytest
from docarray import Document, DocumentArray
from docarray.typing import Image, Text
from pytest_mock import MockerFixture

from now.app.image_text_retrieval.app import ImageTextRetrieval
from now.constants import DatasetTypes
from now.data_loading.data_loading import from_files_local, load_data
from now.demo_data import DemoDatasetNames
from now.now_dataclasses import UserInput
from now.run_backend import create_dataclass


@pytest.fixture()
def da() -> DocumentArray:
    return DocumentArray([Document(text='foo'), Document(text='bar')])


@pytest.fixture(autouse=True)
def mock_download(mocker: MockerFixture, da: DocumentArray):
    def fake_download(url: str, filename: str) -> str:
        da.save_binary(filename)
        return filename

    mocker.patch('now.utils.download', fake_download)


@pytest.fixture(autouse=True)
def mock_pull(mocker: MockerFixture, da: DocumentArray):
    def fake_pull(secret: str) -> DocumentArray:
        return da

    mocker.patch('now.data_loading.data_loading._pull_docarray', fake_pull)


@pytest.fixture()
def local_da(da: DocumentArray, tmpdir: str) -> Tuple[str, DocumentArray]:
    save_path = os.path.join(tmpdir, 'da.bin')
    da.save_binary(save_path)
    yield save_path, da
    if os.path.isfile(save_path):
        os.remove(save_path)


def is_da_text_equal(da_a: DocumentArray, da_b: DocumentArray):
    for a, b in zip(da_a, da_b):
        if a.text != b.text:
            return False
    return True


def test_da_pull(da: DocumentArray):
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.DOCARRAY
    user_input.dataset_name = 'secret-token'

    loaded_da = load_data(user_input)

    assert is_da_text_equal(da, loaded_da)


def test_da_local_path(local_da: DocumentArray):
    path, da = local_da
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.PATH
    user_input.dataset_path = path

    loaded_da = load_data(user_input)

    assert is_da_text_equal(da, loaded_da)


def test_da_local_path_image_folder(image_resource_path: str):
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.PATH
    user_input.dataset_path = image_resource_path
    user_input.search_fields = ['a.jpg']
    user_input.search_fields_modalities = {'a.jpg': Image}
    data_class = create_dataclass(user_input)
    loaded_da = load_data(user_input, data_class)

    assert len(loaded_da) == 2, (
        f'Expected two images, got {len(loaded_da)}.'
        f' Check the tests/resources/image folder'
    )
    for doc in loaded_da:
        assert doc.uri


def test_da_custom_ds(da: DocumentArray):
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.DEMO
    user_input.dataset_name = DemoDatasetNames.DEEP_FASHION

    loaded_da = load_data(user_input)

    for doc in loaded_da:
        assert doc.content


def test_from_files_local(resources_folder_path):
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.PATH
    user_input.search_fields = ['a.jpg', 'test.txt']
    user_input.search_fields_modalities = {'a.jpg': Image, 'test.txt': Text}
    user_input.dataset_path = os.path.join(
        resources_folder_path, 'subdirectories_structure_folder'
    )

    data_class = create_dataclass(user_input)
    loaded_da = from_files_local(user_input, data_class)

    assert len(loaded_da) == 2


@pytest.fixture
def user_input():
    user_input = UserInput()
    user_input.dataset_path = ''
    user_input.app_instance = ImageTextRetrieval()
    return user_input


def get_data(gif_resource_path, files):
    return DocumentArray(
        Document(uri=os.path.join(gif_resource_path, file)) for file in files
    )
