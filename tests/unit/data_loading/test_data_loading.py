""" This suite tests the data_loading.py module """
import os
from typing import Tuple

import pytest
from docarray import Document, DocumentArray
from pytest_mock import MockerFixture

from now.apps.music_to_music.app import MusicToMusic
from now.apps.text_to_image.app import TextToImage
from now.apps.text_to_text.app import TextToText
from now.constants import DatasetTypes, DemoDatasets
from now.data_loading.data_loading import _load_tags_from_json, load_data
from now.now_dataclasses import UserInput


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
    user_input.is_custom_dataset = True
    user_input.custom_dataset_type = DatasetTypes.DOCARRAY
    user_input.dataset_name = 'secret-token'

    loaded_da = load_data(TextToImage(), user_input)

    assert is_da_text_equal(da, loaded_da)


def test_da_local_path(local_da: DocumentArray):
    path, da = local_da
    user_input = UserInput()
    user_input.is_custom_dataset = True
    user_input.custom_dataset_type = DatasetTypes.PATH
    user_input.dataset_path = path

    loaded_da = load_data(TextToText(), user_input)

    assert is_da_text_equal(da, loaded_da)


def test_da_local_path_image_folder(image_resource_path: str):
    user_input = UserInput()
    user_input.is_custom_dataset = True
    user_input.custom_dataset_type = DatasetTypes.PATH
    user_input.dataset_path = image_resource_path

    app = TextToImage()
    loaded_da = load_data(app, user_input)
    loaded_da = app.preprocess(da=loaded_da, user_input=user_input, is_indexing=True)

    assert len(loaded_da) == 2, (
        f'Expected two images, got {len(loaded_da)}.'
        f' Check the tests/resources/image folder'
    )
    for doc in loaded_da:
        assert doc.blob != b''


def test_da_local_path_music_folder(music_resource_path: str):
    user_input = UserInput()
    user_input.is_custom_dataset = True
    user_input.custom_dataset_type = DatasetTypes.PATH
    user_input.dataset_path = music_resource_path

    app = MusicToMusic()
    loaded_da = load_data(app, user_input)
    loaded_da = app.preprocess(da=loaded_da, user_input=user_input)

    assert len(loaded_da) == 2, (
        f'Expected two music docs, got {len(loaded_da)}.'
        f' Check the tests/resources/music folder'
    )
    for doc in loaded_da:
        assert doc.blob != b''


def test_da_custom_ds(da: DocumentArray):
    user_input = UserInput()
    user_input.is_custom_dataset = False
    user_input.custom_dataset_type = DatasetTypes.DEMO
    user_input.data = DemoDatasets.DEEP_FASHION

    app = TextToImage()
    loaded_da = load_data(app, user_input)
    loaded_da = app.preprocess(da=loaded_da, user_input=user_input, is_indexing=True)

    for doc in loaded_da:
        assert doc.blob != b''


def test_load_tags(gif_resource_path: str):
    user_input = UserInput()
    user_input.dataset_path = ''
    user_input.app = TextToImage()
    da = DocumentArray(
        [
            Document(uri=os.path.join(gif_resource_path, 'folder1/file.gif')),
            Document(uri=os.path.join(gif_resource_path, 'folder1/manifest.json')),
            Document(uri=os.path.join(gif_resource_path, 'folder1/file.txt')),
            Document(uri=os.path.join(gif_resource_path, 'folder2/file.gif')),
            Document(uri=os.path.join(gif_resource_path, 'folder2/manifest.json')),
        ]
    )

    da = _load_tags_from_json(da, user_input)
    print(da[0].summary())
    print(da[1].summary())
    assert 'custom' in da[0].tags
    assert 'custom' in da[1].tags

    assert da[0].tags['custom'] == 'moneystack'
    assert da[1].tags['ml'] == 'visual-arts'

    da1 = DocumentArray(
        [
            Document(uri=os.path.join(gif_resource_path, 'folder1/file.gif')),
            Document(uri=os.path.join(gif_resource_path, 'folder2/file.gif')),
            Document(uri=os.path.join(gif_resource_path, 'folder1/file.txt')),
        ]
    )

    da1 = _load_tags_from_json(da1, user_input)

    for d in da1:
        assert not 'custom' in d.tags
