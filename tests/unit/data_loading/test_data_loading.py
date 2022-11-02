""" This suite tests the data_loading.py module """
import os
from typing import Tuple

import pytest
from docarray import Document, DocumentArray
from pytest_mock import MockerFixture

from now.app.music_to_music.app import MusicToMusic
from now.app.text_to_image.app import TextToImage
from now.app.text_to_text.app import TextToText
from now.app.text_to_text_and_image.app import TextToTextAndImage
from now.constants import DatasetTypes
from now.data_loading.data_loading import _load_tags_from_json_if_needed, load_data
from now.demo_data import DemoDatasetNames
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
    user_input.dataset_type = DatasetTypes.DOCARRAY
    user_input.dataset_name = 'secret-token'

    loaded_da = load_data(TextToImage(), user_input)

    assert is_da_text_equal(da, loaded_da)


def test_da_local_path(local_da: DocumentArray):
    path, da = local_da
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.PATH
    user_input.dataset_path = path

    loaded_da = load_data(TextToText(), user_input)

    assert is_da_text_equal(da, loaded_da)


def test_da_local_path_image_folder(image_resource_path: str):
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.PATH
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
    user_input.dataset_type = DatasetTypes.PATH
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
    user_input.dataset_type = DatasetTypes.DEMO
    user_input.dataset_name = DemoDatasetNames.DEEP_FASHION

    app = TextToImage()
    loaded_da = load_data(app, user_input)
    loaded_da = app.preprocess(da=loaded_da, user_input=user_input, is_indexing=True)

    for doc in loaded_da:
        assert doc.blob != b''


def test_es_online_shop_ds(da: DocumentArray):
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.DEMO
    user_input.dataset_name = DemoDatasetNames.ES_ONLINE_SHOP_50

    app = TextToTextAndImage()
    loaded_da = load_data(app, user_input)
    loaded_da = app.preprocess(da=loaded_da, user_input=user_input)

    for doc in loaded_da:
        assert doc.chunks
        doc.chunks.summary()
        for c in doc.chunks:
            assert c.text is not None or c.uri is not None


@pytest.fixture
def user_input():
    user_input = UserInput()
    user_input.dataset_path = ''
    user_input.app_instance = TextToImage()
    return user_input


def get_data(gif_resource_path, files):
    return DocumentArray(
        Document(uri=os.path.join(gif_resource_path, file)) for file in files
    )


def test_load_tags_ignore_too_many_files(user_input, gif_resource_path: str):
    da = get_data(
        gif_resource_path,
        [
            'folder1/file.gif',
            'folder1/meta.json',
            'folder1/file.txt',
            'folder2/file.gif',
            'folder2/meta.json',
        ],
    )
    da_merged = _load_tags_from_json_if_needed(da, user_input)
    assert len(da_merged) == 2
    assert da_merged[0].tags['tag_uri'].endswith('folder1/meta.json')
    assert da_merged[1].tags['tag_uri'].endswith('folder2/meta.json')


def test_load_tags_no_tags_if_missing(user_input, gif_resource_path: str):
    da = get_data(
        gif_resource_path, ['folder1/file.gif', 'folder2/file.gif', 'folder2/meta.json']
    )
    da_merged = _load_tags_from_json_if_needed(da, user_input)
    assert len(da_merged) == 2
