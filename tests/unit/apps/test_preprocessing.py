import os

import pytest
from docarray import Document, DocumentArray, dataclass
from docarray.typing import Image, Text, Video

from now.app.base.preprocess import preprocess_text
from now.app.search_app import SearchApp


@pytest.fixture
def mm_text_data():
    """Fixture for text data"""

    @dataclass
    class MMDoc:
        text: Text

    return DocumentArray(
        [
            Document(MMDoc(text='test')),
        ]
    )


def test_search_app_preprocessing_query(mm_text_data):
    """Test if the text to video preprocessing works for queries"""
    app = SearchApp()
    da = app.preprocess(mm_text_data)

    assert len(da) == 1
    assert len(da[0].chunks) == 1
    assert da[0].chunks[0].chunks[0].text == 'test'


def test_search_app_preprocessing_indexing(resources_folder_path):
    """Test if the text to video preprocessing works for indexing"""

    @dataclass
    class MMDoc:
        video: Video

    app = SearchApp()
    da = DocumentArray(
        [
            Document(
                MMDoc(
                    video=os.path.join(resources_folder_path, 'gif/folder1/file.gif')
                ),
            )
        ]
    )
    da = app.preprocess(da)
    assert len(da) == 1
    assert len(da[0].chunks[0].chunks) == 3
    assert da[0].chunks[0].chunks[0].blob != b''


@pytest.mark.parametrize(
    'app_cls,is_indexing',
    [
        (SearchApp, False),
        (SearchApp, True),
    ],
)
def test_text_preprocessing(app_cls, is_indexing, mm_text_data):
    """Test if the text to text preprocessing works for queries and indexing"""
    app = app_cls()
    da = app.preprocess(mm_text_data)
    assert len(da) == 1
    assert len(da[0].chunks) == 1
    assert da[0].chunks[0].modality == 'text'
    assert da[0].chunks[0].text == ''
    assert da[0].chunks[0].chunks[0].text == 'test'


@pytest.mark.parametrize(
    'app_cls,is_indexing',
    [
        (SearchApp, False),
        (SearchApp, True),
    ],
)
def test_image_preprocessing(app_cls, is_indexing, resources_folder_path):
    """Test if the image to image preprocessing works for queries and indexing"""

    @dataclass
    class MMDoc:
        image: Image

    app = app_cls()
    uri = os.path.join(resources_folder_path, 'image/a.jpg')
    da = DocumentArray([Document(MMDoc(image=uri))])
    da = app.preprocess(da)
    assert len(da) == 1
    assert len(da[0].chunks) == 1
    assert da[0].chunks[0].chunks[0].modality == 'image'
    assert da[0].chunks[0].chunks[0].uri == uri
    assert da[0].chunks[0].chunks[0].content


def test_preprocess_text():
    result = preprocess_text(Document(text='test. test', modality='text'))
    assert len(result.chunks) == 2
