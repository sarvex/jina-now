import os

import pytest
from docarray import Document, DocumentArray

from now.app.image_text_retrieval.app import ImageTextRetrieval
from now.app.text_to_text.app import TextToText
from now.app.text_to_text_and_image.app import TextToTextAndImage
from now.app.text_to_video.app import TextToVideo
from now.constants import DatasetTypes
from now.data_loading.data_loading import load_data
from now.data_loading.transform_docarray import transform_docarray
from now.demo_data import DemoDatasetNames
from now.now_dataclasses import UserInput


def test_text_to_video_preprocessing_query():
    """Test if the text to video preprocessing works for queries"""
    app = TextToVideo()
    da = DocumentArray([Document(text='test')])
    da = transform_docarray(da, search_fields=[])
    da = app.preprocess(da=da, user_input=UserInput())

    assert len(da) == 1
    assert len(da[0].chunks) == 1
    assert da[0].chunks[0].text == 'test'


def test_text_to_video_preprocessing_indexing(resources_folder_path):
    """Test if the text to video preprocessing works for indexing"""
    app = TextToVideo()
    da = DocumentArray(
        [Document(uri=os.path.join(resources_folder_path, 'gif/folder1/file.gif'))]
    )
    da = transform_docarray(da, search_fields=[])
    da = app.preprocess(
        da=da, user_input=UserInput(), process_index=True, process_query=False
    )
    assert len(da) == 1
    assert len(da[0].chunks[0].chunks) == 3
    assert da[0].chunks[0].chunks[0].blob != b''


@pytest.mark.parametrize(
    'app_cls,is_indexing',
    [
        (TextToText, False),
        (TextToText, True),
        (ImageTextRetrieval, False),
        (ImageTextRetrieval, True),
    ],
)
def test_text_preprocessing(app_cls, is_indexing):
    """Test if the text to text preprocessing works for queries and indexing"""
    app = app_cls()
    da = DocumentArray([Document(text='test')])
    da = transform_docarray(da, search_fields=[])
    da = app.preprocess(
        da=da,
        user_input=UserInput(),
        process_index=is_indexing,
        process_query=not is_indexing,
    )
    assert len(da) == 1
    assert len(da[0].chunks) == 1
    assert da[0].chunks[0].modality == 'text'
    if is_indexing and app_cls == TextToText:
        assert da[0].chunks[0].chunks[0].text == 'test'
    else:
        assert da[0].chunks[0].text == 'test'


@pytest.mark.parametrize(
    'app_cls,is_indexing',
    [
        (ImageTextRetrieval, False),
        (ImageTextRetrieval, True),
    ],
)
def test_image_preprocessing(app_cls, is_indexing, resources_folder_path):
    """Test if the image to image preprocessing works for queries and indexing"""
    app = app_cls()
    uri = os.path.join(resources_folder_path, 'image/5109112832.jpg')
    da = DocumentArray([Document(uri=uri)])
    da = transform_docarray(da, search_fields=[])
    user_input = UserInput(output_modality='image')
    da = app.preprocess(
        da=da,
        user_input=user_input,
        process_index=is_indexing,
        process_query=not is_indexing,
    )

    assert len(da) == 1
    assert len(da[0].chunks) == 1
    assert da[0].chunks[0].modality == 'image'
    assert da[0].chunks[0].uri == uri
    assert da[0].chunks[0].content


@pytest.mark.parametrize('is_indexing', [False, True])
def test_nested_preprocessing(is_indexing, get_task_config_path):
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.DEMO
    user_input.dataset_name = DemoDatasetNames.ES_ONLINE_SHOP_50
    app = TextToTextAndImage()

    if is_indexing:
        da = DocumentArray(load_data(app, user_input)[0])
        task_config = app._create_task_config(user_input=user_input, data_example=da[0])
        user_input.indexer_scope = task_config.indexer_scope
    else:
        da = DocumentArray(Document(text='query text'))

    processed_da = app.preprocess(da=da, user_input=user_input, is_indexing=is_indexing)
    # assert len(processed_da) == 1
    # if is_indexing:
    #     assert len(processed_da[0].chunks) == 3  # to be investigated
    #     assert processed_da[0].chunks[0].text
    #     assert processed_da[0].chunks[1].uri
    # else:
    #     assert processed_da[0].text == 'query text'
