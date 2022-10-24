import os

import pytest
from docarray import Document, DocumentArray
from jina import Flow

from now.app.text_to_image.app import TextToImage
from now.data_loading.utils import transform_uni_modal_data
from now.executor.preprocessor import NOWPreprocessor
from now.now_dataclasses import UserInput
from docarray import dataclass
from docarray.typing import Image, Text


@pytest.fixture
def single_modal_data():
    d1 = Document(text='some text', tags={'color': 'red', 'author': 'saba'})
    d2 = Document(text='text some', tags={'color': 'blue', 'author': 'florian'})
    return DocumentArray([d1, d2])


@pytest.fixture
def multi_modal_data(resources_folder_path):
    @dataclass
    class Page:
        main_text: Text
        image: Image
        color: Text

    p1 = Page(
        main_text='main text 1',
        image=os.path.join(resources_folder_path, 'image', '5109112832.jpg'),
        color='red',
    )
    p2 = Page(
        main_text='not main text',
        image=os.path.join(resources_folder_path, 'image', '6785325056.jpg'),
        color='blue',
    )
    pages = [p1, p2]

    return DocumentArray([Document(page) for page in pages])


@pytest.mark.parametrize(
    'data_type, search_fields, filter_fields',
    [
        ('single_modal', ['default_field'], ['color']),
        ('multi_modal', ['main_text', 'image'], ['color']),
    ],
)
def test_preprocess_and_encode(
    data_type, search_fields, filter_fields, single_modal_data, multi_modal_data
):
    if data_type == 'single_modal':
        data = single_modal_data
    else:
        data = multi_modal_data
    app_instance = TextToImage()
    f = (
        Flow()
        .add(uses=NOWPreprocessor, uses_with={'app': app_instance.app_name})
        .add(
            uses='jinahub+docker://CLIPOnnxEncoder/latest-gpu',
            host='encoderclip-bh-5f4efaff13.wolf.jina.ai',
            port=443,
            tls=True,
            external=True,
            uses_with={'name': 'ViT-B-32::openai'},
        )
    )
    user_input = UserInput()
    user_input.search_fields = search_fields
    user_input.filter_fields = filter_fields
    with f:
        encoded_d = f.post(
            '/encode',
            data,
            parameters={
                'user_input': user_input.__dict__,
                'access_paths': app_instance.index_query_access_paths(search_fields),
            },
        )

    assert len(encoded_d) == len(data)
    assert len(search_fields) == len(encoded_d[0].chunks)
    for chunk, field in zip(encoded_d[0].chunks, search_fields):
        assert chunk.embedding.any()
        assert field == chunk.tags['field_name']
    for filter_field in filter_fields:
        assert filter_field in encoded_d[0].tags['filter_fields']


def test_uni_to_multi_modal(resources_folder_path: str):
    data = single_modal_data()
    data.append(
        Document(
            uri=os.path.join(resources_folder_path, 'image', '5109112832.jpg'),
            tags={'color': 'red'},
        )
    )
    transformed_data = transform_uni_modal_data(documents=data, filter_fields=['color'])

    assert len(transformed_data) == len(data)
    assert 'color' in transformed_data[0].tags['filter_fields']
    assert len(transformed_data[1].chunks) == 1
