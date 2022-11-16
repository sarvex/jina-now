import os

from docarray import Document, DocumentArray, dataclass
from docarray.typing import Text, Image
from jina import Flow

from now.app.text_to_image.app import TextToImage
from now.app.text_to_text.app import TextToText
from now.app.text_to_video.app import TextToVideo
from now.constants import DatasetTypes
from now.data_loading.data_loading import load_data
from now.data_loading.transform_docarray import transform_uni_modal_data
from now.demo_data import DemoDatasetNames
from now.executor.preprocessor import NOWPreprocessor
from now.now_dataclasses import UserInput
from tests.executor.indexer.base.in_memory_indexer import InMemoryIndexer
import pytest


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


@pytest.mark.parametrize('input_type', ['demo_dataset', 'single_modal', 'multi_modal'])
def test_transform_inside_flow(input_type, single_modal_data, multi_modal_data):
    user_input = UserInput()
    if input_type == 'demo_dataset':
        app_instance = TextToVideo()
        user_input.search_fields = []
        user_input.dataset_type = DatasetTypes.DEMO
        user_input.dataset_name = DemoDatasetNames.TUMBLR_GIFS_10K
        data = load_data(app_instance, user_input)[:10]  # includes 2 videos
    elif input_type == 'single_modal':
        app_instance = TextToText()
        data = single_modal_data
    else:
        app_instance = TextToImage()
        data = multi_modal_data
        user_input.search_fields = ['main_text', 'image']

    query = Document(text='query_text')
    num_expected_matches = 2

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
        .add(
            uses=InMemoryIndexer,
            uses_with={
                'columns': [
                    'split',
                    'str',
                    'finetuner_label',
                    'str',
                    'content_type',
                    'str',
                ],
                'dim': 512,
            },
        )
    )
    with f:
        f.post(
            '/index',
            data,
            parameters={
                'user_input': user_input.__dict__,
                'access_paths': app_instance.get_index_query_access_paths(),
                'traversal_paths': app_instance.get_index_query_access_paths(),
            },
        )

        query_res = f.post(
            '/search',
            query,
            parameters={
                'user_input': user_input.__dict__,
                'access_paths': app_instance.get_index_query_access_paths(),
                'traversal_paths': app_instance.get_index_query_access_paths(),
            },
            return_results=True,
        )
    assert query_res[0].matches
    # assert len(query_res[0].matches) == num_expected_matches


def test_uni_to_multi_modal(resources_folder_path, single_modal_data):
    data = single_modal_data
    data.append(
        Document(
            uri=os.path.join(resources_folder_path, 'gif', 'folder1/file.gif'),
            tags={'color': 'red'},
        )
    )
    transformed_data = transform_uni_modal_data(documents=data, filter_fields=['color'])

    assert len(transformed_data) == len(data)
    assert 'color' in transformed_data[0].tags['filter_fields']
    assert len(transformed_data[1].chunks) == 1
