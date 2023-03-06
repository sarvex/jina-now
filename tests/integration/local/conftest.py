import os

import hubble
import pytest
from docarray import Document, DocumentArray
from docarray.typing import Image, Text

from now.admin.utils import get_default_request_kwargs
from now.common.options import construct_app
from now.constants import Apps, DatasetTypes, Models, S3_CUSTOM_MM_DATA_PATH
from now.data_loading.create_dataclass import create_dataclass
from now.data_loading.data_loading import load_data
from now.demo_data import DemoDatasetNames

# special imports to make executors visible for flow yaml construction
from now.executor.autocomplete import NOWAutoCompleteExecutor2  # noqa: F401
from now.executor.gateway.now_gateway import NOWGateway  # noqa: F401
from now.executor.indexer.elastic import NOWElasticIndexer  # noqa: F401
from now.now_dataclasses import UserInput
from now.utils import get_aws_profile

BASE_URL = 'http://localhost:8081/api/v1'
SEARCH_URL = f'{BASE_URL}/search-app/search'


def get_request_body():
    request_headers, request_body = get_default_request_kwargs()
    return request_headers, request_body


@pytest.fixture
def data_with_tags(mm_dataclass):
    user_input = UserInput()
    user_input.admin_name = 'team-now'
    user_input.dataset_type = DatasetTypes.DOCARRAY
    user_input.index_fields = ['text_field']
    user_input.filter_fields = ['color']
    user_input.index_field_candidates_to_modalities = {'text_field': Text}
    user_input.field_names_to_dataclass_fields = {'text_field': 'text_field'}
    user_input.app_instance = construct_app(Apps.SEARCH_APP)
    user_input.flow_name = 'nowapi-local'
    user_input.model_choices = {'text_field_model': [Models.CLIP_MODEL]}

    docs = DocumentArray([Document(mm_dataclass(text_field='test')) for _ in range(10)])
    for index, doc in enumerate(docs):
        doc.tags['color'] = 'Blue Color' if index == 0 else 'Red Color'
        doc.tags['price'] = 0.5 + index

    return docs, user_input


@pytest.fixture
def api_key_data(mm_dataclass):
    user_input = UserInput()
    user_input.admin_name = 'team-now'
    user_input.dataset_type = DatasetTypes.DOCARRAY
    user_input.index_fields = ['text_field']
    user_input.index_field_candidates_to_modalities = {'text_field': Text}
    user_input.field_names_to_dataclass_fields = {'text_field': 'text_field'}
    user_input.app_instance = construct_app(Apps.SEARCH_APP)
    user_input.flow_name = 'nowapi-local'
    user_input.model_choices = {'text_field_model': [Models.CLIP_MODEL]}
    user_input.admin_emails = [
        hubble.Client(
            token=hubble.get_token(),
            max_retries=None,
            jsonify=True,
        )
        .get_user_info()['data']
        .get('email')
    ]
    user_input.secured = True
    docs = DocumentArray([Document(mm_dataclass(text_field='test')) for _ in range(10)])
    return docs, user_input


@pytest.fixture
def artworks_data():
    user_input = UserInput()
    user_input.admin_name = 'team-now'
    user_input.dataset_type = DatasetTypes.DEMO
    user_input.dataset_name = DemoDatasetNames.BEST_ARTWORKS
    user_input.index_fields = ['image']
    user_input.filter_fields = ['label']
    user_input.index_field_candidates_to_modalities = {'image': Image}
    user_input.field_names_to_dataclass_fields = {'image': 'image'}
    user_input.app_instance = construct_app(Apps.SEARCH_APP)
    user_input.flow_name = 'nowapi-local'
    user_input.model_choices = {'image_model': [Models.CLIP_MODEL]}
    user_input.jwt = {'token': hubble.get_token()}

    docs = load_data(user_input)
    return docs, user_input


@pytest.fixture
def pop_lyrics_data():
    user_input = UserInput()
    user_input.admin_name = 'team-now'
    user_input.dataset_type = DatasetTypes.DEMO
    user_input.dataset_name = DemoDatasetNames.POP_LYRICS
    user_input.index_fields = ['lyrics']
    user_input.index_field_candidates_to_modalities = {'lyrics': Text}
    user_input.field_names_to_dataclass_fields = {'lyrics': 'lyrics'}
    user_input.app_instance = construct_app(Apps.SEARCH_APP)
    user_input.flow_name = 'nowapi-local'
    user_input.model_choices = {'lyrics_model': [Models.CLIP_MODEL]}
    user_input.jwt = {'token': hubble.get_token()}

    docs = load_data(user_input)
    return docs, user_input


@pytest.fixture
def elastic_data(setup_online_shop_db, es_connection_params):
    _, index_name = setup_online_shop_db
    connection_str, _ = es_connection_params
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.ELASTICSEARCH
    user_input.es_index_name = index_name
    user_input.index_fields = ['title']
    user_input.filter_fields = ['product_id']
    user_input.index_field_candidates_to_modalities = {'title': Text}
    user_input.filter_field_candidates_to_modalities = {'product_id': 'str'}
    user_input.jwt = {'token': hubble.get_token()}
    data_class, user_input.field_names_to_dataclass_fields = create_dataclass(
        user_input=user_input
    )
    user_input.es_host_name = connection_str
    user_input.app_instance = construct_app(Apps.SEARCH_APP)
    user_input.flow_name = 'nowapi-local'
    user_input.model_choices = {'title_model': [Models.CLIP_MODEL]}
    docs = load_data(user_input=user_input, data_class=data_class)
    return docs, user_input


@pytest.fixture
def local_folder_data(pulled_local_folder_data):
    user_input = UserInput()
    user_input.admin_name = 'team-now'
    user_input.dataset_type = DatasetTypes.PATH
    user_input.dataset_path = pulled_local_folder_data
    user_input.index_fields = ['image.png', 'test.txt']
    user_input.filter_fields = ['title']
    user_input.index_field_candidates_to_modalities = {
        'image.png': Image,
        'test.txt': Text,
    }
    user_input.filter_field_candidates_to_modalities = {'title': 'str'}
    data_class, user_input.field_names_to_dataclass_fields = create_dataclass(
        user_input=user_input
    )
    user_input.app_instance = construct_app(Apps.SEARCH_APP)
    user_input.flow_name = 'nowapi-local'
    user_input.model_choices = {
        'test.txt_model': [Models.CLIP_MODEL],
        'image.png_model': [Models.CLIP_MODEL],
    }
    user_input.jwt = {'token': hubble.get_token()}

    docs = load_data(user_input, data_class=data_class)
    return docs, user_input


@pytest.fixture
def s3_bucket_data():
    aws_profile = get_aws_profile()
    user_input = UserInput()
    user_input.admin_name = 'team-now'
    user_input.dataset_type = DatasetTypes.S3_BUCKET
    user_input.dataset_path = S3_CUSTOM_MM_DATA_PATH
    user_input.aws_access_key_id = aws_profile.aws_access_key_id
    user_input.aws_secret_access_key = aws_profile.aws_secret_access_key
    user_input.aws_region_name = aws_profile.region
    user_input.index_fields = ['image.png']
    user_input.filter_fields = ['title']
    user_input.index_field_candidates_to_modalities = {'image.png': Image}
    user_input.filter_field_candidates_to_modalities = {'title': 'str'}
    data_class, user_input.field_names_to_dataclass_fields = create_dataclass(
        user_input=user_input
    )
    user_input.app_instance = construct_app(Apps.SEARCH_APP)
    user_input.flow_name = 'nowapi-local'
    user_input.model_choices = {'image.png_model': [Models.CLIP_MODEL]}
    user_input.jwt = {'token': hubble.get_token()}

    docs = load_data(user_input, data_class=data_class)
    return docs, user_input
