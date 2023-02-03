import pytest
from docarray import Document, DocumentArray
from docarray.typing import Image, Text
from jina import Flow

from now.admin.utils import get_default_request_body
from now.constants import EXTERNAL_CLIP_HOST, DatasetTypes
from now.data_loading.create_dataclass import create_dataclass
from now.data_loading.data_loading import load_data
from now.demo_data import DemoDatasetNames
from now.executor.indexer.elastic import NOWElasticIndexer
from now.executor.preprocessor import NOWPreprocessor
from now.now_dataclasses import UserInput

BASE_URL = 'http://localhost:8080/api/v1'
SEARCH_URL = f'{BASE_URL}/search-app/search'
HOST = 'grpc://0.0.0.0'
PORT = 9089


def get_request_body(secured):
    request_body = get_default_request_body(host=HOST, secured=secured)
    request_body['port'] = PORT
    return request_body


def get_flow(preprocessor_args=None, indexer_args=None, tmpdir=None):
    """
    :param preprocessor_args: additional arguments for the preprocessor,
        e.g. {'admin_emails': [admin_email]}
    :param indexer_args: additional arguments for the indexer,
        e.g. {'admin_emails': [admin_email]}
    """
    preprocessor_args = preprocessor_args or {}
    indexer_args = indexer_args or {}
    metas = {'workspace': str(tmpdir)}
    f = (
        Flow(port_expose=PORT)
        .add(
            uses=NOWPreprocessor,
            uses_with=preprocessor_args,
            uses_metas=metas,
        )
        .add(
            host=EXTERNAL_CLIP_HOST,
            port=443,
            tls=True,
            external=True,
        )
        .add(
            uses=NOWElasticIndexer,
            uses_with={
                'hosts': 'http://localhost:9200',
                **indexer_args,
            },
            uses_metas=metas,
            no_reduce=True,
        )
    )
    return f


@pytest.fixture
def data_with_tags(mm_dataclass):
    docs = DocumentArray([Document(mm_dataclass(text_field='test')) for _ in range(10)])
    for index, doc in enumerate(docs):
        doc.tags['color'] = 'Blue Color' if index == 0 else 'Red Color'
        doc.tags['price'] = 0.5 + index

    return docs


@pytest.fixture
def simple_data(mm_dataclass):
    return DocumentArray([Document(mm_dataclass(text_field='test')) for _ in range(10)])


@pytest.fixture
def artworks_data():
    user_input = UserInput()
    user_input.admin_name = 'team-now'
    user_input.dataset_type = DatasetTypes.DEMO
    user_input.dataset_name = DemoDatasetNames.BEST_ARTWORKS
    user_input.index_fields = ['image']
    user_input.filter_fields = ['label']
    user_input.index_field_candidates_to_modalities = {'image': Image}
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
    user_input.filter_field_candidates_to_modalities = {'product_id': str}
    data_class, user_input.field_names_to_dataclass_fields = create_dataclass(
        user_input=user_input
    )
    user_input.es_host_name = connection_str
    docs = load_data(user_input=user_input, data_class=data_class)
    return docs, user_input


@pytest.fixture
def local_folder_data(resources_folder_path):
    user_input = UserInput()
    user_input.admin_name = 'team-now'
    user_input.dataset_type = DatasetTypes.PATH
    user_input.dataset_path = "/Users/tanguy/Downloads/gif"
    user_input.index_fields = ['.gif']
    user_input.filter_fields = []
    user_input.index_field_candidates_to_modalities = {'.gif': Image}
    user_input.filter_field_candidates_to_modalities = {}
    data_class, user_input.field_names_to_dataclass_fields = create_dataclass(
        user_input=user_input
    )
    docs = load_data(user_input, data_class=data_class)
    return docs, user_input
