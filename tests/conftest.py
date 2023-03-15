""" Module holds reusable fixtures """
import base64
import json
import os
import random
import time
from collections import namedtuple
from warnings import catch_warnings, filterwarnings

import hubble
import numpy as np
import pytest
from docarray import Document, DocumentArray, dataclass, field
from docarray.typing import Image, Text, Video
from elasticsearch import Elasticsearch
from tests.integration.local.conftest import get_request_body
from tests.unit.data_loading.elastic.example_dataset import ExampleDataset
from tests.unit.data_loading.elastic.utils import delete_es_index
from urllib3.exceptions import InsecureRequestWarning, SecurityWarning

from now.common.options import construct_app
from now.constants import S3_CUSTOM_MM_DATA_PATH, Apps, DatasetTypes, Models
from now.data_loading.create_dataclass import create_dataclass
from now.data_loading.data_loading import _list_s3_file_paths, load_data
from now.data_loading.elasticsearch import ElasticsearchConnector
from now.demo_data import DemoDatasetNames
from now.deployment.deployment import cmd
from now.executor.preprocessor import NOWPreprocessor
from now.executor.preprocessor.s3_download import get_bucket
from now.now_dataclasses import UserInput
from now.utils.authentication.helpers import get_aws_profile


@pytest.fixture()
def mm_dataclass():
    """Fixture for mmdocs data"""

    @dataclass
    class MMDoc:
        text_field: Text = field(default=None)
        image_field: Image = field(default=None)
        video_field: Video = field(default=None)

    return MMDoc


@pytest.fixture()
def resources_folder_path(tests_folder_path) -> str:
    return os.path.join(tests_folder_path, 'resources')


@pytest.fixture()
def tests_folder_path() -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)))


@pytest.fixture
def base64_image_string(resources_folder_path: str) -> str:
    with open(os.path.join(resources_folder_path, 'image', 'a.jpg'), 'rb') as f:
        binary = f.read()
        img_string = base64.b64encode(binary).decode('utf-8')
    return img_string


@pytest.fixture(scope='session')
def service_account_file_path() -> str:
    return os.path.join(
        os.path.dirname(os.path.realpath(__file__)), '..', 'service_account.json'
    )


@pytest.fixture(autouse=True, scope='session')
def setup_env():
    os.environ['NOW_CI_RUN'] = 'True'
    # 1 and true are not working in the current core version therefore, we give it another value
    os.environ['JINA_OPTOUT_TELEMETRY'] = 'someValueToDeactivateTelemetry'
    os.environ['JCLOUD_LOGLEVEL'] = 'DEBUG'


@pytest.fixture()
def image_resource_path(resources_folder_path: str) -> str:
    return os.path.join(resources_folder_path, 'image')


@pytest.fixture()
def gif_resource_path(resources_folder_path: str) -> str:
    return os.path.join(resources_folder_path, 'gif')


@pytest.fixture
def admin_email():
    return 'alpha.omega@jina.ai'


@pytest.fixture
def user_email():
    return 'abc.def@jina.ai'


@pytest.fixture
def domain_user_email():
    return 'abc.def@test.ai'


@pytest.fixture
def mock_hubble_user_email(monkeypatch, user_email):
    class MockedClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_user_info(self, *args, **kwargs):
            return {
                'code': 200,
                'data': {'email': user_email},
            }

    monkeypatch.setattr(hubble, 'Client', MockedClient)


@pytest.fixture
def mock_hubble_domain_user_email(monkeypatch, domain_user_email):
    class MockedClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_user_info(self, *args, **kwargs):
            return {
                'code': 200,
                'data': {'email': domain_user_email},
            }

    monkeypatch.setattr(hubble, 'Client', MockedClient)


@pytest.fixture()
def mock_hubble_admin_email(monkeypatch, admin_email):
    class MockedClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_user_info(self, *args, **kwargs):
            return {
                'code': 200,
                'data': {'email': admin_email},
            }

    monkeypatch.setattr(hubble, 'Client', MockedClient)
    # hubble.Client = MockedClient


@pytest.fixture(scope="session")
def es_connection_params():
    connection_str = 'http://localhost:9200'
    connection_args = {'verify_certs': False}
    return connection_str, connection_args


@pytest.fixture(scope="function")
def dump_user_input(request) -> None:
    # If user_input.json exists, then remove it
    if os.path.exists(os.path.join(os.path.expanduser('~'), 'user_input.json')):
        os.remove(os.path.join(os.path.expanduser('~'), 'user_input.json'))
    # Now dump the user input
    with open(os.path.join(os.path.expanduser('~'), 'user_input.json'), 'w') as f:
        json.dump(request.param.to_safe_dict(), f)
    yield
    os.remove(os.path.join(os.path.expanduser('~'), 'user_input.json'))


@pytest.fixture(scope='session')
def setup_service_running(es_connection_params) -> None:
    docker_compose_file = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'resources/elastic/docker-compose.yml',
    )
    cmd(f'docker-compose -f {docker_compose_file} up -d')
    hosts, _ = es_connection_params
    os.environ['ES_HOSTS'] = hosts
    os.environ['ES_API_KEY'] = 'TestApiKey'
    wait_until_cluster_is_up(es=Elasticsearch(hosts=hosts), hosts=hosts)
    yield
    cmd('docker-compose -f tests/resources/elastic/docker-compose.yml down')


@pytest.fixture
def get_aws_info():
    dataset_path = os.environ.get('S3_SCHEMA_FOLDER_PATH')
    aws_profile = get_aws_profile()
    region = aws_profile.region
    if not region:
        region = 'eu-west-1'
    return (
        dataset_path,
        aws_profile.aws_access_key_id,
        aws_profile.aws_secret_access_key,
        region,
    )


@pytest.fixture
def random_index_name():
    os.environ['ES_INDEX_NAME'] = f"test-index-{random.randint(0, 10000)}"


@pytest.fixture
def es_inputs(gif_resource_path) -> namedtuple:
    np.random.seed(42)

    user_input = UserInput()
    user_input.index_fields = ['title', 'excerpt', 'gif']
    user_input.index_field_candidates_to_modalities = {
        'title': Text,
        'excerpt': Text,
        'gif': Video,
    }
    user_input.field_names_to_dataclass_fields = {
        'title': 'title',
        'excerpt': 'excerpt',
        'gif': 'gif',
    }

    @dataclass
    class MMDoc:
        title: Text
        excerpt: Text
        gif: Image

    @dataclass
    class MMQuery:
        query_text: Text

    document_mappings = [['clip', 8, ['title', 'gif']]]

    default_score_calculation = [
        ['query_text', 'title', 'clip', 1],
        ['query_text', 'gif', 'clip', 1],
        ['query_text', 'title', 'bm25', 10],
    ]
    docs = [
        MMDoc(
            title='cat test title cat',
            excerpt='cat test excerpt cat',
            gif=os.path.join(gif_resource_path, 'folder1/file.gif'),
        ),
        MMDoc(
            title='test title dog',
            excerpt='test excerpt 2',
            gif=os.path.join(gif_resource_path, 'folder1/file.gif'),
        ),
    ]
    clip_docs = DocumentArray()
    # encode our documents
    for i, doc in enumerate(docs):
        prep_doc = Document(doc)
        prep_doc = NOWPreprocessor().preprocess(DocumentArray(prep_doc))[0]
        prep_doc.tags['color'] = random.choice(['red', 'blue', 'green'])
        prep_doc.tags['price'] = i + 0.5
        prep_doc.id = str(i)
        clip_doc = Document(prep_doc, copy=True)
        clip_doc.id = prep_doc.id

        clip_doc.title.chunks[0].embedding = np.random.random(8)
        clip_doc.gif.chunks[0].embedding = np.random.random(8)

        clip_docs.append(clip_doc)

    index_docs_map = {
        'clip': clip_docs,
    }

    query_doc = Document(MMQuery(query_text='cat'))
    clip_doc = Document(query_doc, copy=True)
    clip_doc.id = query_doc.id

    preprocessor = NOWPreprocessor()
    da_clip = preprocessor.preprocess(DocumentArray([clip_doc]), {})

    clip_doc.query_text.chunks[0].embedding = np.random.random(8)

    query_docs_map = {
        'clip': da_clip,
    }
    EsInputs = namedtuple(
        'EsInputs',
        [
            'index_docs_map',
            'query_docs_map',
            'document_mappings',
            'default_score_calculation',
            'user_input',
        ],
    )
    return EsInputs(
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_score_calculation,
        user_input,
    )


@pytest.fixture
def setup_elastic_db(setup_service_running, es_connection_params):
    connection_str, connection_args = es_connection_params
    with catch_warnings():
        filterwarnings('ignore', category=InsecureRequestWarning)
        filterwarnings('ignore', category=SecurityWarning)
        with ElasticsearchConnector(
            connection_str=connection_str, connection_args=connection_args
        ) as es_connector:
            # return connector to interact with the es database
            yield es_connector

            index_list = list(es_connector.es.indices.get(index='*').keys())
            for index in index_list:
                es_connector.es.indices.delete(index=str(index))


@pytest.fixture
def online_shop_resources(resources_folder_path):
    corpus_path = os.path.join(
        resources_folder_path, 'text+image/online_shop_corpus.jsonl.gz'
    )
    mapping_path = os.path.join(
        resources_folder_path, 'text+image/online_shop_mapping.json'
    )
    return corpus_path, mapping_path, 'online_shop_data'


@pytest.fixture()
def setup_online_shop_db(setup_elastic_db, es_connection_params, online_shop_resources):
    """
    This fixture loads data from Online shop data into an Elasticsearch instance.
    """
    es_connector = setup_elastic_db
    connection_str, connection_args = es_connection_params
    corpus_path, mapping_path, index_name = online_shop_resources

    # number of documents to import
    dataset_size = 50

    # load online shop data from some resource file
    dataset = ExampleDataset(corpus_path)
    dataset.import_to_elastic_search(
        connection_str=connection_str,
        connection_args=connection_args,
        index_name=index_name,
        mapping_path=mapping_path,
        size=dataset_size,
    )

    # return connector to interact with the es database
    yield es_connector, index_name

    # delete index
    delete_es_index(connector=es_connector, name=index_name)


def wait_until_cluster_is_up(es, hosts):
    MAX_RETRIES = 300
    SLEEP = 3
    retries = 0
    while retries < MAX_RETRIES:
        try:
            if es.ping():
                break
            else:
                retries += 1
                time.sleep(SLEEP)
        except Exception:
            print(
                f'Elasticsearch is not running yet, are you connecting to the right hosts? {hosts}'
            )
    if retries >= MAX_RETRIES:
        raise RuntimeError(f'Elasticsearch is not running after {MAX_RETRIES} retries.')


@pytest.fixture(scope='session')
def pulled_local_folder_data(tmpdir_factory):
    aws_profile = get_aws_profile()
    bucket = get_bucket(
        uri=S3_CUSTOM_MM_DATA_PATH,
        aws_access_key_id=aws_profile.aws_access_key_id,
        aws_secret_access_key=aws_profile.aws_secret_access_key,
        region_name=aws_profile.region,
    )
    folder_prefix = '/'.join(S3_CUSTOM_MM_DATA_PATH.split('/')[3:])
    file_paths = _list_s3_file_paths(bucket, folder_prefix)
    temp_dir = str(tmpdir_factory.mktemp('local_folder_data'))
    for path in file_paths:
        local_path = os.path.join(temp_dir, path)
        if not os.path.exists(os.path.dirname(local_path)):
            os.makedirs(os.path.dirname(local_path))
        bucket.download_file(path, local_path)
    return os.path.join(temp_dir, folder_prefix)


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
            token=get_request_body(secured=True)['jwt']['token'],
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
    data_class, user_input.field_names_to_dataclass_fields = create_dataclass(
        user_input=user_input
    )
    user_input.es_host_name = connection_str
    user_input.app_instance = construct_app(Apps.SEARCH_APP)
    user_input.flow_name = 'nowapi-local'
    user_input.model_choices = {'title_model': [Models.CLIP_MODEL]}
    docs = load_data(user_input=user_input)
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

    docs = load_data(user_input)
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

    docs = load_data(user_input)
    return docs, user_input
