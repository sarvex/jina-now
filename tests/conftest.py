""" Module holds reusable fixtures """
import base64
import os
import random
import time
from collections import namedtuple
from typing import List

import hubble
import numpy as np
import pytest
from docarray import Document, DocumentArray, dataclass
from docarray.typing import Image, Text
from elasticsearch import Elasticsearch

from now.deployment.deployment import cmd
from now.executor.indexer.elastic.es_query_building import SemanticScore


@pytest.fixture()
def resources_folder_path(tests_folder_path) -> str:
    return os.path.join(tests_folder_path, 'resources')


@pytest.fixture()
def tests_folder_path() -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)))


@pytest.fixture
def base64_image_string(resources_folder_path: str) -> str:
    with open(
        os.path.join(resources_folder_path, 'image', '5109112832.jpg'), 'rb'
    ) as f:
        binary = f.read()
        img_string = base64.b64encode(binary).decode('utf-8')
    return img_string


@pytest.fixture
def base64_image_string(resources_folder_path: str) -> str:
    with open(
        os.path.join(resources_folder_path, 'image', '5109112832.jpg'), 'rb'
    ) as f:
        binary = f.read()
        img_string = base64.b64encode(binary).decode('utf-8')
    return img_string


@pytest.fixture()
def setup_qdrant(tests_folder_path):
    docker_file_path = os.path.join(
        tests_folder_path, 'executor/indexer/base/docker-compose.yml'
    )
    cmd(
        f"docker-compose -f {docker_file_path} --project-directory . up  --build -d --remove-orphans"
    )
    yield
    cmd(
        f"docker-compose -f {docker_file_path} --project-directory . down --remove-orphans"
    )


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
def get_task_config_path(resources_folder_path: str) -> str:
    return os.path.join(resources_folder_path, 'text+image/config.json')


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


MAX_RETRIES = 20


@pytest.fixture(scope="session")
def es_connection_params():
    connection_str = 'http://localhost:9200'
    connection_args = {'verify_certs': False}
    return connection_str, connection_args


@pytest.fixture(scope='session')
def setup_service_running(es_connection_params) -> None:
    docker_compose_file = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'resources/elastic/docker-compose.yml',
    )
    cmd(f'docker-compose -f {docker_compose_file} up -d')
    hosts, _ = es_connection_params
    retries = 0
    while retries < MAX_RETRIES:
        try:
            es = Elasticsearch(hosts=hosts)
            if es.ping():
                break
            else:
                retries += 1
                time.sleep(5)
        except Exception:
            print('Elasticsearch is not running')
    if retries >= MAX_RETRIES:
        raise RuntimeError('Elasticsearch is not running')
    yield
    cmd('docker-compose -f tests/resources/elastic/docker-compose.yml down')


@pytest.fixture
def random_index_name():
    return f"test-index-{random.randint(0, 10000)}"


@pytest.fixture
def es_inputs() -> namedtuple:
    np.random.seed(42)

    @dataclass
    class MMDoc:
        title: Text
        excerpt: Text
        gif: List[Image]

    @dataclass
    class MMQuery:
        query_text: Text

    document_mappings = [
        ('clip', 8, ['title', 'gif']),
        ('sbert', 5, ['title', 'excerpt']),
    ]

    default_semantic_scores = [
        SemanticScore('query_text', 'title', 'clip', 1),
        SemanticScore('query_text', 'gif', 'clip', 1),
        SemanticScore('query_text', 'title', 'sbert', 1),
        SemanticScore('query_text', 'excerpt', 'sbert', 3),
        SemanticScore('query_text', 'my_bm25_query', 'bm25', 1),
    ]
    docs = [
        MMDoc(
            title='cat test title cat',
            excerpt='cat test excerpt cat',
            gif=[
                'https://product-finder.wordlift.io/wp-content/uploads/2021/06/93217825.jpeg',
                'https://product-finder.wordlift.io/wp-content/uploads/2021/06/93217825.jpeg',
                'https://product-finder.wordlift.io/wp-content/uploads/2021/06/93217825.jpeg',
            ],
        ),
        MMDoc(
            title='test title dog',
            excerpt='test excerpt 2',
            gif=[
                'https://product-finder.wordlift.io/wp-content/uploads/2021/06/93217825.jpeg',
                'https://product-finder.wordlift.io/wp-content/uploads/2021/06/93217825.jpeg',
                'https://product-finder.wordlift.io/wp-content/uploads/2021/06/93217825.jpeg',
            ],
        ),
    ]
    clip_docs = DocumentArray()
    sbert_docs = DocumentArray()
    # encode our documents
    for i, doc in enumerate(docs):
        prep_doc = Document(doc)
        prep_doc.tags['color'] = random.choice(['red', 'blue', 'green'])
        prep_doc.tags['price'] = i + 0.5
        prep_doc.id = str(i)
        clip_doc = Document(prep_doc, copy=True)
        clip_doc.id = prep_doc.id
        sbert_doc = Document(prep_doc, copy=True)
        sbert_doc.id = prep_doc.id

        clip_doc.title.embedding = np.random.random(8)
        clip_doc.gif[0].embedding = np.random.random(8)
        clip_doc.gif[1].embedding = np.random.random(8)
        clip_doc.gif[2].embedding = np.random.random(8)
        sbert_doc.title.embedding = np.random.random(5)
        sbert_doc.excerpt.embedding = np.random.random(5)

        clip_docs.append(clip_doc)
        sbert_docs.append(sbert_doc)

    index_docs_map = {
        'clip': clip_docs,
        'sbert': sbert_docs,
    }

    query = MMQuery(query_text='cat')

    query_doc = Document(query)
    clip_doc = Document(query_doc, copy=True)
    clip_doc.id = query_doc.id
    sbert_doc = Document(query_doc, copy=True)
    sbert_doc.id = query_doc.id

    clip_doc.query_text.embedding = np.random.random(8)
    sbert_doc.query_text.embedding = np.random.random(5)

    query_docs_map = {
        'clip': DocumentArray([clip_doc]),
        'sbert': DocumentArray([sbert_doc]),
    }
    EsInputs = namedtuple(
        'EsInputs',
        [
            'index_docs_map',
            'query_docs_map',
            'document_mappings',
            'default_semantic_scores',
        ],
    )
    return EsInputs(
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_semantic_scores,
    )
