from multiprocessing import Process

import pytest
from docarray import Document, DocumentArray
from jina import Flow

from docarray import dataclass
from docarray.typing import Text

from deployment.bff.app.app import run_server
from now.constants import ACCESS_PATHS, EXTERNAL_CLIP_HOST
from now.executor.indexer.elastic import NOWElasticIndexer
from now.executor.preprocessor import NOWPreprocessor
from now.now_dataclasses import UserInput
from tests.conftest import random_index_name

BASE_URL = 'http://localhost:8080/api/v1'
SEARCH_URL = f'{BASE_URL}/search-app/search'
HOST = 'grpc://0.0.0.0'
PORT = 9089


@pytest.fixture()
def start_bff():
    p1 = Process(target=run_server, args=(8080,))
    p1.daemon = True
    p1.start()
    yield
    p1.terminate()


def index_data(f, **kwargs):
    @dataclass
    class Doc:
        title: Text

    docs = DocumentArray([Document(Doc(title='test')) for _ in range(10)])
    for index, doc in enumerate(docs):
        doc.tags['color'] = 'blue' if index == 0 else 'red'

    f.index(
        docs,
        parameters={
            'user_input': UserInput().__dict__,
            'access_paths': ACCESS_PATHS,
            **kwargs,
        },
    )


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
        Flow(port_expose=9089)
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
                'index_name': 'random_index_name111',
                'document_mappings': [['encoderclip', 512, ['title']]],
                **indexer_args,
            },
            uses_metas=metas,
            no_reduce=True,
        )
    )
    return f
