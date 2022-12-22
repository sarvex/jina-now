from multiprocessing import Process

import pytest
from docarray import Document
from jina import Flow

from deployment.bff.app.app import run_server
from now.constants import ACCESS_PATHS, EXTERNAL_CLIP_HOST, NOW_QDRANT_INDEXER_VERSION
from now.executor.indexer.in_memory import InMemoryIndexer
from now.executor.indexer.qdrant import NOWQdrantIndexer16
from now.executor.name_to_id_map import name_to_id_map
from now.executor.preprocessor import NOWPreprocessor
from now.now_dataclasses import UserInput

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
    docs = [Document(text='test', tags={'color': 'red'}) for _ in range(9)]
    docs.append(Document(text='test', tags={'color': 'blue'}))
    f.index(
        docs,
        parameters={
            'user_input': UserInput().__dict__,
            'access_paths': ACCESS_PATHS,
            **kwargs,
        },
    )


def get_flow(use_qdrant=True, preprocessor_args=None, indexer_args=None):
    """
    :param use_qdrant: if True, uses the NOWQdrantIndexer16 indexer, otherwise InMemoryIndexer.
    :param preprocessor_args: additional arguments for the preprocessor,
        e.g. {'admin_emails': [admin_email]}
    :param indexer_args: additional arguments for the indexer,
        e.g. {'admin_emails': [admin_email]}
    """
    preprocessor_args = preprocessor_args or {}
    indexer_args = indexer_args or {}
    f = (
        Flow(port_expose=9089)
        .add(
            uses=NOWPreprocessor,
            uses_with={'app': 'image_text_retrieval', **preprocessor_args},
        )
        .add(
            host=EXTERNAL_CLIP_HOST,
            port=443,
            tls=True,
            external=True,
        )
        .add(
            uses=f'jinahub+docker://{name_to_id_map.get("NOWQdrantIndexer16")}/{NOW_QDRANT_INDEXER_VERSION}'
            if use_qdrant
            else InMemoryIndexer,
            uses_with={'dim': 512, **indexer_args},
        )
    )
    return f
