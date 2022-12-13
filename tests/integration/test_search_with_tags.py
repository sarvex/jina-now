from multiprocessing import Process
from time import sleep

import requests
from docarray import Document
from jina import Flow

from deployment.bff.app.app import run_server
from now.admin.utils import get_default_request_body
from now.constants import (
    ACCESS_PATHS,
    EXTERNAL_CLIP_HOST,
    NOW_PREPROCESSOR_VERSION,
    NOW_QDRANT_INDEXER_VERSION,
)
from now.executor.name_to_id_map import name_to_id_map
from now.now_dataclasses import UserInput


def get_request_body():
    request_body = get_default_request_body('local', False, None)
    request_body['host'] = 'grpc://0.0.0.0'
    request_body['port'] = 9089
    return request_body


def get_flow():
    f = (
        Flow(port_expose=9089)
        .add(
            uses=f'jinahub+docker://{name_to_id_map.get("NOWPreprocessor")}/{NOW_PREPROCESSOR_VERSION}',
            uses_with={'app': 'image_text_retrieval'},
        )
        .add(
            host=EXTERNAL_CLIP_HOST,
            port=443,
            tls=True,
            external=True,
        )
        .add(
            uses=f'jinahub+docker://{name_to_id_map.get("NOWQdrantIndexer16")}/{NOW_QDRANT_INDEXER_VERSION}',
            uses_with={'dim': 512},
        )
    )
    return f


def index(f):
    docs = [Document(text='test', tags={'color': 'red'}) for _ in range(9)]
    docs.append(Document(text='test', tags={'color': 'blue'}))
    f.index(
        docs,
        parameters={
            'user_input': UserInput().__dict__,
            'access_paths': ACCESS_PATHS,
        },
    )


def start_bff(port=8080, daemon=True):
    p1 = Process(target=run_server, args=(port,))
    p1.daemon = daemon
    p1.start()


def test_search_with_filters():
    f = get_flow()
    with f:
        index(f)
        start_bff()
        sleep(5)

        request_body = get_request_body()
        request_body['text'] = 'girl on motorbike'
        request_body['filters'] = {'color': 'blue'}
        base_url = 'http://localhost:8080/api/v1'
        search_url = f'{base_url}/image-or-text-to-image-or-text/search'
        response = requests.post(
            search_url,
            json=request_body,
        )
        print(response)
        assert response.status_code == 200
        assert len(response.json()) == 1
