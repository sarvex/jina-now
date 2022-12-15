import os
from multiprocessing import Process
from time import sleep

import pytest
import requests
from docarray import Document
from jina import Flow

from deployment.bff.app.app import run_server
from now.admin.utils import get_default_request_body
from now.constants import (
    ACCESS_PATHS,
    EXTERNAL_CLIP_HOST,
)
from now.deployment.deployment import cmd
from now.executor.indexer.qdrant import NOWQdrantIndexer16
from now.executor.preprocessor import NOWPreprocessor
from now.now_dataclasses import UserInput


@pytest.fixture()
def run_docker(tests_folder_path):
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


def get_request_body():
    request_body = get_default_request_body('local', False, None)
    request_body['host'] = 'grpc://0.0.0.0'
    request_body['port'] = 9089
    return request_body


def get_flow():
    f = (
        Flow(port_expose=9089)
        .add(
            uses=NOWPreprocessor,
            uses_with={'app': 'image_text_retrieval'},
        )
        .add(
            host=EXTERNAL_CLIP_HOST,
            port=443,
            tls=True,
            external=True,
        )
        .add(
            uses=NOWQdrantIndexer16,
            uses_with={'dim': 512, 'columns': ['color', 'str']},
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


def test_search_with_filters(run_docker):
    f = get_flow()
    with f:
        index(f)
        start_bff()
        sleep(5)
        request_body = get_request_body()
        request_body['text'] = 'test'
        request_body['filters'] = {'color': 'blue'}
        base_url = 'http://localhost:8080/api/v1'
        search_url = f'{base_url}/image-or-text-to-image-or-text/search'
        response = requests.post(
            search_url,
            json=request_body,
        )

    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]['tags']['color'] == 'blue'
