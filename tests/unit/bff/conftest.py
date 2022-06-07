from typing import Callable, Dict, Generator, Optional, Union

import pytest
import requests
from docarray import Document, DocumentArray
from fastapi import FastAPI
from fastapi.testclient import TestClient

from deployment.bff.app.app import build_app
from deployment.bff.app.v1.dependencies.jina_client import get_jina_client
from deployment.bff.app.v1.routers import image, music, text

data_url = 'https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/jpeg/best-artworks.img10.bin'


class MockedJinaClient:
    """
    This class is used to override the JinaClient dependency in the bff.
    On each call, it returns a `DocumentArray` with the call args in the `Document` tags.
    """

    def __init__(self, host: str, port: int, response: DocumentArray):
        self.host = host
        self.port = port
        self.response = response

    def post(
        self,
        url: str,
        query_docs: Union[Document, DocumentArray],
        parameters: Optional[Dict] = None,
    ) -> DocumentArray:
        for doc in self.response.flatten():
            doc.tags = {'url': url, 'parameters': parameters}
        return self.response


@pytest.fixture
def client() -> Generator[requests.Session, None, None]:
    with TestClient(build_app()) as client:
        yield client


@pytest.fixture(scope='function')
def client_with_mocked_jina_client() -> Callable[[DocumentArray], requests.Session]:
    # can't use `build_app`, apparently the Starlette mount disables the dep overrides
    app = FastAPI()
    app.include_router(image.router, prefix='/api/v1/image', tags=['Image'])
    app.include_router(text.router, prefix='/api/v1/text', tags=['Text'])
    app.include_router(music.router, prefix='/api/v1/music', tags=['Music'])

    def _test_client(jina_client_response: DocumentArray):
        app.dependency_overrides = {
            get_jina_client: lambda host='localhost', port=30000: MockedJinaClient(
                host, port, jina_client_response
            ),
        }
        return TestClient(app)

    return _test_client
