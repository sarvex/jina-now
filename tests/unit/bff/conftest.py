from typing import Callable, Dict, Generator, Optional, Union

import pytest
import requests
from docarray import Document, DocumentArray
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture

data_url = 'https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/jpeg/best-artworks.img10.bin'


# needed to mock DataRequest object from jina which is fairly complex
class MockJinaDataRequest:
    def __init__(self, docs: DocumentArray):
        self.header = MockJinaDataRequestHeader()
        self.docs = docs


class MockJinaDataRequestHeader:
    def __init__(self):
        self.status = MockJinaDataRequestStatus()


class MockJinaDataRequestStatus:
    def __init__(self):
        self.code = 0


class MockedJinaClient:
    """
    This class is used to override the JinaClient dependency in the bff.
    On each call, it returns a `DocumentArray` with the call args in the `Document` tags.
    """

    def __init__(self, response: DocumentArray):
        self.response = response

    def post(
        self,
        inputs: Union[Document, DocumentArray],
        parameters: Optional[Dict] = None,
        *args,
        **kwargs
    ) -> DocumentArray:
        for doc in self.response.flatten():
            doc.tags['parameters'] = parameters
        return self.response


@pytest.fixture
def client() -> Generator[requests.Session, None, None]:
    from now.executor.gateway.bff.app.app import build_app

    with TestClient(build_app()) as client:
        yield client


@pytest.fixture(scope='function')
def client_with_mocked_jina_client(
    mocker: MockerFixture,
) -> Callable[[DocumentArray], requests.Session]:
    def _fixture(response: DocumentArray) -> requests.Session:
        from now.executor.gateway.bff.app.app import build_app
        from now.executor.gateway.bff.app.settings import init_user_input

        def _get_jina_client(host, port):
            return MockedJinaClient(response)

        mocker.patch(
            'now.executor.gateway.bff.app.v1.routers.helper.get_jina_client',
            _get_jina_client,
        )
        init_user_input()
        return TestClient(build_app())

    return _fixture
