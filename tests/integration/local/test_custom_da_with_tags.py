import hubble
import pytest
import requests
from jina import Client
from tests.integration.local.conftest import (  # noqa
    SEARCH_URL,
    get_flow,
    get_request_body,
)

from now.constants import ACCESS_PATHS


@pytest.mark.parametrize(
    'get_flow',
    ['data_with_tags'],
    indirect=True,
)
def test_search_filters(get_flow, setup_service_running):
    docs, _ = get_flow
    client = Client(host='grpc://localhost:8085')
    client.index(
        docs,
        parameters={
            'access_paths': ACCESS_PATHS,
        },
        metadata=(('authorization', hubble.get_token()),),
    )
    _, request_body = get_request_body()
    request_body['query'] = [{'name': 'text', 'value': 'test', 'modality': 'text'}]
    request_body['filters'] = {'color': 'Blue Color'}
    response = requests.post(
        SEARCH_URL,
        json=request_body,
        headers={'Authorization': f'token {hubble.get_token()}'},
    )

    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]['tags']['color'] == 'Blue Color'
