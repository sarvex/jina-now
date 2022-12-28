import pytest
import requests
from now.admin.utils import get_default_request_body
from tests.integration.bff.conftest import HOST, PORT, SEARCH_URL, index_data, get_flow


def get_request_body():
    request_body = get_default_request_body('local', False, None)
    request_body['host'] = HOST
    request_body['port'] = PORT
    return request_body


@pytest.mark.parametrize(
    'use_qdrant',
    [True, False],
)
def test_search_filters(use_qdrant, start_bff):
    f = get_flow(use_qdrant=use_qdrant, indexer_args={'columns': ['color', 'str']})
    with f:
        index_data(f)
        request_body = get_request_body()
        request_body['query'] = {'text_field': {'text': 'test'}}
        request_body['filters'] = {'color': 'blue'}
        response = requests.post(
            SEARCH_URL,
            json=request_body,
        )

    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]['tags']['color'] == 'blue'
