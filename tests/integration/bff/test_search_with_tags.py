import requests
from tests.integration.bff.conftest import HOST, PORT, SEARCH_URL, get_flow, index_data

from now.admin.utils import get_default_request_body


def get_request_body():
    request_body = get_default_request_body('local', False, None)
    request_body['host'] = HOST
    request_body['port'] = PORT
    return request_body


def test_search_filters(start_bff, tmpdir):
    f = get_flow(indexer_args={'columns': ['color', 'str']}, tmpdir=tmpdir)
    with f:
        index_data(f)
        request_body = get_request_body()
        request_body['query'] = {'query_text': {'text': 'test'}}
        request_body['filters'] = {'color': 'blue'}
        response = requests.post(
            SEARCH_URL,
            json=request_body,
        )

    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]['tags']['color'] == 'blue'
