import pytest
import requests
from jina import Client
from tests.integration.local.conftest import BASE_URL, SEARCH_URL, get_request_body
from tests.integration.remote.assertions import assert_search

from now.constants import ACCESS_PATHS

API_KEY = 'my_key'
update_api_keys_url = f'{BASE_URL}/admin/updateApiKeys'
update_emails_url = f'{BASE_URL}/admin/updateUserEmails'


@pytest.mark.parametrize(
    'get_flow',
    ['api_key_data'],
    indirect=True,
)
def test_add_key(get_flow, setup_service_running):
    docs, user_input = get_flow
    client = Client(host='grpc://localhost:8085')
    client.index(
        docs,
        parameters={
            'access_paths': ACCESS_PATHS,
            'jwt': get_request_body(secured=True)['jwt'],
        },
    )
    request_body = get_request_body(secured=True)
    # Test adding user email
    request_body['user_emails'] = ['florian.hoenicke@jina.ai']
    response = requests.post(
        update_emails_url,
        json=request_body,
    )
    assert response.status_code == 200
    # test api keys
    # search with invalid api key
    request_body = get_request_body(secured=True)
    request_body['query'] = [
        {'name': 'text', 'value': 'girl on motorbike', 'modality': 'text'}
    ]
    del request_body['jwt']
    request_body['api_key'] = API_KEY
    request_body['limit'] = 9
    assert_search(SEARCH_URL, request_body, expected_status_code=401)

    print('# add api key')
    request_body_update_keys = get_request_body(secured=True)
    request_body_update_keys['api_keys'] = [API_KEY]
    response = requests.post(
        update_api_keys_url,
        json=request_body_update_keys,
    )
    if response.status_code != 200:
        print(response.text)
        print(response.json()['message'])
        raise Exception(f'Response status is {response.status_code}')
    print('# the same search should work now')
    assert_search(SEARCH_URL, request_body)
