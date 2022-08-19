from multiprocessing import Process
from time import sleep

import pytest
import requests
from docarray import Document
from jina import Flow
from tests.integration.test_end_to_end import assert_search, get_default_request_body

from deployment.bff.app.app import run_server
from now.constants import CLIP_USES, NOW_AUTH_EXECUTOR_VERSION

API_KEY = 'my_key'
base_url = 'http://localhost:8080/api/v1'
search_url = f'{base_url}/text-to-image/search'
update_api_keys_url = f'{base_url}/admin/updateApiKeys'
update_emails_url = f'{base_url}/admin/updateUserEmails'
host = 'grpc://0.0.0.0'
port = 9090


def get_reqest_body():
    request_body = get_default_request_body('local', True)
    request_body['host'] = 'grpc://0.0.0.0'
    request_body['port'] = 9090
    return request_body


def test_add_key():

    user_id = get_reqest_body()['jwt']['user']['_id']
    print('### spin up flow')
    with Flow(port_expose=9090).add(
        uses=f'jinahub+docker://AuthExecutor2/{NOW_AUTH_EXECUTOR_VERSION}',
        uses_with={
            'admin_emails': [user_id],
            'user_emails': [],
        },
    ).add(uses=f'jinahub+docker://{CLIP_USES}',).add(
        uses=f'jinahub+docker://AnnLiteIndexer/v0.1',
        uses_with={'n_dim': 512},
    ) as f:
        f.index(
            [Document(text='test') for i in range(10)],
            parameters={'jwt': get_reqest_body()['jwt']},
        )
        print('### flow started')
        p1 = Process(target=run_server, args=())
        p1.daemon = True
        p1.start()
        print('### server started')
        sleep(5)

        request_body = get_reqest_body()
        print('# Test adding user email')
        request_body['user_emails'] = ['florian.hoenicke@jina.ai']
        response = requests.post(
            update_emails_url,
            json=request_body,
        )
        assert response.status_code == 200

        print('# test api keys')
        print('# search with invalid api key')
        request_body = get_reqest_body()
        request_body['text'] = 'girl on motorbike'
        del request_body['jwt']
        request_body['api_key'] = 'my_key'
        request_body['limit'] = 9
        with pytest.raises(Exception):
            assert_search(search_url, request_body)
        print('# add api key')
        request_body_update_keys = get_reqest_body()
        request_body_update_keys['api_keys'] = ['my_key']
        response = requests.post(
            update_api_keys_url,
            json=request_body_update_keys,
        )
        if response.status_code != 200:
            print(response.text)
            print(response.json()['message'])
            raise Exception(f'Response status is {response.status_code}')
        print('# the same search should work now')
        assert_search(search_url, request_body)
