import base64
import json
import os

import pytest
import requests
from docarray import DocumentArray

from now.admin.utils import get_default_request_body
from now.demo_data import DemoDatasetNames


@pytest.fixture
def test_search_image(resources_folder_path: str):
    with open(os.path.join(resources_folder_path, 'image', 'a.jpg'), 'rb') as f:
        binary = f.read()
        img_query = base64.b64encode(binary).decode('utf-8')
    return img_query


def assert_deployment_response(response):
    host_grpc = response['host_grpc']
    assert host_grpc.startswith('grpcs://')
    assert host_grpc.endswith('.wolf.jina.ai')
    host_http = response['host_http']
    assert host_http.startswith('https://')
    assert host_http.endswith('.wolf.jina.ai')
    assert response['bff'] == f'{host_http}/api/v1/search-app/docs'
    assert response['playground'] == f'{host_http}/playground'


def assert_deployment_queries(
    kwargs,
    response,
    search_modality,
    dataset=None,
):
    host = response.get('host_http')
    url = f'{host}/api/v1'
    # normal case
    request_body = get_search_request_body(
        kwargs=kwargs,
        search_modality=search_modality,
        dataset=dataset,
    )
    search_url = f'{url}/search-app/search'
    assert_search(search_url, request_body)

    if kwargs.secured:
        # test add email
        request_body = get_default_request_body(secured=kwargs.secured)
        request_body['user_emails'] = ['florian.hoenicke@jina.ai']
        response = requests.post(
            f'{url}/admin/updateUserEmails',
            json=request_body,
        )
        assert (
            response.status_code == 200
        ), f"text: {json.dumps(response.json(), indent=2)}"

        # add api key
        del request_body['user_emails']
        request_body['api_keys'] = ['my_key']
        response = requests.post(
            f'{url}/admin/updateApiKeys',
            json=request_body,
        )
        if response.status_code != 200:
            print(response.text)
            print(response.json()['message'])
            raise Exception(f'Response status is {response.status_code}')
        # the same search should work now
        request_body = get_search_request_body(
            kwargs=kwargs,
            search_modality=search_modality,
            dataset=dataset,
        )
        assert_search(search_url, request_body)
        # search with invalid api key
        del request_body['jwt']
        request_body['api_key'] = 'no_key'
        with pytest.raises(Exception):
            assert_search(search_url, request_body)


def get_search_request_body(
    kwargs,
    search_modality,
    dataset=None,
):
    request_body = get_default_request_body(secured=kwargs.secured)
    request_body['limit'] = 9
    # Perform end-to-end check via bff
    if search_modality == 'text':
        if dataset == DemoDatasetNames.BEST_ARTWORKS:
            search_text = 'impressionism'
        elif dataset == DemoDatasetNames.NFT_MONKEY:
            search_text = 'laser eyes'
        else:
            search_text = 'test'
        request_body['query'] = [
            {'name': 'text', 'value': search_text, 'modality': 'text'}
        ]
    elif search_modality == 'image':
        request_body['query'] = [
            {'name': 'blob', 'value': test_search_image, 'modality': 'image'}
        ]
    return request_body


def assert_search(search_url, request_body, expected_status_code=200):
    response = requests.post(
        search_url,
        json=request_body,
    )
    assert response.status_code == expected_status_code, (
        f"Received code {response.status_code} but {expected_status_code} was expected. \n"
        f"text: {json.dumps(response.json(), indent=2)}"
    )
    if response.status_code == 200:
        assert len(response.json()) == 9


def assert_suggest(suggest_url, request_body):
    old_request_text = request_body.pop('query')
    request_body['text'] = old_request_text[0]['value']
    response = requests.post(
        suggest_url,
        json=request_body,
    )
    assert (
        response.status_code == 200
    ), f"Received code {response.status_code} with text: {response.json()['message']}"
    docs = DocumentArray.from_json(response.content)
    assert 'suggestions' in docs[0].tags, f'No suggestions found in {docs[0].tags}'
    assert docs[0].tags['suggestions'] == [old_request_text[0]['value']], (
        f"Expected suggestions to be {old_request_text[0]['value']} but got "
        f"{docs[0].tags['suggestions']}"
    )


def assert_search_custom_s3(host, mm_type, dataset_length, create_temp_link=False):
    request_body = {
        'query': [{'name': 'text', 'value': 'Hello', 'modality': 'text'}],
        'limit': 9,
        'create_temp_link': create_temp_link,
    }

    response = requests.post(
        f'{host}/api/v1/search-app/search',
        json=request_body,
    )

    assert (
        response.status_code == 200
    ), f"Received code {response.status_code} with text: {response.json()['message']}"

    response_json = response.json()
    assert len(response_json) == 9 if dataset_length > 9 else dataset_length
    for doc in response_json:
        fields = list(doc['fields'].values())
        for field in fields:
            if field['uri']:
                if create_temp_link:
                    assert not field['uri'].startswith('s3://'), f"received: {doc}"
                else:
                    assert field['uri'].startswith('s3://'), f"received: {doc}"
            assert (
                'blob' not in field.keys()
                or field['blob'] is None
                or field['blob'] == ''
            )
    if mm_type:
        for doc in response_json:
            assert len(doc['tags']) > 0


def assert_indexed_all_docs(flow_details, kwargs):
    request_body = get_default_request_body(secured=kwargs.secured)
    response = requests.post(
        f"{flow_details['host']}/api/v1/search-app/count/?limit=50",
        json=request_body,
    )
    response_json = response.json()
    assert response_json['number_of_docs'] == 50
