import hubble
import pytest
import requests
from docarray.typing import Image, Text
from jina import Client
from tests.integration.local.conftest import (  # noqa: F401
    BASE_URL,
    SEARCH_URL,
    get_flow,
    get_request_body,
)

from now.constants import ACCESS_PATHS


@pytest.mark.parametrize(
    'get_flow',
    [
        'artworks_data',
        'pop_lyrics_data',
        'elastic_data',
        'local_folder_data',
        's3_bucket_data',
    ],
    indirect=True,
)
def test_end_to_end(get_flow, setup_service_running):
    docs, user_input = get_flow
    client = Client(host='grpc://localhost:8085')

    client.index(
        docs,
        parameters={
            'access_paths': ACCESS_PATHS,
        },
        metadata=(('authorization', hubble.get_token()),),
    )

    request_headers, request_body = get_request_body(secured=False)
    request_body['query'] = [{'name': 'text', 'value': 'test', 'modality': 'text'}]
    response = requests.post(
        SEARCH_URL,
        headers=request_headers,
        json=request_body,
    )

    assert response.status_code == 200
    assert len(response.json()) == min(len(docs), 10)

    for field in user_input.filter_fields:
        dataclass_field = user_input.field_names_to_dataclass_fields.get(field) or field
        assert dataclass_field in response.json()[0]['tags']

    for field in user_input.index_fields:
        dataclass_field = (
            user_input.field_names_to_dataclass_fields[field]
            if user_input.field_names_to_dataclass_fields
            else field
        )
        if user_input.index_field_candidates_to_modalities[field] == Text:
            assert response.json()[0]['fields'][dataclass_field]['text']
        elif user_input.index_field_candidates_to_modalities[field] == Image:
            assert response.json()[0]['fields'][dataclass_field]['blob'] != b''

    count_url = f'{BASE_URL}/info/count'
    request_body['limit'] = len(docs)
    count_response = requests.post(
        count_url, headers=request_headers, json=request_body
    )
    assert count_response.json()['number_of_docs'] == len(docs)
