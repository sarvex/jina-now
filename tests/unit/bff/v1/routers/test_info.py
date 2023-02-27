import json
from typing import Callable

import hubble
import requests
from docarray import DocumentArray
from starlette import status


def test_tags_response(
    client_with_mocked_jina_client: Callable[[DocumentArray], requests.Session],
    sample_search_response_text: DocumentArray,
    base64_image_string: str,
):
    response = client_with_mocked_jina_client(sample_search_response_text).post(
        '/api/v1/info/tags',
        json={
            'query': [
                {'name': 'blob', 'value': base64_image_string, 'modality': 'image'},
                {'name': 'text', 'value': 'Hello', 'modality': 'text'},
            ]
        },
        headers={'Authorization': f'token {hubble.get_token()}'},
    )

    assert response.status_code == status.HTTP_200_OK
    assert len(json.loads(response.content)['tags']) == 1


def test_count_response(
    client_with_mocked_jina_client: Callable[[DocumentArray], requests.Session],
    sample_search_response_text: DocumentArray,
    base64_image_string: str,
):
    response = client_with_mocked_jina_client(sample_search_response_text).post(
        '/api/v1/info/count',
        json={
            'query': [
                {'name': 'blob', 'value': base64_image_string, 'modality': 'image'},
                {'name': 'text', 'value': 'Hello', 'modality': 'text'},
            ]
        },
        headers={'Authorization': f'token {hubble.get_token()}'},
    )

    assert response.status_code == status.HTTP_200_OK
    assert json.loads(response.content)['number_of_docs'] == 1


def test_field_names_to_dataclass_fields_response(
    client_with_mocked_jina_client: Callable[[DocumentArray], requests.Session],
    sample_search_response_text: DocumentArray,
    base64_image_string: str,
):
    response = client_with_mocked_jina_client(sample_search_response_text).post(
        '/api/v1/info/field_names_to_dataclass_fields',
        json={
            'query': [
                {'name': 'blob', 'value': base64_image_string, 'modality': 'image'},
                {'name': 'text', 'value': 'Hello', 'modality': 'text'},
            ]
        },
        headers={'Authorization': f'token {hubble.get_token()}'},
    )

    assert response.status_code == status.HTTP_200_OK
    assert json.loads(response.content)['field_names_to_dataclass_fields'] == {}


def test_encoder_to_dataclass_fields_mods_response(
    client_with_mocked_jina_client: Callable[[DocumentArray], requests.Session],
    sample_search_response_text: DocumentArray,
    base64_image_string: str,
):
    response = client_with_mocked_jina_client(sample_search_response_text).post(
        '/api/v1/info/encoder_to_dataclass_fields_mods',
        json={
            'query': [
                {'name': 'blob', 'value': base64_image_string, 'modality': 'image'},
                {'name': 'text', 'value': 'Hello', 'modality': 'text'},
            ]
        },
        headers={'Authorization': f'token {hubble.get_token()}'},
    )

    assert response.status_code == status.HTTP_200_OK
    assert json.loads(response.content)['encoder_to_dataclass_fields_mods'] == {}
