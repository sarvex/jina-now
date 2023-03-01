from typing import Callable

import pytest
import requests
from docarray import Document, DocumentArray
from starlette import status

from now.now_dataclasses import UserInput


def test_text_search_fails_with_incorrect_query(client):
    with pytest.raises(ValueError):
        client.post(
            f'/api/v1/search-app/search',
            json={
                'data': [
                    (
                        [{'name': 'text', 'value': 'Hello', 'modality': 'text'}],
                        [
                            {
                                'name': 'uri',
                                'value': 'example.png',
                                'modality': 'image',
                            }
                        ],
                        [],
                    )
                ]
            },
        )


def test_text_search_fails_with_empty_query(client: requests.Session):
    with pytest.raises(ValueError):
        client.post(
            f'/api/v1/search-app/search',
            json={},
        )


def test_image_search_calls_flow(
    client_with_mocked_jina_client: Callable[[DocumentArray], requests.Session],
    sample_search_response_text: DocumentArray,
    base64_image_string: str,
):
    response = client_with_mocked_jina_client(sample_search_response_text).post(
        '/api/v1/search-app/search',
        json={
            'query': [
                {'name': 'blob', 'value': base64_image_string, 'modality': 'image'},
            ]
        },
    )

    assert response.status_code == status.HTTP_200_OK
    results = DocumentArray.from_json(response.content)
    # the mock writes the call args into the response tags, not url isn't written anymore by changes in mock
    assert results[0].tags['parameters']['limit'] == 10


def test_multimodal_search_calls_flow(
    client_with_mocked_jina_client: Callable[[DocumentArray], requests.Session],
    sample_search_response_text: DocumentArray,
    base64_image_string: str,
):
    response = client_with_mocked_jina_client(sample_search_response_text).post(
        '/api/v1/search-app/search',
        json={
            'query': [
                {'name': 'blob', 'value': base64_image_string, 'modality': 'image'},
                {'name': 'text', 'value': 'Hello', 'modality': 'text'},
            ]
        },
    )

    assert response.status_code == status.HTTP_200_OK
    results = DocumentArray.from_json(response.content)
    # the mock writes the call args into the response tags
    assert results[0].tags['parameters']['limit'] == 10


def test_image_search_parse_response(
    client_with_mocked_jina_client: Callable[[DocumentArray], requests.Session],
    sample_search_response_text: DocumentArray,
    base64_image_string: str,
):
    response_raw = client_with_mocked_jina_client(sample_search_response_text).post(
        '/api/v1/search-app/search',
        json={
            'query': [
                {'name': 'blob', 'value': base64_image_string, 'modality': 'image'},
            ]
        },
    )

    assert response_raw.status_code == status.HTTP_200_OK
    results = DocumentArray()
    # todo: use multimodal doc in the future
    for response_json in response_raw.json():
        content = list(response_json['fields'].values())[0]
        doc = Document(
            id=response_json['id'],
            tags=response_json['tags'],
            scores=response_json['scores'],
            **content,
        )
        results.append(doc)
    assert len(results) == len(sample_search_response_text[0].matches)
    assert results[0].text == sample_search_response_text[0].matches[0].chunks[0].text


def get_user_input() -> UserInput:
    user_input = UserInput()
    user_input.index_fields = ['text']
    user_input.field_names_to_dataclass_fields = {'text': 'text'}
    return user_input


@pytest.mark.parametrize('dump_user_input', [get_user_input()], indirect=True)
def test_text_search_with_semantic_scores(
    dump_user_input,
    client_with_mocked_jina_client: Callable[[DocumentArray], requests.Session],
    sample_search_response_text: DocumentArray,
    base64_image_string: str,
):
    """
    Test that semantic_scores can be passed as parameters to the search endpoint.
    """
    response = client_with_mocked_jina_client(sample_search_response_text).post(
        '/api/v1/search-app/search',
        json={
            'query': [
                {'name': 'text', 'value': 'this crazy text', 'modality': 'text'},
            ],
            'semantic_scores': [['text', 'image.png', 'clip', 1]],
        },
    )

    assert response.status_code == status.HTTP_200_OK
    results = DocumentArray.from_json(response.content)
    # the mock writes the call args into the response tags
    assert results[0].tags['parameters']['semantic_scores']
    assert results[0].tags['parameters']['semantic_scores'] == [
        ['text_0', 'text', 'clip', 1]
    ]
