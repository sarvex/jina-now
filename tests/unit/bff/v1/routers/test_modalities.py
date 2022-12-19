from typing import Callable

import pytest
import requests
from docarray import DocumentArray
from starlette import status
from tests.unit.bff.v1.conftest import base64_audio_string, base64_image_string

content_dict = {
    'image': base64_image_string(),
    'text': 'Hello',
    'music': base64_audio_string(),
    'video': base64_image_string(),
}


@pytest.mark.parametrize(
    'app_name, input_modality, output_modality, attribute_name_input, attribute_name_output',
    [
        ('search', 'image', 'image', 'blob', 'blob'),
        ('search', 'image', 'text', 'blob', 'text'),
        ('search', 'text', 'image', 'text', 'blob'),
        ('search', 'text', 'text', 'text', 'text'),
        ('search', 'music', 'music', 'blob', 'blob'),
        ('search', 'text', 'video', 'text', 'blob'),
    ],
)
class TestEndpoints:
    def test_index_fails_with_no_flow_running(
        self,
        client: requests.Session,
        app_name,
        input_modality,
        output_modality,
        attribute_name_input,
        attribute_name_output,
    ):
        with pytest.raises(ConnectionError):
            result = client.post(
                f'/api/v1/{app_name}/index',
                json={
                    f'document_list': [
                        {
                            'fields': {
                                'product_image': {
                                    'image': content_dict['image'],
                                }
                            },
                            'tags': {},
                        }
                    ]
                },
            )

    def test_search_fails_with_no_flow_running(
        self,
        client: requests.Session,
        app_name,
        input_modality,
        output_modality,
        attribute_name_input,
        attribute_name_output,
    ):
        with pytest.raises(ConnectionError):
            client.post(
                f'/api/v1/{app_name}/search',
                json={
                    'fields': {
                        'product_image': {
                            'image': content_dict['image'],
                        }
                    },
                },
            )

    def test_index(
        self,
        client_with_mocked_jina_client: Callable[[DocumentArray], requests.Session],
        app_name,
        input_modality,
        output_modality,
        attribute_name_input,
        attribute_name_output,
    ):
        response = client_with_mocked_jina_client(DocumentArray()).post(
            f'/api/v1/{app_name}/index',
            json={
                f'document_list': [
                    {
                        'fields': {
                            'product_description': {
                                'text': 'test 1, 2, 3',
                            }
                        },
                        'tags': {},
                    }
                ]
            },
        )
        assert response.status_code == status.HTTP_200_OK

    def test_search_calls_flow(
        self,
        client_with_mocked_jina_client: Callable[[DocumentArray], requests.Session],
        app_name,
        sample_search_response,
        input_modality,
        output_modality,
        attribute_name_input,
        attribute_name_output,
    ):
        response = client_with_mocked_jina_client(sample_search_response).post(
            f'/api/v1/{app_name}/search',
            json={
                'fields': {
                    'product_image': {
                        'text': 'test 1, 2, 3',
                    }
                },
                'limit': 10,
            },
        )

        assert response.status_code == status.HTTP_200_OK
        results = DocumentArray.from_json(response.content)
        # the mock writes the call args into the response tags
        assert results[0].tags['url'] == '/search'
        assert set(results[0].tags['parameter_keys'].split(',')) == {'filter', 'limit'}

    def test_search_parse_response(
        self,
        client_with_mocked_jina_client: Callable[[DocumentArray], requests.Session],
        app_name,
        sample_search_response,
        input_modality,
        output_modality,
        attribute_name_input,
        attribute_name_output,
    ):
        response = client_with_mocked_jina_client(sample_search_response).post(
            f'/api/v1/{app_name}/search',
            json={attribute_name_input: content_dict[input_modality]},
        )

        assert response.status_code == status.HTTP_200_OK
        results = DocumentArray.from_json(response.content)
        assert len(results) == len(sample_search_response[0].matches)
        assert results[0].uri == sample_search_response[0].matches[0].uri
