import pytest
import requests
from docarray.typing import Image, Text
from tests.integration.local.conftest import SEARCH_URL, get_flow, get_request_body
from now.constants import ACCESS_PATHS, Models


@pytest.mark.parametrize(
    'data',
    [
        'artworks_data',
        'pop_lyrics_data',
        'elastic_data',
    ],
)
def test_end_to_end(
    data, start_bff, setup_service_running, random_index_name, request, tmpdir
):
    docs, user_input = request.getfixturevalue(data)
    fields_for_mapping = (
        [
            user_input.field_names_to_dataclass_fields[field_name]
            for field_name in user_input.index_fields
        ]
        if user_input.field_names_to_dataclass_fields
        else user_input.index_fields
    )
    f = get_flow(
        tmpdir=tmpdir,
        indexer_args={
            'index_name': random_index_name,
            'user_input_dict': {
                'filter_fields': user_input.filter_fields,
            },
            'document_mappings': [[Models.CLIP_MODEL, 512, fields_for_mapping]],
        },
    )
    with f:
        f.index(
            docs,
            parameters={
                'access_paths': ACCESS_PATHS,
            },
        )
        request_body = get_request_body(secured=False)
        request_body['query'] = [{'name': 'text', 'value': 'test', 'modality': 'text'}]
        response = requests.post(
            SEARCH_URL,
            json=request_body,
        )

    assert response.status_code == 200
    assert len(response.json()) == 10

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
