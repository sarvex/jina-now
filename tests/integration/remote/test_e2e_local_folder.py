import json
from argparse import Namespace

import pytest
from tests.integration.remote.assertions import (
    assert_deployment_queries,
    assert_deployment_response,
    assert_indexed_all_docs,
    assert_info_endpoints,
    assert_suggest,
    get_search_request_body,
)

from now.cli import cli
from now.constants import DatasetTypes, Models


@pytest.mark.remote
@pytest.mark.timeout(60 * 10)
def test_end_to_end(
    cleanup,
    random_flow_name,
    pulled_local_folder_data,
):
    kwargs = {
        'now': 'start',
        'flow_name': random_flow_name,
        'dataset_type': DatasetTypes.PATH,
        'admin_name': 'team-now',
        'dataset_path': pulled_local_folder_data,
        'index_fields': ['image.png', 'test.txt'],
        'filter_fields': ['title'],
        'image.png_model': [Models.CLIP_MODEL],
        'test.txt_model': [Models.CLIP_MODEL, Models.SBERT_MODEL],
        'secured': True,
        'api_key': None,
        'additional_user': False,
    }
    kwargs = Namespace(**kwargs)
    response = cli(args=kwargs)

    assert_deployment_response(response)
    assert_deployment_queries(
        kwargs=kwargs,
        response=response,
        search_modality='text',
    )
    request_body = get_search_request_body(
        kwargs=kwargs,
        search_modality='text',
    )
    suggest_url = f'{response["host_http"]}/api/v1/search-app/suggestion'
    info_url = f'{response["host_http"]}/api/v1/info/'
    assert_info_endpoints(info_url, request_body)
    assert_suggest(suggest_url, request_body)
    assert_indexed_all_docs(response['host_http'], kwargs=kwargs, limit=10)
