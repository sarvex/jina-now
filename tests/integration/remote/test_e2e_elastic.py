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
from now.constants import MAX_DOCS_FOR_TESTING, DatasetTypes, Models


@pytest.mark.remote
@pytest.mark.timeout(60 * 10)
def test_end_to_end(
    cleanup,
    random_flow_name,
    setup_online_shop_db,
    es_connection_params,
):
    _, index_name = setup_online_shop_db
    connection_str, _ = es_connection_params
    kwargs = {
        'now': 'start',
        'flow_name': random_flow_name,
        'dataset_type': DatasetTypes.ELASTICSEARCH,
        'admin_name': 'team-now',
        'es_host_name': connection_str,
        'es_index_name': index_name,
        'es_additional_args': None,
        'index_fields': ['title'],
        'filter_fields': ['product_id'],
        'title_model': [Models.CLIP_MODEL],
        'secured': True,
        'api_key': None,
        'additional_user': False,
    }
    kwargs = Namespace(**kwargs)
    response = cli(args=kwargs)

    assert_deployment_response(response)
    assert_deployment_queries(
        index_fields=['title'],
        kwargs=kwargs,
        response=response,
        search_modality='text',
    )
    request_body = get_search_request_body(
        kwargs=kwargs,
        search_modality='text',
    )
    suggest_url = f'{response["host_http"]}/api/v1/search-app/suggestion'
    info_url = f'{response["host_http"]}/api/v1/search-app/'
    assert_info_endpoints(info_url, request_body)
    assert_suggest(suggest_url, request_body)
    assert_indexed_all_docs(
        response['host_http'], kwargs=kwargs, limit=MAX_DOCS_FOR_TESTING
    )
