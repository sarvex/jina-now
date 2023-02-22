import json
from argparse import Namespace

import pytest
from tests.integration.remote.assertions import (
    assert_deployment_queries,
    assert_deployment_response,
    assert_indexed_all_docs,
    assert_suggest,
    get_search_request_kwargs,
)

from now.cli import cli
from now.constants import MAX_DOCS_FOR_TESTING, DatasetTypes, Models


@pytest.mark.remote
@pytest.mark.timeout(60 * 10)
def test_end_to_end(
    cleanup,
    setup_online_shop_db,
    es_connection_params,
):
    _, index_name = setup_online_shop_db
    connection_str, _ = es_connection_params
    kwargs = {
        'now': 'start',
        'flow_name': 'nowapi',
        'dataset_type': DatasetTypes.ELASTICSEARCH,
        'admin_name': 'team-now@jina.ai',
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
    # Dump the flow details from response host to a tmp file
    flow_details = {'host': response['host_http']}
    with open(f'{cleanup}/flow_details.json', 'w') as f:
        json.dump(flow_details, f)

    assert_deployment_response(response)
    assert_deployment_queries(
        kwargs=kwargs,
        response=response,
        search_modality='text',
    )
    request_headers, request_body = get_search_request_kwargs(
        kwargs=kwargs,
        search_modality='text',
    )
    suggest_url = f'{response["host_http"]}/api/v1/search-app/suggestion'
    assert_suggest(suggest_url, request_headers, request_body)
    assert_indexed_all_docs(
        flow_details['host'], kwargs=kwargs, limit=MAX_DOCS_FOR_TESTING
    )
