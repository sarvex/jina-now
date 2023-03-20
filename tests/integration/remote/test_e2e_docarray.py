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
from now.demo_data import DemoDatasetNames


@pytest.mark.remote
@pytest.mark.parametrize(
    'query_fields, index_fields, filter_fields, model_selection, dataset',
    [
        # needs to be put back once wolf can handle it
        # (
        #     'image',
        #     ['image', 'label'],
        #     [],
        #     {
        #         'image_model': [Models.CLIP_MODEL],
        #         'label_model': [Models.CLIP_MODEL, Models.SBERT_MODEL],
        #     },
        #     DemoDatasetNames.BIRD_SPECIES,
        # ),
        (
            'text',
            ['video', 'description'],
            [],
            {
                'video_model': [Models.CLIP_MODEL],
                'description_model': [Models.CLIP_MODEL],
            },
            DemoDatasetNames.TUMBLR_GIFS_10K,
        ),
    ],
)
@pytest.mark.timeout(60 * 10)
def test_end_to_end(
    cleanup,
    random_flow_name,
    query_fields,
    index_fields,
    filter_fields,
    model_selection,
    dataset,
):
    kwargs = {
        'now': 'start',
        'flow_name': random_flow_name,
        'dataset_type': DatasetTypes.DEMO,
        'admin_name': 'team-now',
        'index_fields': index_fields,
        'filter_fields': filter_fields,
        'dataset_name': dataset,
        'secured': True,
        'api_key': None,
        'additional_user': False,
    }
    kwargs.update(model_selection)
    kwargs = Namespace(**kwargs)
    response = cli(args=kwargs)

    assert_deployment_response(response)
    assert_deployment_queries(
        index_fields=index_fields,
        kwargs=kwargs,
        response=response,
        search_modality='text',
        dataset=dataset,
    )
    if query_fields == 'text':
        request_body = get_search_request_body(
            kwargs=kwargs,
            search_modality='text',
            dataset=dataset,
        )
        suggest_url = f'{response["host_http"]}/api/v1/search-app/suggestion'
        info_url = f'{response["host_http"]}/api/v1/search-app/'
        assert_info_endpoints(info_url, request_body)
        assert_suggest(suggest_url, request_body)

    assert_indexed_all_docs(
        response['host_http'], kwargs=kwargs, limit=MAX_DOCS_FOR_TESTING
    )
