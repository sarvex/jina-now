import json
from argparse import Namespace

import pytest
from tests.integration.remote.assertions import (
    assert_deployment_queries,
    assert_deployment_response,
    assert_suggest,
    get_search_request_body,
)

from now.cli import cli
from now.constants import DatasetTypes
from now.demo_data import DemoDatasetNames


@pytest.mark.remote
@pytest.mark.parametrize(
    'query_fields, index_fields, filter_fields, model_selection, dataset',
    [
        (
            'image',
            ['image', 'label'],
            [],
            {
                'model_image': 'encoderclip',
                'model_label': ['encoderclip', 'encodersbert'],
            },
            DemoDatasetNames.BIRD_SPECIES,
        ),
        (
            'image',
            ['image'],
            ['label'],
            ['encoderclip'],
            DemoDatasetNames.BIRD_SPECIES,
        ),
        (
            'text',
            ['lyrics'],
            [],
            ['encodersbert', 'encoderclip'],
            DemoDatasetNames.POP_LYRICS,
        ),
        (
            'text',
            ['video', 'description'],
            [],
            ['encoderclip'],
            DemoDatasetNames.TUMBLR_GIFS_10K,
        ),
        (
            'text',
            ['image'],
            ['label'],
            ['encoderclip'],
            DemoDatasetNames.BEST_ARTWORKS,
        ),
    ],
)
@pytest.mark.timeout(60 * 10)
def test_end_to_end(
    cleanup,
    start_bff,
    query_fields,
    index_fields,
    filter_fields,
    model_selection,
    dataset,
):
    kwargs = {
        'now': 'start',
        'flow_name': 'nowapi',
        'dataset_type': DatasetTypes.DEMO,
        'admin_name': 'team-now',
        'index_fields': index_fields,
        'filter_fields': filter_fields,
        'dataset_name': dataset,
        'secured': True,
        'api_key': None,
        'additional_user': False,
    }
    if isinstance(model_selection, dict):
        kwargs.update(model_selection)
    else:
        for index_field in index_fields:
            kwargs[f'{index_field}_model'] = model_selection
    kwargs = Namespace(**kwargs)
    response = cli(args=kwargs)
    # Dump the flow details from response host to a tmp file
    flow_details = {'host': response['host']}
    with open(f'{cleanup}/flow_details.json', 'w') as f:
        json.dump(flow_details, f)

    assert_deployment_response(response)
    assert_deployment_queries(
        kwargs=kwargs,
        response=response,
        search_modality='text',
        dataset=dataset,
    )
    if query_fields == 'text':
        host = response.get('host')
        request_body = get_search_request_body(
            kwargs=kwargs,
            host=host,
            search_modality='text',
            dataset=dataset,
        )
        suggest_url = f'http://localhost:8080/api/v1/search-app/suggestion'
        assert_suggest(suggest_url, request_body)
