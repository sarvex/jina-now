import json
import os
from argparse import Namespace

import pytest
from tests.integration.remote.assertions import (
    assert_deployment_response,
    assert_search_custom_s3,
)

from now.cli import cli
from now.constants import DatasetTypes, Models


@pytest.mark.remote
@pytest.mark.parametrize('dataset', ['custom_s3_bucket'])
@pytest.mark.parametrize(
    'dataset_path,index_fields,filter_fields,mm_type,dataset_length',
    [
        (os.environ.get('S3_CUSTOM_DATA_PATH'), ['.jpeg'], [], False, 2),
        (os.environ.get('S3_CUSTOM_MM_DATA_PATH'), ['image.png'], ['title'], True, 10),
    ],
)
@pytest.mark.parametrize('query_fields', ['image'])
def test_backend_custom_data(
    start_bff,
    dataset: str,
    dataset_path: str,
    index_fields: list,
    filter_fields: list,
    mm_type: bool,
    dataset_length: int,
    query_fields: str,
    cleanup,
    with_hubble_login_patch,
):
    kwargs = {
        'now': 'start',
        'flow_name': 'nowapi',
        'dataset_type': DatasetTypes.S3_BUCKET,
        'dataset_path': dataset_path,
        'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY'),
        'aws_region_name': 'eu-west-1',
        'index_fields': index_fields,
        f'{index_fields[0]}_model': [Models.CLIP_MODEL],
        'filter_fields': filter_fields,
        'secured': False,
    }
    kwargs = Namespace(**kwargs)
    response = cli(args=kwargs)

    # Dump the flow details from response host to a tmp file for post cleanup
    flow_details = {'host': response['host']}
    with open(f'{cleanup}/flow_details.json', 'w') as f:
        json.dump(flow_details, f)

    assert_deployment_response(response)

    assert_search_custom_s3(
        host=response['host'],
        mm_type=mm_type,
        create_temp_link=False,
        dataset_length=dataset_length,
    )
    assert_search_custom_s3(
        host=response['host'],
        mm_type=mm_type,
        create_temp_link=True,
        dataset_length=dataset_length,
    )
