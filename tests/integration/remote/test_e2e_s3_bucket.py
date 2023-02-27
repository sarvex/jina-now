import json
import os
from argparse import Namespace

import pytest
from tests.integration.remote.assertions import (
    assert_deployment_response,
    assert_indexed_all_docs,
    assert_search_custom_s3,
)

from now.cli import cli
from now.constants import DatasetTypes, Models
from now.utils import get_aws_profile


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
    dataset: str,
    dataset_path: str,
    index_fields: list,
    filter_fields: list,
    mm_type: bool,
    dataset_length: int,
    query_fields: str,
    cleanup,
    random_flow_name,
    with_hubble_login_patch,
):
    aws_profile = get_aws_profile()
    kwargs = {
        'now': 'start',
        'flow_name': random_flow_name,
        'dataset_type': DatasetTypes.S3_BUCKET,
        'dataset_path': dataset_path,
        'aws_access_key_id': aws_profile.aws_access_key_id,
        'aws_secret_access_key': aws_profile.aws_secret_access_key,
        'aws_region_name': aws_profile.region,
        'index_fields': index_fields,
        f'{index_fields[0]}_model': [Models.CLIP_MODEL],
        'filter_fields': filter_fields,
        'secured': False,
    }
    kwargs = Namespace(**kwargs)
    response = cli(args=kwargs)

    # Dump the flow details from response host to a tmp file for post cleanup
    flow_details = {'host': response['host_http']}
    with open(f'{cleanup}/flow_details.json', 'w') as f:
        json.dump(flow_details, f)

    assert_deployment_response(response)

    assert_search_custom_s3(
        host=response['host_http'],
        mm_type=mm_type,
        create_temp_link=False,
        dataset_length=dataset_length,
    )
    assert_search_custom_s3(
        host=response['host_http'],
        mm_type=mm_type,
        create_temp_link=True,
        dataset_length=dataset_length,
    )
    assert_indexed_all_docs(flow_details['host'], kwargs=kwargs, limit=dataset_length)
