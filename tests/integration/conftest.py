import json
import logging
import os
import tempfile
import time

import hubble
import pytest
from pytest_mock import MockerFixture

from now.data_loading.data_loading import _list_s3_file_paths
from now.deployment.deployment import terminate_wolf
from now.executor.preprocessor.s3_download import get_bucket
from now.utils import get_aws_profile, get_flow_id

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


class HubbleAuthPatch:
    @staticmethod
    async def login():
        pass

    @staticmethod
    def get_auth_token() -> str:
        token = os.environ.get('WOLF_TOKEN')
        if token:
            log.debug(f'Found token in env *** (Len={len(token)})')
            return token
        else:
            raise RuntimeError(
                'WOLF token not found in environment under key `WOLF_TOKEN`'
            )


@pytest.fixture
def with_hubble_login_patch(mocker: MockerFixture) -> None:
    # WOLF token is required for deployment, but not set locally (only in the CI)
    # If you are running this locally, the WOLF_TOKEN env variable will be set using hubble
    # Otherwise, it will be set in the CI.
    if 'WOLF_TOKEN' not in os.environ:
        hubble.login()
        os.environ['WOLF_TOKEN'] = hubble.Auth.get_auth_token()


@pytest.fixture()
def cleanup():
    with tempfile.TemporaryDirectory() as tmpdir:
        start = time.time()
        yield tmpdir
        print('start cleanup')
        try:
            with open(f'{tmpdir}/flow_details.json', 'r') as f:
                flow_details = json.load(f)
            if 'host' not in flow_details:
                print('nothing to clean up')
                return
            host = flow_details['host']
            flow_id = get_flow_id(host)
            terminate_wolf(flow_id)
        except Exception as e:
            print('no clean up')
            print(e)
            return
        print('cleaned up')
        now = time.time() - start
        mins = int(now / 60)
        secs = int(now % 60)
        print(50 * '#')
        print(f'Time taken to execute deployment: {mins}m {secs}s')
        print(50 * '#')


@pytest.fixture(scope='session')
def pulled_local_folder_data(tmpdir_factory):
    aws_profile = get_aws_profile()
    bucket = get_bucket(
        uri=os.environ.get('S3_CUSTOM_MM_DATA_PATH'),
        aws_access_key_id=aws_profile.aws_access_key_id,
        aws_secret_access_key=aws_profile.aws_secret_access_key,
        region_name=aws_profile.region,
    )
    folder_prefix = '/'.join(os.environ.get('S3_CUSTOM_MM_DATA_PATH').split('/')[3:])
    file_paths = _list_s3_file_paths(bucket, folder_prefix)
    temp_dir = str(tmpdir_factory.mktemp('local_folder_data'))
    for path in file_paths:
        local_path = os.path.join(temp_dir, path)
        if not os.path.exists(os.path.dirname(local_path)):
            os.makedirs(os.path.dirname(local_path))
        bucket.download_file(path, local_path)
    return os.path.join(temp_dir, folder_prefix)
