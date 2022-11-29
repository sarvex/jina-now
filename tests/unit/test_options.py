import os

from build.lib.tests.unit.data_loading.test_data_loading import user_input

from now.common.options import _get_schema_local_folder, _get_schema_s3_bucket
from now.now_dataclasses import UserInput


def test_get_schema_local_folder(gif_resource_path):
    user_input = UserInput()
    user_input.dataset_path = gif_resource_path

    _get_schema_local_folder(user_input)

    assert len(user_input.field_names) == 3
    assert user_input.field_names == ['file.txt', 'file.gif', 'meta.json']


def test_get_schema_s3_bucket():
    user_input = UserInput()
    user_input.dataset_path = os.environ.get('S3_SCHEMA_FOLDER_PATH')
    user_input.aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    user_input.aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    user_input.aws_region_name = 'eu-west-1'

    _get_schema_s3_bucket(user_input)

    assert len(user_input.field_names) == 2
    assert user_input.field_names == ['image.png', 'test.txt']
