import os

from now.common.detect_schema import _get_schema_local_folder, _get_schema_s3_bucket
from now.now_dataclasses import UserInput


def test_get_schema_local_folder(gif_resource_path):
    user_input = UserInput()
    user_input.dataset_path = gif_resource_path

    _get_schema_local_folder(user_input)

    assert len(user_input.field_names) == 4
    assert set(user_input.field_names) == {'file.txt', 'file.gif', 'a1', 'a2'}


def test_get_schema_local_folder_all_files(image_resource_path):
    user_input = UserInput()
    user_input.dataset_path = image_resource_path

    _get_schema_local_folder(user_input)

    assert len(user_input.field_names) == 0


def test_get_schema_s3_bucket():
    user_input = UserInput()
    user_input.dataset_path = os.environ.get('S3_SCHEMA_FOLDER_PATH')
    print(user_input.dataset_path)
    user_input.aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    user_input.aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    user_input.aws_region_name = 'eu-west-1'

    _get_schema_s3_bucket(user_input)

    assert len(user_input.field_names) == 6
    assert set(user_input.field_names) == {
        'image.png',
        'test.txt',
        'tags',
        'id',
        'link',
        'title',
    }


def test_get_schema_s3_bucket_all_files(image_resource_path):
    user_input = UserInput()
    user_input.dataset_path = os.environ.get('S3_SCHEMA_FOLDER_PATH') + 'folder1/'
    print(user_input.dataset_path)
    user_input.aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    user_input.aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    _get_schema_local_folder(user_input)

    assert len(user_input.field_names) == 0
