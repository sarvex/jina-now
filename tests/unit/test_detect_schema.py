import os

import pytest

from now.common.detect_schema import (
    set_field_names_from_docarray,
    set_field_names_from_local_folder,
    set_field_names_from_s3_bucket,
)
from now.now_dataclasses import UserInput


@pytest.fixture
def get_aws_info():
    dataset_path = os.environ.get('S3_SCHEMA_FOLDER_PATH')
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    region = 'eu-west-1'

    return dataset_path, aws_access_key_id, aws_secret_access_key, region


def test_set_field_names_from_local_folder(gif_resource_path):
    user_input = UserInput()
    user_input.dataset_path = gif_resource_path

    set_field_names_from_local_folder(user_input)

    assert len(user_input.field_names) == 4
    assert set(user_input.field_names) == {'file.txt', 'file.gif', 'a1', 'a2'}


def test_set_field_names_from_local_folder_all_files(image_resource_path):
    user_input = UserInput()
    user_input.dataset_path = image_resource_path

    set_field_names_from_local_folder(user_input)

    assert len(user_input.field_names) == 0


def test_set_field_names_from_s3_bucket(get_aws_info):
    user_input = UserInput()
    (
        user_input.dataset_path,
        user_input.aws_access_key_id,
        user_input.aws_secret_access_key,
        user_input.aws_region_name,
    ) = get_aws_info

    set_field_names_from_s3_bucket(user_input)

    assert len(user_input.field_names) == 6
    assert set(user_input.field_names) == {
        'image.png',
        'test.txt',
        'tags',
        'id',
        'link',
        'title',
    }


def test_set_field_names_from_s3_bucket_all_files(get_aws_info):
    user_input = UserInput()
    (
        user_input.dataset_path,
        user_input.aws_access_key_id,
        user_input.aws_secret_access_key,
        user_input.aws_region_name,
    ) = get_aws_info
    user_input.dataset_path = user_input.dataset_path + 'folder1/'
    set_field_names_from_s3_bucket(user_input)

    assert len(user_input.field_names) == 0


def test_set_field_names_from_docarray():
    user_input = UserInput()
    user_input.dataset_name = 'subset_laion'
    user_input.jwt = {'token': os.environ['WOLF_TOKEN']}

    set_field_names_from_docarray(user_input)

    assert len(user_input.field_names) == 8
    assert set(user_input.field_names) == {
        'text',
        'uri',
        'original_height',
        'similarity',
        'NSFW',
        'height',
        'original_width',
        'width',
    }
