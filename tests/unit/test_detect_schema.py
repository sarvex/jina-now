import os

import pytest

from now.common.detect_schema import (
    _create_candidate_search_filter_fields,
    set_field_names_from_docarray,
    set_field_names_from_local_folder,
    set_field_names_from_s3_bucket,
)
from now.constants import DatasetTypes, Modalities
from now.now_dataclasses import UserInput


@pytest.fixture
def get_aws_info():
    dataset_path = os.environ.get('S3_SCHEMA_FOLDER_PATH')
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    region = 'eu-west-1'

    return dataset_path, aws_access_key_id, aws_secret_access_key, region


@pytest.mark.parametrize(
    'dataset_path, field_names',
    [
        ('gif_resource_path', {'file.txt', 'file.gif', 'a1', 'a2'}),
        ('image_resource_path', set()),
    ],
)
def test_set_fields_names_from_local_folder(dataset_path, field_names, request):
    user_input = UserInput()
    user_input.dataset_path = request.getfixturevalue(dataset_path)

    set_field_names_from_local_folder(user_input)

    assert set(user_input.field_names) == field_names


@pytest.mark.parametrize(
    'dataset_path, field_names',
    [
        (
            '',
            {
                'image.png',
                'test.txt',
                'tags',
                'id',
                'link',
                'title',
            },
        ),
        ('folder1/', set()),
    ],
)
def test_set_field_names_from_s3_bucket(dataset_path, field_names, get_aws_info):
    user_input = UserInput()
    (
        user_input.dataset_path,
        user_input.aws_access_key_id,
        user_input.aws_secret_access_key,
        user_input.aws_region_name,
    ) = get_aws_info
    user_input.dataset_path = user_input.dataset_path + dataset_path

    set_field_names_from_s3_bucket(user_input)

    assert set(user_input.field_names) == field_names


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


def test_create_candidate_search_fields():

    (
        search_fields_modalities,
        search_fields_candidates,
        filter_fields_candidates,
    ) = _create_candidate_search_filter_fields(
        DatasetTypes.S3_BUCKET, ['image.png', 'test.txt', 'tags', 'id', 'link', 'title']
    )

    assert len(search_fields_candidates) == 2
    assert len(search_fields_modalities.keys()) == 2
    assert search_fields_modalities['image.png'] == Modalities.IMAGE
    assert search_fields_modalities['test.txt'] == Modalities.TEXT

    assert len(filter_fields_candidates) == 5
