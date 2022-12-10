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
    'dataset_path, search_field_names, filter_field_names',
    [
        ('gif_resource_path', {'file.txt', 'file.gif'}, {'file.txt', 'a1', 'a2'}),
        ('image_resource_path', set(), set()),
    ],
)
def test_set_fields_names_from_local_folder(
    dataset_path, search_field_names, filter_field_names, request
):
    user_input = UserInput()
    user_input.dataset_path = request.getfixturevalue(dataset_path)

    set_field_names_from_local_folder(user_input)

    assert set(user_input.search_fields_modalities.keys()) == search_field_names
    assert set(user_input.filter_fields_modalities.keys()) == filter_field_names


@pytest.mark.parametrize(
    'dataset_path, search_field_names, filter_field_names',
    [
        (
            '',
            {
                'image.png',
                'test.txt',
            },
            {'test.txt', 'tags', 'id', 'link', 'title'},
        ),
        ('folder1/', set(), set()),
    ],
)
def test_set_field_names_from_s3_bucket(
    dataset_path, search_field_names, filter_field_names, get_aws_info
):
    user_input = UserInput()
    (
        user_input.dataset_path,
        user_input.aws_access_key_id,
        user_input.aws_secret_access_key,
        user_input.aws_region_name,
    ) = get_aws_info
    user_input.dataset_path = user_input.dataset_path + dataset_path

    set_field_names_from_s3_bucket(user_input)

    assert set(user_input.search_fields_modalities.keys()) == search_field_names
    assert set(user_input.filter_fields_modalities.keys()) == filter_field_names


def test_set_field_names_from_docarray():
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.DOCARRAY
    user_input.dataset_name = 'subset_laion'
    user_input.jwt = {'token': os.environ['WOLF_TOKEN']}

    set_field_names_from_docarray(user_input)

    assert len(user_input.search_fields_modalities.keys()) == 8
    assert set(user_input.search_fields_modalities.keys()) == {
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
    fields_to_modalities = {
        'image.png': Modalities.IMAGE,
        'test.txt': Modalities.TEXT,
        'tags': 'str',
        'id': 'str',
        'link': 'str',
        'title': 'str',
    }
    (
        search_fields_modalities,
        filter_fields_modalities,
    ) = _create_candidate_search_filter_fields(fields_to_modalities)

    assert len(search_fields_modalities.keys()) == 2
    assert search_fields_modalities['image.png'] == Modalities.IMAGE
    assert search_fields_modalities['test.txt'] == Modalities.TEXT

    assert len(filter_fields_modalities.keys()) == 5
