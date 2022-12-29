import os

import pytest
from docarray.typing import Image, Text

from now.common.detect_schema import (
    _create_candidate_search_filter_fields,
    set_field_names_from_docarray,
    set_field_names_from_local_folder,
    set_field_names_from_s3_bucket,
)
from now.constants import DatasetTypes
from now.now_dataclasses import UserInput


@pytest.mark.parametrize(
    'dataset_path, search_field_names, filter_field_names',
    [
        ('gif_resource_path', {'file.txt', 'file.gif'}, {'file.txt', 'a1', 'a2'}),
        ('image_resource_path', {'.jpg'}, set()),
    ],
)
def test_set_fields_names_from_local_folder(
    dataset_path, search_field_names, filter_field_names, request
):
    user_input = UserInput()
    user_input.dataset_path = request.getfixturevalue(dataset_path)

    set_field_names_from_local_folder(user_input)

    assert set(user_input.candidate_search_mods.keys()) == search_field_names
    assert set(user_input.candidate_filter_mods.keys()) == filter_field_names


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
        ('folder1/', {'.png', '.txt'}, {'.txt', '.json'}),
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

    assert set(user_input.candidate_search_mods.keys()) == search_field_names
    assert set(user_input.candidate_filter_mods.keys()) == filter_field_names


def test_set_field_names_from_docarray():
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.DOCARRAY
    # subset_laion dataset is not multi-modal
    user_input.dataset_name = 'best-artworks'
    user_input.jwt = {'token': os.environ['WOLF_TOKEN']}

    set_field_names_from_docarray(user_input)

    assert len(user_input.candidate_search_mods.keys()) == 2
    assert set(user_input.candidate_filter_mods.keys()) == {
        'label',
        'image',
    }


def test_failed_uni_modal_docarray():
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.DOCARRAY
    user_input.dataset_name = 'test_lj'
    user_input.jwt = {'token': os.environ['WOLF_TOKEN']}
    with pytest.raises(RuntimeError):
        set_field_names_from_docarray(user_input)


def test_create_candidate_search_fields():
    fields_to_modalities = {
        'image.png': Image,
        'test.txt': Text,
        'tags': str,
        'id': str,
        'link': str,
        'title': str,
    }
    (
        search_fields_modalities,
        filter_fields_modalities,
    ) = _create_candidate_search_filter_fields(fields_to_modalities)

    assert len(search_fields_modalities.keys()) == 2
    assert search_fields_modalities['image.png'] == Image
    assert search_fields_modalities['test.txt'] == Text

    assert len(filter_fields_modalities.keys()) == 5
