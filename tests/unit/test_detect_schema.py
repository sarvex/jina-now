import os

import pytest
from docarray.typing import Image, Text

from now.common.detect_schema import (
    _create_candidate_index_filter_fields,
    set_field_names_elasticsearch,
    set_field_names_from_docarray,
    set_field_names_from_local_folder,
    set_field_names_from_s3_bucket,
)
from now.constants import DatasetTypes
from now.now_dataclasses import UserInput


@pytest.mark.parametrize(
    'dataset_path, index_field_names, filter_field_names',
    [
        (
            'gif_resource_path',
            {'file.txt', 'file.gif', 'a1', 'a2'},
            {'file.txt', 'a1', 'a2'},
        ),
        ('image_resource_path', {'.jpg'}, set()),
    ],
)
def test_set_fields_names_from_local_folder(
    dataset_path, index_field_names, filter_field_names, request
):
    user_input = UserInput()
    user_input.dataset_path = request.getfixturevalue(dataset_path)

    set_field_names_from_local_folder(user_input)

    assert (
        set(user_input.index_field_candidates_to_modalities.keys()) == index_field_names
    )
    assert (
        set(user_input.filter_field_candidates_to_modalities.keys())
        == filter_field_names
    )


@pytest.mark.parametrize(
    'dataset_path, index_field_names, filter_field_names',
    [
        (
            '',
            {
                'id',
                'image.png',
                'link',
                'tags__colors__name',
                'tags__colors__slug',
                'tags__custom__name',
                'tags__custom__slug',
                'tags__ml__name',
                'tags__ml__slug',
                'test.txt',
                'title',
            },
            {
                'id',
                'tags__ml__slug',
                'title',
                'tags__colors__name',
                'tags__custom__name',
                'tags__colors__slug',
                'tags__ml__name',
                'link',
                'test.txt',
                'tags__custom__slug',
            },
        ),
        ('folder1/', {'.png', '.txt', '.json'}, {'.txt', '.json'}),
    ],
)
def test_set_field_names_from_s3_bucket(
    dataset_path, index_field_names, filter_field_names, get_aws_info
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

    assert (
        set(user_input.index_field_candidates_to_modalities.keys()) == index_field_names
    )
    assert (
        set(user_input.filter_field_candidates_to_modalities.keys())
        == filter_field_names
    )


def test_set_field_names_from_docarray():
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.DOCARRAY
    # subset_laion dataset is not multi-modal
    user_input.dataset_name = 'best-artworks'
    user_input.user_name = 'team-now'
    user_input.jwt = {'token': os.environ['WOLF_TOKEN']}

    set_field_names_from_docarray(user_input)

    assert len(user_input.index_field_candidates_to_modalities.keys()) == 2
    assert set(user_input.filter_field_candidates_to_modalities.keys()) == {'label'}


def test_set_field_names_elasticsearch(setup_online_shop_db, es_connection_params):
    _, index_name = setup_online_shop_db
    connection_str, _ = es_connection_params
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.ELASTICSEARCH
    user_input.es_index_name = index_name
    user_input.es_host_name = connection_str

    set_field_names_elasticsearch(user_input)
    assert len(user_input.index_field_candidates_to_modalities.keys()) == 5
    assert user_input.index_field_candidates_to_modalities == {
        'title': Text,
        'text': Text,
        'url': Text,
        'product_id': Text,
        'id': Text,
    }
    assert len(user_input.filter_field_candidates_to_modalities.keys()) == 5
    assert user_input.filter_field_candidates_to_modalities == {
        'title': str,
        'text': str,
        'url': str,
        'product_id': str,
        'id': str,
    }


def test_failed_uni_modal_docarray():
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.DOCARRAY
    user_input.dataset_name = 'test_lj'
    user_input.user_name = 'team-now'
    user_input.jwt = {'token': os.environ['WOLF_TOKEN']}
    with pytest.raises(RuntimeError):
        set_field_names_from_docarray(user_input)


def test_create_candidate_index_fields():
    fields_to_modalities = {
        'image.png': 'image.png',
        'test.txt': 'test.txt',
        'tags': str,
        'id': str,
        'link': str,
        'title': str,
    }
    (
        index_field_candidates_to_modalities,
        filter_field_candidates_to_modalities,
    ) = _create_candidate_index_filter_fields(fields_to_modalities)

    assert len(index_field_candidates_to_modalities.keys()) == 6
    assert index_field_candidates_to_modalities['image.png'] == Image
    assert index_field_candidates_to_modalities['test.txt'] == Text

    assert len(filter_field_candidates_to_modalities.keys()) == 5
