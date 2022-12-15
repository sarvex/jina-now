import pytest
from docarray import field
from docarray.typing import Image, Text

from now.constants import DatasetTypes
from now.now_dataclasses import UserInput
from now.run_backend import (
    create_annotations_and_class_attributes,
    create_dataclass,
    create_s3_type,
)

S3Object, my_setter, my_getter = create_s3_type()


@pytest.mark.parametrize(
    "fields, fields_modalities, dataset_type, expected_annotations, expected_class_attributes",
    [
        (
            ['image.png', 'description.txt'],
            {'image.png': Image, 'description.txt': Text},
            DatasetTypes.PATH,
            {'image_png': Image, 'description_txt': Text},
            {'image_png': None, 'description_txt': None},
        ),
        (
            ['image.png', 'description.txt'],
            {'image.png': Image, 'description.txt': Text},
            DatasetTypes.S3_BUCKET,
            {'image_png': S3Object, 'description_txt': S3Object},
            {
                'image_png': field(setter=my_setter, getter=my_getter, default=''),
                'description_txt': field(
                    setter=my_setter, getter=my_getter, default=''
                ),
            },
        ),
        (
            ['price', 'description'],
            {'price': float, 'description': str},
            DatasetTypes.PATH,
            {'price': float, 'description': str},
            {'price': None, 'description': None},
        ),
    ],
)
def test_create_annotations_and_class_attributes(
    fields,
    fields_modalities,
    dataset_type,
    expected_annotations,
    expected_class_attributes,
):
    annotations, class_attributes = create_annotations_and_class_attributes(
        fields, fields_modalities, dataset_type
    )
    for key, value in expected_annotations.items():
        assert str(annotations[key]) == str(value)
    for key, value in expected_class_attributes.items():
        assert str(class_attributes[key]) == str(value)


@pytest.mark.parametrize(
    "dataset_type, search_fields, search_fields_modalities, filter_fields, filter_fields_modalities, expected_dataclass",
    [
        (
            DatasetTypes.PATH,
            ['image.png', 'description.txt'],
            {'image.png': Image, 'description.txt': Text},
            ['price', 'description'],
            {'price': float, 'description': str},
            {
                'image_png': Image,
                'description_txt': Text,
                'price': float,
                'description': str,
            },
        ),
        (
            DatasetTypes.S3_BUCKET,
            ['image.png', 'description.txt'],
            {'image.png': Image, 'description.txt': Text},
            ['price', 'description'],
            {'price': float, 'description': str},
            {
                'image_png': S3Object,
                'description_txt': S3Object,
                'price': S3Object,
                'description': S3Object,
                'json_s3': S3Object,
            },
        ),
    ],
)
def test_create_dataclass(
    dataset_type,
    search_fields,
    search_fields_modalities,
    filter_fields,
    filter_fields_modalities,
    expected_dataclass,
):
    user_input = UserInput()
    user_input.dataset_type = dataset_type
    user_input.search_fields = search_fields
    user_input.search_fields_modalities = search_fields_modalities
    user_input.filter_fields = filter_fields
    user_input.filter_fields_modalities = filter_fields_modalities
    mm_doc = create_dataclass(user_input)
    assert str(mm_doc) == "<class 'now.run_backend.MMDoc'>"

    for key, value in mm_doc.__annotations__.items():
        assert str(value) == str(expected_dataclass[key])
