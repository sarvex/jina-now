import pytest
from docarray import field
from docarray.typing import Image, Text

from now.constants import DatasetTypes
from now.data_loading.create_dataclass import (
    create_annotations_and_class_attributes,
    create_blob_type,
    create_dataclass,
    create_dataclass_fields_file_mappings,
    create_local_text_type,
    create_s3_type,
)
from now.now_dataclasses import UserInput

S3Object_text, s3_setter_text, s3_getter_text = create_s3_type('Text')
S3Object_image, s3_setter_image, s3_getter_image = create_s3_type('Image')
ImageType, image_setter, image_getter = create_blob_type('Image')
LocalTextType, local_text_setter, local_text_getter = create_local_text_type()


@pytest.mark.parametrize(
    "fields, fields_modalities, expected_field_names_to_dataclass_fields",
    [
        (
            ['image.png', 'description.txt'],
            {'image.png': Image, 'description.txt': Text},
            {'image.png': 'image_0', 'description.txt': 'text_0'},
        ),
        (
            ['image1.png', 'image2.png'],
            {'image1.png': Image, 'image2.png': Image},
            {'image1.png': 'image_0', 'image2.png': 'image_1'},
        ),
    ],
)
def test_create_dataclass_fields_file_mappings(
    fields, fields_modalities, expected_field_names_to_dataclass_fields
):
    field_names_to_dataclass_fields = create_dataclass_fields_file_mappings(
        fields, fields_modalities
    )
    assert field_names_to_dataclass_fields == expected_field_names_to_dataclass_fields


@pytest.mark.parametrize(
    "fields,"
    "fields_modalities,"
    "field_names_to_dataclass_fields,"
    "dataset_type,"
    "expected_annotations,"
    "expected_class_attributes",
    [
        (
            ['image.png', 'description.txt'],
            {'image.png': Image, 'description.txt': Text},
            {'image.png': 'image_0', 'description.txt': 'text_0'},
            DatasetTypes.PATH,
            {'image_0': ImageType, 'text_0': LocalTextType},
            {
                'image_0': field(setter=image_setter, getter=image_getter, default=''),
                'text_0': field(
                    setter=local_text_setter, getter=local_text_getter, default=''
                ),
            },
        ),
        (
            ['image.png', 'description.txt'],
            {'image.png': Image, 'description.txt': Text},
            {'image.png': 'image_0', 'description.txt': 'text_0'},
            DatasetTypes.S3_BUCKET,
            {'image_0': S3Object_image, 'text_0': S3Object_text},
            {
                'image_0': field(
                    setter=s3_setter_image, getter=s3_getter_image, default=''
                ),
                'text_0': field(
                    setter=s3_setter_text, getter=s3_getter_text, default=''
                ),
            },
        ),
        (
            ['price', 'description'],
            {'price': float, 'description': str},
            {'price': 'filter_0', 'description': 'filter_1'},
            DatasetTypes.PATH,
            {'filter_0': float, 'filter_1': str},
            {'filter_0': None, 'filter_1': None},
        ),
    ],
)
def test_create_annotations_and_class_attributes(
    fields,
    fields_modalities,
    field_names_to_dataclass_fields,
    dataset_type,
    expected_annotations,
    expected_class_attributes,
):
    annotations, class_attributes = create_annotations_and_class_attributes(
        fields, fields_modalities, field_names_to_dataclass_fields, dataset_type
    )
    for key, value in expected_annotations.items():
        assert str(annotations[key]) == str(value)
    for key, value in expected_class_attributes.items():
        assert str(class_attributes[key]) == str(value)


@pytest.mark.parametrize(
    "dataset_type, index_fields, index_field_candidates_to_modalities, filter_fields, "
    "filter_field_candidates_to_modalities, expected_dataclass",
    [
        (
            DatasetTypes.PATH,
            ['image.png', 'description.txt'],
            {'image.png': Image, 'description.txt': Text},
            ['price', 'description'],
            {'price': float, 'description': str},
            {
                'image_0': Image,
                'text_0': Text,
                'filter_0': float,
                'filter_1': str,
            },
        ),
        (
            DatasetTypes.S3_BUCKET,
            ['image.png', 'description.txt'],
            {'image.png': Image, 'description.txt': Text},
            ['price', 'description'],
            {'price': float, 'description': str},
            {
                'image_0': S3Object_image,
                'text_0': S3Object_text,
            },
        ),
    ],
)
def test_create_dataclass(
    dataset_type,
    index_fields,
    index_field_candidates_to_modalities,
    filter_fields,
    filter_field_candidates_to_modalities,
    expected_dataclass,
):
    user_input = UserInput()
    user_input.dataset_type = dataset_type
    user_input.index_fields = index_fields
    user_input.index_field_candidates_to_modalities = (
        index_field_candidates_to_modalities
    )
    user_input.filter_fields = filter_fields
    user_input.filter_field_candidates_to_modalities = (
        filter_field_candidates_to_modalities
    )
    mm_doc, _ = create_dataclass(user_input=user_input)
    assert str(mm_doc) == "<class 'now.data_loading.create_dataclass.MMDoc'>"

    for key, value in mm_doc.__annotations__.items():
        assert str(value) == str(expected_dataclass[key])
