from typing import Dict, List

from pydantic import BaseModel, Field


class TagsResponseModel(BaseModel):
    tags: Dict[str, List] = Field(
        default={},
        description='Get all tags and their possible values in the index',
        example={
            'tags__price': [1, 2, 3],
            'tags__color': [
                'red',
                'blue',
                'green',
            ],
        },
    )


class FieldNamesToDataclassFieldsResponseModel(BaseModel):
    field_names_to_dataclass_fields: Dict[str, str] = Field(
        default={},
        description='Dictionary which maps dataclass fields to their field names',
        example={'title': 'text_0', 'image': 'image_0'},
    )


class EncoderToDataclassFieldsModsResponseModel(BaseModel):
    encoder_to_dataclass_fields_mods: Dict[str, Dict[str, str]] = Field(
        default={},
        description='Dictionary which maps encoder names to the dataclass fields they encode and their modality',
        example={
            'encoderclip': {
                'text_0': 'text',
                'image_0': 'image',
            },
            'encodersbert': {
                'image_0': 'text',
            },
        },
    )
