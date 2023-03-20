from typing import Dict, List

from pydantic import BaseModel, Field

from now.executor.gateway.bff.app.v1.models.shared import BaseRequestModel


class FiltersResponseModel(BaseModel):
    filters: Dict[str, List] = Field(
        default={},
        description='Get all the filter fields and their possible values in the index',
        example={
            'price': [1, 2, 3],
            'color': [
                'red',
                'blue',
                'green',
            ],
        },
    )


class CountRequestModel(BaseRequestModel):
    limit: int = Field(default=10, description='Maximum count', example=10)


class CountResponseModel(BaseModel):
    number_of_docs: int = Field(
        default=0,
        description='Get the number of documents in the index',
        example=100,
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
