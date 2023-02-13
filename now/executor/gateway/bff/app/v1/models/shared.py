from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, root_validator

from now.utils import get_chunk_by_field_name


class BaseRequestModel(BaseModel):
    jwt: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Dictionary with key 'token' which maps to Jina Cloud token value."
        " To be passed when the flow is secure",
        example={'token': '<your jina cloud token>'},
    )
    api_key: Optional[str] = Field(
        default=None,
        description='Used to authenticate machines',
        example='<your api key>',
    )

    class Config:
        allow_mutation = False
        case_sensitive = False
        arbitrary_types_allowed = True


class ModalityModel(BaseModel):
    uri: Optional[str] = Field(
        default=None,
        description='URI of the file or data URI',
        example='https://example.com/image.jpg',
    )
    text: Optional[str] = Field(default=None, description='Text', example='cute cats')
    blob: Optional[str] = Field(
        default=None, description='Base64 encoded `utf-8` str format', example='xxx'
    )

    @root_validator(pre=True)
    def validate_only_one_exists(cls, values):
        # Get the names of all fields that are set (i.e. have a non-None value)
        set_fields = [name for name, value in values.items() if value is not None]
        if len(set_fields) != 1:
            raise ValueError(f"Only one of {set_fields} can be set.")
        return values

    @property
    def content(self):
        for field_name in self.__fields_set__:
            field_value = get_chunk_by_field_name(self, field_name)
            if field_value:
                return field_value
