from typing import Optional

from pydantic import BaseModel, Field, root_validator


class ModalityModel(BaseModel):
    uri: Optional[str] = Field(default=None, description='URI of the file or data URI')
    text: Optional[str] = Field(default=None, description='Text')
    blob: Optional[str] = Field(
        default=None, description='Base64 encoded `utf-8` str format'
    )

    @root_validator(pre=True)
    def validate_only_one_exists(cls, values):
        # Get the names of all fields that are set (i.e. have a non-None value)
        set_fields = [name for name, value in values.items() if value is not None]
        if len(set_fields) != 1:
            raise ValueError(f"Only one of {set_fields} can be set.")
        return values
