from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, root_validator

from deployment.bff.app.v1.models.helper import BaseRequestModel


class NowBaseModalityModel(BaseModel):
    uri: Optional[str] = Field(
        default=None, description='URI of the file or data URI of the query'
    )

    @root_validator(pre=True)
    def validate_only_one_exists(cls, values):
        # Get the names of all fields that are set (i.e. have a non-None value)
        set_fields = [name for name, value in values.items() if value is not None]
        if len(set_fields) != 1:
            raise ValueError(f"Only one of {set_fields} can be set.")


class NowImageModel(NowBaseModalityModel):
    blob: Optional[str] = Field(
        default=None, description='Base64 encoded image in `utf-8` str format'
    )


class NowTextModel(NowBaseModalityModel):
    text: Optional[str] = Field(default=None, description='Plan text')


class NowVideoModel(NowBaseModalityModel):
    blob: Optional[str] = Field(
        default=None, description='Base64 encoded video in `utf-8` str format'
    )


class NowSearchIndexRequestModel(BaseRequestModel):
    tags: List[Dict[str, Any]] = Field(
        default={}, description='List of tags of the documents to be indexed.'
    )
    fields: List[Dict[str, Union[NowImageModel, NowTextModel, NowVideoModel]]] = Field(
        default={},
        description='List of dictionaries where each dictionary maps the field name to its value.',
    )
