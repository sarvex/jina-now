from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

from deployment.bff.app.v1.models.helper import BaseRequestModel
from deployment.bff.app.v1.models.modality import ModalityModel

_ProtoValueType = Optional[Union[bool, float, str, list, dict]]
_StructValueType = Union[
    _ProtoValueType, List[_ProtoValueType], Dict[str, _ProtoValueType]
]


class _NamedScore(BaseModel):
    value: Optional[float] = None


class IndexRequestModel(BaseRequestModel):
    tags: List[Dict[str, Any]] = Field(
        default={}, description='List of tags of the documents to be indexed.'
    )
    data: List[Dict[str, ModalityModel]] = Field(
        default={},
        description='List of dictionaries where each dictionary maps the field name to its value. '
        'Each dictionary represents one multi-modal document.',
    )


class SearchRequestModel(BaseRequestModel):
    limit: int = Field(default=10, description='Number of matching results to return')
    filters: Optional[Dict[str, str]] = Field(
        default=None,
        description='dictionary with filters for search results  {"tag_name" : "tag_value"}',
    )
    data: List[Dict[str, ModalityModel]] = Field(
        default={}, description='Dictionary which maps the field name to its value. '
    )


class SearchResponseModel(BaseRequestModel):
    id: str = Field(
        default=..., nullable=False, description='Id of the matching result.'
    )
    scores: Optional[Dict[str, '_NamedScore']] = Field(
        description='Similarity score with respect to the query.'
    )
    tags: Optional[Dict[str, '_StructValueType']] = Field(
        description='Additional tags associated with the file.'
    )
    fields: List[Dict[str, ModalityModel]] = Field(
        default={}, description='Dictionary which maps the field name to its value. '
    )

    class Config:
        case_sensitive = False
        arbitrary_types_allowed = True


class SuggestionRequestModel(BaseRequestModel):
    text: Optional[str] = Field(default=None, description='Text')


IndexRequestModel.update_forward_refs()
SearchRequestModel.update_forward_refs()
SearchResponseModel.update_forward_refs()
