from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

_ProtoValueType = Optional[Union[bool, float, str, list, dict]]
_StructValueType = Union[
    _ProtoValueType, List[_ProtoValueType], Dict[str, _ProtoValueType]
]


class _NamedScore(BaseModel):
    value: Optional[float] = None


class NowBaseModel(BaseModel):
    class Config:
        allow_mutation = False
        case_sensitive = False
        arbitrary_types_allowed = True


class BaseRequestModel(NowBaseModel):
    host: Optional[str] = Field(
        default='localhost', description='Host address returned by the flow deployment.'
    )
    port: Optional[int] = Field(default=31080, description='Port at which to connect.')
    jwt: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Dictionary with key 'token' which maps to token. To be passed when the flow is secure",
    )
    api_key: Optional[str] = Field(
        default=None,
        description='Used to authenticate machines',
    )


class BaseResponseModel(NowBaseModel):
    pass


# Index extending Base Request
class BaseIndexRequestModel(NowBaseModel):
    pass


class BaseIndexResponseModel(NowBaseModel):
    pass


# Search extending Base Request
class BaseSearchRequestModel(NowBaseModel):
    limit: int = Field(default=10, description='Number of matching results to return')
    filters: Optional[Dict[str, str]] = Field(
        default=None,
        description='dictionary with filters for search results  {"tag_name" : "tag_value"}',
    )


# Base Request for Search
class BaseSearchResponseModel(NowBaseModel):
    id: str = Field(
        default=..., nullable=False, description='Id of the matching result.'
    )
    scores: Optional[Dict[str, '_NamedScore']] = Field(
        description='Similarity score with respect to the query.'
    )


class TagsMixin(NowBaseModel):
    tags: Optional[Dict[str, str]] = Field(
        default={},
        description='Tags of the document.',
    )


class UriMixin(NowBaseModel):
    uri: Optional[str] = Field(
        default=None,
        description='URI of the document.',
    )
