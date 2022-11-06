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
