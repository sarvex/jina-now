from typing import List

from pydantic import Field
from pydantic.main import BaseModel

from deployment.bff.app.v1.models.helper import BaseRequestModel


class CloudTempLinkRequestModel(BaseRequestModel):
    ids: List[str] = Field(
        default=...,
        description='IDs of documents for whom temporary links are created. Is used to later update the right docs.',
    )
    uris: List[str] = Field(
        default=..., description='List of cloud bucket URIs of files'
    )


class CloudTempLinkResponseModel(BaseModel):
    id: str = Field(
        default=...,
        description='ID of document for whom temporary links was created. Is used to later update the right doc.',
    )
    uri: str = Field(default=None, description='URI with temporary link to file')


CloudTempLinkResponseModel.update_forward_refs()
