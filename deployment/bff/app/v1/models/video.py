from typing import List, Optional

from pydantic import Field

from deployment.bff.app.v1.models.helper import (
    BaseIndexRequestModel,
    BaseSearchRequestModel,
    BaseSearchResponseModel,
)


# Request Model
class NowVideoIndexRequestModel(BaseIndexRequestModel):
    videos: Optional[List[str]] = Field(
        default=...,
        description='List of Videos to index. Videos should be base64encoded in `utf-8` format',
    )


class NowVideoSearchRequestModel(BaseSearchRequestModel):
    video: Optional[str] = Field(
        default=None,
        description='Video query. Video should be base64encoded in `utf-8` format',
    )


# Response Model
class NowVideoResponseModel(BaseSearchResponseModel):
    blob: Optional[str] = Field(
        description='Base64 encoded video in `utf-8` str format.'
    )


NowVideoResponseModel.update_forward_refs()
