from typing import List, Optional

from pydantic import BaseModel, Field

from deployment.bff.app.v1.models.helper import (
    BaseIndexRequestModel,
    BaseSearchResponseModel,
)


# Request Model
class NowVideoIndexRequestModel(BaseIndexRequestModel):
    videos: Optional[List[str]] = Field(
        default=...,
        description='List of Videos to index. Videos should be base64encoded in `utf-8` format',
    )


# Response Model
class NowVideoResponseModel(BaseSearchResponseModel):
    blob: Optional[str] = Field(
        description='Base64 encoded video in `utf-8` str format.'
    )


class NowVideoListResponseModel(BaseModel):
    __root__: List[NowVideoResponseModel] = Field(description='list of video responses')


NowVideoResponseModel.update_forward_refs()
