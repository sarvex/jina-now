from typing import List, Optional

from pydantic import Field

from deployment.bff.app.v1.models.helper import (
    BaseIndexRequestModel,
    BaseSearchRequestModel,
    BaseSearchResponseModel,
)


# Request Model
class NowTextImageIndexRequestModel(BaseIndexRequestModel):
    texts: Optional[List[str]] = Field(
        default=[], description='List of Texts to index.'
    )
    images: Optional[List[str]] = Field(
        default=[],
        description='Image query. Image should be base64encoded in `utf-8` format',
    )


class NowTextImageSearchRequestModel(BaseSearchRequestModel):
    image: Optional[str] = Field(
        default=None,
        description='Image query. Image should be base64encoded in `utf-8` format',
    )
    text: Optional[str] = Field(default=None, description='Text query')


# Response Model
class NowTextImageResponseModel(BaseSearchResponseModel):
    text: Optional[str] = Field(description='Matching text result.', default='')
    blob: Optional[str] = Field(
        description='Base64 encoded image in `utf-8` str format'
    )


NowTextImageResponseModel.update_forward_refs()
