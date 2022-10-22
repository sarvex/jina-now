from typing import List, Optional

from pydantic import Field

from deployment.bff.app.v1.models.helper import (
    BaseIndexRequestModel,
    BaseSearchResponseModel,
)


# Request Model
class NowTextAndImageIndexRequestModel(BaseIndexRequestModel):
    texts: Optional[List[str]] = Field(
        default=..., description='List of Texts to index.'
    )
    images: Optional[str] = Field(
        default=None,
        description='Image query. Image should be base64encoded in `utf-8` format',
    )


# Response Model
class NowTextAndImageResponseModel(BaseSearchResponseModel):
    text: Optional[str] = Field(description='Matching text result.', default='')
    blob: Optional[str] = Field(
        description='Base64 encoded image in `utf-8` str format'
    )


NowTextAndImageResponseModel.update_forward_refs()
