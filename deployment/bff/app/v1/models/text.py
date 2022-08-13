from typing import List, Optional

from pydantic import Field

from deployment.bff.app.v1.models.helper import (
    BaseIndexRequestModel,
    BaseSearchRequestModel,
    BaseSearchResponseModel,
)


# Request Model
class NowTextIndexRequestModel(BaseIndexRequestModel):
    texts: Optional[List[str]] = Field(
        default=..., description='List of Texts to index.'
    )


class NowTextSearchRequestModel(BaseSearchRequestModel):
    text: Optional[str] = Field(default=None, description='Text query')


# Response Model
class NowTextResponseModel(BaseSearchResponseModel):
    text: Optional[str] = Field(description='Matching text result.', default='')


NowTextResponseModel.update_forward_refs()
