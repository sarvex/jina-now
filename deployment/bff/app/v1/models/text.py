from typing import Optional

from pydantic import Field

from deployment.bff.app.v1.models.helper import BaseSearchRequestModel


class NowTextSearchRequestModel(BaseSearchRequestModel):
    text: Optional[str] = Field(default=None, description='Text query')
