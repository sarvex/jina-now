from typing import Dict, Optional

from pydantic import Field

from deployment.bff.app.models import _NamedScore


class ListEndpoint:
    """Endpoint to run search requests"""

    def base_request_model(self):
        class BaseSearchRequestModel:
            limit: int = Field(
                default=10, description='Number of matching results to return'
            )
            filters: Optional[Dict[str, str]] = Field(
                default=None,
                description='dictionary with filters for search results  {"tag_name" : "tag_value"}',
            )

        return BaseSearchRequestModel

    def base_response_model(self):
        class BaseSearchResponseModel:  # automatically inherits from NowBaseModel
            id: str = Field(
                default=..., nullable=False, description='Id of the matching result.'
            )
            scores: Optional[Dict[str, '_NamedScore']] = Field(
                description='Similarity score with respect to the query.'
            )

        return BaseSearchResponseModel

    def description(self):
        pass

    def has_tags(self, is_request):
        return False

    def has_modality(self, is_request):
        return False

    def is_list(self, is_request):
        return False
