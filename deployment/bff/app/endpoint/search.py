from typing import Any, Dict, Optional

from docarray import DocumentArray
from pydantic import BaseModel, Field

from deployment.bff.app.endpoint.base import BaseEndpoint
from deployment.bff.app.models import BaseRequestModel, _NamedScore


class SearchEndpoint(BaseEndpoint):
    """Endpoint to run search requests"""

    @property
    def name(self):
        return 'search'

    def base_request_model(self):
        class BaseSearchRequestModel(BaseRequestModel):
            limit: int = Field(
                default=10, description='Number of matching results to return'
            )
            filters: Optional[Dict[str, str]] = Field(
                default=None,
                description='dictionary with filters for search results  {"tag_name" : "tag_value"}',
            )

        return BaseSearchRequestModel

    def base_response_model(self):
        class BaseSearchResponseModel(
            BaseModel
        ):  # automatically inherits from NowBaseModel
            id: str = Field(
                default=..., nullable=False, description='Id of the matching result.'
            )
            scores: Optional[Dict[str, '_NamedScore']] = Field(
                description='Similarity score with respect to the query.'
            )

        return BaseSearchResponseModel

    def get_parameters(self, inputs):
        """This method is used to get the parameters for the request to Jina.
        It is called before the request is sent to Jina.
        """
        parameters = {}
        filter_parameters = get_filter(inputs['filters'])
        parameters.update(filter_parameters)
        parameters.update({'limit': inputs['limit']})
        return parameters

    def map_outputs(self, docs: DocumentArray) -> Any:
        return docs[0].matches.to_dict()

    def description(self, input_modality, output_modality):
        return (
            f'Endpoint to send search requests. '
            f'You can provide data of modality {input_modality} '
            f'and retrieve a list of outputs from {output_modality} modality.'
            f'You can also provide a filter conditions to only retrieve '
            f'certain documents.'
        )

    def has_tags(self, is_request):
        return not is_request

    def has_modality(self, is_request):
        return True

    def has_uri(self, is_request):
        return True

    def is_list(self, is_request):
        return not is_request

    def is_list_root(self, is_request):
        return True


def get_filter(conditions):
    filter_query = {}
    if conditions:
        filter_query = []
        # construct filtering query from dictionary
        for key, value in conditions.items():
            filter_query.append({f'{key}': {'$eq': value}})
        filter_query = {
            '$and': filter_query
        }  # different conditions are aggregated using and
    return {'filter': filter_query}
