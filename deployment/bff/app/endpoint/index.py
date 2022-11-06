from typing import Any

from docarray import DocumentArray

from deployment.bff.app.endpoint.base import BaseEndpoint


class IndexEndpoint(BaseEndpoint):
    """Endpoint to run search requests"""

    @property
    def name(self):
        return 'index'

    def request_modality(self, input_modality, output_modality):
        return output_modality

    def map_outputs(self, docs: DocumentArray) -> Any:
        return {}

    def description(self, input_modality, output_modality):
        return (
            f'Endpoint to index documents. '
            f'You can provide a list of documents of type {output_modality} including tags.'
            f'The tags can be used to filter the documents when you send a search request.'
        )

    def has_tags(self, is_request):
        return is_request

    def has_modality(self, is_request):
        return is_request

    def has_uri(self, is_request):
        return is_request

    def is_list(self, is_request):
        return is_request

    def is_list_root(self, is_request):
        return False
