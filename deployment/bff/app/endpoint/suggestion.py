from typing import Any

from docarray import DocumentArray

from deployment.bff.app.endpoint.base import BaseEndpoint


class SuggestionEndpoint(BaseEndpoint):
    """Endpoint to run search requests"""

    @property
    def name(self):
        return 'suggestion'

    def request_modality(self, input_modality, output_modality):
        return input_modality

    def response_modality(self, input_modality, output_modality):
        return input_modality

    def map_outputs(self, docs: DocumentArray) -> Any:
        return docs.to_dict()

    def is_active(self, input_modality, output_modality):
        return input_modality == 'text'

    def description(self, input_modality, output_modality):
        return f'Get auto complete suggestion for query.'

    def has_tags(self, is_request):
        return False

    def has_modality(self, is_request):
        return True

    def is_list(self, is_request):
        return not is_request

    def is_list_root(self, is_request):
        return True
