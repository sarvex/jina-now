from typing import Any

from docarray import DocumentArray

from deployment.bff.app.models import BaseRequestModel, NowBaseModel


class BaseEndpoint:
    """All endpoints are derived from this base class.
    You can create a new Endpoint like Search, Index, Generate
    and have to override certain methods that will determine your request and response models"""

    @property
    def name(self):
        """The name of the endpoint."""
        raise NotImplementedError('Each endpoint needs a name')

    def request_modality(self, input_modality, output_modality):
        """The modality for the request model.
        Don't mix it with input_modality which is a term on application level
        and describes the data that is gone be indexed.
        """
        return input_modality

    def response_modality(self, input_modality, output_modality):
        """See request_modality"""
        return output_modality

    def get_parameters(self, inputs):
        """This method is used to get the parameters for the request to Jina.
        It is called before the request is sent to Jina.
        """
        return {}

    def map_outputs(self, docs: DocumentArray) -> Any:
        """This method is called after the response is received from Jina.
        It can be used to map the response to a different format.
        """
        raise NotImplementedError('Each endpoint needs a map_outputs method')

    def is_active(self, input_modality, output_modality):
        """If set active, the endpoint will be included.
        Otherwise it is not shown in the swagger ui
        and can not be used."""
        return True

    def base_request_model(
        self,
    ):  # TODO BaseRequestModel should be choosen outside of this class
        """It is used as basic model and is extended by the other methods."""
        return BaseRequestModel

    def base_response_model(self):  # TODO automatically inherits from NowBaseModel
        """It is used as basic model and is extended by the other methods."""
        return NowBaseModel

    def description(self, input_modality, output_modality):
        """The description of the endpoint."""
        pass

    def has_tags(self, is_request):
        """If has_tags is True, the response model will have a tags field."""
        return False

    def has_modality(self, is_request):
        """If has_modality is True, the response model will have a modality field."""
        return False

    def is_list(self, is_request):
        """If is_list is True, the response model will be wrapped as list."""
        return False

    def is_list_root(self, is_request):
        """If is_list is True, this method determines if the list is the root of the response model."""
        return False
