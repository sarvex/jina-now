import json
import re

import requests
from jina.serve.runtimes.gateway.http.models import JinaResponseModel
from now_common.options import _construct_app
from pydantic import parse_obj_as

from deployment.bff.app.v1.routers.helper import jina_client_post


class Client:
    """
    This is the jina NOW client which can be used to run requests against apps which are deployed using jcloud.
    """

    def __init__(self, jcloud_id, app, api_key):
        self.jcloud_id = jcloud_id
        self.app = app
        self.api_key = api_key

    def get_maps(self, app_instance, path):
        """
        checks if the specified path is matching one of the provided patterns
        and returns the related request and response mappings
        """
        for path_regex, (
            request_model,
            response_model,
            request_map,
            response_map,
        ) in app_instance.bff_mapping_fns.items():
            path_compiled_regex = re.compile(fr'{path_regex}')
            if path_compiled_regex.match(path):
                return request_model, response_model, request_map, response_map
        raise ValueError(
            f'the path does not match the following patterns: {[path for path, (_, _) in app_instance.bff_mapping_fns.items()]}'
        )

    def send_request_bff(self, endpoint: str, **kwargs):
        request_body = {
            "host": f'grpcs://nowapi-{self.jcloud_id}.wolf.jina.ai',
            "api_key": self.api_key,
            **kwargs,
        }
        response = requests.post(
            f'https://nowrun.jina.ai/api/v1/{endpoint}',
            json=request_body,
        )
        return response

    def send_request(self, endpoint: str, **kwargs):
        """
        Client to run requests against a deployed flow
        """

        app_instance = _construct_app(self.app)
        request_model, response_model, request_map, response_map = self.get_maps(
            app_instance, endpoint
        )
        app_request = request_model(
            host=f'grpcs://nowapi-{self.jcloud_id}.wolf.jina.ai',
            api_key=self.api_key,
            **kwargs,
        )
        jina_request = request_map(app_request)
        jina_response = jina_client_post(
            app_request,
            endpoint,
            jina_request.data,
            jina_request.parameters,
        )
        jina_response_model = JinaResponseModel()
        jina_response_model.data = jina_response
        app_response = response_map(app_request, jina_response_model)
        parsed_response = json.loads(parse_obj_as(response_model, app_response).json())
        if '__root__' in parsed_response:
            parsed_response = parsed_response['__root__']
        return parsed_response
