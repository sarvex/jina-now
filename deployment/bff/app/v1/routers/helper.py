import base64
import itertools
import os
from copy import deepcopy
from tempfile import TemporaryDirectory
from typing import Dict, Type

import filetype
from docarray import Document, DocumentArray
from docarray.typing import Text
from fastapi import HTTPException, status
from jina import Client
from jina.excepts import BadServer, BadServerFlow

from now.constants import SUPPORTED_FILE_TYPES
from now.utils import get_flow_id


def field_dict_to_mm_doc(
    field_dict: Dict,
    data_class: Type,
    modalities_dict: Dict,
    field_names_to_dataclass_fields={},
) -> Document:
    """Converts a dictionary of field names to their values to a document.

    :param field_dict: key-value pairs of field names and their values
    :param data_class: @docarray.dataclass class which encapsulates the fields of the multimodal document
    :param modalities_dict: dictionary of field names to their modalities
    :param field_names_to_dataclass_fields: mapping of field names to data class fields (e.g. {'title': 'text_0'})
    :return: multi-modal document
    """

    with TemporaryDirectory() as tmp_dir:
        try:
            if field_names_to_dataclass_fields:
                field_dict_orig = deepcopy(field_dict)
                modalities_dict_orig = deepcopy(modalities_dict)
                field_dict = {
                    field_name_data_class: field_dict_orig[file_name]
                    for file_name, field_name_data_class in field_names_to_dataclass_fields.items()
                }
                modalities_dict = {
                    field_name_data_class: modalities_dict_orig[file_name]
                    for file_name, field_name_data_class in field_names_to_dataclass_fields.items()
                }
            data_class_kwargs = {}
            for field_name_data_class, field_value in field_dict.items():
                # save blob into a temporary file such that it can be loaded by the multimodal class
                if modalities_dict[
                    field_name_data_class
                ] != Text and not field_value.startswith('http'):
                    base64_decoded = base64.b64decode(field_value.encode('utf-8'))
                    file_ending = filetype.guess(base64_decoded)
                    if not file_ending:
                        raise ValueError(
                            f'Could not guess file type of blob {field_value}. '
                            f'Please provide a valid file type.'
                        )
                    file_ending = file_ending.extension
                    if file_ending not in itertools.chain(
                        *SUPPORTED_FILE_TYPES.values()
                    ):
                        raise ValueError(
                            f'File type {file_ending} is not supported. '
                            f'Please provide a valid file type.'
                        )
                    file_path = os.path.join(
                        tmp_dir, field_name_data_class + '.' + file_ending
                    )
                    with open(file_path, 'wb') as f:
                        f.write(base64_decoded)
                    field_value = file_path
                if field_value:
                    data_class_kwargs[field_name_data_class] = field_value
                else:
                    raise ValueError(
                        f'Content of field {field_name_data_class} is None. '
                    )
            doc = Document(data_class(**data_class_kwargs))
        except BaseException as e:
            raise HTTPException(
                status_code=500,
                detail=f'Not a correctly encoded request. Please see the error stack for more information. \n{e}',
            )

    return doc


def get_jina_client(host: str, port: int) -> Client:
    if 'wolf.jina.ai' in host or 'dev.jina.ai' in host:
        return Client(host=host)
    else:
        return Client(host=host, port=port)


def jina_client_post(
    request_model,
    endpoint: str,
    inputs: Document,
    parameters=None,
    *args,
    **kwargs,
) -> DocumentArray:
    """Posts to the endpoint of the Jina client.

    :param request_model: contains the request model of the flow
    :param endpoint: endpoint which shall be called, e.g. '/index' or '/search'
    :param inputs: document(s) which shall be passed in
    :param parameters: parameters to pass to the executors, e.g. jwt for securitization or limit for search
    :param args: any additional arguments passed to the `client.post` method
    :param kwargs: any additional keyword arguments passed to the `client.post` method
    :return: response of `client.post`
    """
    if parameters is None:
        parameters = {}
    client = get_jina_client(host=request_model.host, port=request_model.port)
    auth_dict = {}
    if request_model.api_key is not None:
        auth_dict['api_key'] = request_model.api_key
    if request_model.jwt is not None:
        auth_dict['jwt'] = request_model.jwt
    try:
        result = client.post(
            endpoint,
            inputs=inputs,
            parameters={
                **auth_dict,
                **parameters,
                'access_paths': '@cc',
            },
            *args,
            **kwargs,
        )
    except (BadServer, BadServerFlow) as e:
        flow_id = get_flow_id(request_model.host)
        raise handle_exception(e, flow_id)
    return result


def handle_exception(e, flow_id):
    if isinstance(e, BadServer):
        if 'not a valid user' in e.args[0].status.description.lower():
            return HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail='You are not authorised to use this flow',
            )
        else:
            return e
    elif isinstance(e, BadServerFlow):
        if 'no route matched' in e.args[0].lower():
            return Exception(f'Flow with ID {flow_id} can not be found')
        else:
            return e
