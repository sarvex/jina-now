import base64

from docarray import Document
from fastapi import HTTPException

from deployment.bff.app.v1.models.base import NoResponseModel
from deployment.bff.app.v1.models.model_factory import get_pydantic_model
from deployment.bff.app.v1.routers.helper import jina_client_post


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


def get_parameters(data, endpoint_name):
    parameters = {}
    if endpoint_name == 'search':
        filter_parameters = get_filter(data.filters)
        parameters.update(filter_parameters)
        parameters.update({'limit': data.limit})
    return parameters


def map_inputs(inputs, input_modality):
    return map_inputs_for_modality(**{input_modality: getattr(inputs, input_modality)})


def map_inputs_for_modality(
    text: str = '', blob: str = b'', uri: str = None
) -> Document:
    """
    Processes query image or text  into a document and prepares the filetring query
    for the results.
    Currently we support '$and' between different conditions means we return results
    that have all the conditions. Also we only support '$eq' opperand for tag
    which means a tag should be equal to an exact value.
    Same query is passed to indexers, in docarray
    executor we do preprocessing by adding tags__ to the query

    :param text: text of the query
    :param blob: the blob of the image
    :param uri: uri of the ressource provided
    :param conditions: dictionary with the conditions to apply as filter
        tag should be the key and desired value is assigned as value
        to the key
    """
    if bool(text) + bool(blob) + bool(uri) != 1:
        raise ValueError(
            f'Can only set one value but have text={text}, blob={blob[:100]}, uri={uri}'
        )
    try:
        if uri:
            query_doc = Document(uri=uri)
        elif text:
            query_doc = Document(text=text, mime_type='text')
        elif blob:
            base64_bytes = blob.encode('utf-8')
            message_bytes = base64.decodebytes(base64_bytes)
            query_doc = Document(blob=message_bytes, mime_type='image')
        else:
            raise ValueError('None of the attributes uri, text or blob is set.')
    except BaseException as e:
        raise HTTPException(
            status_code=500,
            detail=f'Not a correct encoded query. Please see the error stack for more information. \n{e}',
        )
    return query_doc


def map_outputs(response_docs, output_modality):
    if output_modality == 'text':
        return response_docs[0].text
    elif output_modality == 'image':
        return response_docs[0].blob
    else:
        raise ValueError(f'Unknown output modality {output_modality}')


def create_endpoint_function(
    RequestModel, ResponseModel, endpoint_name, input_modality, output_modality
):
    def endpoint(data: RequestModel) -> ResponseModel:
        parameters = get_parameters(data, endpoint_name)
        inputs = map_inputs(data, input_modality)
        response_docs = jina_client_post(
            data=data,
            inputs=inputs,
            parameters=parameters,
            endpoint=f'/{endpoint_name}',
        )
        return map_outputs(response_docs, output_modality)

    return endpoint


def create_endpoints(router, input_modality, output_modality):
    RequestModelIndex = get_pydantic_model(output_modality, is_request=True)
    RequestModelSearch = get_pydantic_model(input_modality, is_request=True)
    ResponseModelSearch = get_pydantic_model(output_modality, is_request=False)

    for RequestModel, ResponseModel, endpoint_name in [
        (RequestModelIndex, NoResponseModel, 'index'),
        (RequestModelSearch, ResponseModelSearch, 'search'),
    ]:
        endpoint = create_endpoint_function(RequestModel, ResponseModel, endpoint_name)
        router.add_api_route(
            f'/{endpoint_name}',
            endpoint,
            response_model=ResponseModel,
            methods=['POST'],
        )
