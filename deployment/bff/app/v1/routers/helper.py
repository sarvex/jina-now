from docarray import Document, DocumentArray
from fastapi import HTTPException, status
from jina import Client
from jina.excepts import BadServer, BadServerFlow

from now.utils import get_flow_id


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
