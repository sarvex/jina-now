from docarray import DocumentArray
from fastapi import HTTPException, status
from jina import Client
from jina.excepts import BadServer


def get_jina_client(host: str, port: int) -> Client:
    if 'wolf.jina.ai' in host or 'dev.jina.ai' in host:
        return Client(host=host)
    else:
        return Client(host=host, port=port)


def jina_client_post(
    data, endpoint: str, inputs, parameters=None, *args, **kwargs
) -> DocumentArray:
    """Posts to the endpoint of the Jina client.

    :param data: contains the request model of the flow
    :param endpoint: endpoint which shall be called, e.g. '/index' or '/search'
    :param inputs: document(s) which shall be passed in
    :param parameters: parameters to pass to the executors, e.g. jwt for securitization or limit for search
    :param args: any additional arguments passed to the `client.post` method
    :param kwargs: any additional keyword arguments passed to the `client.post` method
    :return: response of `client.post`
    """
    if parameters is None:
        parameters = {}
    client = get_jina_client(host=data.host, port=data.port)
    auth_dict = {}
    if data.api_key is not None:
        auth_dict['api_key'] = data.api_key
    if data.jwt is not None:
        auth_dict['jwt'] = data.jwt
    try:
        result = client.post(
            endpoint,
            inputs=inputs,
            parameters={**auth_dict, **parameters},
            *args,
            **kwargs,
        )
    except BadServer as e:
        if 'Not a valid user' in e.args[0].status.description:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail='You are not authorised to use this flow',
            )
        else:
            raise e
    return result
