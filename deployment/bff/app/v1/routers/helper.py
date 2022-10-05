import base64

from docarray import Document, DocumentArray
from fastapi import HTTPException, status
from jina import Client
from jina.excepts import BadServer


def process_query(
    text: str = '', blob: str = b'', uri: str = None, conditions: dict = None
) -> Document:
    """
    Processes query image or text  into a document and prepares the filetring query
    for the results.
    Currently we support '$and' between different conditions means we return results
    that have all the conditions. Also we only support '$eq' opperand for tag
    which means a tag should be equal to an exact value.
    Same query is passed to either docarray and annlite indexers, in docarray
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
            f'Can only set one value but have text={text}, blob={blob}, uri={uri}'
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
    query = {}
    if conditions:
        filter_query = []
        # construct filtering query from dictionary
        for key, value in conditions.items():
            filter_query.append({f'{key}': {'$eq': value}})
        query = {'$and': filter_query}  # different conditions are aggregated using and
    return query_doc, query


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
