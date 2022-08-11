import base64

from docarray import Document
from fastapi import HTTPException, status
from jina import Client
from jina.excepts import BadServer


def process_query(text: str = None, blob: str = None) -> Document:
    if text is None and blob is None:
        raise ValueError('Please pass the query to make a search')
    try:
        if text is not None:
            query_doc = Document(text=text, mime_type='text')
        else:
            base64_bytes = blob.encode('utf-8')
            message_bytes = base64.decodebytes(base64_bytes)
            query_doc = Document(blob=message_bytes, mime_type='image')
    except BaseException as e:
        raise HTTPException(
            status_code=500,
            detail=f'Not a correct encoded query. Please see the error stack for more information. \n{e}',
        )
    return query_doc


def get_jina_client(host: str, port: int) -> Client:
    if 'wolf.jina.ai' in host:
        return Client(host=host)
    else:
        return Client(host=host, port=port)


def index_all_docs(host: str, port: int, docs, jwt):
    send_request('index', host, port, docs, jwt)


def search_doc(host: str, port: int, doc, limit, jwt):
    return send_request('search', host, port, doc, jwt, {"limit": limit})


def send_request(on, host: str, port: int, docs, jwt, parameters=None):
    client = get_jina_client(host, port)
    if parameters is None:
        parameters = {}
    try:
        result = client.post(
            f'/{on}',
            docs,
            parameters={**parameters, 'jwt': jwt},
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
