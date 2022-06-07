import base64
from typing import List

from docarray import Document, DocumentArray
from fastapi import APIRouter, Depends

from deployment.bff.app.v1.dependencies.jina_client import get_jina_client
from deployment.bff.app.v1.models.image import (
    NowImageIndexRequestModel,
    NowImageResponseModel,
    NowImageSearchRequestModel,
)
from deployment.bff.app.v1.routers.helper import process_query

router = APIRouter()


# Index
@router.post(
    "/index",
    summary='Add more data to the indexer',
)
def index(data: NowImageIndexRequestModel, jina_client=Depends(get_jina_client)):
    """
    Append the list of image data to the indexer. Each image data should be
    `base64` encoded using human-readable characters - `utf-8`.
    """
    index_docs = DocumentArray()
    for image in data.images:
        base64_bytes = image.encode('utf-8')
        message = base64.decodebytes(base64_bytes)
        index_docs.append(Document(blob=message))

    jina_client.post('/index', index_docs)


# Search
@router.post(
    "/search",
    response_model=List[NowImageResponseModel],
    summary='Search image data via text or image as query',
)
def search(data: NowImageSearchRequestModel, jina_client=Depends(get_jina_client)):
    """
    Retrieve matching images for a given query. Image query should be `base64` encoded
    using human-readable characters - `utf-8`.
    """
    query_doc = process_query(data.text, data.image)
    docs = jina_client.post('/search', query_doc, parameters={"limit": data.limit})
    return docs[0].matches.to_dict()
