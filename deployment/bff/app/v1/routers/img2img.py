import base64
from typing import List

from docarray import Document, DocumentArray
from fastapi import APIRouter

from deployment.bff.app.v1.models.image import (
    NowImageIndexRequestModel,
    NowImageResponseModel,
    NowImageSearchRequestModel,
)
from deployment.bff.app.v1.routers.helper import (
    get_jina_client,
    index_all_docs,
    process_query,
    search_doc,
)

router = APIRouter()


# Index
@router.post(
    "/index",
    summary='Add more image data to the indexer',
)
def index(data: NowImageIndexRequestModel):
    """
    Append the list of image data to the indexer. Each image data should be
    `base64` encoded using human-readable characters - `utf-8`.
    """
    index_docs = DocumentArray()
    jwt = data.jwt
    for image, tags in zip(data.images, data.tags):
        base64_bytes = image.encode('utf-8')
        message = base64.decodebytes(base64_bytes)
        index_docs.append(Document(blob=message, tags=tags))

    index_all_docs(get_jina_client(data.host, data.port), index_docs, jwt)


# Search
@router.post(
    "/search",
    response_model=List[NowImageResponseModel],
    summary='Search image data via image as query',
)
def search(data: NowImageSearchRequestModel):
    """
    Retrieve matching images for a given image query. Image query should be
    `base64` encoded using human-readable characters - `utf-8`.
    """
    query_doc = process_query(blob=data.image)
    jwt = data.jwt

    docs = search_doc(
        get_jina_client(data.host, data.port),
        query_doc,
        data.limit,
        jwt,
    )

    return docs[0].matches.to_dict()
