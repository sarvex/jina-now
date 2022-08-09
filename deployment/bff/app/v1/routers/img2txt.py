from typing import List

from docarray import Document, DocumentArray
from fastapi import APIRouter

from deployment.bff.app.v1.models.image import NowImageSearchRequestModel
from deployment.bff.app.v1.models.text import (
    NowTextIndexRequestModel,
    NowTextResponseModel,
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
    summary='Add more text data to the indexer',
)
def index(data: NowTextIndexRequestModel):
    """
    Append the list of text to the indexer.
    """
    index_docs = DocumentArray()
    jwt = data.jwt
    for text, tags in zip(data.texts, data.tags):
        index_docs.append(Document(text=text, tags=tags))

    index_all_docs(get_jina_client(data.host, data.port), index_docs, jwt)


# Search
@router.post(
    "/search",
    response_model=List[NowTextResponseModel],
    summary='Search text data via image as query',
)
def search(data: NowImageSearchRequestModel):
    """
    Retrieve matching text for a given image query. Image query should be
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
