from typing import List

from docarray import Document, DocumentArray
from fastapi import APIRouter

from deployment.bff.app.v1.models.text import (
    NowTextIndexRequestModel,
    NowTextResponseModel,
    NowTextSearchRequestModel,
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
    Append the list of text data to the indexer.
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
    summary='Search text data via text as query',
)
def search(data: NowTextSearchRequestModel):
    """
    Retrieve matching text for a given text as query.
    """
    query_doc = process_query(text=data.text)
    jwt = data.jwt

    docs = search_doc(
        get_jina_client(data.host, data.port),
        query_doc,
        data.limit,
        jwt,
    )

    return docs[0].matches.to_dict()
