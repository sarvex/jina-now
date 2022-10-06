from typing import List

from docarray import Document, DocumentArray
from fastapi import APIRouter

from deployment.bff.app.v1.models.text import (
    NowTextIndexRequestModel,
    NowTextResponseModel,
    NowTextSearchRequestModel,
)
from deployment.bff.app.v1.routers.helper import jina_client_post, process_query

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
    for text, uri, tags in zip(data.texts, data.uris, data.tags):
        if bool(text) + bool(uri) != 1:
            raise ValueError(f'Can only set one value but have text={text}, uri={uri}')
        if text:
            index_docs.append(Document(text=text, tags=tags))
        else:
            index_docs.append(Document(uri=uri, tags=tags))

    jina_client_post(
        data=data,
        inputs=index_docs,
        parameters={},
        endpoint='/index',
    )


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
    query_doc, filter_query = process_query(
        text=data.text, uri=data.uri, conditions=data.filters
    )

    docs = jina_client_post(
        data=data,
        inputs=query_doc,
        parameters={'limit': data.limit, 'filter': filter_query},
        endpoint='/search',
    )

    return docs[0].matches.to_dict()
