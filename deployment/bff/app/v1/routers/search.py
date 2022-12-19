from typing import List

from docarray import Document, DocumentArray
from fastapi import APIRouter

from deployment.bff.app.v1.models.search import (
    IndexRequestModel,
    SearchRequestModel,
    SearchResponseModel,
    SuggestionRequestModel,
)
from deployment.bff.app.v1.routers.helper import field_dict_to_doc, jina_client_post

router = APIRouter()


@router.post(
    "/index",
    summary='Add more data to the indexer',
)
def index(data: IndexRequestModel):
    index_docs = DocumentArray()
    for field_dict in data.data:
        index_docs.append(field_dict_to_doc(field_dict))

    jina_client_post(
        data=data,
        inputs=index_docs,
        endpoint='/index',
    )


@router.post(
    "/search",
    response_model=List[SearchResponseModel],
    summary='Search data via query',
)
def search(data: SearchRequestModel):
    query_doc = field_dict_to_doc(data.data)
    list_query_filter = {}
    for key, value in data.filters.items():
        list_query_filter.append({f'{key}': {'$eq': value}})
    query_filter = {
        '$and': list_query_filter
    }  # different conditions are aggregated using and

    docs = jina_client_post(
        endpoint='/search',
        inputs=query_doc,
        parameters={'limit': data.limit, 'filter': query_filter},
        data=data,
    )

    return docs[0].matches.to_dict()


@router.post(
    "/suggestion",
    summary='Get auto complete suggestion for query',
)
def suggestion(data: SuggestionRequestModel):
    suggest_doc = Document(text=data.text)
    docs = jina_client_post(
        endpoint='/suggestion',
        inputs=suggest_doc,
        data=data,
    )
    return docs.to_dict()
