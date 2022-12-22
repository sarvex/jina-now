import base64
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
    for field_dict, tags_dict in data.data:
        doc = field_dict_to_doc(field_dict)
        doc.tags.update(tags_dict)
        index_docs.append(doc)

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
    query_doc = field_dict_to_doc(data.query)

    list_query_filter = {}
    for key, value in data.filters.items():
        list_query_filter.append({f'{key}': {'$eq': value}})
    query_filter = {'$and': list_query_filter}

    docs = jina_client_post(
        endpoint='/search',
        inputs=query_doc,
        parameters={'limit': data.limit, 'filter': query_filter},
        data=data,
    )
    matches = []
    for doc in docs[0].matches:
        # todo: use multimodal doc in the future
        scores = {}
        for score_name, named_score in doc.scores.items():
            scores[score_name] = named_score.to_dict()
        if doc.uri:
            result = {'uri': doc.uri}
        elif doc.blob:
            result = {'blob': base64.b64encode(doc.blob).decode('utf-8')}
        elif doc.text:
            result = {'text': doc.text}
        match = SearchResponseModel(
            id=doc.id,
            scores=scores,
            tags=doc.tags,
            fields={'result_field': result},
        )
        matches.append(match)
    return matches


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
