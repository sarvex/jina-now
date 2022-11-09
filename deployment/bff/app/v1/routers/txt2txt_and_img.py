import base64
from typing import List

from docarray import Document, DocumentArray
from fastapi import APIRouter

from deployment.bff.app.v1.models.text import NowTextSearchRequestModel
from deployment.bff.app.v1.models.text_and_image import (
    NowTextAndImageIndexRequestModel,
    NowTextAndImageResponseModel,
)
from deployment.bff.app.v1.routers.helper import jina_client_post, process_query

router = APIRouter()


# Index
@router.post(
    "/index",
    summary='Add more text and image data to the indexer',
)
def index(data: NowTextAndImageIndexRequestModel):
    """
    Append the list of image data to the indexer. Each image data should be
    `base64` encoded using human-readable characters - `utf-8`.
    """
    index_docs = DocumentArray()
    for text, image, uri, tags in zip(data.texts, data.images, data.uris, data.tags):
        if bool(image) + bool(uri) != 1:
            raise ValueError(
                f'Can only set one value but have image={image}, uri={uri}'
            )
        if text and image:
            base64_bytes = image.encode('utf-8')
            image = base64.decodebytes(base64_bytes)
            index_docs.append(
                Document(tags=tags, chunks=[Document(text=text), Document(image=image)])
            )
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
    response_model=List[NowTextAndImageResponseModel],
    summary='Search text and image data via text as query',
)
def search(data: NowTextSearchRequestModel):
    """
    Retrieve matching text and images for a given text as query.
    """
    query_doc, filter_query = process_query(
        text=data.text, uri=data.uri, conditions=data.filters
    )
    docs = jina_client_post(
        data=data,
        inputs=query_doc,
        parameters={'limit': data.limit, 'filter': filter_query, 'apply_bm25': True},
        endpoint='/search',
    )
    return docs[0].matches.to_dict()


@router.post(
    "/suggestion",
    summary='Get auto complete suggestion for query',
)
def suggestion(data: NowTextSearchRequestModel):
    """
    Return text suggestions for the rest of the query text
    """
    query_doc, filter_query = process_query(
        text=data.text, uri=data.uri, conditions=data.filters
    )

    docs = jina_client_post(
        data=data,
        inputs=query_doc,
        endpoint='/suggestion',
        parameters={'limit': data.limit, 'filter': filter_query, 'apply_bm25': True},
    )
    return docs.to_dict()
