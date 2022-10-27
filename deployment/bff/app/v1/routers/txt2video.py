import base64
from typing import List

from docarray import Document, DocumentArray
from fastapi import APIRouter

from deployment.bff.app.v1.models.text import NowTextSearchRequestModel
from deployment.bff.app.v1.models.video import (
    NowVideoIndexRequestModel,
    NowVideoResponseModel,
)
from deployment.bff.app.v1.routers.helper import jina_client_post, process_query

router = APIRouter()


# Index
@router.post(
    "/index",
    summary='Add more video data to the indexer',
)
def index(data: NowVideoIndexRequestModel):
    """
    Append the list of video data to the indexer. Each video data should be
    `base64` encoded using human-readable characters - `utf-8`.
    """
    index_docs = DocumentArray()
    for video, uri, tags in zip(data.videos, data.uris, data.tags):
        if bool(video) + bool(uri) != 1:
            raise ValueError(
                f'Can only set one value but have video={video}, uri={uri}'
            )
        if video:
            base64_bytes = video.encode('utf-8')
            message = base64.decodebytes(base64_bytes)
            index_docs.append(Document(blob=message, tags=tags))
        else:
            index_docs.append(Document(uri=uri, tags=tags))

    # TODO: should use app.index_query_access_paths
    jina_client_post(
        data=data,
        inputs=index_docs,
        parameters={
            'traversal_paths': '@c',
            'access_paths': '@c',
        },
        endpoint='/index',
    )


# Search
@router.post(
    "/search",
    response_model=List[NowVideoResponseModel],
    summary='Search video data via text as query',
)
def search(data: NowTextSearchRequestModel):
    """
    Retrieve matching videos for a given text as query.
    """
    query_doc, filter_query = process_query(
        text=data.text, uri=data.uri, conditions=data.filters
    )

    # for video the search requests have to be on chunk-level
    docs = jina_client_post(
        data=data,
        inputs=Document(chunks=query_doc),
        parameters={
            'limit': data.limit,
            'filter': filter_query,
            'traversal_paths': '@c',
            'access_paths': '@c',
        },
        endpoint='/search',
    )

    return docs[0].matches[: data.limit].to_dict()
