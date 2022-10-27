import base64
from typing import List

from docarray import Document, DocumentArray
from fastapi import APIRouter

from deployment.bff.app.v1.models.image import (
    NowImageIndexRequestModel,
    NowImageResponseModel,
)
from deployment.bff.app.v1.models.text import NowTextSearchRequestModel
from deployment.bff.app.v1.routers.helper import jina_client_post, process_query

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
    for image, uri, tags in zip(data.images, data.uris, data.tags):
        if bool(image) + bool(uri) != 1:
            raise ValueError(
                f'Can only set one value but have image={image}, uri={uri}'
            )
        if image:
            base64_bytes = image.encode('utf-8')
            image = base64.decodebytes(base64_bytes)
            index_docs.append(Document(blob=image, tags=tags))
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
    response_model=List[NowImageResponseModel],
    summary='Search image data via text as query',
)
def search(data: NowTextSearchRequestModel):
    """
    Retrieve matching images for a given text as query.
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
