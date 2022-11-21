import base64
from itertools import zip_longest
from typing import List

from docarray import Document, DocumentArray
from fastapi import APIRouter

from deployment.bff.app.v1.models.text_and_image import (
    NowTextAndImageIndexRequestModel,
    NowTextAndImageResponseModel,
    NowTextAndImageSearchRequestModel,
)
from deployment.bff.app.v1.routers.helper import jina_client_post, process_query

router = APIRouter()


# Index
@router.post(
    "/index",
    summary='Add more image or text data to the indexer',
)
def index(data: NowTextAndImageIndexRequestModel):
    """
    Append the list of image or text data to the indexer. Each image data should be
    `base64` encoded using human-readable characters - `utf-8`.
    """
    index_docs = DocumentArray()
    for text, image, uri, tags in zip_longest(
        data.texts, data.images, data.uris, data.tags
    ):
        if bool(image) + bool(uri) > 1:
            raise ValueError(
                f'Can only set one value but have image={image}, uri={uri}'
            )
        if bool(text) + bool(uri) > 1:
            raise ValueError(f'Can only set one value but have text={text}, uri={uri}')
        if text and image:
            raise ValueError(
                f'Can only set one value but have text={text}, image={image}'
            )
        if image:
            base64_bytes = image.encode('utf-8')
            image = base64.decodebytes(base64_bytes)
            index_docs.append(Document(blob=image, tags=tags))
        elif text:
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
    response_model=List[NowTextAndImageResponseModel],
    summary='Search image or text data via image or text as query',
)
def search(data: NowTextAndImageSearchRequestModel):
    """
    Retrieve matching images or texts for a given image or text query. Image query should be
    `base64` encoded using human-readable characters - `utf-8`.
    """
    # enable text search as well
    query_doc, filter_query = process_query(
        blob=data.image, text=data.text, uri=data.uri, conditions=data.filters
    )

    docs = jina_client_post(
        data=data,
        inputs=query_doc,
        parameters={'limit': data.limit, 'filter': filter_query},
        endpoint='/search',
    )

    return docs[0].matches.to_dict()
