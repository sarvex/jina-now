import base64
from typing import List

from docarray import Document, DocumentArray
from fastapi import APIRouter

from deployment.bff.app.v1.models.music import (
    NowMusicIndexRequestModel,
    NowMusicResponseModel,
    NowMusicSearchRequestModel,
)
from deployment.bff.app.v1.routers.helper import jina_client_post, process_query

router = APIRouter()


@router.post(
    "/index",
    summary='Add more data to the indexer',
)
def index(data: NowMusicIndexRequestModel):
    """
    Append the list of songs to the indexer. Each song data request should be
    `base64` encoded using human-readable characters - `utf-8`.
    """
    index_docs = DocumentArray()
    for audio, uri, tags in zip(data.songs, data.uris, data.tags):
        if bool(audio) + bool(uri) != 1:
            raise ValueError(
                f'Can only set one value but have image={audio}, uri={uri}'
            )
        if audio:
            base64_bytes = audio.encode('utf-8')
            message = base64.decodebytes(base64_bytes)
            index_docs.append(Document(blob=message, tags=tags))
        else:
            index_docs.append(Document(tags=tags, uri=uri))

    jina_client_post(
        data=data,
        inputs=index_docs,
        parameters={},
        endpoint='/index',
    )


@router.post(
    "/search",
    response_model=List[NowMusicResponseModel],
    summary='Search music data via text or music as query',
)
def search(data: NowMusicSearchRequestModel):
    """
    Retrieve matching songs for a given query. Song query should be `base64` encoded
    using human-readable characters - `utf-8`. In the case of music, the docs are already the matches.
    """
    query_doc, filter_query = process_query(
        blob=data.song, uri=data.uri, conditions=data.filters
    )

    docs = jina_client_post(
        data=data,
        inputs=query_doc,
        parameters={'limit': data.limit, 'filter': filter_query},
        endpoint='/search',
    )

    return docs[0].matches.to_dict()
