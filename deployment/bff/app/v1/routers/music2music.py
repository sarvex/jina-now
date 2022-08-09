import base64
from typing import List

from docarray import Document, DocumentArray
from fastapi import APIRouter

from deployment.bff.app.v1.models.music import (
    NowMusicIndexRequestModel,
    NowMusicResponseModel,
    NowMusicSearchRequestModel,
)
from deployment.bff.app.v1.routers.helper import (
    get_jina_client,
    index_all_docs,
    process_query,
    search_doc,
)

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
    jwt = data.jwt
    for audio, tags in zip(data.songs, data.tags):
        base64_bytes = audio.encode('utf-8')
        message = base64.decodebytes(base64_bytes)
        index_docs.append(Document(blob=message, tags=tags))

    index_all_docs(get_jina_client(data.host, data.port), index_docs, jwt)


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
    query_doc = process_query(blob=data.song)
    jwt = data.jwt

    docs = search_doc(
        get_jina_client(data.host, data.port),
        query_doc,
        data.limit,
        jwt,
    )

    return docs.to_dict()
