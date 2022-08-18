from typing import List

from docarray import Document, DocumentArray
from fastapi import APIRouter

from deployment.bff.app.v1.models.cloud_temp_link import (
    CloudTempLinkRequestModel,
    CloudTempLinkResponseModel,
)
from deployment.bff.app.v1.routers.helper import jina_client_post

router = APIRouter()


@router.post(
    '/temp_link',
    response_model=List[CloudTempLinkResponseModel],
    summary='Downloads data from cloud storage bucket',
)
def temp_link(data: CloudTempLinkRequestModel):
    """Downloads files as defined in URI and returns them as blobs."""
    docs = []
    for id, uri in zip(data.ids, data.uris):
        docs.append(Document(id=id, uri=uri))

    docs = jina_client_post(
        data=data,
        inputs=DocumentArray(docs),
        endpoint='/temp_link_cloud_bucket',
        parameters={},
        target_executor=r'\Asecurity_check\Z|\Apreprocessor\Z',
    )

    return docs.to_dict()
