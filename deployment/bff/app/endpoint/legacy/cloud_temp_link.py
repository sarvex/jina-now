from typing import List

from docarray import Document, DocumentArray
from fastapi import APIRouter
from pydantic import Field
from pydantic.main import BaseModel

from deployment.bff.app.client import jina_client_post
from deployment.bff.app.models import BaseRequestModel

router = APIRouter()


class CloudTempLinkRequestModel(BaseRequestModel):
    ids: List[str] = Field(
        default=...,
        description='IDs of documents for whom temporary links are created. Is used to later update the right docs.',
    )
    uris: List[str] = Field(
        default=..., description='List of cloud bucket URIs of files'
    )


class CloudTempLinkResponseModel(BaseModel):
    id: str = Field(
        default=...,
        description='ID of document for whom temporary links was created. Is used to later update the right doc.',
    )
    uri: str = Field(default=None, description='URI with temporary link to file')


CloudTempLinkResponseModel.update_forward_refs()


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
