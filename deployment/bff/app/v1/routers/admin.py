from docarray import Document
from fastapi import APIRouter

from deployment.bff.app.v1.models.admin import (
    UpdateApiKeysRequestModel,
    UpdateEmailsRequestModel,
)
from deployment.bff.app.v1.routers.helper import jina_client_post

router = APIRouter()


@router.post(
    "/updateUserEmails",
    summary='update user emails during runtime',
)
def update_user_email(data: UpdateEmailsRequestModel):
    """
    Update the list of emails for the security executor
    """
    jina_client_post(
        host=data.host,
        port=data.port,
        inputs=[Document()],
        endpoint='/admin/updateUserEmails',
        parameters={'jwt': data.jwt, 'user_emails': data.user_emails},
    )


@router.post(
    "/updateApiKeys",
    summary='update api keys during runtime',
)
def update_user_email(data: UpdateApiKeysRequestModel):
    """
    Update the list of api keys for the security executor
    """
    jina_client_post(
        host=data.host,
        port=data.port,
        inputs=[Document()],
        endpoint='/admin/updateApiKeys',
        parameters={'jwt': data.jwt, 'api_keys': data.api_keys},
    )
