from typing import List

from docarray import Document
from fastapi import APIRouter
from pydantic import Field

from deployment.bff.app.client import jina_client_post
from deployment.bff.app.models import BaseRequestModel

router = APIRouter()


class UpdateEmailsRequestModel(BaseRequestModel):
    user_emails: List[str] = Field(
        default=...,
        description='List of user emails who are allowed to access the flow',
    )


class UpdateApiKeysRequestModel(BaseRequestModel):
    api_keys: List[str] = Field(
        default=...,
        description='List of api keys which allow to access the flow in an automated way',
    )


@router.post(
    "/updateUserEmails",
    summary='update user emails during runtime',
)
def update_user_email(data: UpdateEmailsRequestModel):
    """
    Update the list of emails for the security executor
    """
    jina_client_post(
        data=data,
        inputs=[Document()],
        endpoint='/admin/updateUserEmails',
        parameters={'user_emails': data.user_emails},
    )


@router.post(
    "/updateApiKeys",
    summary='update api keys during runtime',
)
def update_api_keys(data: UpdateApiKeysRequestModel):
    """
    Update the list of api keys for the security executor
    """
    jina_client_post(
        data=data,
        inputs=[Document()],
        endpoint='/admin/updateApiKeys',
        parameters={'api_keys': data.api_keys},
    )
