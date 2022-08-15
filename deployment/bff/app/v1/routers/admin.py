from fastapi import APIRouter

from deployment.bff.app.v1.models.admin import UpdateEmailsRequestModel
from deployment.bff.app.v1.routers.helper import jina_client_post

router = APIRouter()


# Index
@router.post(
    "/admin/updateUserEmails",
    summary='update user emails during runtime',
)
def update_user_email(data: UpdateEmailsRequestModel):
    """
    Append the list of image data to the indexer. Each image data should be
    `base64` encoded using human-readable characters - `utf-8`.
    """
    jina_client_post(
        host=data.host,
        port=data.port,
        inputs=[],
        endpoint='/admin/updateEmails',
        parameters={'jwt': data.jwt},
        target_executor=r'\Asecurity_check\Z',
    )
