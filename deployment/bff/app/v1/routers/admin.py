from fastapi import APIRouter

from deployment.bff.app.v1.models.admin import UpdateEmailsRequestModel
from deployment.bff.app.v1.routers.helper import send_request

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
    jwt = data.jwt
    send_request(
        'admin/updateEmails',
        data.host,
        data.port,
        [],
        jwt,
        parameters={'user_emails': data.user_emails},
    )
