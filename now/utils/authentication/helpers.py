import logging
from dataclasses import dataclass

import boto3
import hubble

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class AWSProfile:
    aws_access_key_id: str
    aws_secret_access_key: str
    region: str


def get_aws_profile():
    session = boto3.Session()
    credentials = session.get_credentials()
    aws_profile = (
        AWSProfile(credentials.access_key, credentials.secret_key, session.region_name)
        if credentials
        else AWSProfile(None, None, session.region_name)
    )
    return aws_profile


@hubble.login_required
def jina_auth_login():
    pass


def get_info_hubble(user_input):
    client = hubble.Client(max_retries=None, jsonify=True)
    response = client.get_user_info()
    user_input.admin_emails = (
        [response['data']['email']] if 'email' in response['data'] else []
    )
    if not user_input.admin_emails:
        print(
            'Your hubble account is not verified. Please verify your account to deploy your flow as admin.'
        )
    user_input.jwt = {'token': client.token}
    user_input.admin_name = response['data']['name']
    return response['data'], client.token
