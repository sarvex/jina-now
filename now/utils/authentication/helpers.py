import logging
import os
from dataclasses import dataclass

import boto3
import hubble
from hubble.payment.client import PaymentClient

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
    m2m_token = os.environ.get('M2M_TOKEN')
    if not m2m_token:
        raise ValueError(
            'M2M_TOKEN not set in the environment. Please set before running CLI'
        )
    logger.info(f'M2M_TOKEN: {m2m_token[:10]}...{m2m_token[-10:]}')
    logger.info(f'User token: {client.token[:10]}...{client.token[-10:]}')
    payment_client = PaymentClient(m2m_token=m2m_token)
    user_input.authorized_jwt = payment_client.get_authorized_jwt(
        user_token=client.token
    )['data']
    return response['data'], client.token
