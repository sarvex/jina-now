from concurrent.futures import Executor
from typing import Dict

import hubble
import pytest
from executor import AuthExecutor2
from src import security
from src.constants import SecurityLevel
from src.security import secure_request


@pytest.fixture
def admin_email():
    return ['florian.hoenicke@jina.ai']


@pytest.fixture
def mock_hubble():
    class MockedClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_user_info(self, *args, **kwargs):
            return {
                'code': 200,
                'data': {'email': 'florian.hoenicke@jina.ai', 'email_verified': True},
            }

        def list_personal_access_tokens(self, *args, **kwargs):
            return {'code': 200, 'data': {'personal_access_tokens': 'token'}}

    hubble.Client = MockedClient


@pytest.fixture
def owner_jwt():
    # User trying to access with their JWT
    return {
        'user': {
            '_id': '62723cc6a5d1c4b707b62693',
            'name': 'auth0-unified-dbd6ad61fc666b4f',
            'nickname': 'kalim.akram',
            'avatarUrl': 'https://lh3.googleusercontent.com/a-/AOh14Gh1L7-yKliXDVka7QknqMSOgg6aobLhRopiFMou=s96-c',
            'createdAt': '2022-05-04T08:43:50.867Z',
            'updatedAt': '2022-05-04T08:43:50.867Z',
        },
        'identity': {
            '_id': '62723cc6b2c5d89642f35410',
            'user': '62723cc6a5d1c4b707b62693',
            'provider': 'auth0-unified',
            'identifier': 'google-oauth2|110300548630292003811',
            'createdAt': '2022-05-04T08:43:50.870Z',
            'updatedAt': '2022-08-01T11:04:13.110Z',
            'scope': 'openid profile email',
            'scopes': ['openid', 'profile', 'email'],
            'userInfo': {
                'sub': 'google-oauth2|110300548630292003811',
                'name': 'Mohammad Kalim Akram',
                'given_name': 'Mohammad Kalim',
                'family_name': 'Akram',
                'nickname': 'kalim.akram',
                'picture': 'https://lh3.googleusercontent.com/a-/AFdZucryD7w1i-hDsTUzF99RAMlAyw-4cdGrUu7Y5BXP=s96-c',
                'email': 'kalim.akram@jina.ai',
                'email_verified': True,
                'locale': 'en',
                'updated_at': '2022-08-01T11:04:01.281Z',
                'givenName': 'Mohammad Kalim',
                'familyName': 'Akram',
                'updatedAt': '2022-08-01T11:04:01.281Z',
                'emailVerified': True,
            },
        },
        'token': '1e0b28a1ed48d24b62f0064512e80ecc:8ed2cd47af48260749ac64638a56a47503efa032',
    }


@pytest.fixture
def user_jwt():
    return {
        'user': {
            '_id': '62d6e7dca3e078bb7c9e9cf2',
            'name': 'auth0-unified-356c161beb2fa6c8',
            'nickname': 'makram93',
            'avatarUrl': 'https://avatars.githubusercontent.com/u/6537525?v=4',
            'createdAt': '2022-07-19T17:20:28.094Z',
            'updatedAt': '2022-07-19T17:20:28.094Z',
        },
        'identity': {
            '_id': '62d6e7dce8d34249a7c4059d',
            'user': '62d6e7dca3e078bb7c9e9cf2',
            'provider': 'auth0-unified',
            'identifier': 'github|6537525',
            'createdAt': '2022-07-19T17:20:28.096Z',
            'updatedAt': '2022-08-01T10:56:04.421Z',
            'scope': 'openid profile email',
            'scopes': ['openid', 'profile', 'email'],
            'userInfo': {
                'sub': 'github|6537525',
                'name': 'Mohammad Kalim Akram',
                'nickname': 'makram93',
                'picture': 'https://avatars.githubusercontent.com/u/6537525?v=4',
                'email': 'kalimakram@gmail.com',
                'email_verified': True,
                'updated_at': '2022-08-01T10:55:34.788Z',
                'updatedAt': '2022-08-01T10:55:34.788Z',
                'emailVerified': True,
            },
        },
        'token': '27983481608e63a2597cbe48321f88e8:196e94831f72ebd5092c18fc7c0a1fb43cc517eb',
    }


def test_endpoints_empty(admin_email):
    sec_exec = AuthExecutor2(admin_email)
    # without jwt
    with pytest.raises(PermissionError):
        sec_exec.check()


def test_endpoints_owner(admin_email, owner_jwt, mock_hubble):
    sec_exec = AuthExecutor2(admin_email)
    sec_exec.check(parameters={'jwt': owner_jwt})


def test_endpoints_user_ids(admin_email, user_jwt):
    sec_exec = AuthExecutor2(
        admin_email,
        user_emails=['kalim.akram@jina.ai', 'kalimakram@gmail.com', 'dummy@yahoo.com'],
    )
    sec_exec.check(parameters={'jwt': user_jwt})


def test_endpoints_wrong_user_ids(admin_email, user_jwt, mock_hubble):
    sec_exec = AuthExecutor2(admin_email=[], user_emails=['attacker@jina.ai'])
    with pytest.raises(PermissionError):
        sec_exec.check(parameters={'jwt': user_jwt})


def test_decorator(mock_hubble):
    class AuthExecutor2(Executor):
        admin_emails = ['florian.hoenicke@jina.ai']
        user_emails = []

        @secure_request(on='admin/setEmails', level=SecurityLevel.ADMIN)
        def set_emails(self, parameters: Dict = None, **kwargs):
            pass

    def patch(*args, **kwargs):
        return True

    security._check_if_user_exists_and_verified = patch
    executor = AuthExecutor2()
    executor.set_emails(parameters={'jwt': {'token': 1}})
