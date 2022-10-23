from concurrent.futures import Executor
from typing import Dict

import pytest

from now.executor.abstract.auth import NOWAuthExecutor, SecurityLevel, secure_request


@pytest.fixture
def owner_jwt():
    return {
        'token': 'admin:token',
    }


@pytest.fixture
def user_jwt():
    return {'token': 'user:token'}


def test_endpoints_empty(admin_email):
    sec_exec = NOWAuthExecutor(admin_emails=[admin_email])
    # without jwt
    with pytest.raises(PermissionError):
        sec_exec.check()


def test_authorization_successful_admin_email(
    admin_email, owner_jwt, mock_hubble_admin_email
):
    sec_exec = NOWAuthExecutor(admin_emails=[admin_email])
    sec_exec.check(parameters={'jwt': owner_jwt})


def test_authorization_successful_user_email(
    admin_email, user_email, user_jwt, mock_hubble_user_email
):
    sec_exec = NOWAuthExecutor(
        admin_emails=[admin_email],
        user_emails=[user_email],
    )
    sec_exec.check(parameters={'jwt': user_jwt})


def test_authorization_failed_user_email(user_jwt, mock_hubble_domain_user_email):
    sec_exec = NOWAuthExecutor(admin_email=[], user_emails=['attacker@jina.ai'])
    with pytest.raises(PermissionError):
        sec_exec.check(parameters={'jwt': user_jwt})


def test_authorization_failed_api_key(admin_email):
    sec_exec = NOWAuthExecutor(admin_emails=[admin_email], api_keys=['valid_key'])
    with pytest.raises(PermissionError):
        sec_exec.check(parameters={'api_key': 'invalid api_key'})


def test_authorization_success_api_key(admin_email):
    sec_exec = NOWAuthExecutor(admin_emails=[admin_email], api_keys=['valid_key'])
    sec_exec.check(parameters={'api_key': 'valid_key'})


def test_decorator(admin_email, mock_hubble_admin_email):
    class AnotherAuthExecutor(Executor):
        admin_emails = [admin_email]
        user_emails = []
        api_keys = []

        @secure_request(on='admin/setEmails', level=SecurityLevel.ADMIN)
        def set_emails(self, parameters: Dict = None, **kwargs):
            pass

    executor = AnotherAuthExecutor()
    executor.set_emails(parameters={'jwt': {'token': 1}})
