import json

import pytest
from docarray import Document
from jina import Executor, Flow, requests


@pytest.fixture
def auth_executor(monkeypatch):
    from now.executor.abstract.auth.auth import get_auth_executor_class

    NOWAuthExecutor = get_auth_executor_class()
    return NOWAuthExecutor


def test_executor_persistence(tmpdir, auth_executor):

    api_key = 'my_key'
    e = auth_executor(api_keys=[api_key], metas={'workspace': tmpdir})
    e.update_user_emails(
        parameters={'api_key': api_key, 'user_emails': ['abc@test.ai']}
    )
    with open(e.user_emails_path, 'r') as fp:
        json.load(fp)
    e.update_api_keys(parameters={'api_key': api_key, 'api_keys': ['your_key']})
    with open(e.user_emails_path, 'r') as fp:
        json.load(fp)


class SecondExecutor(Executor):
    @requests
    def do_something(self, *args, **kwargs):
        print('do something')


def test_authorization_success_api_key(admin_email, auth_executor):
    print('start')
    # NOWAuthExecutor = get_auth_executor_class()
    with (
        Flow()
        .add(
            uses=auth_executor,
            uses_with={
                'admin_emails': [admin_email],
                'api_keys': ['valid_key'],
            },
        )
        .add(uses=SecondExecutor)
    ) as f:
        f.index(inputs=Document(text='abc'), parameters={'api_key': 'valid_key'})


def test_authorization_failed(admin_email, auth_executor):
    with (
        Flow()
        .add(
            uses=auth_executor,
            uses_with={'admin_emails': [admin_email]},
        )
        .add(uses=SecondExecutor)
    ) as f:
        with pytest.raises(Exception):
            f.index(
                inputs=Document(text='abc'),
                parameters={'jwt': {'token': 'invalid token of abc.def@jina.ai'}},
            )


def test_authorization_successful(admin_email, mock_hubble_admin_email, auth_executor):
    with (
        Flow()
        .add(
            uses=auth_executor,
            uses_with={'admin_emails': [admin_email]},
        )
        .add(uses=SecondExecutor)
    ) as f:
        f.index(
            inputs=Document(text='abc'),
            parameters={
                'jwt': {
                    "token": 'yet:another:admin:token',
                }
            },
        )


def test_authorization_success_domain_users(
    mock_hubble_domain_user_email, auth_executor
):
    with (
        Flow()
        .add(
            uses=auth_executor,
            uses_with={'user_emails': ['test.ai']},
        )
        .add(uses=SecondExecutor)
    ) as f:
        f.index(
            inputs=Document(text='abc'),
            parameters={
                'jwt': {
                    "token": 'another:test:ai:user:token',
                }
            },
        )


def test_authorization_success_jina_users(mock_hubble_user_email, auth_executor):
    with (
        Flow()
        .add(
            uses=auth_executor,
            uses_with={'admin_emails': ['test.ai']},
        )
        .add(uses=SecondExecutor)
    ) as f:
        f.index(
            inputs=Document(text='abc'),
            parameters={
                'jwt': {
                    "token": 'another:jina:ai:user:token',
                }
            },
        )


def test_authorization_failed_domain_users(
    mock_hubble_domain_user_email, auth_executor
):
    with (
        Flow()
        .add(
            uses=auth_executor,
            uses_with={'user_emails': ['hello.ai']},
        )
        .add(uses=SecondExecutor)
    ) as f:
        with pytest.raises(Exception):
            f.index(
                inputs=Document(text='abc'),
                parameters={
                    'jwt': {
                        "token": 'another:test:ai:user:token',
                    }
                },
            )


def test_authorization_failed_api_key(admin_email, auth_executor):
    with (
        Flow()
        .add(
            uses=auth_executor,
            uses_with={'admin_emails': [admin_email]},
        )
        .add(uses=SecondExecutor)
    ) as f:
        with pytest.raises(Exception):
            f.index(
                inputs=Document(text='abc'), parameters={'api_key': 'invalid api_key'}
            )
