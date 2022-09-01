import pytest
from docarray import Document
from executor import AuthExecutor2
from jina import Executor, Flow, requests


class SecondExecutor(Executor):
    @requests
    def do_something(self, *args, **kwargs):
        print('do something')


@pytest.mark.parametrize(
    'params',
    [
        {'jwt': {'token': 'invalid token'}},
        # TODO: for some reason if we have more than one trial, the others get stuck
        # {'jwt': {}},
        # {},
    ],
)
def test_authorization_failed(params):
    with pytest.raises(Exception):
        with (
            Flow()
            .add(
                uses=AuthExecutor2,
                uses_with={'admin_emails': ['florian.hoenicke@jina.ai']},
            )
            .add(uses=SecondExecutor)
        ) as f:
            response = f.index(inputs=[Document(text='abc')], parameters=params)
            print(response)


def test_authorization_failed_api_key():
    with pytest.raises(Exception):
        with (
            Flow()
            .add(
                uses=AuthExecutor2,
                uses_with={'admin_emails': ['florian.hoenicke@jina.ai']},
            )
            .add(uses=SecondExecutor)
        ) as f:
            response = f.index(
                inputs=[Document(text='abc')], parameters={'api_key': 'invalid api_key'}
            )
            print(response)


def test_authorization_success_api_key():
    with (
        Flow()
        .add(
            uses=AuthExecutor2,
            uses_with={
                'admin_emails': ['florian.hoenicke@jina.ai'],
                'api_keys': ['valid_key'],
            },
        )
        .add(uses=SecondExecutor)
    ) as f:
        response = f.index(
            inputs=[Document(text='abc')], parameters={'api_key': 'valid_key'}
        )
        print(response)


@pytest.mark.parametrize(
    'params',
    [
        {
            'jwt': {
                "user": {
                    "_id": "62f4f82fa4f2c67fa31aa5c8",
                    "name": "auth0-unified-92c380ad6f0156d1",
                    "nickname": "florian.hoenicke+lottiefiles",
                    "avatarUrl": "https://s.gravatar.com/avatar/766362873373fce729070ac5f692c7e8?s=480&r=pg&d=https%3A%2F%2Fcdn.auth0.com%2Favatars%2Ffl.png",
                    "createdAt": "2022-08-11T12:38:07.164Z",
                    "updatedAt": "2022-08-11T12:38:07.164Z",
                },
                "identity": {
                    "_id": "62f4f82f52a59770892e8d05",
                    "user": "62f4f82fa4f2c67fa31aa5c8",
                    "provider": "auth0-unified",
                    "identifier": "auth0|62f4f776a8801679d7d6d7d8",
                    "createdAt": "2022-08-11T12:38:07.167Z",
                    "updatedAt": "2022-08-11T16:14:35.006Z",
                    "scope": "openid profile email",
                    "scopes": ["openid", "profile", "email"],
                    "userInfo": {
                        "sub": "auth0|62f4f776a8801679d7d6d7d8",
                        "name": "florian.hoenicke+lottiefiles@jina.ai",
                        "nickname": "florian.hoenicke+lottiefiles",
                        "picture": "https://s.gravatar.com/avatar/766362873373fce729070ac5f692c7e8?s=480&r=pg&d=https%3A%2F%2Fcdn.auth0.com%2Favatars%2Ffl.png",
                        "email": "florian.hoenicke+lottiefiles@jina.ai",
                        "email_verified": True,
                        "updated_at": "2022-08-11T16:14:15.894Z",
                        "updatedAt": "2022-08-11T16:14:15.894Z",
                        "emailVerified": True,
                    },
                },
                "token": "a863b5e90935e989e1dfd7f1208a5cad:7c59f539d833ef86b1a3725b8a315181540d60a2",
            }
        }
    ],
)
def test_authorization_successful(params):
    print('###start')

    with (
        Flow()
        .add(
            uses=AuthExecutor2,
            uses_with={'admin_emails': ['florian.hoenicke+lottiefiles@jina.ai']},
        )
        .add(uses=SecondExecutor)
    ) as f:
        response = f.index(inputs=[Document(text='abc')], parameters=params)
        print(response)
    print('###end')
