from functools import lru_cache

import hubble
from hubble.excepts import AuthenticationRequiredError
from jina import requests


class SecurityLevel:
    ADMIN = 1
    USER = 2


def secure_request(level: int, on: str = None):
    def decorator(func):
        @requests(on=on)
        def wrapper(*args, **kwargs):
            _check_user(
                kwargs,
                level,
                args[0].user_emails,
                args[0].admin_emails,
                args[0].api_keys,
            )
            return func(*args, **kwargs)

        return wrapper

    return decorator


def _check_user(kwargs, level, user_emails, admin_emails, api_keys):
    if user_emails == [] and admin_emails == [] and api_keys == []:
        return

    if 'parameters' not in kwargs or (
        'api_key' not in kwargs['parameters'] and 'jwt' not in kwargs['parameters']
    ):
        raise PermissionError(
            f'`jwt` or `api_key` needs to be part of the request parameters.'
        )

    if 'api_key' in kwargs['parameters']:
        if kwargs['parameters']['api_key'] in api_keys:
            return
        else:
            raise PermissionError(f'`api_key` is invalid')
    else:
        jwt = kwargs['parameters']['jwt']

    user_info = _get_user_info(jwt['token'])
    for email in admin_emails + user_emails + ['jina.ai']:
        if _valid_user(user_info.get('email'), email):
            if level == SecurityLevel.ADMIN and email not in admin_emails:
                raise PermissionError(f'User {email} is not an admin.')
            return
    raise PermissionError(
        f'User {user_info.get("email") or user_info["_id"]} has no permission'
    )


def _valid_user(user_email, allowed_email):
    if '@' not in allowed_email:
        return user_email.split('@')[1] == allowed_email
    else:
        return user_email == allowed_email


@lru_cache(maxsize=128, typed=False)
def _get_user_info(token):
    try:
        client = hubble.Client(token=token, max_retries=None, jsonify=True)
        user_info = client.get_user_info()
        if user_info['code'] != 200:
            raise PermissionError(
                'Wrong token passed or the token is expired! Please re-login'
            )
        return user_info['data']
    except AuthenticationRequiredError as e:
        raise PermissionError(
            'Token expired or invalid. Please use the updated token', e
        )
