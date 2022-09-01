from functools import lru_cache
from typing import Dict, List

import hubble
from docarray import DocumentArray
from hubble.excepts import AuthenticationRequiredError
from jina import Executor, requests


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

    user_info, _ = _get_token_detail(jwt['token'])
    for email in user_emails + admin_emails:
        # TODO hack - user_info['_id'] should not be used. Use email once hubble provides it
        if user_info['_id'] == email:
            return

        if 'identity' in jwt and jwt['identity']['userInfo']['email'] == email:
            if not jwt['identity']['userInfo']['email_verified']:
                raise PermissionError(
                    f'User {email} needs to be verified. Check if you received verification link.'
                )
            if level == SecurityLevel.ADMIN and email not in admin_emails:
                raise PermissionError(f'User {email} is not an admin.')
            return
    raise PermissionError(
        f'User {jwt["identity"]["userInfo"]["email"] if "identity" in jwt else user_info["_id"]} has no permission'
    )


@lru_cache(maxsize=128, typed=False)
def _get_token_detail(token):
    try:
        client = hubble.Client(token=token, max_retries=None, jsonify=True)
        user_info = client.get_user_info()
        pats = client.list_personal_access_tokens()
        if user_info['code'] != 200 or pats['code'] != 200:
            raise PermissionError(
                'Wrong token passed or the token is expired! Please re-login'
            )
        user_info = user_info['data']
        pats = pats['data']['personal_access_tokens']
    except AuthenticationRequiredError as e:
        raise PermissionError(
            'Token expired or invalid. Please use the updated token', e
        )
    return user_info, pats


class AuthExecutor2(Executor):
    """
    AuthExecutor2 performs the token check for authorization. It stores the owner ID belonging
    to the authorised user and also the `user_id` of the allowed users with access to the flow
    deployed by the user.
    """

    def __init__(
        self,
        admin_emails: List[str] = None,
        user_emails: List[str] = None,
        api_keys: List[str] = None,
        *args,
        **kwargs,
    ):
        """
        :param admin_email: ID of the user deploying this flow. ID is obtained from Hubble
        :param user_emails: Comma separated Email IDs of the allowed users with access to this flow.
            The Email ID from the incoming request to this flow will be verified against this.
        :param pats: List of PATs of the allowed users with access to this flow.
        """
        super().__init__(*args, **kwargs)

        self.admin_emails = admin_emails if admin_emails else []
        self.user_emails = user_emails if user_emails else []
        self.api_keys = api_keys if api_keys else []
        self._user = None

    @secure_request(on='/admin/updateUserEmails', level=SecurityLevel.ADMIN)
    def update_user_emails(self, parameters: Dict = None, **kwargs):
        """
        Update the email addresses during runtime. That way, we don't have to restart it.
        """
        self.user_emails = parameters['user_emails']

    @secure_request(on='/admin/updateApiKeys', level=SecurityLevel.ADMIN)
    def update_api_keys(self, parameters: Dict = None, **kwargs):
        """
        Update the api keys during runtime. That way, we don't have to restart it.
        """
        self.api_keys = parameters['api_keys']

    @secure_request(level=SecurityLevel.USER)
    def check(self, docs: DocumentArray, **kwargs):
        """
        Check the authorization for each incoming request. The logic of the function is in the decorator.
        """
        return docs
