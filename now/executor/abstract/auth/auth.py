import json
import os
from functools import lru_cache
from pprint import pprint
from typing import Dict, List

import hubble
from docarray import DocumentArray
from hubble.excepts import AuthenticationRequiredError
from jina import Executor, requests
from jina.enums import LogVerbosity
from jina.logging.logger import JinaLogger

from now.now_dataclasses import UserInput


class SecurityLevel:
    ADMIN = 1
    USER = 2


def secure_request(level: int, on: str = None):
    """decorator to check the authorization of the incoming request"""

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

            cls_instance = args[0]
            cls_instance.logger.info(f"responding to endpoint")
            if cls_instance.logger.logger.level <= LogVerbosity.DEBUG:
                pprint(kwargs)

            if 'docs' in kwargs and kwargs['docs']:
                cls_instance.logger.info(f"processing len({kwargs['docs']}) docs")
                if cls_instance.logger.logger.level <= LogVerbosity.DEBUG:
                    kwargs['docs'][0].summary()

            if 'docs_matrix' in kwargs and kwargs['docs_matrix']:
                cls_instance.logger.info(
                    f"processing len({kwargs['docs_matrix']}) docs_matrix"
                )

            if 'docs_map' in kwargs and kwargs['docs_map']:
                cls_instance.logger.info(
                    f"processing len({kwargs['docs_map']}) docs_map"
                )

            res = func(*args, **kwargs)

            cls_instance.logger.info(f"output len({res}) docs")
            if cls_instance.logger.logger.level <= LogVerbosity.DEBUG:
                res[0].summary()

            return res

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


def get_auth_executor_class():
    class NOWAuthExecutor(Executor):
        """
        NOWAuthExecutor performs the token check for authorization. It stores the owner ID belonging
        to the authorised user and also the `user_id` of the allowed users with access to the flow
        deployed by the user.

        If no `admin_emals`, `user_emails` and `api_keys` are provided, no checks will be performed.
        """

        def __init__(
            self,
            user_input_dict: Dict = {},
            admin_emails: List[str] = [],
            user_emails: List[str] = [],
            api_keys: List[str] = [],
            *args,
            **kwargs,
        ):
            """
            :param user_input_dict: kwargs to construct UserInput
            :param admin_email: ID of the user deploying this flow. ID is obtained from Hubble
            :param user_emails: Comma separated Email IDs of the allowed users with access to this flow.
                The Email ID from the incoming request to this flow will be verified against this.
            :param pats: List of PATs of the allowed users with access to this flow.
            """
            super().__init__(*args, **kwargs)
            self.logger = JinaLogger(self.__class__.__name__)
            self.admin_emails = admin_emails
            self.user_emails = user_emails
            self.api_keys = api_keys
            self._user = None

            # self.logger.debug("Initializing executor with args: " f"{args}" f"{kwargs}")

            self.user_input = UserInput()
            for attr_name, prev_value in self.user_input.__dict__.items():
                setattr(
                    self.user_input,
                    attr_name,
                    user_input_dict.get(attr_name, prev_value),
                )

            # TODO admin email must be persisted as well
            # TODO also, please remove duplicate code

            self.api_keys_path = (
                os.path.join(self.workspace, 'api_keys.json')
                if self.workspace
                else None
            )
            self.user_emails_path = (
                os.path.join(self.workspace, 'user_emails.json')
                if self.workspace
                else None
            )

            if self.api_keys_path and os.path.exists(self.api_keys_path):
                with open(self.api_keys_path, 'r') as fp:
                    self.api_keys = json.load(fp)
            if self.user_emails_path and os.path.exists(self.user_emails_path):
                with open(self.user_emails_path, 'r') as fp:
                    self.user_emails = json.load(fp)

        @secure_request(on='/admin/updateUserEmails', level=SecurityLevel.ADMIN)
        def update_user_emails(self, parameters: Dict = None, **kwargs):
            """
            Update the email addresses during runtime. That way, we don't have to restart it.
            """
            self.user_emails = parameters['user_emails']
            if self.user_emails_path:
                with open(self.user_emails_path, 'w') as fp:
                    json.dump(self.user_emails, fp)

        @secure_request(on='/admin/updateApiKeys', level=SecurityLevel.ADMIN)
        def update_api_keys(self, parameters: Dict = None, **kwargs):
            """
            Update the api keys during runtime. That way, we don't have to restart it.
            """
            self.api_keys = parameters['api_keys']
            if self.api_keys_path:
                with open(self.api_keys_path, 'w') as fp:
                    json.dump(self.api_keys, fp)

        @secure_request(level=SecurityLevel.USER)
        def check(self, docs: DocumentArray = None, **kwargs):
            """
            Check the authorization for each incoming request. The logic of the function is in the decorator.
            """
            return docs

    return NOWAuthExecutor
