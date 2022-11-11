import json
import os
from typing import Dict, List

from docarray import DocumentArray
from jina.logging.logger import JinaLogger

from now.common.auth import SecurityLevel, secure_request


# TODO This is a workaround since core has currently limitations with inheritance
def get_auth_executor_class():
    from jina import Executor

    class NOWAuthExecutor(Executor):
        """
        NOWAuthExecutor performs the token check for authorization. It stores the owner ID belonging
        to the authorised user and also the `user_id` of the allowed users with access to the flow
        deployed by the user.

        If no `admin_emals`, `user_emails` and `api_keys` are provided, no checks will be performed.
        """

        def __init__(
            self,
            admin_emails: List[str] = [],
            user_emails: List[str] = [],
            api_keys: List[str] = [],
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
            self.logger = JinaLogger(self.__class__.__name__)
            self.admin_emails = admin_emails
            self.user_emails = user_emails
            self.api_keys = api_keys
            self._user = None

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
