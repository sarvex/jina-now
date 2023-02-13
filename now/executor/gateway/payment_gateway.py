import logging
from typing import Callable, Optional, Tuple, Union

import requests
from fastapi import HTTPException, Request, status

from now.executor.gateway.base_payment_gateway import BasePaymentGateway
from now.executor.gateway.interceptor import PaymentInterceptor
from now.executor.gateway.security_wrapper import get_security_app

logger = logging.getLogger(__file__)


class SearchPaymentGateway(BasePaymentGateway):
    def __init__(
        self,
        backend_endpoint: str = 'https://api.clip.jina.ai',  # to change
        deployment_id: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        # Authentication options
        self._backend_endpoint = backend_endpoint
        self._deployment_id = deployment_id

    def extra_interceptors(self):
        return [
            SearchPaymentInterceptor(
                _backend_endpoint=self._backend_endpoint,
                internal_app_id=self._internal_app_id,
                internal_product_id=self._internal_product_id,
                deployment_id=self._deployment_id,
                usage_client_id=self._usage_client_id,
                usage_client_secret=self._usage_client_secret,
                logger=self.logger,
            )
        ]

    @property
    def app(self):
        from jina.helper import extend_rest_interface

        return extend_rest_interface(
            get_security_app(
                streamer=self.streamer,
                logger=self.logger,
                usage_client_id=self._usage_client_id,
                usage_client_secret=self._usage_client_secret,
                request_authenticate=self._get_request_authenticate(),
                report_usage=self._get_report_usage(),
            )
        )

    def _get_report_usage(self) -> Callable:
        def report_usage(
            current_user: dict,
            usage_client_id: str,
            usage_client_secret: str,
            usage_detail: dict,
        ):
            """Report usage to the backend"""

            try:
                usage_detail['current_user'] = current_user
                resp = requests.post(
                    f'{self.backend_endpoint}/api/v1/reports/',
                    auth=(usage_client_id, usage_client_secret),
                    json=usage_detail,
                )
                if resp.status_code != 200:
                    resp.raise_for_status()

            except Exception as ex:
                # TODO: handle the exception
                # catch all exceptions to avoid breaking the inference
                logger.error(f'Failed to report usage: {ex}')

        return report_usage

    def _get_request_authenticate(self):
        def request_authenticate(requests: Request):
            try:
                access_token = get_user_token(requests)
                return authenticate(
                    backend_endpoint=self._backend_endpoint,
                    access_token=access_token,
                    internal_app_id=self.internal_app_id,
                    internal_product_id=self.internal_product_id,
                    deployment_id=self.deployment_id,
                )
            except Exception as ex:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=str(ex),
                )

        return request_authenticate


class SearchPaymentInterceptor(PaymentInterceptor):
    def __init__(self, _backend_endpoint: str, **kwargs):
        super().__init__(**kwargs)
        self._backend_endpoint = _backend_endpoint

    # TODO: implement LRU cache
    def authenticate_and_authorize(
        self, handler_call_details
    ) -> Tuple[bool, Union[dict, str]]:
        metadata = handler_call_details.invocation_metadata
        metadata = {m.key: m.value for m in metadata}

        try:
            # reject if no authorization header
            if not metadata or 'authorization' not in metadata:
                raise Exception('No authorization header')

            access_token = metadata.get("authorization", "")

            return authenticate(
                backend_endpoint=self._backend_endpoint,
                access_token=access_token,
                internal_app_id=self._internal_app_id,
                internal_product_id=self._internal_product_id,
                deployment_id=self._deployment_id,
            )

        except HTTPException as ex:
            return False, ex.detail
        except Exception as ex:
            return False, str(ex)


def get_user_token(request: Request) -> str:
    """Get current user from Hubble API based on token.

    :param request: The request header sent along the request.
    :return: The extracted user token from request header.
    """
    cookie = request.cookies
    if cookie and 'st' in cookie:
        token = cookie.get('st')
    else:
        token = request.headers.get('Authorization')

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Empty authentication credentials',
        )
    token = token.replace('token ', '')
    return token  # noqa: E203


# change to use search authenticate

# TODO: implement LRU cache
def authenticate(
    backend_endpoint,
    access_token,
    internal_app_id: str = 'search-api',
    internal_product_id: str = 'free',
    deployment_id: Optional[str] = None,
) -> Tuple[bool, dict]:
    """Authenticate and authorize the request

    It combines the authentication and authorization logic.
      - Authentication: check if the request is authenticated
      - Authorization: check if the request is authorized to access the resource
    :param backend_endpoint: the auth provider used to authenticate the metadata in the request
    :param access_token: the access token in the request
    :param internal_app_id: the internal app id
    :param internal_product_id: the internal product id
    :param deployment_id: the deployment id
    :return: a tuple of (is_authenticated, authorized_user)
    """

    try:
        resp = requests.post(
            f'{backend_endpoint}/api/v1/admin/getStatus',
            headers={'Authorization': f'token {access_token}'},
            json={
                'internalAppId': internal_app_id,
                'internalProductId': internal_product_id,
                'deploymentId': deployment_id,
            },
        )

        if resp.ok:
            current_user = resp.json()['data']
            current_user['access_token'] = access_token
            return True, current_user
        else:
            raise HTTPException(
                status_code=resp.status_code, detail=resp.json()['detail']
            )

    except Exception as ex:
        raise
