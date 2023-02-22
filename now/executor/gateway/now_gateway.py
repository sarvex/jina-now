import copy
import json
import logging
import os
from time import sleep
from typing import Any, Callable, Dict, List, Tuple, Union

import hubble
from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse
from hubble.excepts import (
    AuthenticationFailedError,
    AuthenticationRequiredError,
    RequestedEntityNotFoundError,
)
from hubble.payment.client import PaymentClient
from jina.enums import GatewayProtocolType

from now.constants import NOWGATEWAY_BFF_PORT
from now.deployment.deployment import cmd
from now.executor.abstract.auth.auth import SecurityLevel, check_user
from now.executor.gateway.base_gateway.base_payment_gateway import BasePaymentGateway
from now.executor.gateway.base_gateway.interceptor import PaymentInterceptor
from now.executor.gateway.bff_gateway import BFFGateway
from now.executor.gateway.playground_gateway import PlaygroundGateway
from now.now_dataclasses import UserInput

logger = logging.getLogger(__file__)
cur_dir = os.path.dirname(__file__)

ENTERPRISE_USERS = [
    'aziz.belaweid@jina.ai',
    'florian.hoenicke@jina.ai',
    'kalim.akram@jina.ai',
    'joschka.braun@jina.ai',
    'team-now@jina.ai',
    'isabelle.mohr@jina.ai',
    'saba.saturua@jina.ai',
    'iyadh.khalfallah@jina.ai',
]
PROFESSIONAL_USERS = []


user_input_now_gateway = UserInput()


class NOWGateway(BasePaymentGateway):
    """The NOWGateway assumes that the gateway has been started with http on port 8081 and grpc on port 8085.
    This is the port on which the nginx process listens. After nginx has been started,
    it will start the playground on port 8501 and the BFF on port 8080. The actual
    HTTP gateway will start on port 8082.
    Nginx is configured to route the requests in the following way:
    - /playground -> playground on port 8501
    - /api -> BFF on port 8080
    - / -> HTTP gateway on port 8082
    No rerouting is done for the grpc gateway.
    """

    def __init__(
        self,
        user_input_dict: Dict,
        m2m_token: str,
        internal_app_id: str = 'search',
        internal_product_id: str = 'free-plan',
        **kwargs,
    ):
        # need to update port ot 8082, as nginx will listen on 8081
        http_idx = kwargs['runtime_args']['protocol'].index(GatewayProtocolType.HTTP)
        http_port = kwargs['runtime_args']['port'][http_idx]
        gprc_idx = kwargs['runtime_args']['protocol'].index(GatewayProtocolType.GRPC)
        grpc_port = kwargs['runtime_args']['port'][gprc_idx]
        if kwargs['runtime_args']['port'][http_idx] != 8081:
            raise ValueError(
                f'Please, let http port ({http_port}) be 8081 for nginx to work'
            )
        if grpc_port in [8080, 8081, 8082, 8501]:
            raise ValueError(
                f'Please, let grpc port ({grpc_port}) be different from 8080 (BFF), 8081 (nginx), 8082 (http) and '
                f'8501 (playground)'
            )
        kwargs['runtime_args']['port'][http_idx] = 8082
        super().__init__(
            internal_app_id=internal_app_id,
            internal_product_id=internal_product_id,
            **kwargs,
        )

        self.m2m_token = m2m_token
        self.user_input = UserInput()
        for attr_name, prev_value in user_input_dict.items():
            setattr(
                self.user_input,
                attr_name,
                user_input_dict.get(attr_name, prev_value),
            )
        global user_input_now_gateway
        user_input_now_gateway = self.user_input

        # we need to write the user input to a file so that the playground can read it; this is a workaround
        # for the fact that we cannot pass arguments to streamlit (StreamlitServer doesn't respect it)
        # we also need to do this for the BFF
        # save user_input to file in home directory of user
        with open(os.path.join(os.path.expanduser('~'), 'user_input.json'), 'w') as f:
            json.dump(self.user_input.__dict__, f)

        # remove potential clashing arguments from kwargs
        kwargs.pop("port", None)
        kwargs.pop("protocol", None)

        # note order is important
        self.bff_gateway = self._create_gateway(
            BFFGateway,
            NOWGATEWAY_BFF_PORT,
            **kwargs,
        )
        self.playground_gateway = self._create_gateway(
            PlaygroundGateway,
            8501,
            **kwargs,
        )

        self.setup_nginx()
        self.nginx_was_shutdown = False

    async def setup_server(self):
        # note order is important
        await self.playground_gateway.setup_server()
        await self.bff_gateway.setup_server()
        await super().setup_server()

    async def run_server(self):
        await self.playground_gateway.run_server()
        await self.bff_gateway.run_server()
        await super().run_server()

    async def shutdown(self):
        await self.playground_gateway.shutdown()
        await self.bff_gateway.shutdown()
        await super().shutdown()
        if not self.nginx_was_shutdown:
            self.shutdown_nginx()
            self.nginx_was_shutdown = True

    def setup_nginx(self):
        command = [
            'nginx',
            '-c',
            os.path.join(cur_dir, '', 'nginx.conf'),
        ]
        output, error = self._run_nginx_command(command)
        self.logger.info('Nginx started')
        self.logger.info(f'nginx output: {output}')
        self.logger.info(f'nginx error: {error}')

    def shutdown_nginx(self):
        command = ['nginx', '-s', 'stop']
        output, error = self._run_nginx_command(command)
        self.logger.info('Nginx stopped')
        self.logger.info(f'nginx output: {output}')
        self.logger.info(f'nginx error: {error}')

    def _run_nginx_command(self, command: List[str]) -> Tuple[bytes, bytes]:
        self.logger.info(f'Running command: {command}')
        output, error = cmd(command)
        if error != b'':
            # on CI we need to use sudo; using NOW_CI_RUN isn't good if running test locally
            self.logger.info(f'nginx error: {error}')
            command.insert(0, 'sudo')
            self.logger.info(f'So running command: {command}')
            output, error = cmd(command)
        sleep(10)
        return output, error

    def _create_gateway(self, gateway_cls, port, protocol='http', **kwargs):
        # ignore metrics_registry since it is not copyable
        runtime_args = self._deepcopy_with_ignore_attrs(
            self.runtime_args, ['metrics_registry']
        )
        runtime_args.port = [port]
        runtime_args.protocol = [protocol]
        gateway_kwargs = {k: v for k, v in kwargs.items() if k != 'runtime_args'}
        gateway_kwargs['runtime_args'] = dict(vars(runtime_args))
        gateway = gateway_cls(**gateway_kwargs)
        gateway.streamer = self.streamer
        return gateway

    def extra_interceptors(self) -> List[PaymentInterceptor]:
        return [
            SearchPaymentInterceptor(
                internal_app_id=self._internal_app_id,
                internal_product_id=self._internal_product_id,
                logger=self.logger,
                report_usage=self._get_report_usage(),
                m2m_token=self.m2m_token,
            )
        ]

    def _get_report_usage(self) -> Callable:
        # todo: report usage to hubble
        def report_usage(
            current_user: dict,
            usage_detail: dict,
        ):
            """Report usage to the backend"""

            try:
                usage_detail['current_user'] = current_user
                from hubble.payment.client import PaymentClient

                client = PaymentClient(
                    m2m_token=self.m2m_token,
                )
                resp = client.report_usage(
                    current_user['token'],
                    self._internal_app_id,
                    self._internal_product_id,
                    usage_detail['credits'],
                )
                if resp['code'] != 200:
                    raise Exception('Failed to credit user')

            except Exception as ex:
                # TODO: handle the exception
                # catch all exceptions to avoid breaking the inference
                logger.error(f'Failed to report usage: {ex}')

        return report_usage

    def _get_request_authenticate(self):
        def request_authenticate(request: Request):
            try:
                # todo: add authentication mechanism from now.executors.abstract.auth which is based
                # on parameters in body
                # parameters has been already extracted from the request body
                client = PaymentClient(
                    m2m_token=self.m2m_token,
                )
                current_user = get_current_user(request, client)
                if current_user is None:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail='User is not authenticated',
                    )
                remain_credits, has_payment_method = get_app_summary(
                    current_user, client
                )

                if remain_credits <= 0 and not has_payment_method:
                    return JSONResponse(
                        status_code=status.HTTP_403_FORBIDDEN,
                        content={
                            'message': 'User has reached quota limit, please upgrade subscription'
                        },
                    )

                return True, current_user
            except Exception as ex:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=str(ex),
                )

        return request_authenticate

    @staticmethod
    def _deepcopy_with_ignore_attrs(obj: Any, ignore_attrs: List[str]) -> Any:
        """Deep copy an object and ignore some attributes

        :param obj: the object to copy
        :param ignore_attrs: the attributes to ignore
        :return: the copied object
        """

        memo = {}
        for k in ignore_attrs:
            if hasattr(obj, k):
                memo[id(getattr(obj, k))] = None  # getattr(obj, k)

        return copy.deepcopy(obj, memo)


class SearchPaymentInterceptor(PaymentInterceptor):
    def authenticate_and_authorize(
        self, handler_call_details
    ) -> Tuple[bool, Union[dict, str]]:
        metadata = handler_call_details.invocation_metadata
        metadata = {m.key: m.value for m in metadata}

        # reject if no authorization header
        if not metadata or 'authorization' not in metadata:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail='User is not authenticated',
            )

        token = metadata.get("authorization", "")
        client = PaymentClient(
            m2m_token=self._m2m_token,
        )
        token = authenticate_user(token, client)
        current_user = get_user_info(token)
        current_user['token'] = token
        if current_user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail='User is not authenticated',
            )

        remain_credits, has_payment_method = get_app_summary(current_user, client)

        if remain_credits <= 0 and not has_payment_method:
            raise Exception('User has reached quota limit, please upgrade subscription')
        return True, current_user


def get_user_info(token: str) -> dict:
    """Get current user from Hubble API based on token.
       Cache will be used here.
    :param token: User token.
    :return: If user exist, return user id, else return `None`.
    """

    try:
        hubble_client = hubble.Client(token=token)
        resp = hubble_client.get_user_info(variant='data')
        return resp
    except (AuthenticationFailedError, AuthenticationRequiredError) as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=e.message,
        )
    except Exception as exc:
        logger.error(f'An error occurred while authenticating user: {str(exc)}')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail='Unknown error on the server-side',
        )


def get_current_user(request: Request, payment_client) -> dict:
    """Get current user from Hubble API based on token.

    :param request: The request header sent along the request.
    :param payment_client: The payment client.
    :return: If user exist, return a dict which contains user info
    """
    token = request.headers.get('authorization')
    token = authenticate_user(token, payment_client)
    resp = get_user_info(token)

    resp['token'] = token
    return resp


def authenticate_user(token, payment_client) -> str:
    """Get current user from Hubble API based on token.

    :param token: The token sent along the request.
    :param payment_client: The payment client.
    :return: The extracted user token from request header.
    """
    global user_input_now_gateway
    # put check and throw meaningful error here with an example of how to consume it

    if token.startswith('key '):
        token = token.replace('key ', '')
        check_user(
            {'parameters': {'api_key': token}},
            SecurityLevel.USER,
            user_input_now_gateway.user_emails,
            user_input_now_gateway.admin_emails,
            [user_input_now_gateway.api_key],
        )
        token = payment_client.get_authorized_jwt(
            user_token=user_input_now_gateway.jwt['token']
        )['data']
    else:
        if token.startswith('token '):
            token = token.replace('token ', '')
        check_user(
            {'parameters': {'jwt': {'token': token}}},
            SecurityLevel.USER,
            user_input_now_gateway.user_emails,
            user_input_now_gateway.admin_emails,
            [user_input_now_gateway.api_key],
        )

    return token  # noqa: E203


def get_app_summary(user: dict, payment_client):
    """Get the app summary of the user, including the subscription, usage, and the method to pay."""

    # default values for unexpected errors
    has_payment_method = False
    remain_credits = 100
    # hardcode the subscription type for now
    email = user.get('email', '')
    if email in ENTERPRISE_USERS + PROFESSIONAL_USERS:
        return (
            remain_credits,
            has_payment_method,
        )

    try:
        resp = payment_client.get_summary(token=user['token'], app_id='search-api')
        has_payment_method = resp['data'].get('hasPaymentMethod', False)
        remain_credits = resp['data'].get('credits', None)
        if remain_credits is None:
            remain_credits = 0.00001
        else:
            logger.error(f'Failed to get payment summary: {resp}')
    except RequestedEntityNotFoundError as ex:
        logger.warning(f'Failed to get app summary: {ex!r}')
    except Exception as e:
        logger.error(f'Failed to get app summary: {e!r}')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail='Unknown error on the server-side',
        )

    return remain_credits, has_payment_method
