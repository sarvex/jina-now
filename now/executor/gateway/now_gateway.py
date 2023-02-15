import copy
import json
import logging
import os
from time import sleep
from typing import Any, Callable, Dict, List, Tuple, Union

from fastapi import HTTPException, Request, status
from jina.enums import GatewayProtocolType

logger = logging.getLogger(__file__)
from now.constants import NOWGATEWAY_BFF_PORT
from now.deployment.deployment import cmd
from now.executor.abstract.auth.auth import check_user
from now.executor.gateway.base_payment_gateway import BasePaymentGateway
from now.executor.gateway.bff_gateway import BFFGateway
from now.executor.gateway.interceptor import PaymentInterceptor
from now.executor.gateway.playground_gateway import PlaygroundGateway
from now.now_dataclasses import UserInput

cur_dir = os.path.dirname(__file__)


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
        user_input_dict: Dict = {},
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
                f'Please, let grpc port ({grpc_port}) be different from 8080 (BFF), 8081 (ngix), 8082 (http) and 8501 (playground)'
            )
        kwargs['runtime_args']['port'][http_idx] = 8082
        super().__init__(
            internal_app_id=internal_app_id,
            internal_product_id=internal_product_id,
            **kwargs,
        )

        self.user_input = UserInput()
        for attr_name, prev_value in self.user_input.__dict__.items():
            setattr(
                self.user_input,
                attr_name,
                user_input_dict.get(attr_name, prev_value),
            )
        # we need to write the user input to a file so that the playground can read it; this is a workaround
        # for the fact that we cannot pass arguments to streamlit (StreamlitServer doesn't respect it)
        # we also need to do this for the BFF
        # save user_input to file in home directory of user
        with open(os.path.join(os.path.expanduser('~'), 'user_input.json'), 'w') as f:
            json.dump(self.user_input.__dict__, f)

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
                usage_client_id=self._usage_client_id,
                usage_client_secret=self._usage_client_secret,
                logger=self.logger,
                report_usage=self._get_report_usage(),
            )
        ]

    def _get_report_usage(self) -> Callable:
        # todo: report usage to hubble
        def report_usage(
            current_user: dict,
            usage_client_id: str,
            usage_client_secret: str,
            usage_detail: dict,
        ):
            """Report usage to the backend"""

            try:
                usage_detail['current_user'] = current_user
                from hubble.payment.client import PaymentClient

                client = PaymentClient(
                    m2m_token=os.environ['M2M_TOKEN'],
                )
                resp = client.report_usage(
                    current_user['token'],
                    self._internal_app_id,
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
                parameters = request.json()['parameters']
                print('parameters', parameters)
                check_user(parameters)
                user = {'token': parameters['jwt']}
                return True, user
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
        print(metadata)
        check_user(**metadata)
        user = {'token': metadata['token']}
        return True, user


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
