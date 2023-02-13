import json
import os
from time import sleep
from typing import Dict, List, Tuple

from jina.enums import GatewayProtocolType
from jina.serve.runtimes.gateway import CompositeGateway

from now.constants import NOWGATEWAY_BFF_PORT
from now.deployment.deployment import cmd
from now.executor.gateway import BFFGateway, PlaygroundGateway
from now.now_dataclasses import UserInput

cur_dir = os.path.dirname(__file__)


class NOWGateway(CompositeGateway):
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

    def __init__(self, user_input_dict: Dict = {}, **kwargs):
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
        super().__init__(**kwargs)

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

        # note order is important
        self._add_gateway(
            BFFGateway,
            NOWGATEWAY_BFF_PORT,
            **kwargs,
        )
        self._add_gateway(
            PlaygroundGateway,
            8501,
            **kwargs,
        )

        self.setup_nginx()
        self.nginx_was_shutdown = False

    async def shutdown(self):
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

    def _add_gateway(self, gateway_cls, port, protocol='http', **kwargs):
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
        self.gateways.insert(0, gateway)
