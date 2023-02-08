import json
import os
from time import sleep
from typing import List, Tuple

import streamlit.web.bootstrap
from jina import Gateway
from jina.serve.runtimes.gateway import CompositeGateway
from jina.serve.runtimes.gateway.http.fastapi import FastAPIBaseGateway
from jina.serve.runtimes.gateway.http.models import JinaHealthModel
from streamlit.web.server import Server as StreamlitServer

from now.constants import NOWGATEWAY_BFF_PORT
from now.deployment.deployment import cmd
from now.now_dataclasses import UserInput

cur_dir = os.path.dirname(__file__)


class PlaygroundGateway(Gateway):
    def __init__(self, secured: bool, **kwargs):
        super().__init__(**kwargs)
        self.secured = secured
        self.streamlit_script = 'playground/playground.py'

    async def setup_server(self):
        streamlit.web.bootstrap._fix_sys_path(self.streamlit_script)
        streamlit.web.bootstrap._fix_matplotlib_crash()
        streamlit.web.bootstrap._fix_tornado_crash()
        streamlit.web.bootstrap._fix_sys_argv(self.streamlit_script, ())
        streamlit.web.bootstrap._fix_pydeck_mapbox_api_warning()
        streamlit_cmd = (
            f'"python -m streamlit" run --browser.serverPort 12983 {self.streamlit_script} --server.address=0.0.0.0 '
            f'--server.baseUrlPath /playground '
        )
        if self.secured:
            streamlit_cmd += '-- --secured'
        self.streamlit_server = StreamlitServer(
            os.path.join(cur_dir, self.streamlit_script), streamlit_cmd
        )

    async def run_server(self):
        await self.streamlit_server.start()
        streamlit.web.bootstrap._on_server_start(self.streamlit_server)
        streamlit.web.bootstrap._set_up_signal_handler(self.streamlit_server)

    async def shutdown(self):
        self.streamlit_server.stop()


class BFFGateway(FastAPIBaseGateway):
    @property
    def app(self):
        from now.executor.gateway.bff.app.app import application

        # fix to use starlette instead of FastAPI app (throws warning that "/" is used for health checks
        application.add_route(
            path='/', route=lambda: JinaHealthModel(), methods=['GET']
        )

        return application


class NOWGateway(CompositeGateway):
    """The NOWGateway assumes that the gateway has been started with http on port 8081.
    This is the port on which the nginx process listens. After nginx has been started,
    it will start the playground on port 8501 and the BFF on port 8080. The actual
    HTTP gateway will start on port 8082.
    Nginx is configured to route the requests in the following way:
    - /playground -> playground on port 8501
    - /api -> BFF on port 8080
    - / -> HTTP gateway on port 8082
    """

    def __init__(
        self, user_input_dict: str = '', with_playground: bool = True, **kwargs
    ):
        # need to update port ot 8082, as nginx will listen on 8081
        kwargs['runtime_args']['port'] = [8082]
        super().__init__(**kwargs)

        self.user_input = UserInput()
        if not isinstance(user_input_dict, dict) and isinstance(user_input_dict, str):
            user_input_dict = json.loads(user_input_dict) if user_input_dict else {}
        for attr_name, prev_value in self.user_input.__dict__.items():
            setattr(
                self.user_input,
                attr_name,
                user_input_dict.get(attr_name, prev_value),
            )

        # note order is important
        self._add_gateway(BFFGateway, NOWGATEWAY_BFF_PORT, **kwargs)
        if with_playground:
            self._add_gateway(
                PlaygroundGateway,
                8501,
                **{'secured': self.user_input.secured, **kwargs},
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
