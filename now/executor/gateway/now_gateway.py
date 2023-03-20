import json
import os
import shutil
from time import sleep
from typing import Dict, List, Tuple

import streamlit.web.bootstrap
from jina import Gateway
from jina.enums import GatewayProtocolType
from jina.serve.runtimes.gateway import CompositeGateway
from jina.serve.runtimes.gateway.http.fastapi import FastAPIBaseGateway
from jina.serve.runtimes.gateway.http.models import JinaHealthModel
from streamlit.file_util import get_streamlit_file_path
from streamlit.web.server import Server as StreamlitServer

from now.constants import NOWGATEWAY_BFF_PORT
from now.deployment.deployment import cmd
from now.executor.gateway.hubble_report import start_base_fee_thread
from now.now_dataclasses import UserInput

cur_dir = os.path.dirname(__file__)
TIMEOUT = 60


class PlaygroundGateway(Gateway):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.streamlit_script = 'playground/playground.py'
        # copy playground/config.toml to streamlit config.toml
        streamlit_config_toml_src = os.path.join(cur_dir, 'playground', 'config.toml')
        streamlit_config_toml_dest = get_streamlit_file_path("config.toml")
        # create streamlit_config_toml_dest if it doesn't exist
        os.makedirs(os.path.dirname(streamlit_config_toml_dest), exist_ok=True)
        shutil.copyfile(streamlit_config_toml_src, streamlit_config_toml_dest)

    async def setup_server(self):
        streamlit.web.bootstrap._fix_sys_path(self.streamlit_script)
        streamlit.web.bootstrap._fix_matplotlib_crash()
        streamlit.web.bootstrap._fix_tornado_crash()
        streamlit.web.bootstrap._fix_sys_argv(self.streamlit_script, ())
        streamlit.web.bootstrap._fix_pydeck_mapbox_api_warning()
        streamlit_cmd = f'streamlit run {self.streamlit_script}'

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
        grpc_idx = kwargs['runtime_args']['protocol'].index(GatewayProtocolType.GRPC)
        grpc_port = kwargs['runtime_args']['port'][grpc_idx]
        if kwargs['runtime_args']['port'][http_idx] != 8081:
            raise ValueError(
                f'Please, let http port ({http_port}) be 8081 for nginx to work'
            )
        if grpc_port in [8080, 8081, 8082, 8501]:
            raise ValueError(
                f'Please, let grpc port ({grpc_port}) be different from 8080 (BFF), '
                f'8081 (nginx), 8082 (http) and 8501 (playground)'
            )
        kwargs['runtime_args']['port'][http_idx] = 8082
        super().__init__(**kwargs)
        self.storage_dir = None
        self.authorized_jwt = None

        # Hacky method since `workspace` class variable is not available in Gateway
        try:
            self.storage_dir = [
                folder
                for folder in os.listdir('/data')
                if folder.startswith('jnamespace-')
            ]
            if len(self.storage_dir) == 0:
                self.logger.info('No storage directory found')
            else:
                self.logger.info(f'Found storage directory: {self.storage_dir}')
                self.storage_dir = self.storage_dir[0]
        except Exception as e:
            self.logger.info(f'Error while getting storage directory: {e}')

        self._check_env_vars()

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

        # remove potential clashing arguments from kwargs
        kwargs.pop("port", None)
        kwargs.pop("protocol", None)

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

        if self.storage_dir:
            if os.path.isfile(f'{self.storage_dir}/cred.json'):
                self.logger.info('Found cred.json file. Loading from it')
                with open(f'{self.storage_dir}/cred.json', 'r') as f:
                    cred_data = json.load(f)
                    self.authorized_jwt = cred_data.get('authorized_jwt', None)
            else:
                self.logger.info('No cred.json file found to load from')

        try:
            start_base_fee_thread(
                self.user_input.jwt['token'], self.authorized_jwt, self.storage_dir
            )
        except Exception as e:
            self.logger.error(f'Could not start base fee thread: {e}')

    def _check_env_vars(self):
        while 'M2M' not in os.environ:
            timeout_counter = 0
            if timeout_counter < TIMEOUT:
                timeout_counter += 5
                self.logger.info('Environment variables not set yet. Waiting...')
                sleep(5)
            else:
                self.logger.error(
                    'Gateway environment variables not set after 60 seconds. Exiting...'
                )
                raise Exception('Gateway environment variables not set')

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
            self.runtime_args,
            [
                'metrics_registry',
                'tracer_provider',
                'grpc_tracing_server_interceptors',
                'aio_tracing_client_interceptors',
                'tracing_client_interceptor',
            ],
        )
        runtime_args.port = [port]
        runtime_args.protocol = [protocol]
        gateway_kwargs = {k: v for k, v in kwargs.items() if k != 'runtime_args'}
        gateway_kwargs['runtime_args'] = dict(vars(runtime_args))
        gateway = gateway_cls(**gateway_kwargs)
        gateway.streamer = self.streamer
        self.gateways.insert(0, gateway)
