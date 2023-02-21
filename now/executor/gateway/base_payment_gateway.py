import abc
import logging
import os
import sys
from typing import Callable, List, Optional

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection
from jina.enums import GatewayProtocolType
from jina.helper import get_full_version
from jina.importer import ImportExtensions
from jina.proto import jina_pb2, jina_pb2_grpc
from jina.serve.gateway import BaseGateway
from jina.serve.runtimes.helper import _get_grpc_server_options
from jina.types.request.status import StatusMessage
from starlette.responses import PlainTextResponse

from now.executor.gateway.fast_api import get_security_app
from now.executor.gateway.interceptor import PaymentInterceptor


class BasePaymentGateway(BaseGateway):
    def __init__(
        self,
        internal_app_id: str,
        internal_product_id: str,
        usage_client_id: str = None,
        usage_client_secret: str = None,
        grpc_server_options: Optional[dict] = None,
        ssl_keyfile: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        no_debug_endpoints: Optional[bool] = False,
        no_crud_endpoints: Optional[bool] = False,
        expose_endpoints: Optional[str] = None,
        expose_graphql_endpoint: Optional[bool] = False,
        cors: Optional[bool] = True,
        uvicorn_kwargs: Optional[dict] = None,
        proxy: Optional[bool] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.title = title
        self.description = description

        self.port_dict = {}
        for p, l in zip(self.ports, self.protocols):
            self.port_dict[l] = p
        assert set(self.port_dict.keys()) == {
            GatewayProtocolType.GRPC,
            GatewayProtocolType.HTTP,
        }, f'{self.__class__.__name__} only supports grpc and http protocol'

        # gRPC server options
        self.grpc_server_options = grpc_server_options
        self.ssl_keyfile = ssl_keyfile
        self.ssl_certfile = ssl_certfile
        self.health_servicer = health.aio.HealthServicer()

        # HTTP server options
        self.title = title
        self.description = description
        self.no_debug_endpoints = no_debug_endpoints
        self.no_crud_endpoints = no_crud_endpoints
        self.expose_endpoints = expose_endpoints
        self.expose_graphql_endpoint = expose_graphql_endpoint
        self.cors = cors

        self.uvicorn_kwargs = uvicorn_kwargs or {}
        if ssl_keyfile and 'ssl_keyfile' not in self.uvicorn_kwargs.keys():
            self.uvicorn_kwargs['ssl_keyfile'] = ssl_keyfile
        if ssl_certfile and 'ssl_certfile' not in self.uvicorn_kwargs.keys():
            self.uvicorn_kwargs['ssl_certfile'] = ssl_certfile
        if not proxy and os.name != 'nt':
            os.unsetenv('http_proxy')
            os.unsetenv('https_proxy')

        # metering options
        self._internal_app_id = internal_app_id
        self._internal_product_id = internal_product_id
        self._usage_client_id = usage_client_id
        self._usage_client_secret = usage_client_secret

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler(sys.stdout))

    @abc.abstractmethod
    def extra_interceptors(self) -> List[PaymentInterceptor]:
        raise NotImplementedError

    async def _setup_grpc_server(self):
        """Setup the GRPC server."""

        self.grpc_server = grpc.aio.server(
            options=_get_grpc_server_options(self.grpc_server_options),
            interceptors=self.extra_interceptors()
            + (self.grpc_tracing_server_interceptors or []),
        )

        jina_pb2_grpc.add_JinaRPCServicer_to_server(
            self.streamer._streamer, self.grpc_server
        )

        jina_pb2_grpc.add_JinaGatewayDryRunRPCServicer_to_server(self, self.grpc_server)
        jina_pb2_grpc.add_JinaInfoRPCServicer_to_server(self, self.grpc_server)

        service_names = (
            jina_pb2.DESCRIPTOR.services_by_name['JinaRPC'].full_name,
            jina_pb2.DESCRIPTOR.services_by_name['JinaGatewayDryRunRPC'].full_name,
            jina_pb2.DESCRIPTOR.services_by_name['JinaInfoRPC'].full_name,
            reflection.SERVICE_NAME,
        )
        # Mark all services as healthy.
        health_pb2_grpc.add_HealthServicer_to_server(
            self.health_servicer, self.grpc_server
        )

        for service in service_names:
            await self.health_servicer.set(
                service, health_pb2.HealthCheckResponse.SERVING
            )
        reflection.enable_server_reflection(service_names, self.grpc_server)

        bind_addr = f'{self.host}:{self.port_dict[GatewayProtocolType.GRPC]}'

        if self.ssl_keyfile and self.ssl_certfile:
            with open(self.ssl_keyfile, 'rb') as f:
                private_key = f.read()
            with open(self.ssl_certfile, 'rb') as f:
                certificate_chain = f.read()

            server_credentials = grpc.ssl_server_credentials(
                (
                    (
                        private_key,
                        certificate_chain,
                    ),
                )
            )
            self.grpc_server.add_secure_port(bind_addr, server_credentials)
        elif (
            self.ssl_keyfile != self.ssl_certfile
        ):  # if we have only ssl_keyfile and not ssl_certfile or vice versa
            raise ValueError(
                f"you can't pass a ssl_keyfile without a ssl_certfile and vice versa"
            )
        else:
            self.grpc_server.add_insecure_port(bind_addr)
        self.logger.debug(f'start server bound to {bind_addr}')
        await self.grpc_server.start()

    @property
    def app(self):
        from jina.helper import extend_rest_interface

        app = get_security_app(
            streamer=self.streamer,
            title=self.title,
            description=self.description,
            no_debug_endpoints=self.no_debug_endpoints,
            no_crud_endpoints=self.no_crud_endpoints,
            expose_endpoints=self.expose_endpoints,
            expose_graphql_endpoint=self.expose_graphql_endpoint,
            cors=self.cors,
            logger=self.logger,
            tracing=self.tracing,
            tracer_provider=self.tracer_provider,
            usage_client_id=self._usage_client_id,
            usage_client_secret=self._usage_client_secret,
            request_authenticate=self._get_request_authenticate(),
            report_usage=self._get_report_usage(),
        )

        @app.get('/', response_class=PlainTextResponse)
        def read_root() -> str:
            """
            Root path welcome message.
            """
            return f'Works! ✨ '

        return extend_rest_interface(app)

    @abc.abstractmethod
    def _get_request_authenticate(self) -> Callable:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_report_usage(self) -> Callable:
        raise NotImplementedError

    async def _setup_http_server(self):
        """Setup the HTTP server."""
        with ImportExtensions(required=True):
            from uvicorn import Config, Server

        class UviServer(Server):
            """The uvicorn server."""

            async def setup(self, sockets=None):
                """
                Setup uvicorn server.
                :param sockets: sockets of server.
                """
                config = self.config
                if not config.loaded:
                    config.load()
                self.lifespan = config.lifespan_class(config)
                await self.startup(sockets=sockets)
                if self.should_exit:
                    return

            async def serve(self, **kwargs):
                """
                Start the server.
                :param kwargs: keyword arguments
                """
                await self.main_loop()

        self.http_server = UviServer(
            config=Config(
                app=self.app,
                host=self.host,
                port=self.port_dict[GatewayProtocolType.HTTP],
                log_level=os.getenv('JINA_LOG_LEVEL', 'error').lower(),
                **self.uvicorn_kwargs,
            )
        )

        await self.http_server.setup()

    async def setup_server(self):
        """Setup the server."""
        await self._setup_grpc_server()
        await self._setup_http_server()

    async def run_server(self):
        await self.grpc_server.wait_for_termination()
        await self.http_server.serve()

    async def shutdown(self):
        """Free other resources allocated with the server, e.g, gateway object, ..."""
        await self.grpc_server.stop(0)
        await self.health_servicer.enter_graceful_shutdown()

        self.http_server.should_exit = True
        await self.http_server.shutdown()

    async def dry_run(self, empty, context) -> jina_pb2.StatusProto:
        """
        Process the call requested by having a dry run call to every Executor in the graph

        :param empty: The service expects an empty protobuf message
        :param context: grpc context
        :returns: the response request
        """
        from docarray import Document, DocumentArray
        from jina.serve.executors import __dry_run_endpoint__

        da = DocumentArray([Document()])
        try:
            async for _ in self.streamer.stream_docs(
                docs=da, exec_endpoint=__dry_run_endpoint__, request_size=1
            ):
                pass
            status_message = StatusMessage()
            status_message.set_code(jina_pb2.StatusProto.SUCCESS)
            return status_message.proto
        except Exception as ex:
            status_message = StatusMessage()
            status_message.set_exception(ex)
            return status_message.proto

    async def _status(self, empty, context) -> jina_pb2.JinaInfoProto:
        """
        Process the the call requested and return the JinaInfo of the Runtime

        :param empty: The service expects an empty protobuf message
        :param context: grpc context
        :returns: the response request
        """
        info_proto = jina_pb2.JinaInfoProto()
        version, env_info = get_full_version()
        for k, v in version.items():
            info_proto.jina[k] = str(v)
        for k, v in env_info.items():
            info_proto.envs[k] = str(v)
        return info_proto
