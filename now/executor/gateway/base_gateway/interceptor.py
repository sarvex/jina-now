import abc
import logging
import uuid
from typing import Callable, Tuple, Union

import grpc

from now.executor.gateway.base_gateway.helper import current_time, wrap_rpc_behavior

logger = logging.getLogger(__name__)


class PaymentInterceptor(grpc.aio.ServerInterceptor):
    def __init__(self, logger, report_usage: Callable, **kwargs):
        self._internal_app_id = kwargs.get('internal_app_id')
        self._internal_product_id = kwargs.get('internal_product_id')
        self._m2m_token = kwargs.get('m2m_token')
        self._logger = logger
        self._report_usage = report_usage

    def get_error_handler(self, status_code, message):
        def _error_handler(_, context):
            context.abort(status_code, message)

        return grpc.unary_unary_rpc_method_handler(_error_handler)

    @abc.abstractmethod
    def authenticate_and_authorize(
        self, handler_call_details
    ) -> Tuple[bool, Union[dict, str]]:
        """Authenticate and authorize the request

        It combines the authentication and authorization logic.
          - Authentication: check if the request is authenticated
          - Authorization: check if the request is authorized to access the resource

        :return: a tuple of (is_authenticated, authorized_user)
        """
        raise NotImplementedError

    def _intercept_aio_server_unary(self, behavior, handler_call_details, current_user):
        async def _unary_interceptor(request_or_iterator, context):
            # And now we run the actual RPC.
            try:
                response = await behavior(request_or_iterator, context)
                num_docs = len(response.data.docs)
                if num_docs > 0:
                    self._report_usage(
                        current_user=current_user,
                        usage_detail={
                            'token': current_user['token'],
                            'id': str(uuid.uuid4()),
                            'credits': num_docs,
                            'internalAppId': self._internal_app_id,
                            'internalProductId': self._internal_product_id,
                        },
                    )
                else:
                    self._logger.info(
                        'Billing report not submitted as number of docs is 0'
                    )
                self._logger.info(
                    {
                        'timestamp': current_time(),
                        'num_docs': num_docs,
                        'exec_endpoint': response.header.exec_endpoint,
                        **current_user,
                    }
                )
                return response

            except Exception as error:
                # Bare exceptions are likely to be gRPC aborts, which
                # we handle in our context wrapper.
                # Here, we're interested in uncaught exceptions.
                # pylint:disable=unidiomatic-typecheck
                if type(error) != Exception:
                    print(f'Uncaught exception: {error}')
                raise error

        return _unary_interceptor

    def _intercept_aio_server_stream(
        self, behavior, handler_call_details, current_user
    ):
        async def _stream_interceptor(request_or_iterator, context):
            async def _tail_requests(request_or_iterator):
                async for request in request_or_iterator:
                    # print(request.header)
                    yield request

            try:
                # method = handler_call_details.method
                async for response in behavior(
                    _tail_requests(request_or_iterator), context
                ):
                    num_docs = len(response.data.docs)
                    self._report_usage(
                        current_user=current_user,
                        usage_detail={
                            'token': current_user['token'],
                            'id': str(uuid.uuid4()),
                            'credits': num_docs,
                            'internalAppId': self._internal_app_id,
                            'internalProductId': self._internal_product_id,
                        },
                        use_free_credits=False,
                    )
                    self._logger.info(
                        {
                            'type': 'GRPC',
                            'timestamp': current_time(),
                            'num_docs': num_docs,
                            'exec_endpoint': response.header.exec_endpoint,
                            **current_user,
                        }
                    )
                    yield response

            except Exception as error:
                # pylint:disable=unidiomatic-typecheck
                if type(error) != Exception:
                    print(f'Uncaught exception: {error}')
                raise error

        return _stream_interceptor

    async def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method

        # only authorize for the methods that need it
        if method == '/jina.JinaRPC/Call':
            authorized, current_user = self.authenticate_and_authorize(
                handler_call_details
            )

            if not authorized:
                return self.get_error_handler(
                    grpc.StatusCode.UNAUTHENTICATED, current_user
                )

            def _wrapper(behavior, request_streaming, response_streaming):
                # handle streaming responses specially
                if response_streaming:
                    return self._intercept_aio_server_stream(
                        behavior,
                        handler_call_details,
                        current_user,
                    )

                return self._intercept_aio_server_unary(
                    behavior,
                    handler_call_details,
                    current_user,
                )

            next_handler = await continuation(handler_call_details)
            return wrap_rpc_behavior(
                next_handler,
                _wrapper,
            )
        else:
            return await continuation(handler_call_details)
