import uuid
from typing import Callable, Dict, Optional, Tuple, Union

from fastapi import Depends, Response, status
from jina.clients.request import request_generator
from jina.excepts import InternalNetworkError
from jina.helper import extend_rest_interface
from jina.serve.runtimes.gateway.http.models import (
    JinaEndpointRequestModel,
    JinaResponseModel,
)

from .helper import current_time


def get_security_app(
    streamer: 'GatewayStreamer',
    logger: Union['JinaLogger', 'Logger'],
    internal_app_id: str = None,
    internal_product_id: str = None,
    usage_client_id: Optional[str] = None,
    usage_client_secret: Optional[str] = None,
    request_authenticate: Optional[Callable] = None,
    report_usage: Optional[Callable] = None,
):
    from now.executor.gateway.bff.app.app import application

    app = extend_rest_interface(application)

    # patch the app to overwrite the /post endpoint
    for i, r in enumerate(app.router.routes):
        if r.path_format == '/post':
            del app.router.routes[i]

    @app.post(
        path='/post',
        summary='Post a data request to some endpoint',
        response_model=JinaResponseModel,
        tags=['Debug']
        # do not add response_model here, this debug endpoint should not restrict the response model
    )
    async def post(
        body: JinaEndpointRequestModel,
        response: Response,
        authorized: Tuple[bool, dict] = Depends(request_authenticate),
    ):  # 'response' is a FastAPI response, not a Jina response
        """
        Post a data request to some endpoint.
        This is equivalent to the following:
            from jina import Flow
            f = Flow().add(...)
            with f:
                f.post(endpoint, ...)
        .. # noqa: DAR201
        .. # noqa: DAR101
        """

        # The above comment is written in Markdown for better rendering in FastAPI
        from jina.enums import DataInputType

        authorized, current_user = authorized
        if not authorized:
            from jina.proto.serializer import DataRequest

            response.status_code = status.HTTP_401_UNAUTHORIZED
            return {
                'header': {
                    'status': {
                        'code': DataRequest().status.ERROR,
                        'description': 'Unauthorized, please provide a valid token',
                    }
                }
            }

        bd = body.dict()  # type: Dict
        req_generator_input = bd
        req_generator_input['data_type'] = DataInputType.DICT
        if bd['data'] is not None and 'docs' in bd['data']:
            req_generator_input['data'] = req_generator_input['data']['docs']

        try:
            result = await _get_singleton_result(
                request_generator(**req_generator_input)
            )
            num_docs = len(result['data'])
            report_usage(
                current_user=current_user,
                usage_client_id=usage_client_id,
                usage_client_secret=usage_client_secret,
                usage_detail={
                    'token': current_user['access_token'],
                    'id': str(uuid.uuid4()),
                    'rootId': str(uuid.uuid4()),
                    'quantity': num_docs,
                    'internalAppId': internal_app_id,
                    'internalProductId': internal_product_id,
                },
            )

            logger.info(
                {
                    'timestamp': current_time(),
                    'num_docs': num_docs,
                    'exec_endpoint': result['header']['exec_endpoint'],
                    **current_user,
                }
            )
        except InternalNetworkError as err:
            import grpc

            if err.code() == grpc.StatusCode.UNAVAILABLE:
                response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            elif err.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                response.status_code = status.HTTP_504_GATEWAY_TIMEOUT
            else:
                response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            result = bd  # send back the request
            result['header'] = _generate_exception_header(
                err
            )  # attach exception details to response header
            logger.error(
                f'Error while getting responses from deployments: {err.details()}'
            )
        except Exception as ex:
            response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            logger.error(f'Error while getting responses: {ex!r}')
            return {
                'header': {
                    'status': {
                        'code': 500,
                        'description': 'Internal Server Error',
                    }
                }
            }

        return result

    def _generate_exception_header(error: InternalNetworkError):
        import traceback

        from jina.proto.serializer import DataRequest

        exception_dict = {
            'name': str(error.__class__),
            'stacks': [
                str(x) for x in traceback.extract_tb(error.og_exception.__traceback__)
            ],
            'executor': '',
        }
        status_dict = {
            'code': DataRequest().status.ERROR,
            'description': error.details() if error.details() else '',
            'exception': exception_dict,
        }
        header_dict = {'request_id': error.request_id, 'status': status_dict}
        return header_dict

    async def _get_singleton_result(request_iterator) -> Dict:
        """
        Streams results from AsyncPrefetchCall as a dict
        :param request_iterator: request iterator, with length of 1
        :return: the first result from the request iterator
        """
        async for k in streamer.stream(request_iterator=request_iterator):
            request_dict = k.to_dict()
            return request_dict

    return app
