import datetime

import grpc


def current_time():
    return datetime.datetime.utcnow().isoformat() + 'Z'


# wrap an RPC call
# see https://github.com/grpc/grpc/issues/18191
# see https://github.com/open-telemetry/opentelemetry-python-contrib/blob/main/instrumentation/opentelemetry-instrumentation-grpc/src/opentelemetry/instrumentation/grpc/_server.py
def wrap_rpc_behavior(handler, continuation):
    if handler is None:
        return None

    if handler.request_streaming and handler.response_streaming:
        behavior_fn = handler.stream_stream
        handler_factory = grpc.stream_stream_rpc_method_handler
    elif handler.request_streaming and not handler.response_streaming:
        behavior_fn = handler.stream_unary
        handler_factory = grpc.stream_unary_rpc_method_handler
    elif not handler.request_streaming and handler.response_streaming:
        behavior_fn = handler.unary_stream
        handler_factory = grpc.unary_stream_rpc_method_handler
    else:
        behavior_fn = handler.unary_unary
        handler_factory = grpc.unary_unary_rpc_method_handler

    return handler_factory(
        continuation(
            behavior_fn, handler.request_streaming, handler.response_streaming
        ),
        request_deserializer=handler.request_deserializer,
        response_serializer=handler.response_serializer,
    )
