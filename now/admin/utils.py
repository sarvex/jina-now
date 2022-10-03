import os

import hubble


def get_default_request_body(deployment_type, secured, remote_host=None):
    request_body = {}
    if deployment_type == 'local':
        request_body['host'] = 'gateway'
        request_body['port'] = 8080
    elif deployment_type == 'remote':
        if remote_host:
            request_body['host'] = remote_host
        else:
            raise ValueError('Remote host must be provided for remote deployment')
    if secured:
        if 'WOLF_TOKEN' in os.environ:
            os.environ['JINA_AUTH_TOKEN'] = os.environ['WOLF_TOKEN']
        request_body['jwt'] = {'token': hubble.get_token()}
    return request_body
