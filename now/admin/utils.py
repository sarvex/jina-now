import os

import hubble


def get_default_request_kwargs():
    headers = {}
    request_body = {}
    if 'WOLF_TOKEN' in os.environ:
        os.environ['JINA_AUTH_TOKEN'] = os.environ['WOLF_TOKEN']
    headers['Authorization'] = f'token {hubble.get_token()}'
    request_body['jwt'] = {'token': hubble.get_token()}
    return headers, request_body
