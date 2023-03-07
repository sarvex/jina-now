import os

import hubble
from docarray import Document, DocumentArray
from jina import Flow

from now.executor.autocomplete.executor import NOWAutoCompleteExecutor2
from now.executor.gateway.now_gateway import NOWGateway
from now.now_dataclasses import UserInput


def test_autocomplete(tmpdir, mm_dataclass):
    user_input = UserInput()
    user_input.user_emails = ['team-now@jina.ai']
    user_input.admin_emails = ['team-now@jina.ai']
    user_input.api_key = []
    user_input.jwt = {'token': hubble.get_token()}
    with Flow().config_gateway(
        uses=NOWGateway,
        protocol=['http', 'grpc'],
        port=[8081, 8085],
        env={'JINA_LOG_LEVEL': 'DEBUG'},
        uses_with={
            'user_input_dict': user_input.__dict__,
            'm2m_token': os.environ['M2M_TOKEN'],
        },
    ).add(uses=NOWAutoCompleteExecutor2, workspace=tmpdir) as f:
        f.post(
            on='/search',
            inputs=DocumentArray(
                [
                    Document(mm_dataclass(text_field='background')),
                    Document(mm_dataclass(text_field='background')),
                    Document(mm_dataclass(text_field='bang')),
                    Document(mm_dataclass(text_field='loading')),
                    Document(mm_dataclass(text_field='loading')),
                    Document(mm_dataclass(text_field='laugh')),
                    Document(mm_dataclass(text_field='hello')),
                    Document(mm_dataclass(text_field='red long dress')),
                ]
            ),
            headers={'authorization': f'token {hubble.get_token()}'},
        )
        response = f.post(
            on='/suggestion',
            inputs=DocumentArray([Document(text='b')]),
            headers={'authorization': f'token {hubble.get_token()}'},
        )
        assert response[0].tags['suggestions'] == ['background', 'bang']
        response = f.post(
            on='/suggestion',
            inputs=DocumentArray([Document(text='l')]),
            headers={'authorization': f'token {hubble.get_token()}'},
        )
        assert response[0].tags['suggestions'] == ['loading', 'long', 'laugh']
        response = f.post(
            on='/suggestion',
            inputs=DocumentArray(
                [Document(text='bac')],
            ),
            headers={'authorization': f'token {hubble.get_token()}'},
        )
        assert response[0].tags['suggestions'] == ['background']
        response = f.post(
            on='/suggestion',
            inputs=DocumentArray([Document(text='fuc')]),
            headers={'authorization': f'token {hubble.get_token()}'},
        )
        assert response[0].tags['suggestions'] == []
        response = f.post(
            on='/suggestion',
            inputs=DocumentArray([Document(text='red')]),
            headers={'authorization': f'token {hubble.get_token()}'},
        )
        assert response[0].tags['suggestions'] == ['red', 'red long dress']
        response = f.post(
            on='/suggestion',
            inputs=DocumentArray([Document(text='d')]),
            headers={'authorization': f'token {hubble.get_token()}'},
        )
        assert response[0].tags['suggestions'] == ['dress']
