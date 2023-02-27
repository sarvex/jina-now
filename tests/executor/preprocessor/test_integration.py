import os

import hubble
from docarray import Document, DocumentArray
from jina import Flow

from now.executor.gateway.now_gateway import NOWGateway
from now.executor.preprocessor import NOWPreprocessor
from now.now_dataclasses import UserInput


def test_search_app(resources_folder_path, tmpdir, mm_dataclass):
    docs = DocumentArray(
        [
            Document(mm_dataclass(text_field='test')),
            Document(
                mm_dataclass(
                    video_field=os.path.join(
                        resources_folder_path, 'gif/folder1/file.gif'
                    )
                )
            ),
        ]
    )
    # this is needed as using http compression leads to a tensor of dtype uint8 is received as int64
    docs[1].video_field.load_uri_to_blob(timeout=10)
    user_input = UserInput()
    user_input.user_emails = ['team-now@jina.ai']
    user_input.admin_emails = []
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
    ).add(uses=NOWPreprocessor, workspace=tmpdir, env={'JINA_LOG_LEVEL': 'DEBUG'}) as f:
        for endpoint in ['index', 'search']:
            result = f.post(
                on=f'/{endpoint}',
                inputs=docs,
                show_progress=True,
                headers={
                    'authorization': f'token {hubble.get_token()}',
                    'Content-Type': 'application/json',
                },
            )

            assert len(result) == 2
            assert result[0].text == ''
            assert result[0].chunks[0].chunks[0].text == 'test'
            assert result[1].chunks[0].chunks[0].blob
