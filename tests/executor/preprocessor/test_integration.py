import os

import hubble
import pytest
from docarray import Document, DocumentArray
from jina import Client

from now.common.options import construct_app
from now.constants import Apps
from now.now_dataclasses import UserInput


@pytest.fixture
def preprocessor_test(resources_folder_path, mm_dataclass):
    user_input = UserInput()
    user_input.user_emails = ['team-now@jina.ai']
    user_input.admin_emails = []
    user_input.api_key = []
    user_input.jwt = {'token': hubble.get_token()}
    user_input.app_instance = construct_app(Apps.SEARCH_APP)

    def func(*args, **kwargs):
        preprocess_config = user_input.app_instance.preprocessor_stub(True)
        preprocess_config.pop('needs')
        return [preprocess_config]

    user_input.app_instance.get_executor_stubs = func
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
    docs[1].video_field.load_uri_to_blob(timeout=10)
    return docs, user_input


@pytest.mark.parametrize(
    'get_flow',
    ['preprocessor_test'],
    indirect=True,
)
def test_search_app(get_flow):
    docs, user_input = get_flow
    client = Client(host='grpc://localhost:8085')

    # this is needed as using http compression leads to a tensor of dtype uint8 is received as int64
    for endpoint in ['index', 'search']:
        result = client.post(
            on=f'/{endpoint}',
            inputs=docs,
            show_progress=True,
            metadata=(('authorization', hubble.get_token()),),
        )

        assert len(result) == 2
        assert result[0].text == ''
        assert result[0].chunks[0].chunks[0].text == 'test'
        assert result[1].chunks[0].chunks[0].blob
