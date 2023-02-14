import os

from docarray import Document, DocumentArray
from jina import Flow

from now.executor.gateway.now_gateway import NOWGateway
from now.executor.preprocessor import NOWPreprocessor


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

    with Flow().config_gateway(
        uses=NOWGateway,
        protocol=['http', 'grpc'],
        port=[8081, 8085],
        env={'JINA_LOG_LEVEL': 'DEBUG'},
    ).add(uses=NOWPreprocessor, workspace=tmpdir, env={'JINA_LOG_LEVEL': 'DEBUG'}) as f:
        for endpoint in ['index', 'search']:
            result = f.post(
                on=f'/{endpoint}',
                inputs=docs,
                show_progress=True,
            )

            assert len(result) == 2
            assert result[0].text == ''
            assert result[0].chunks[0].chunks[0].text == 'test'
            assert result[1].chunks[0].chunks[0].blob
