import pytest
from docarray import Document
from jina import Flow

from now.constants import EXTERNAL_CLIP_HOST, EXTERNAL_SBERT_HOST


@pytest.mark.parametrize('executor_endpoint', [EXTERNAL_CLIP_HOST, EXTERNAL_SBERT_HOST])
def test_external_executor(executor_endpoint):
    with Flow(protocol='grpc').add(
        host=executor_endpoint, port=443, tls=True, external=True
    ) as f:
        response = f.index(
            inputs=[Document(chunks=Document(chunks=Document(text='test')))],
            parameters={'access_paths': '@cc'},
        )
        print('response', response)
        assert response[0].chunks[0].chunks[0].embedding is not None
