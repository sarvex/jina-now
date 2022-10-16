from copy import deepcopy

from docarray import Document, DocumentArray
from jina import Flow, requests
from now_common.abstract_executors.NOWBaseIndexer.base_indexer import (
    NOWBaseIndexer as Executor,
)


class InMemoryIndexer(Executor):
    """InMemoryIndexer indexes Documents into a DocumentArray with `storage='memory'`"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print('InMemoryIndexer init')

    def construct(self, **kwargs):
        self._index = DocumentArray()

    def batch_iterator(self):
        for doc in self._index:
            yield doc

    def index(self, docs, parameters, **kwargs):
        self._index.extend(docs)

    def delete(self, docs, **kwargs):
        for doc in docs:
            del self._index[doc.id]

    def search(self, docs, **kwargs):
        docs.match(self._index)

    @requests(on='/test_list')
    def list(self, **kwargs):
        return self._index


def test_no_blob_with_working_uri(tmpdir):
    metas = {'workspace': str(tmpdir)}
    with Flow().add(
        uses=InMemoryIndexer,
        uses_with={
            'dim': 128,
        },
        uses_metas=metas,
    ) as f:
        doc_blob = Document(
            uri='https://jina.ai/assets/images/text-to-image-output.png',
        ).load_uri_to_blob()

        doc_tens = Document(
            uri='https://jina.ai/assets/images/text-to-image-output.png',
        ).load_uri_to_image_tensor()

        inputs = DocumentArray(
            [
                Document(text='hi'),
                Document(blob=b'b12'),
                Document(blob=b'b12', uri='file_will.never_exist'),
                doc_blob,
                doc_tens,
            ]
        )

        f.post('/index', deepcopy(inputs), parameters={})

        response = f.post('/test_list')

        assert response[0] == inputs[0]
        assert response[1] == inputs[1]
        assert response[2] == inputs[2]
        assert response[3].blob == b''
        assert response[4].tensor is None
