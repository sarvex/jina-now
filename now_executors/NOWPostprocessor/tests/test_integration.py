from copy import deepcopy

from docarray import Document, DocumentArray
from executor import NOWPostprocessorV2
from jina import Flow


def test_postprocessing():
    with Flow().add(uses=NOWPostprocessorV2) as f:
        doc_blob = Document(
            uri='https://upload.wikimedia.org/wikipedia/commons/thumb/b/b3/Wikipedia-logo-v2-en.svg/270px-Wikipedia-logo-v2-en.svg.png'
        )
        doc_blob.load_uri_to_blob()

        doc_tens = Document(
            uri='https://upload.wikimedia.org/wikipedia/commons/thumb/b/b3/Wikipedia-logo-v2-en.svg/270px-Wikipedia-logo-v2-en.svg.png'
        )
        doc_tens.load_uri_to_image_tensor()

        inputs = DocumentArray(
            [
                Document(text='hi'),
                Document(blob=b'b12'),
                Document(blob=b'b12', uri='file_will.never_exist'),
                doc_blob,
                doc_tens,
            ]
        )

        response = f.index(inputs=deepcopy(inputs))

        assert response[0] == inputs[0]
        assert response[1] == inputs[1]
        assert response[2] == inputs[2]
        assert response[3].blob == b''
        assert response[4].tensor is None
