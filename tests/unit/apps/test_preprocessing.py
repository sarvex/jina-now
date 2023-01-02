from docarray import Document

from now.app.base.preprocess import preprocess_text


def test_preprocess_text():
    result = preprocess_text(Document(text='test. test', modality='text'))
    assert len(result.chunks) == 2
