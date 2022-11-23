from docarray import Document

from now.app.base.preprocess import preprocess_text


def test_preprocess_long_da_with_split():
    result = preprocess_text(Document(text='test. test', modality='text'))
    assert len(result.chunks) == 2
