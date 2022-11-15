from docarray import Document, DocumentArray

from now.common.preprocess import preprocess_text


def test_preprocess_long_da_with_split():
    da = DocumentArray([Document(text='Test. Testing a long document')])
    result = preprocess_text(da, split_by_sentences=True)
    assert len(result) == 2


def test_preprocess_long_da_no_split():
    da = DocumentArray([Document(text='Test. Testing a long document')])
    result = preprocess_text(da, split_by_sentences=False)
    assert len(result) == 1


def test_preprocess_short_da_with_split():
    da = DocumentArray([Document(text='Test.')])
    result = preprocess_text(da, split_by_sentences=True)
    assert len(result) == 1


def test_preprocess_short_da_no_split():
    da = DocumentArray([Document(text='Test.')])
    result = preprocess_text(da, split_by_sentences=False)
    assert len(result) == 1
