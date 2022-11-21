from docarray import Document, DocumentArray

from now.common.preprocess import preprocess_text


def test_preprocess_long_da_with_split():
    da = DocumentArray([Document(text='Test. Testing a long document')])
    result = preprocess_text(da, split_by_sentences=True)
    assert len(result) == 1
    assert len(result[0].chunks) == 2


def test_preprocess_long_da_no_split():
    da = DocumentArray(
        [
            Document(
                chunks=[
                    Document(
                        text='This is a long test. Testing the split.', modality='text'
                    )
                ]
            )
        ]
    )
    result = preprocess_text(da, split_by_sentences=False)
    assert len(result) == 1
    assert len(result[0].chunks) == 1
    assert len(result[0].chunks[0].chunks) == 0


def test_preprocess_short_da_with_split():
    da = DocumentArray(
        [Document(chunks=[Document(text='test. test', modality='text')])]
    )
    result = preprocess_text(da, split_by_sentences=True)
    assert len(result) == 1
    assert len(result[0].chunks) == 1
    assert len(result[0].chunks[0].chunks) == 2


def test_preprocess_short_da_no_split():
    da = DocumentArray(
        [Document(chunks=[Document(text='test. test', modality='text')])]
    )
    result = preprocess_text(da, split_by_sentences=False)
    assert len(result) == 1
    assert len(result[0].chunks) == 1
    assert len(result[0].chunks[0].chunks) == 0
