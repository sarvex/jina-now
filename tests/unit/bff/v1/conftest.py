import pytest
from docarray import Document, DocumentArray


@pytest.fixture
def sample_search_response_image() -> DocumentArray:
    result = DocumentArray([Document()])
    matches = DocumentArray([Document(uri='match')])
    result[0].matches = matches
    return result


@pytest.fixture
def sample_search_response_text() -> DocumentArray:
    result = DocumentArray([Document()])
    matches = DocumentArray([Document(text='match')])
    result[0].matches = matches
    return result
