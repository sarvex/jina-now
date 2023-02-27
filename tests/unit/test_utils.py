from docarray import Document, dataclass
from docarray.typing import Text

from now.utils import get_chunk_by_field_name, extract_flow_id


@dataclass
class ConflictingDoc:
    label: Text
    id: Text


def test_flow_id():
    assert (
        extract_flow_id('https://nowapi-92625e8747-http.wolf.jina.ai')
        == 'nowapi-92625e8747'
    )
    assert (
        extract_flow_id('https://test-nowapi-92625e8747-http.wolf.jina.ai')
        == 'test-nowapi-92625e8747'
    )
    assert (
        extract_flow_id('https://something-test-nowapi-92625e8747-http.wolf.jina.ai')
        == 'something-test-nowapi-92625e8747'
    )
    assert (
        extract_flow_id('https://somethi.ng-test-nowapi-92625e8747-http.wolf.jina.ai')
        == 'somethi.ng-test-nowapi-92625e8747'
    )


def test_conflicting_doc_fields():
    doc = Document(ConflictingDoc(label='test_label', id='test_id'))
    assert get_chunk_by_field_name(doc, 'id').text == 'test_id'
