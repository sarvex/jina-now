import os

import pytest
from docarray import Document, dataclass
from docarray.typing import Image, Text, Video

from now.utils.common.helpers import hide_string_chars, to_camel_case
from now.utils.docarray.helpers import (
    docarray_typing_to_modality_string,
    get_chunk_by_field_name,
    modality_string_to_docarray_typing,
)
from now.utils.jcloud.helpers import get_flow_id


@dataclass
class ConflictingDoc:
    label: Text
    id: Text


def test_flow_id():
    assert (
        get_flow_id('https://nowapi-92625e8747-http.wolf.jina.ai')
        == 'nowapi-92625e8747'
    )
    assert (
        get_flow_id('https://test-nowapi-92625e8747-http.wolf.jina.ai')
        == 'test-nowapi-92625e8747'
    )
    assert (
        get_flow_id('https://something-test-nowapi-92625e8747-http.wolf.jina.ai')
        == 'something-test-nowapi-92625e8747'
    )
    assert (
        get_flow_id('https://somethi.ng-test-nowapi-92625e8747-http.wolf.jina.ai')
        == 'somethi.ng-test-nowapi-92625e8747'
    )


def test_conflicting_doc_fields():
    doc = Document(ConflictingDoc(label='test_label', id='test_id'))
    assert get_chunk_by_field_name(doc, 'id').text == 'test_id'


def test_get_chunk_by_field_name(mm_dataclass, resources_folder_path):
    text_field = 'test'
    image_field = os.path.join(resources_folder_path, 'image/a.jpg')
    doc = Document(mm_dataclass(text_field=text_field, image_field=image_field))
    assert get_chunk_by_field_name(doc, 'text_field').text == text_field
    assert get_chunk_by_field_name(doc, 'image_field').uri == image_field

    with pytest.raises(Exception):
        get_chunk_by_field_name(doc, 'some_field_name')


@pytest.mark.parametrize(
    'input_text, expected_output',
    [('base_app', 'BaseApp'), ('some test', 'SomeTest'), ('app', 'App')],
)
def test_to_camel_case(input_text, expected_output):
    assert to_camel_case(input_text) == expected_output


@pytest.mark.parametrize(
    'input_type, expected_string',
    [(Image, 'image'), (Text, 'text'), (Video, 'video')],
)
def test_docarray_typing_to_modality_string(input_type, expected_string):
    assert docarray_typing_to_modality_string(input_type) == expected_string


@pytest.mark.parametrize(
    'input_string, expected_type',
    [('image', Image), ('text', Text), ('video', Video)],
)
def test_modality_string_to_docarray_typing(input_string, expected_type):
    assert modality_string_to_docarray_typing(input_string) == expected_type


@pytest.mark.parametrize(
    'input_string, hidden_string',
    [('123456789', '*****6789'), ('abcdefghijkl', '********ijkl')],
)
def test_hide_string_chars(input_string, hidden_string):
    assert hide_string_chars(input_string) == hidden_string
