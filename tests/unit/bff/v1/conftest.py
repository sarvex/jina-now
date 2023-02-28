import base64
import os

import pytest
from docarray import Document, DocumentArray, dataclass
from docarray.typing import Text


@pytest.fixture
def base64_image_string(resources_folder_path: str) -> str:
    with open(os.path.join(resources_folder_path, 'image', 'a.jpg'), 'rb') as f:
        binary = f.read()
        img_string = base64.b64encode(binary).decode('utf-8')
    return img_string


@pytest.fixture
def base64_image_string(resources_folder_path: str) -> str:
    with open(os.path.join(resources_folder_path, 'image', 'a.jpg'), 'rb') as f:
        binary = f.read()
        img_string = base64.b64encode(binary).decode('utf-8')
    return img_string


@pytest.fixture
def sample_search_response_image() -> DocumentArray:
    result = DocumentArray([Document()])
    matches = DocumentArray([Document(text='match')])
    result[0].matches = matches
    return result


@pytest.fixture
def sample_search_response_text() -> DocumentArray:
    @dataclass
    class MMDoc:
        title: Text

    result = DocumentArray([Document()])
    matches = DocumentArray([Document(MMDoc(title='match'))])
    result[0].matches = matches
    result[0].tags['tags'] = {'color': ['blue']}
    result[0].tags['count'] = 1
    return result
