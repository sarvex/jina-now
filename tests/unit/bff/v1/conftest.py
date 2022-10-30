import base64
import os

import pytest
from docarray import Document, DocumentArray


@pytest.fixture
def base64_image_string(resources_folder_path: str) -> str:
    with open(
        os.path.join(resources_folder_path, 'image', '5109112832.jpg'), 'rb'
    ) as f:
        binary = f.read()
        img_string = base64.b64encode(binary).decode('utf-8')
    return img_string


@pytest.fixture
def base64_image_string(resources_folder_path: str) -> str:
    with open(
        os.path.join(resources_folder_path, 'image', '5109112832.jpg'), 'rb'
    ) as f:
        binary = f.read()
        img_string = base64.b64encode(binary).decode('utf-8')
    return img_string


@pytest.fixture
def sample_search_response_image() -> DocumentArray:
    return DocumentArray([Document(matches=[Document(uri='match')])])


@pytest.fixture
def sample_search_response_text() -> DocumentArray:
    result = DocumentArray([Document()])
    matches = DocumentArray([Document(text='match')])
    result[0].matches = matches
    return result
