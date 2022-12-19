import base64
import os

import pytest
from docarray import Document, DocumentArray
from tests.conftest import get_resource_folder_path


def base64_image_string() -> str:
    path = os.path.join(get_resource_folder_path(), 'image', '5109112832.jpg')
    return Document(uri=path).convert_uri_to_datauri().uri


def base64_audio_string() -> str:
    with open(
        os.path.join(
            get_resource_folder_path(),
            'music',
            '0ae22dba39adebd474025d6f97059d5e425e2cf2.mp3',
        ),
        'rb',
    ) as f:
        binary = f.read()
        audio_string = base64.b64encode(binary).decode('utf-8')
    return audio_string


@pytest.fixture
def sample_search_response() -> DocumentArray:
    return DocumentArray([Document(matches=[Document(uri='match')])])
