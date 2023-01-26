import os

import pytest
from docarray import Document, DocumentArray, dataclass
from docarray.typing import Text, Video
from jina import Flow

from now.executor.preprocessor import NOWPreprocessor


@pytest.mark.parametrize('endpoint', ['index', 'search'])
def test_search_app(resources_folder_path, endpoint, tmpdir):
    @dataclass
    class MMTextDoc:
        text: Text

    @dataclass
    class MMVideoDoc:
        uri: Video

    metas = {'workspace': str(tmpdir)}
    text_docs = DocumentArray(
        [
            Document(MMTextDoc(text='test')),
            Document(
                MMVideoDoc(
                    uri=os.path.join(resources_folder_path, 'gif/folder1/file.gif')
                )
            ),
        ]
    )

    with Flow().add(uses=NOWPreprocessor, uses_metas=metas) as f:
        result = f.post(
            on=f'/{endpoint}',
            inputs=text_docs,
            show_progress=True,
        )
        result = DocumentArray.from_json(result.to_json())

    assert len(result) == 2
    assert result[0].text == ''
    assert result[0].chunks[0].chunks[0].text == 'test'
    assert result[1].chunks[0].chunks[0].blob
