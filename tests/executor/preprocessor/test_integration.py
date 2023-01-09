import json
import os

import pytest
from docarray import Document, DocumentArray
from jina import Flow

from now.constants import TAG_OCR_DETECTOR_TEXT_IN_DOC
from now.executor.preprocessor import NOWPreprocessor


def test_executor_persistence(tmpdir, resources_folder_path):
    e = NOWPreprocessor(metas={'workspace': tmpdir})
    text_docs = DocumentArray(
        [
            Document(chunks=[Document(text='test')]),
            Document(
                chunks=[
                    Document(uri=os.path.join(resources_folder_path, 'image', 'b.jpg'))
                ]
            ),
        ]
    )

    e.preprocess(docs=text_docs)
    with open(e.user_input_path, 'r') as fp:
        json.load(fp)


@pytest.mark.parametrize('endpoint', ['index', 'search'])
def test_search_app(resources_folder_path, endpoint, tmpdir):
    metas = {'workspace': str(tmpdir)}
    text_docs = DocumentArray(
        [
            Document(chunks=[Document(text='test', modality='text')]),
            Document(
                chunks=[
                    Document(
                        uri=os.path.join(resources_folder_path, 'gif/folder1/file.gif'),
                        modality='video',
                    )
                ]
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
    assert TAG_OCR_DETECTOR_TEXT_IN_DOC not in result[0].chunks[0].chunks[0].tags
    assert TAG_OCR_DETECTOR_TEXT_IN_DOC in result[1].chunks[0].chunks[0].tags
