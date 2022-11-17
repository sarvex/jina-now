import json
import os

from docarray import Document, DocumentArray
from jina import Flow

from now.constants import Apps
from now.executor.preprocessor import NOWPreprocessor
from now.now_dataclasses import UserInput


def test_executor_persistence(tmpdir):
    e = NOWPreprocessor(Apps.SENTENCE_TO_SENTENCE, metas={'workspace': tmpdir})
    user_input = UserInput()
    text_docs = DocumentArray(
        [
            Document(text='test'),
            Document(
                chunks=DocumentArray([Document(uri='test.jpg'), Document(text='hi')])
            ),
        ]
    )

    e.index(
        docs=text_docs,
        parameters={'user_input': user_input.__dict__, 'is_indexing': False},
    )
    with open(e.user_input_path, 'r') as fp:
        json.load(fp)


def test_text_to_video(resources_folder_path):
    app = Apps.TEXT_TO_VIDEO
    user_input = UserInput()
    text_docs = DocumentArray(
        [
            Document(chunks=[Document(text='test')]),
            Document(
                uri=os.path.join(resources_folder_path, 'gif', 'folder1/file.gif')
            ),
        ]
    )

    with Flow().add(uses=NOWPreprocessor, uses_with={'app': app}) as f:
        result = f.post(
            on='/search',
            inputs=text_docs,
            parameters={'user_input': user_input.__dict__},
            show_progress=True,
        )
        result = DocumentArray.from_json(result.to_json())

        encode_result = f.post(
            on='/encode',
            inputs=text_docs,
            parameters={'user_input': user_input.__dict__},
            show_progress=True,
        )
        encode_result = DocumentArray.from_json(encode_result.to_json())

    assert len(result) == 1
    assert len(encode_result) == 2
    assert (
        len(
            [
                chunk.text
                for doc in encode_result
                for chunk in doc.chunks
                if chunk.text != ''
            ]
        )
        == 1
    )
    assert len([blob for blob in encode_result.blobs if blob != b'']) == 1


def test_user_input_preprocessing():
    user_input = {'indexer_scope': {'text': 'title', 'image': 'uris'}}
    with Flow().add(
        uses=NOWPreprocessor, uses_with={'app': Apps.TEXT_TO_TEXT_AND_IMAGE}
    ) as f:
        result = f.post(
            on='/index',
            inputs=DocumentArray([Document(text='test')]),
            parameters={'user_input': user_input},
            show_progress=True,
        )
        result = DocumentArray.from_json(result.to_json())
