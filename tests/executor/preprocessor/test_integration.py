import json
import os

from docarray import Document, DocumentArray
from jina import Flow
from now.app.text_to_image.app import TextToImage
from now.data_loading.data_loading import load_data
from now.executor.preprocessor import NOWPreprocessor

from now.demo_data import DemoDatasetNames
from now.constants import Apps, DatasetTypes
from now.now_dataclasses import UserInput


def test_executor_persistence(tmpdir, resources_folder_path):
    e = NOWPreprocessor(Apps.TEXT_TO_TEXT, metas={'workspace': tmpdir})
    user_input = UserInput()
    text_docs = DocumentArray(
        [
            Document(text='test'),
            Document(
                uri=os.path.join(resources_folder_path, 'image', '6785325056.jpg')
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
            Document(text='test'),
            Document(uri=os.path.join(resources_folder_path, 'gif/folder1/file.gif')),
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
        len([res.chunks[0].text for res in encode_result if res.chunks[0].text != ''])
        == 1
    )
    assert (
        len(
            [
                res.chunks[0].chunks[0].blob
                for res in encode_result
                if res.chunks[0].chunks
            ]
        )
        == 1
    )


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
