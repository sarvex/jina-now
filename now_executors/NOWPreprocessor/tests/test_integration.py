import json

from docarray import Document, DocumentArray
from executor import NOWPreprocessor
from jina import Flow
from now_common.options import _construct_app

from now.constants import Apps, Modalities
from now.now_dataclasses import UserInput


def test_executor_persistence():
    e = NOWPreprocessor('image_to_image', metas={'workspace': './workspace'})
    user_input = UserInput()
    text_docs = DocumentArray(
        [
            Document(text='test'),
            Document(uri='test.jpg'),
            # chunks=DocumentArray([Document(uri='test.jpg'), Document(text='hi')])
            # ),
        ]
    )

    ret = e.index(
        docs=text_docs,
        parameters={'user_input': user_input.__dict__, 'is_indexing': False},
    )
    with open(e.user_input_path, 'r') as fp:
        json.load(fp)


# def test_text_image_encoding():


def test_text_to_video():
    app = Apps.TEXT_TO_VIDEO

    user_input = UserInput()
    user_input.output_modality = Modalities.VIDEO
    user_input.app_instance = _construct_app(app)
    user_input.data = 'custom'
    user_input.dataset_path = '/Users/joschkabraun/dev/now/da_tgif.30000.bin'

    text_docs = DocumentArray(
        [
            Document(text='test'),
            Document(
                chunks=DocumentArray([Document(uri='test.jpg'), Document(text='hi')])
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

    assert len(result) == 1
    assert len(result[0].chunks) == 1
