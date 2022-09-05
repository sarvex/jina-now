import dataclasses
import json

from docarray import Document, DocumentArray
from executor import NOWPreprocessor
from jina import Flow

from now.constants import Apps, Modalities
from now.now_dataclasses import UserInput


def test_executor_persistance():
    e = NOWPreprocessor('text_to_text', metas={'workspace': './workspace'})
    user_input = UserInput()
    text_docs = DocumentArray(
        [
            Document(text='test'),
            Document(
                chunks=DocumentArray([Document(uri='test.jpg'), Document(text='hi')])
            ),
        ]
    )

    e.index(docs=text_docs, parameters={'user_input': dataclasses.asdict(user_input)})
    with open(e.user_input_path, 'r') as fp:
        json.load(fp)


def test__text_to_video():
    app = Apps.TEXT_TO_VIDEO

    user_inpuT = UserInput()
    user_inpuT.output_modality = Modalities.VIDEO
    user_inpuT.app = app
    user_inpuT.data = 'custom'
    user_inpuT.is_custom_dataset = True
    user_inpuT.dataset_path = '/Users/joschkabraun/dev/now/da_tgif.30000.bin'

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
            parameters={'user_input': dataclasses.asdict(user_inpuT)},
            show_progress=True,
        )

        result = DocumentArray.from_json(result.to_json())

    assert len(result) == 1
    assert len(result[0].chunks) == 1
