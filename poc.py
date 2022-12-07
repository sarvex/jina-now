from docarray import dataclass
from docarray.typing import Image, Text
from jina import Document, DocumentArray, Executor, Flow, requests

from now.constants import Apps, DatasetTypes
from now.executor.preprocessor import NOWPreprocessor
from now.now_dataclasses import UserInput

app = Apps.IMAGE_TEXT_RETRIEVAL

user_input = UserInput()
user_input.dataset_type = DatasetTypes.S3_BUCKET
user_input.dataset_path = 's3://bucket/folder'
user_input.search_fields = ['main_text', 'image', 'description']


@dataclass
class Page:
    main_text: Text
    image: Image
    description: Text


page = Page(
    main_text='Hello world',
    image='https://jina.ai/assets/images/text-to-image-output.png',
    description='This is the image of an apple',
)

multimodal_doc = DocumentArray([Document(page) for _ in range(1)])

single_modal = DocumentArray(
    [
        Document(text='hi'),
        Document(text='hello'),
    ]
)

"""
Either manually call the preprocessor and transform the data or use it in the flow
"""
executor = NOWPreprocessor(app=app)
transformed_doc = executor.preprocess(
    docs=single_modal, parameters={'app': app, 'user_input': user_input.__dict__}
)


class FooExecutor(Executor):
    @requests
    async def foo(self, docs: DocumentArray, **kwargs):
        print(f'foo was here and got {len(docs)} document')
        docs[0].chunks.summary()


class ImageExecutor(Executor):
    @requests
    async def bar(self, docs: DocumentArray, **kwargs):
        print(
            f'Expecting only `image` modality here and got {len(docs)} documents',
            docs.summary(),
        )


class DescriptionExecutor(Executor):
    @requests
    async def baz(self, docs: DocumentArray, **kwargs):
        print(
            f'Expecting only `text` modality here and got {len(docs)} documents',
            docs.summary(),
        )


f = (
    Flow()
    .add(uses=NOWPreprocessor, uses_with={'app': app}, name='preprocessor')
    .add(uses=FooExecutor, name='fooExecutor')
    .add(
        uses=ImageExecutor,
        name='ImageExecutor',
        needs='fooExecutor',
        when={'tags__modality': {'$eq': 'image'}},
    )
    .add(
        uses=DescriptionExecutor,
        name='DescriptionExecutor',
        needs='fooExecutor',
        when={'tags__modality': {'$eq': 'text'}},
    )
    .needs_all(name='join')
)  # Create Flow with parallel Executors

#                                                   exec1
#                                                 /      \
# Flow topology: Gateway --> preprocessor --> first        join --> Gateway
#                                                 \      /
#                                                  exec2


with f:
    ret = f.post(
        on='/search',
        inputs=multimodal_doc,
        parameters={'user_input': user_input.__dict__},
    )
