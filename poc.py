from docarray import dataclass
from docarray.typing import Image, Text
from jina import Document, DocumentArray, Executor, Flow, requests


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

doc = Document(page)
# transformed_doc = transform_docarray(DocumentArray([doc]), search_fields=['image', 'description'])
# print(transformed_doc)


class FooExecutor(Executor):
    @requests
    async def foo(self, docs: DocumentArray, **kwargs):
        print(f'foo was here and got {len(docs)} document')
        docs[0].chunks.summary()


class ImageExecutor(Executor):
    @requests
    async def bar(self, docs: DocumentArray, **kwargs):
        print(f'Expecting only `image` chunk here')
        docs[0].chunks.summary()


class DescriptionExecutor(Executor):
    @requests
    async def baz(self, docs: DocumentArray, **kwargs):
        print(f'Expecting only `description` chunk here')
        docs[0].chunks.summary()


f = (
    Flow()
    .add(uses=FooExecutor, name='fooExecutor')
    .add(
        uses=ImageExecutor,
        name='barExecutor',
        needs='fooExecutor',
        when={'main_text': {'$exists': True}},
    )
    .add(
        uses=DescriptionExecutor,
        name='bazExecutor',
        needs='fooExecutor',
        when={'image': {'$exists': True}},
    )
    .needs_all(name='join')
)  # Create Flow with parallel Executors

#                                   exec1
#                                 /      \
# Flow topology: Gateway --> first        join --> Gateway
#                                 \      /
#                                  exec2

input_doc = DocumentArray(
    [Document(chunks=[Document(tags={'key': 5}), Document(tags={'key': 4})])]
)

with f:
    ret = f.post(
        on='/search',
        inputs=doc,
    )

print(ret[:, 'tags'])  # Each Document satisfies one parallel branch/filter
