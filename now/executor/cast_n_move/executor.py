from docarray import Document
from jina import DocumentArray, Executor, requests
from torch import Tensor, cat, zeros


class CastNMoveNowExecutor(Executor):
    """If a document has an embedding attribute, it moves it to the tensor attribute and casts it to torch.Tensor.
    Also pads the document if it doesn't fit the size:
    - prepends zeros to the tensor if it is a text document
    - appends zeros to the tensor else"""

    def __init__(self, output_size: int, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.output_size = output_size

    @requests
    def cast_move_pad(self, docs: DocumentArray, **kwargs):
        def cast_move_pad(doc: Document):
            if doc.embedding is not None:
                t = Tensor(doc.embedding)
                if doc.text:
                    if not doc.mime_type or doc.mime_type == '':
                        doc.mime_type = 'text'
                    doc.convert_text_to_datauri()
                    doc.tensor = cat([zeros(self.output_size - len(t)), t])
                else:
                    # need to save blob into datauri as tensr is a content attribute
                    if doc.blob != b'':
                        if not doc.mime_type or doc.mime_type == '':
                            doc.mime_type = 'image'
                        doc.convert_blob_to_datauri()
                    doc.tensor = cat([t, zeros(self.output_size - len(t))])
            return doc

        docs.apply(cast_move_pad)


if __name__ == '__main__':
    executor = CastNMoveNowExecutor(512)
    # executor = Executor.from_hub('jinahub://CastNMoveNowExecutor/latest', uses_with={'input_size': 512})
    import numpy as np

    # doc = Document(uri='/Users/joschkabraun/dev/data/elephants/images.jpeg', embedding=np.array([1] * 256))
    # doc.load_uri_to_blob()
    doc = Document(text='hi', embedding=np.array([1] * 256))

    doc.mime_type = None
    doc.summary()

    inpt = DocumentArray([doc])
    outpt = executor.cast_move_pad(inpt)
    inpt.summary()
    inpt = inpt[0]
    inpt.summary()
    print("embedding", type(inpt.embedding), inpt.embedding.shape)
    print("uri", type(inpt.uri), inpt.uri)
    print("tensor", type(inpt.tensor), inpt.tensor.shape)
    inpt.load_uri_to_text(timeout=10)
    inpt.summary()
