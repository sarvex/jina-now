from typing import Dict

from jina import Document, DocumentArray, Executor, requests

CLOUD_BUCKET_PRE_FIXES = ['s3://']


class NOWPostprocessor(Executor):
    """Post-processes any documents after encoding such that they are ready to be indexed, used as query, ...

    For indexing, itdrops `blob`, `tensor` attribute from documents which have `uri` attribute whose 'uri' is either in
    the cloud or can be loaded.
    """

    def __init__(self, traversal_paths: str = "@r", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.traversal_paths = traversal_paths

    @requests(on='/index')
    def maybe_drop_blob_tensor(
        self, docs: DocumentArray, parameters: Dict = {}, **kwargs
    ):
        traversal_paths = parameters.get("traversal_paths", self.traversal_paths)
        for doc in docs[traversal_paths]:
            if doc.uri:
                if doc.text:
                    continue
                else:
                    try:
                        if not doc.uri.startswith(f'data:{doc.mime_type}') and not any(
                            [
                                doc.uri.startswith(cloud_bucket_prefix)
                                for cloud_bucket_prefix in CLOUD_BUCKET_PRE_FIXES
                            ]
                        ):
                            doc.load_uri_to_blob()
                        doc.blob = None
                        doc.tensor = None
                    except FileNotFoundError:
                        continue

        return docs


if __name__ == '__main__':
    post_processor = NOWPostprocessor(traversal_paths='@c')

    doc_blob = Document(
        uri='https://upload.wikimedia.org/wikipedia/commons/thumb/b/b3/Wikipedia-logo-v2-en.svg/270px-Wikipedia-logo-v2-en.svg.png'
    )
    doc_blob.load_uri_to_blob()

    doc_tens = Document(
        uri='https://upload.wikimedia.org/wikipedia/commons/thumb/b/b3/Wikipedia-logo-v2-en.svg/270px-Wikipedia-logo-v2-en.svg.png'
    )
    doc_tens.load_uri_to_image_tensor()

    docs_pre = DocumentArray(
        [
            Document(
                chunks=DocumentArray(
                    [
                        Document(text='hi'),
                        Document(blob=b'b12'),
                        Document(blob=b'b12', uri='file_will.never_exist'),
                        doc_blob,
                        doc_tens,
                    ]
                )
            )
        ]
    )
    from copy import deepcopy

    docs_post = post_processor.maybe_drop_blob_tensor(deepcopy(docs_pre))

    for doc_pre, doc_post in zip(docs_pre, docs_post):
        print(f"document before post-processing")
        doc_pre.summary()
        print(f"document after post-processing")
        doc_post.summary()

        print('----------------\n')
