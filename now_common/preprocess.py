from docarray import Document, DocumentArray

from now.data_loading.convert_datasets_to_jpeg import to_thumbnail_jpg
from now.data_loading.utils import transform_es_data
from now.now_dataclasses import UserInput


def preprocess_images(da: DocumentArray) -> DocumentArray:
    """Loads all documents into memory to thumbnail them."""

    def convert_fn(d: Document):
        try:
            if d.tensor is None:
                if d.blob != b'':
                    d.convert_blob_to_image_tensor()
                elif d.uri:
                    d.load_uri_to_image_tensor()
            return to_thumbnail_jpg(d)
        except:
            return d

    for d in da:
        convert_fn(d)
    return DocumentArray(d for d in da if d.blob != b'')


def preprocess_text(da: DocumentArray, split_by_sentences=False) -> DocumentArray:
    """If necessary, loads text for all documents. If asked for, splits documents by sentences."""
    import nltk

    nltk.download('punkt', quiet=True)
    from nltk.tokenize import sent_tokenize

    def convert_fn(d: Document):
        try:
            if not d.text:
                if d.uri:
                    d.load_uri_to_text()
                    d.tags['additional_info'] = d.uri
            return d
        except:
            return d

    def gen_split_by_sentences():
        def _get_sentence_docs(batch):
            ret = []
            for d in batch:
                try:
                    ret += [
                        Document(
                            mime_type='text',
                            text=sentence,
                            tags=d.tags,
                        )
                        for sentence in set(sent_tokenize(d.text.replace('\n', ' ')))
                    ]
                except:
                    pass
            return ret

        for batch in da.map_batch(_get_sentence_docs, backend='process', batch_size=64):
            for d in batch:
                yield d

    for d in da:
        convert_fn(d)

    if split_by_sentences:
        da = DocumentArray(d for d in gen_split_by_sentences())

    return DocumentArray(d for d in da if d.text and d.text != '')


def preprocess_nested_docs(da: DocumentArray, user_input: UserInput) -> DocumentArray:
    """
    Process a `DocumentArray` with `Document`s that have `chunks` of nested `Document`s.
    It constructs `Document`s containg two chunks: one containing image data and another
    containing text data. Fields for indexing should be specified in the `UserInput`.

    :param da: A `DocumentArray` containing nested chunks.
    :param user_input: The configured user input.
    :return: A `DocumentArray` with `Document`s containing text and image chunks.
    """
    fields = user_input.task_config.indexer_scope
    transformed_da = transform_es_data(da)
    return DocumentArray(
        [
            Document(
                chunks=[
                    Document(text=doc[fields['text']][0]),
                    Document(uri=doc[fields['image']][0]),
                ]
            )
            for doc in transformed_da
        ]
    )
