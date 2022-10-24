from typing import List

from docarray import Document, DocumentArray

from now.data_loading.convert_datasets_to_jpeg import to_thumbnail_jpg
from now.data_loading.utils import transform_docarray
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
        for chunk in d.chunks:
            if chunk.modality == 'image':
                convert_fn(chunk)
    return da


def preprocess_text(
    da: DocumentArray,
    split_by_sentences=False,
) -> DocumentArray:
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

    def gen_split_by_sentences(document):
        ret = []
        try:
            ret += [
                Document(
                    mime_type='text',
                    modality='text',
                    text=sentence,
                    tags=d.tags,
                )
                for sentence in set(sent_tokenize(document.text.replace('\n', ' ')))
                if sentence
            ]
        except:
            pass
        return ret

    for d in da:
        for chunk in d.chunks:
            if chunk.modality == 'text':
                convert_fn(chunk)
                if split_by_sentences:
                    chunk.chunks = gen_split_by_sentences(chunk)
    return da


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
    texts, uris = [], []
    for doc in da:
        for chunk in doc.chunks:
            if chunk.tags['field_name'] == fields['text']:
                texts.append(chunk.content)
            elif chunk.tags['field_name'] == fields['image']:
                uris.append(chunk.uri)
    return DocumentArray(
        [
            Document(
                chunks=[
                    Document(text=text),
                    Document(uri=uri),
                ]
            )
            for text, uri in zip(texts, uris)
        ]
    )
