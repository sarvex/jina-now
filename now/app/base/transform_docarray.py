import os
from typing import Union

from docarray import Document, DocumentArray

from now.constants import FILETYPE_TO_MODALITY, Modalities


def _get_modality(document: Document):
    """
    Detect document's modality based on its `modality` or `mime_type` attributes.
    """
    for modality in Modalities():
        if modality in document.modality or modality in document.mime_type:
            return modality
    return None


def _get_multi_modal_format(document: Document) -> Document:
    """
    Create a multimodal docarray structure from a unimodal `Document`.
    """
    modality = _get_modality(document)
    if document.blob:
        new_doc = Document(chunks=[Document(blob=document.blob)])
    elif document.uri:
        file_type = os.path.splitext(document.uri)[-1].replace('.', '')
        modality = FILETYPE_TO_MODALITY[file_type]
        new_doc = Document(chunks=[Document(uri=document.uri)])
    elif document.text:
        new_doc = Document(chunks=[Document(text=document.text)])
        modality = Modalities.TEXT
    elif document.tensor:
        new_doc = Document(chunks=[Document(tensor=document.tensor)])
    else:
        document.summary()
        raise Exception(f'Document {document} cannot be transformed.')
    if modality:
        new_doc.chunks[0].modality = modality
    return new_doc


def transform_uni_modal_data(documents: DocumentArray) -> DocumentArray:
    """
    Transform unimodal `Documents` into standardized format, which looks like this:
    Document(
        tags={'color': 'red', 'author': 'me'},
        chunks=[
            Document(
                text='jina ai', tags={'color': 'red', 'author': 'me'}
            )
        ],
    )
    """
    transformed_docs = DocumentArray()
    for document in documents:
        new_doc = _get_multi_modal_format(document)
        new_doc.chunks[0].tags['field_name'] = 'default_field'
        new_doc.chunks[0].embedding = document.embedding
        new_doc.tags = document.tags
        new_doc.chunks[0].tags.update(document.tags)
        new_doc.chunks[0].mime_type = new_doc.chunks[0].modality
        transformed_docs.append(new_doc)

    return transformed_docs


def transform_docarray(
    documents: Union[Document, DocumentArray],
) -> DocumentArray:
    """
    Gets either multimodal or unimodal data and turns it into standardized format.

    :param documents: Data to be transformed.
    :return: Transformed data.
    """
    if not (documents and documents[0].chunks):
        documents = transform_uni_modal_data(documents=documents)
    else:
        for doc in documents:
            for chunk in doc.chunks:
                chunk.modality = chunk.modality or _get_modality(chunk)
    return documents
