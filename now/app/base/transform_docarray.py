import os
from typing import Dict, List, Union

from docarray import Document, DocumentArray

from now.constants import FILETYPE_TO_MODALITY, Modalities


def _get_modality(document: Document):
    """Detect document's modality based on its `modality` or `mime_type` attributes."""
    if document.modality:
        return document.modality
    elif document.mime_type:
        file_from_mime_type = document.mime_type.split('/')[-1]
        return FILETYPE_TO_MODALITY[file_from_mime_type]
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


def transform_multi_modal_data(
    documents: DocumentArray, field_names: Dict[int, str], index_fields: List[str]
):
    """
    Transforms multimodal data into standardized format, which looks like this:
    Document(
        tags={'color': 'red', 'author': 'me'},
        chunks=[
            Document(
                text='jina ai', tags={'color': 'red', 'author': 'me'}
            ),
            Document(
                uri='pic.jpg', tags={'color': 'red', 'author': 'me'}
            ),
        ],
    )
    """
    for document in documents:
        new_chunks = []
        for position, chunk in enumerate(document.chunks):
            field_name = field_names[position]
            modality = chunk.modality or _get_modality(chunk)
            if field_name in index_fields:
                new_chunks.append(
                    Document(
                        content=chunk.content,
                        uri=chunk.uri,
                        modality=modality,
                        mime_type=modality,
                        tags={'field_name': field_name},
                    )
                )
            else:
                if chunk.text:
                    document.tags[field_name] = chunk.content
        document.chunks = new_chunks
        for chunk in document.chunks:
            chunk.tags.update(document.tags)

    return documents


def transform_docarray(
    documents: Union[Document, DocumentArray],
    index_fields: List[str],
) -> DocumentArray:
    """
    Gets either multimodal or unimodal data and turns it into standardized format.

    :param documents: Data to be transformed.
    :param index_fields: Field names for neural search. Only required if multimodal data is given.
    """
    if not (documents and documents[0].chunks):
        documents = transform_uni_modal_data(documents=documents)
    else:
        for doc in documents:
            for chunk in doc.chunks:
                chunk.modality = chunk.modality or _get_modality(chunk)
    return documents
