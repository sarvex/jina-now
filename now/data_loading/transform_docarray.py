import os
from typing import List, Dict, Union

from docarray import dataclass, Document, DocumentArray
from docarray.typing import Image, Text, Blob

from now.constants import Modalities


def _get_modality(document):
    """
    Detect document's modality based on its `modality` or `mime_type` attributes.
    """
    for modality in Modalities():
        if modality in document.modality or modality in document.mime_type:
            return modality
    return None


def _get_multi_modal_format(document):
    """
    Create a multimodal docarray dataclass from a unimodal `Document`,
    and trasnform it back to a `Document` which will have a `multi_modal_schema`.
    """
    from now.app.text_to_video.app import TextToVideo

    @dataclass
    class BaseDocImage:
        default_field: Image

    @dataclass
    class BaseDocText:
        default_field: Text

    @dataclass
    class BaseDocBlob:
        default_field: Blob

    modality = _get_modality(document)
    if document.blob:
        new_doc = BaseDocBlob(default_field=document.blob)
    elif document.uri:
        file_type = os.path.splitext(document.uri)[-1].replace('.', '')
        if (
            modality == Modalities.VIDEO
            or file_type in TextToVideo().supported_file_types
        ):
            new_doc = BaseDocText(default_field=document.uri)
            modality = Modalities.VIDEO
        else:
            new_doc = BaseDocImage(default_field=document.uri)
    elif document.text:
        new_doc = BaseDocText(default_field=document.text)
    else:
        raise Exception(f'Document {document} cannot be transformed.')
    new_doc = Document(new_doc)
    if modality:
        new_doc.chunks[0].modality = modality
    return new_doc


def transform_uni_modal_data(documents: DocumentArray, filter_fields: List[str]):
    """
    Transform unimodal `Documents` into standardized format, which looks like this:
    Document(
        tags={'filter_fields': {'color': 'red'}, 'author': 'me'},
        chunks=[
            Document(
                text='jina ai', tags={'filter_fields': {'color': 'red'}, 'author': 'me'}
            )
        ],
    )
    """
    transformed_docs = DocumentArray()
    for document in documents:
        new_doc = _get_multi_modal_format(document)
        new_doc.tags['filter_fields'] = {}
        new_doc.chunks[0].tags['filter_fields'] = {}
        new_doc.chunks[0].tags['field_name'] = 'default_field'
        new_doc.chunks[0].embedding = document.embedding
        for field, value in document.tags.items():
            if field in filter_fields:
                new_doc.tags['filter_fields'][field] = value
                new_doc.chunks[0].tags['filter_fields'][field] = value
            else:
                new_doc.tags[field] = value
                new_doc.chunks[0].tags[field] = value
        if 'uri' in new_doc.tags:
            new_doc.chunks[0].uri = new_doc.tags['uri']
        transformed_docs.append(new_doc)

    return transformed_docs


def _transform_multi_modal_data(
    field_names: Dict[int, str], search_fields: List[str], filter_fields: List[str]
):
    """
    Transforms multimodal data into standardized format, which looks like this:
    Document(
        tags={'filter_fields': {'color': 'red'}, 'author': 'me'},
        chunks=[
            Document(
                text='jina ai', tags={'filter_fields': {'color': 'red'}, 'author': 'me'}
            ),
            Document(
                uri='pic.jpg', tags={'filter_fields': {'color': 'red'}, 'author': 'me'}
            ),
        ],
    )
    """

    def _transform_fn(document: Document) -> Document:
        document.tags['filter_fields'] = {}
        new_chunks = []
        for position, chunk in enumerate(document.chunks):
            field_name = field_names[position]
            content = chunk.content
            modality = chunk.modality or _get_modality(chunk)
            if chunk.chunks:
                content = [sub_chunk.content for sub_chunk in chunk.chunks]
                modality = chunk.chunks[0].modality or _get_modality(chunk.chunks[0])
            if field_name in search_fields:
                new_chunks.append(
                    Document(
                        content=content,
                        uri=chunk.uri,
                        modality=modality,
                        tags={'field_name': field_name},
                    )
                )
            elif field_name in filter_fields:
                document.tags['filter_fields'][field_name] = content
            else:
                document.tags[field_name] = content
        document.chunks = new_chunks
        for chunk in document.chunks:
            chunk.tags.update(document.tags)
        return document

    return _transform_fn


def transform_docarray(
    documents: Union[Document, DocumentArray],
    search_fields: List[str],
    filter_fields: List[str],
) -> DocumentArray:
    """
    Gets either multimodal or unimodal data and turns it into standardized format.

    :param documents: Data to be transformed.
    :param search_fields: Field names for neural search. Only required if multimodal data is given.
    :param filter_fields: Field names for filtering.
    """
    if not documents:
        return documents
    if isinstance(documents, Document):
        documents = DocumentArray(documents)
    if documents[0].chunks:
        if 'multi_modal_schema' not in documents[0]._metadata:
            raise KeyError(
                'Multi-modal schema is not provided. Please prepare your data following this guide - '
                'https://docarray.jina.ai/datatypes/multimodal/'
            )
        field_names = {
            int(field_info['position']): field_name
            for field_name, field_info in documents[0]
            ._metadata['multi_modal_schema']
            .items()
        }
        documents.apply(
            _transform_multi_modal_data(
                field_names=field_names,
                search_fields=search_fields,
                filter_fields=filter_fields,
            )
        )
    else:
        documents = transform_uni_modal_data(
            documents=documents, filter_fields=filter_fields
        )
    return documents
