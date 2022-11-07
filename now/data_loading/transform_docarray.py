import os
from typing import List, Dict, Union

from docarray import dataclass, Document, DocumentArray
from docarray.typing import Image, Text, Audio, Blob

from now.app.music_to_music.app import MusicToMusic
from now.constants import Modalities


def _get_modality(document):
    for modality in Modalities():
        if modality in document.modality or modality in document.mime_type:
            return modality
    return None


def _get_multi_modal_format(document):
    @dataclass
    class BaseDocImage:
        default_field: Image

    @dataclass
    class BaseDocText:
        default_field: Text

    @dataclass
    class BaseDocMusic:
        default_field: Audio

    @dataclass
    class BaseDocBlob:
        default_field: Blob

    modality = _get_modality(document)
    if document.blob:
        new_doc = BaseDocBlob(default_field=document.blob)
    elif document.uri:
        file_type = os.path.splitext(document.uri)[-1].replace('.', '')
        # if file_type in TextToVideo().supported_file_types:
        #     return Modalities.VIDEO
        if file_type in MusicToMusic().supported_file_types:
            new_doc = BaseDocMusic(default_field=document.uri)
        else:
            new_doc = BaseDocImage(default_field=document.uri)
    elif document.text:
        new_doc = BaseDocText(default_field=document.text)
    else:
        raise Exception(f'{document} modality can not be detected.')
    new_doc = Document(new_doc)
    if modality:
        new_doc.chunks[0].modality = modality
    return new_doc


def transform_uni_modal_data(documents: DocumentArray, filter_fields: List[str]):
    transformed_docs = DocumentArray()
    for document in documents:
        new_doc = _get_multi_modal_format(document)
        new_doc.tags['filter_fields'] = {}
        new_doc.chunks[0].tags['field_name'] = 'default_field'
        new_doc.chunks[0].embedding = document.embedding
        # if modality == 'blob':
        #     new_doc.chunks[0].modality = document.mime_type
        for field, value in document.tags.items():
            if field in filter_fields:
                new_doc.tags['filter_fields'][field] = value
            else:
                new_doc.tags[field] = value
        if 'uri' in new_doc.tags:
            new_doc.chunks[0].uri = new_doc.tags['uri']
        transformed_docs.append(new_doc)

    return transformed_docs


def _transform_multi_modal_data(
    field_names: Dict[int, str], search_fields: List[str], filter_fields: List[str]
):
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
        return document

    return _transform_fn


def transform_docarray(
    documents: Union[Document, DocumentArray],
    search_fields: List[str],
    filter_fields: List[str],
) -> DocumentArray:
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
