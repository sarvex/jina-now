import base64
import os
import pathlib
import pickle
from os.path import join as osp
from typing import List, Dict, Union

from docarray import DocumentArray, Document, dataclass
from docarray.typing import Image, Text

from now.constants import BASE_STORAGE_URL, DEMO_DATASET_DOCARRAY_VERSION, Modalities
from now.utils import download
import logging

logger = logging.getLogger(__name__)


def fetch_da_from_url(
    url: str, downloaded_path: str = '~/.cache/jina-now'
) -> DocumentArray:
    data_dir = os.path.expanduser(downloaded_path)
    if not os.path.exists(osp(data_dir, 'data/tmp')):
        os.makedirs(osp(data_dir, 'data/tmp'))
    data_path = (
        data_dir
        + f"/data/tmp/{base64.b64encode(bytes(url, 'utf-8')).decode('utf-8')}.bin"
    )
    if not os.path.exists(data_path):
        download(url, data_path)

    try:
        da = DocumentArray.load_binary(data_path)
    except pickle.UnpicklingError:
        path = pathlib.Path(data_path).expanduser().resolve()
        os.remove(path)
        download(url, data_path)
        da = DocumentArray.load_binary(data_path)
    return da


def get_dataset_url(dataset: str, output_modality: Modalities) -> str:
    data_folder = None
    docarray_version = DEMO_DATASET_DOCARRAY_VERSION
    if output_modality == Modalities.IMAGE:
        data_folder = 'jpeg'
    elif output_modality == Modalities.TEXT:
        data_folder = 'text'
    elif output_modality == Modalities.MUSIC:
        data_folder = 'music'
    elif output_modality == Modalities.VIDEO:
        data_folder = 'video'
    elif output_modality == Modalities.TEXT_AND_IMAGE:
        data_folder = 'text-image'
    if output_modality not in [
        Modalities.MUSIC,
        Modalities.VIDEO,
        Modalities.TEXT_AND_IMAGE,
    ]:
        model_name = 'ViT-B32'
        return f'{BASE_STORAGE_URL}/{data_folder}/{dataset}.{model_name}-{docarray_version}.bin'
    else:
        return f'{BASE_STORAGE_URL}/{data_folder}/{dataset}-{docarray_version}.bin'


def _get_modality(document):
    if document.uri:
        if os.path.splitext(document.uri)[-1] == '.gif':
            return Modalities.VIDEO
        else:
            return Modalities.IMAGE
    elif document.text:
        return Modalities.TEXT
    elif document.blob:
        return Modalities.IMAGE
    else:
        raise Exception(f'{document} modality can not be detected. {document.uri}')


def transform_uni_modal_data(documents: DocumentArray, filter_fields: List[str]):
    @dataclass
    class BaseDocImage:
        default_field: Image

    @dataclass
    class BaseDocText:
        default_field: Text

    transformed_docs = DocumentArray()
    for document in documents:
        modality = document.modality or _get_modality(document)
        if modality == Modalities.TEXT:
            new_doc = BaseDocText(default_field=document.text)
        elif modality in [Modalities.IMAGE, Modalities.VIDEO]:
            new_doc = BaseDocImage(
                default_field=document.content or document.blob or document.uri
            )
        else:
            raise ValueError(f'Modality {modality} is not supported!')
        new_doc = Document(new_doc)
        new_doc.tags['filter_fields'] = {}
        new_doc.chunks[0].tags['field_name'] = 'default_field'
        new_doc.chunks[0].embedding = document.embedding
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
