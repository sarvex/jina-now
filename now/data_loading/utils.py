import base64
import os
import pathlib
import pickle
from os.path import join as osp
from typing import List, Dict

from docarray import DocumentArray, Document

from now.constants import BASE_STORAGE_URL, DEMO_DATASET_DOCARRAY_VERSION, Modalities
from now.utils import download


def _fetch_da_from_url(
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


def _transform_single_modal_data(filter_fields: List[str]):
    def _transform_fn(document: Document) -> Document:
        document.tags['filtered_fields'] = {}
        doc_tags = document.tags.copy()
        for field, value in doc_tags.items():
            if field in filter_fields:
                document.tags.pop(field)
                document.tags['filtered_fields'][field] = value
        document.chunks = [
            Document(
                content=document.content,
                modality=document.modality,
                tags={'field_name': 'default_field_name'},
            )
        ]
        return document

    return _transform_fn


def _transform_multi_modal_data(
    field_names: Dict[int, str], search_fields: List[str], filter_fields: List[str]
):
    def _transform_fn(document: Document) -> Document:
        document.tags['filtered_fields'] = {}
        new_chunks = []
        for position, chunk in enumerate(document.chunks):
            field_name = field_names[position]
            content = chunk.content
            modality = chunk.modality
            if chunk.chunks:
                content = [sub_chunk.content for sub_chunk in chunk.chunks]
                modality = chunk.chunks[0].modality
            if field_name in search_fields:
                new_chunks.append(
                    Document(
                        content=content,
                        modality=modality,
                        tags={'field_name': field_name},
                    )
                )
            elif field_name in filter_fields:
                document.tags['filtered_fields'][field_name] = content
            else:
                document.tags[field_name] = content
        document.chunks = new_chunks
        return document

    return _transform_fn


def transform_docarray(
    documents: DocumentArray, search_fields: List[str], filter_fields: List[str]
) -> DocumentArray:
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
        documents.apply(_transform_single_modal_data(filter_fields=filter_fields))
    return documents
