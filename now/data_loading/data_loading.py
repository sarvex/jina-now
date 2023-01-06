import itertools
import json
import os
from collections import defaultdict
from copy import deepcopy
from tempfile import TemporaryDirectory
from typing import Dict, List, Type

import filetype
from docarray import Document, DocumentArray
from docarray.dataclasses import is_multimodal

from now.common.detect_schema import (
    get_first_file_in_folder_structure_s3,
    get_s3_bucket_and_folder_prefix,
)
from now.constants import (
    AVAILABLE_MODALITIES_FOR_SEARCH,
    SUPPORTED_FILE_TYPES,
    DatasetTypes,
)
from now.data_loading.create_dataclass import (
    create_dataclass,
    create_dataclass_fields_file_mappings,
)
from now.data_loading.elasticsearch import ElasticsearchExtractor
from now.log import yaspin_extended
from now.now_dataclasses import UserInput
from now.utils import docarray_typing_to_modality_string, sigmap


def _get_multi_modal_format(document: Document) -> Document:
    """
    Create a multimodal docarray structure from a unimodal `Document`.
    """
    modality = _get_modality(document)
    if document.blob:
        modality_value = document.blob
    elif document.uri:
        modality_value = document.uri
    elif document.text:
        modality_value = document.text
    elif document.tensor:
        modality_value = document.tensor
    else:
        document.summary()
        raise Exception(f'Document {document} cannot be transformed.')
    return {modality: modality_value}


def _field_dict_to_mm_doc(
    field_dict: dict, data_class: type, field_names_to_dataclass_fields={}
) -> Document:
    """Converts a dictionary of field names to their values to a document.

    :param field_dict: key-value pairs of field names and their values
    :param data_class: @docarray.dataclass class which encapsulates the fields of the multimodal document
    :param field_names_to_dataclass_fields: mapping of field names to data class fields (e.g. {'title': 'text_0'})
    :return: multi-modal document
    """
    if len(field_dict) != 1:
        raise ValueError(
            f"Multi-modal document isn't supported yet. "
            f"Can only set one value but have {list(field_dict.keys())}"
        )

    with TemporaryDirectory() as tmp_dir:
        try:
            if field_names_to_dataclass_fields:
                field_dict_orig = deepcopy(field_dict)
                field_dict = {
                    field_name_data_class: field_dict_orig[file_name]
                    for file_name, field_name_data_class in field_names_to_dataclass_fields.items()
                }
            data_class_kwargs = {}
            for field_name_data_class, field_value in field_dict.items():
                # save blob into a temporary file such that it can be loaded by the multimodal class
                if field_value.blob:
                    file_ending = filetype.guess(field_value.blob)
                    if file_ending is None:
                        raise ValueError(
                            f'Could not guess file type of blob {field_value.blob}. '
                            f'Please provide a valid file type.'
                        )
                    file_ending = file_ending.extension
                    if file_ending not in itertools.chain(
                        *SUPPORTED_FILE_TYPES.values()
                    ):
                        raise ValueError(
                            f'File type {file_ending} is not supported. '
                            f'Please provide a valid file type.'
                        )
                    file_path = os.path.join(
                        tmp_dir, field_name_data_class + '.' + file_ending
                    )
                    with open(file_path, 'wb') as f:
                        f.write(field_value.blob)
                    field_value.uri = file_path
                    data_class_kwargs[field_name_data_class] = field_value.uri

                elif field_value.uri:
                    data_class_kwargs[field_name_data_class] = field_value.uri
                elif field_value.text:
                    data_class_kwargs[field_name_data_class] = field_value.text
                elif field_value.tensor:
                    data_class_kwargs[field_name_data_class] = field_value.tensor

            doc = Document(data_class(**data_class_kwargs))
        except BaseException as e:
            raise Exception(
                f'Not a correctly encoded request. Please see the error stack for more information. \n{e}'
            )

    return doc


def get_da_with_index_fields(da: DocumentArray, user_input: UserInput):
    dataclass = create_dataclass(user_input)
    clean_da = []
    non_index_fields = list(
        set(user_input.index_fields_modalities.keys()) - set(user_input.index_fields)
    )
    for d in da:
        dict_index_fields = {}
        dict_non_index_fields = {}
        dataclass_mappings = create_dataclass_fields_file_mappings(
            user_input.index_fields, user_input.index_fields_modalities
        )
        for field in non_index_fields:
            non_index_field_doc = getattr(d, field)
            dict_non_index_fields.update(_get_multi_modal_format(non_index_field_doc))
        for field in user_input.index_fields:
            _index_field_doc = getattr(d, field)
            dict_index_fields[field] = _index_field_doc
        mm_doc = _field_dict_to_mm_doc(dict_index_fields, dataclass, dataclass_mappings)
        mm_doc.tags.update(dict_non_index_fields)
        clean_da.append(mm_doc)
    clean_da = DocumentArray(clean_da)
    return clean_da


def load_data(user_input: UserInput, data_class=None) -> DocumentArray:
    """Based on the user input, this function will pull the configured DocumentArray dataset ready for the preprocessing
    executor.
    :param user_input: The configured user object. Result from the Jina Now cli dialog.
    :param data_class: The dataclass that should be used for the DocumentArray.
    :return: The loaded DocumentArray.
    """
    da = None
    if user_input.dataset_type == DatasetTypes.DOCARRAY:
        print('⬇  Pull DocumentArray dataset')
        da = _pull_docarray(user_input.dataset_name)
    elif user_input.dataset_type == DatasetTypes.PATH:
        print('💿  Loading files from disk')
        da = _load_from_disk(user_input=user_input, data_class=data_class)
    elif user_input.dataset_type == DatasetTypes.S3_BUCKET:
        da = _list_files_from_s3_bucket(user_input=user_input, data_class=data_class)
    elif user_input.dataset_type == DatasetTypes.ELASTICSEARCH:
        da = _extract_es_data(user_input=user_input, data_class=data_class)
    elif user_input.dataset_type == DatasetTypes.DEMO:
        print('⬇  Download DocumentArray dataset')
        da = DocumentArray.pull(name=user_input.dataset_name, show_progress=True)
    da = set_modality_da(da)
    if da is None:
        raise ValueError(
            f'Could not load DocumentArray dataset. Please check your configuration: {user_input}.'
        )
    if 'NOW_CI_RUN' in os.environ:
        da = da[:50]
    clean_da = get_da_with_index_fields(da)
    return clean_da


def _pull_docarray(dataset_name: str):
    try:
        docs = DocumentArray.pull(name=dataset_name, show_progress=True)
        if is_multimodal(docs[0]):
            return docs
        else:
            raise ValueError(
                f'The dataset {dataset_name} does not contain a multimodal DocumentArray.'
                f'Please check documentation https://docarray.jina.ai/fundamentals/dataclass/construct/'
            )
    except Exception:
        raise ValueError(
            '💔 oh no, the secret of your docarray is wrong, or it was deleted after 14 days'
        )


def _extract_es_data(user_input: UserInput, data_class: Type) -> DocumentArray:
    query = {
        'query': {'match_all': {}},
        '_source': True,
    }
    es_extractor = ElasticsearchExtractor(
        query=query,
        index=user_input.es_index_name,
        user_input=user_input,
        data_class=data_class,
        connection_str=user_input.es_host_name,
    )
    extracted_docs = es_extractor.extract()
    return extracted_docs


def _load_from_disk(user_input: UserInput, data_class: Type) -> DocumentArray:
    """
    Loads the data from disk into multimodal documents.
    :param user_input: The user input object.
    :param data_class: The dataclass to use for the DocumentArray.
    """
    dataset_path = user_input.dataset_path.strip()
    dataset_path = os.path.expanduser(dataset_path)
    if os.path.isfile(dataset_path):
        try:
            docs = DocumentArray.load_binary(dataset_path)
            if is_multimodal(docs[0]):
                return docs
            else:
                raise ValueError(
                    f'The file {dataset_path} does not contain a multimodal DocumentArray.'
                    f'Please check documentation https://docarray.jina.ai/fundamentals/dataclass/construct/'
                )
        except Exception:
            print(f'Failed to load the binary file provided under path {dataset_path}')
            exit(1)
    elif os.path.isdir(dataset_path):
        with yaspin_extended(
            sigmap=sigmap, text="Loading data from folder", color="green"
        ) as spinner:
            spinner.ok('🏭')
            docs = from_files_local(
                dataset_path,
                user_input.index_fields + user_input.filter_fields,
                user_input.field_names_to_dataclass_fields,
                data_class,
            )
            return docs
    else:
        raise ValueError(
            f'The provided dataset path {dataset_path} does not'
            f' appear to be a valid file or folder on your system.'
        )


def from_files_local(
    path: str,
    fields: List[str],
    field_names_to_dataclass_fields: Dict,
    data_class: Type,
) -> DocumentArray:
    """Creates a Multi Modal documentarray over a list of file path or the content of the files.
    :param path: The path to the directory
    :param fields: The fields to search for in the directory
    :param field_names_to_dataclass_fields: The mapping of the field names to the dataclass fields
    :param data_class: The dataclass to use for the document
    :return: A DocumentArray with the documents
    """

    file_paths = []
    for root, dirs, files in os.walk(path):
        file_paths.extend(
            [os.path.join(root, file) for file in files if not file.startswith('.')]
        )
    folder_generator = os.walk(path, topdown=True)
    current_level = folder_generator.__next__()
    folder_structure = 'sub_folders' if len(current_level[1]) > 0 else 'single_folder'
    if folder_structure == 'sub_folders':
        docs = create_docs_from_subdirectories(
            file_paths, fields, field_names_to_dataclass_fields, data_class
        )
    else:
        docs = create_docs_from_files(
            file_paths, fields, field_names_to_dataclass_fields, data_class
        )
    return DocumentArray(docs)


def create_docs_from_subdirectories(
    file_paths: List,
    fields: List[str],
    field_names_to_dataclass_fields: Dict,
    data_class: Type,
    path: str = None,
    is_s3_dataset: bool = False,
) -> List[Document]:
    """
    Creates a Multi Modal documentarray over a list of subdirectories.
    :param file_paths: The list of file paths
    :param fields: The fields to search for in the directory
    :param field_names_to_dataclass_fields: The mapping of the field names to the dataclass fields
    :param data_class: The dataclass to use for the document
    :param path: The path to the directory
    :param is_s3_dataset: Whether the dataset is stored on s3
    :return: The list of documents
    """

    docs = []
    folder_files = defaultdict(list)
    for file in file_paths:
        path_to_last_folder = (
            '/'.join(file.split('/')[:-1])
            if is_s3_dataset
            else os.sep.join(file.split(os.sep)[:-1])
        )
        folder_files[path_to_last_folder].append(file)
    for folder, files in folder_files.items():
        kwargs = {}
        for file in files:
            file, file_full_path = _extract_file_and_full_file_path(
                file, path, is_s3_dataset
            )
            if file in fields:
                kwargs[field_names_to_dataclass_fields[file]] = file_full_path
                continue
            if file.endswith('.json'):
                if is_s3_dataset:
                    for field in data_class.__annotations__.keys():
                        if field not in kwargs.keys():
                            kwargs[field] = file_full_path
                else:
                    with open(file_full_path) as f:
                        data = json.load(f)
                    for el, value in data.items():
                        if el in field_names_to_dataclass_fields.keys():
                            kwargs[field_names_to_dataclass_fields[el]] = value
        docs.append(Document(data_class(**kwargs)))
    return docs


def create_docs_from_files(
    file_paths: List,
    fields: List[str],
    field_names_to_dataclass_fields: Dict,
    data_class: Type,
    path: str = None,
    is_s3_dataset: bool = False,
) -> List[Document]:
    """
    Creates a Multi Modal documentarray over a list of files.
    :param file_paths: List of file paths
    :param fields: The fields to search for in the directory
    :param field_names_to_dataclass_fields: The mapping of the files to the dataclass fields
    :param data_class: The dataclass to use for the document
    :param path: The path to the directory
    :param is_s3_dataset: Whether the dataset is stored on s3
    :return: A list of documents
    """
    docs = []
    for file in file_paths:
        kwargs = {}
        file, file_full_path = _extract_file_and_full_file_path(
            file, path, is_s3_dataset
        )
        file_extension = file.split('.')[-1]
        if (
            file_extension == fields[0].split('.')[-1]
        ):  # fields should have only one index field in case of files only
            kwargs[field_names_to_dataclass_fields[fields[0]]] = file_full_path
            docs.append(Document(data_class(**kwargs)))
    return docs


def _list_files_from_s3_bucket(
    user_input: UserInput, data_class: Type
) -> DocumentArray:
    """
    Loads the data from s3 into multimodal documents.
    :param user_input: The user input object.
    :param data_class: The dataclass to use for the DocumentArray.
    :return: The DocumentArray with the documents.
    """
    bucket, folder_prefix = get_s3_bucket_and_folder_prefix(user_input)
    first_file = get_first_file_in_folder_structure_s3(
        bucket, folder_prefix, user_input.dataset_path
    )
    objects = list(bucket.objects.filter(Prefix=folder_prefix))
    file_paths = [
        obj.key
        for obj in objects
        if not obj.key.endswith('/') and not obj.key.split('/')[-1].startswith('.')
    ]

    structure_identifier = first_file[len(folder_prefix) :].split('/')
    folder_structure = (
        'sub_folders' if len(structure_identifier) > 1 else 'single_folder'
    )

    with yaspin_extended(
        sigmap=sigmap, text="Listing files from S3 bucket ...", color="green"
    ) as spinner:
        spinner.ok('🏭')
        if folder_structure == 'sub_folders':
            docs = create_docs_from_subdirectories(
                file_paths,
                user_input.index_fields + user_input.filter_fields,
                user_input.field_names_to_dataclass_fields,
                data_class,
                user_input.dataset_path,
                is_s3_dataset=True,
            )
        else:
            docs = create_docs_from_files(
                file_paths,
                user_input.index_fields + user_input.filter_fields,
                user_input.field_names_to_dataclass_fields,
                data_class,
                user_input.dataset_path,
                is_s3_dataset=True,
            )
    return DocumentArray(docs)


def _extract_file_and_full_file_path(file_path, path=None, is_s3_dataset=False):
    """
    Extracts the file name and the full file path from s3 object.
    :param file_path: The file path
    :param path: The path to the directory
    :param is_s3_dataset: Whether the dataset is stored on s3
    :return: The file name and the full file path
    """
    if is_s3_dataset:
        file = file_path.split('/')[-1]
        file_full_path = '/'.join(path.split('/')[:3]) + '/' + file_path
    else:
        file_full_path = file_path
        file = file_path.split(os.sep)[-1]
    return file, file_full_path


def _get_modality(document: Document):
    """
    Detect document's modality based on its `modality` or `mime_type` attributes.
    :param document: The document to detect the modality for.
    """
    for modality in AVAILABLE_MODALITIES_FOR_SEARCH:
        modality_string = docarray_typing_to_modality_string(modality)
        if (
            modality_string in document.modality
            or modality_string in document.mime_type
        ):
            return modality_string
    return None


def set_modality_da(documents: DocumentArray):
    """
    Set document's modality based on its `modality` or `mime_type` attributes.
    :param documents: The DocumentArray to set the modality for.
    :return: The DocumentArray with the modality set.
    """
    for doc in documents:
        for chunk in doc.chunks:
            chunk.modality = chunk.modality or _get_modality(chunk)
    return documents
