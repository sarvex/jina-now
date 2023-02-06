import json
import os
from collections import defaultdict
from typing import Dict, List, Type

from docarray import Document, DocumentArray
from docarray.dataclasses import is_multimodal

from now.common.detect_schema import (
    get_first_file_in_folder_structure_s3,
    get_s3_bucket_and_folder_prefix,
)
from now.constants import DatasetTypes
from now.data_loading.elasticsearch import ElasticsearchExtractor
from now.log import yaspin_extended
from now.now_dataclasses import UserInput
from now.utils import flatten_dict, sigmap


def load_data(
    user_input: UserInput, data_class=None, print_callback=print
) -> DocumentArray:
    """Based on the user input, this function will pull the configured DocumentArray dataset ready for the preprocessing
    executor.

    :param user_input: The configured user object. Result from the Jina Now cli dialog.
    :param data_class: The dataclass that should be used for the DocumentArray.
    :param print_callback: The callback function that should be used to print the status.
    :return: The loaded DocumentArray.
    """
    da = None
    if user_input.dataset_type in [DatasetTypes.DOCARRAY, DatasetTypes.DEMO]:
        print_callback('⬇  Pull DocumentArray dataset')
        da = _pull_docarray(user_input.dataset_name, user_input.admin_name)
        da = _add_tags_to_da(da, user_input)
    elif user_input.dataset_type == DatasetTypes.PATH:
        print_callback('💿  Loading files from disk')
        da = _load_from_disk(user_input=user_input, data_class=data_class)
    elif user_input.dataset_type == DatasetTypes.S3_BUCKET:
        print_callback('🗄  Loading files from S3')
        da = _list_files_from_s3_bucket(user_input=user_input, data_class=data_class)
    elif user_input.dataset_type == DatasetTypes.ELASTICSEARCH:
        print_callback('🔍  Loading data from Elasticsearch')
        da = _extract_es_data(user_input=user_input, data_class=data_class)
    da = set_modality_da(da)
    _add_metadata_to_da(da, user_input)
    if da is None:
        raise ValueError(
            f'Could not load DocumentArray dataset. Please check your configuration: {user_input}.'
        )
    if 'NOW_CI_RUN' in os.environ:
        da = da[:50]
    return da


def _add_metadata_to_da(da, user_input):
    dataclass_fields_to_field_names = {
        v: k for k, v in user_input.field_names_to_dataclass_fields.items()
    }
    for doc in da:
        for dataclass_field, meta_dict in doc._metadata['multi_modal_schema'].items():
            field_name = dataclass_fields_to_field_names.get(dataclass_field, None)
            if 'position' in meta_dict:
                getattr(doc, dataclass_field)._metadata['field_name'] = field_name


def _add_tags_to_da(da: DocumentArray, user_input: UserInput):
    """Add tags to da, remove non-index chunks, and update multi modal schema."""
    if not da:
        return da

    for doc in da:
        filtered_chunks = []
        for field in doc._metadata['multi_modal_schema'].keys():
            field_doc = getattr(doc, field)
            if field not in user_input.index_fields:
                if field_doc.blob or field_doc.tensor is not None:
                    continue
                doc.tags.update(
                    {
                        field: field_doc.content
                        if isinstance(field_doc.content, str)
                        else field_doc.uri
                    }
                )
            else:
                filtered_chunks.append(field_doc)
        doc.chunks = filtered_chunks
        # keep only the index fields in metadata
        doc._metadata['multi_modal_schema'] = {
            field: doc._metadata['multi_modal_schema'][field]
            for field in user_input.index_fields
        }
        # Update the positions accordingly to access the chunks
        for position, field in enumerate(user_input.index_fields):
            doc._metadata['multi_modal_schema'][field]['position'] = int(position)

    return da


def _pull_docarray(dataset_name: str, admin_name: str) -> DocumentArray:
    dataset_name = (
        admin_name + '/' + dataset_name if '/' not in dataset_name else dataset_name
    )
    try:
        docs = DocumentArray.pull(name=dataset_name, show_progress=True)
        if is_multimodal(docs[0]):
            return docs
        else:
            raise ValueError(
                f'The dataset {dataset_name} does not contain a multimodal DocumentArray. '
                f'Please check documentation https://docarray.jina.ai/fundamentals/dataclass/construct/'
            )
    except Exception:
        raise ValueError(
            'DocumentArray does not exist or you do not have access to it. '
            'Make sure to add user name as a prefix. Check documentation here. '
            'https://docarray.jina.ai/fundamentals/cloud-support/data-management/'
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
            da = DocumentArray.load_binary(dataset_path)
            if is_multimodal(da[0]):
                da = _add_tags_to_da(da, user_input)
                return da
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
                user_input.index_fields,
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
        tags_loaded_local = {}
        _s3_uri_for_tags = ''
        file_info = [
            _extract_file_and_full_file_path(file, path, is_s3_dataset)
            for file in files
        ]
        # first store index fields given as files
        for file, file_full_path in file_info:
            if file in fields:
                kwargs[field_names_to_dataclass_fields[file]] = file_full_path
        # next check json files that can also contain index fields, and carry on data
        for file, file_full_path in file_info:
            if file.endswith('.json'):
                if is_s3_dataset:
                    _s3_uri_for_tags = file_full_path
                    for field in data_class.__annotations__.keys():
                        if field not in kwargs.keys():
                            kwargs[field] = file_full_path
                else:
                    with open(file_full_path) as f:
                        json_data = flatten_dict(json.load(f))
                    for field, value in json_data.items():
                        if field in fields:
                            kwargs[field_names_to_dataclass_fields[field]] = value
                        else:
                            tags_loaded_local[field] = value
        doc = Document(data_class(**kwargs))
        if _s3_uri_for_tags:
            doc._metadata['_s3_uri_for_tags'] = _s3_uri_for_tags
        elif tags_loaded_local:
            doc.tags.update(tags_loaded_local)
        docs.append(doc)
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
                user_input.index_fields,
                user_input.field_names_to_dataclass_fields,
                data_class,
                user_input.dataset_path,
                is_s3_dataset=True,
            )
        else:
            docs = create_docs_from_files(
                file_paths,
                user_input.index_fields,
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

    modalities = ['text', 'image', 'video']
    if document.modality:
        return document.modality
    mime_type_class = document.mime_type.split('/')[0]
    if document.mime_type == 'application/json':
        return 'text'
    if mime_type_class in modalities:
        return mime_type_class
    document.summary()
    raise ValueError(f'Unknown modality')


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
