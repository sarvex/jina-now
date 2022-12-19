import base64
import json
import os
import pathlib
import pickle
from os.path import join as osp
from typing import Dict, List, Type

from docarray import Document, DocumentArray

from now.constants import (
    BASE_STORAGE_URL,
    DEMO_DATASET_DOCARRAY_VERSION,
    DatasetTypes,
    Modalities,
)
from now.data_loading.elasticsearch import ElasticsearchExtractor
from now.demo_data import AVAILABLE_DATASETS
from now.log import yaspin_extended
from now.now_dataclasses import UserInput
from now.utils import download, sigmap


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
        da = _load_from_disk(user_input=user_input, dataclass=data_class)
    elif user_input.dataset_type == DatasetTypes.S3_BUCKET:
        da = _list_files_from_s3_bucket(user_input=user_input, data_class=data_class)
    elif user_input.dataset_type == DatasetTypes.ELASTICSEARCH:
        da = _extract_es_data(user_input)
    elif user_input.dataset_type == DatasetTypes.DEMO:
        print('⬇  Download DocumentArray dataset')
        url = get_dataset_url(user_input.dataset_name)
        da = fetch_da_from_url(url)
    if da is None:
        raise ValueError(
            f'Could not load DocumentArray dataset. Please check your configuration: {user_input}.'
        )
    if 'NOW_CI_RUN' in os.environ:
        da = da[:50]
    return da


def _pull_docarray(dataset_name: str):
    try:
        return DocumentArray.pull(name=dataset_name, show_progress=True)
    except Exception:
        print(
            '💔 oh no, the secret of your docarray is wrong, or it was deleted after 14 days'
        )
        exit(1)


def _extract_es_data(user_input: UserInput) -> DocumentArray:
    query = {
        'query': {'match_all': {}},
        '_source': True,
    }
    es_extractor = ElasticsearchExtractor(
        query=query,
        index=user_input.es_index_name,
        connection_str=user_input.es_host_name,
    )
    extracted_docs = es_extractor.extract(search_fields=user_input.search_fields)
    return extracted_docs


def _load_from_disk(user_input: UserInput, dataclass) -> DocumentArray:
    """
    Loads the data from disk into multimodal documents.

    :param user_input: The user input object.
    :param dataclass: The dataclass to use for the DocumentArray.
    """
    dataset_path = user_input.dataset_path.strip()
    dataset_path = os.path.expanduser(dataset_path)
    if os.path.isfile(dataset_path):
        try:
            return DocumentArray.load_binary(dataset_path)
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
                user_input.search_fields + user_input.filter_fields,
                user_input.files_to_dataclass_fields,
                dataclass,
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
    files_to_dataclass_fields: dict,
    data_class: Type,
) -> DocumentArray:
    """Creates a Multi Modal documentarray over a list of file path or the content of the files.

    :param path: The path to the directory
    :param fields: The fields to search for in the directory
    :param files_to_dataclass_fields: The mapping of the files to the dataclass fields
    :param data_class: The dataclass to use for the document

    :return: A DocumentArray with the documents
    """

    def get_subdirectories_local_path(directory):
        return [
            name
            for name in os.listdir(directory)
            if os.path.isdir(os.path.join(directory, name))
        ]

    subdirectories = get_subdirectories_local_path(path)
    if subdirectories:
        docs = create_docs_from_subdirectories(
            subdirectories, path, fields, files_to_dataclass_fields, data_class
        )
    else:
        docs = create_docs_from_files(
            path, fields, files_to_dataclass_fields, data_class
        )
    return DocumentArray(docs)


def create_docs_from_subdirectories(
    subdirectories: List,
    path: str,
    fields: List[str],
    files_to_dataclass_fields: Dict,
    data_class: Type,
) -> List[Document]:
    """
    Creates a Multi Modal documentarray over a list of subdirectories.

    :param subdirectories: The list of subdirectories
    :param path: The path to the directory
    :param fields: The fields to search for in the directory
    :param files_to_dataclass_fields: The mapping of the files to the dataclass fields
    :param data_class: The dataclass to use for the document

    :return: The list of documents
    """

    docs = []
    kwargs = {}
    for subdirectory in subdirectories:
        for file in os.listdir(os.path.join(path, subdirectory)):
            if file in fields:
                kwargs[files_to_dataclass_fields[file]] = os.path.join(
                    path, subdirectory, file
                )
                continue
            if file.endswith('.json'):
                json_f = open(os.path.join(path, subdirectory, file))
                data = json.load(json_f)
                for el, value in data.items():
                    kwargs[files_to_dataclass_fields[el]] = value
        docs.append(Document(data_class(**kwargs)))
    return docs


def create_docs_from_files(
    path: str, fields: List[str], files_to_dataclass_fields: Dict, data_class: Type
) -> List[Document]:
    """
    Creates a Multi Modal documentarray over a list of files.

    :param path: The path to the directory
    :param fields: The fields to search for in the directory
    :param files_to_dataclass_fields: The mapping of the files to the dataclass fields
    :param data_class: The dataclass to use for the document

    :return: A list of documents
    """
    docs = []
    for file in os.listdir(os.path.join(path)):
        kwargs = {}
        file_extension = file.split('.')[-1]
        if (
            file_extension == fields[0].split('.')[-1]
        ):  # fields should have only one search field in case of files only
            kwargs[files_to_dataclass_fields[fields[0]]] = os.path.join(path, file)
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

    def get_subdirectories(s3_bucket, root_folder):
        """
        Gets the subdirectories of a given folder in a s3 bucket.

        :param s3_bucket: The s3 bucket.
        :param root_folder: The root folder.

        :return: The list of subdirectories.
        """
        sub_directories = []
        for obj in list(s3_bucket.objects.filter(Prefix=root_folder))[1:]:
            if obj.key.endswith('/'):
                sub_directories.append(obj.key)
        return sub_directories

    with yaspin_extended(
        sigmap=sigmap, text="Listing files from S3 bucket ...", color="green"
    ) as spinner:
        spinner.ok('🏭')
        subdirectories = get_subdirectories(bucket, folder_prefix)
        if subdirectories:
            docs = create_docs_from_subdirectories_s3(
                subdirectories,
                folder_prefix,
                user_input.search_fields + user_input.filter_fields,
                user_input.files_to_dataclass_fields,
                data_class,
                bucket,
            )
        else:
            docs = create_docs_from_files_s3(
                folder_prefix,
                user_input.dataset_path,
                user_input.search_fields + user_input.filter_fields,
                user_input.files_to_dataclass_fields,
                data_class,
                bucket,
            )
    return DocumentArray(docs)


def create_docs_from_subdirectories_s3(
    subdirectories: List,
    path: str,
    fields: List[str],
    files_to_dataclass_fields: Dict,
    data_class: Type,
    bucket,
) -> List[Document]:
    """
    Creates a Multi Modal documentarray over a list of subdirectories.

    :param subdirectories: The list of subdirectories
    :param path: The path to the directory
    :param fields: The fields to search for in the directory
    :param files_to_dataclass_fields: The mapping of the files to the dataclass fields
    :param data_class: The dataclass to use for the document
    :param bucket: The s3 bucket

    :return: The list of documents
    """
    docs = []
    kwargs = {}
    for subdirectory in subdirectories:
        for obj in list(bucket.objects.filter(Prefix=subdirectory))[1:]:
            file = obj.key.split('/')[-1]
            file_full_path = '/'.join(path.split('/')[:3]) + '/' + obj.key
            if file in fields:
                kwargs[files_to_dataclass_fields[file]] = file_full_path
                continue
            if file.endswith('.json'):
                kwargs['json_s3'] = file_full_path
        docs.append(Document(data_class(**kwargs)))
    return docs


def create_docs_from_files_s3(
    folder: str,
    path: str,
    fields: List[str],
    files_to_dataclass_fields: Dict,
    data_class: Type,
    bucket,
) -> List[Document]:
    """
    Creates a Multi Modal documentarray over a list of files.

    :param folder: The folder to search for files
    :param path: The path to the directory
    :param fields: The fields to search for in the directory
    :param files_to_dataclass_fields: The mapping of the files to the dataclass fields
    :param data_class: The dataclass to use for the document
    :param bucket: The s3 bucket

    :return: A list of documents
    """
    docs = []
    for obj in list(bucket.objects.filter(Prefix=folder))[1:]:
        kwargs = {}
        file = obj.key.split('/')[-1]
        file_full_path = '/'.join(path.split('/')[:3]) + '/' + obj.key
        if file in fields:
            kwargs[
                files_to_dataclass_fields[files_to_dataclass_fields]
            ] = file_full_path
            docs.append(Document(data_class(**kwargs)))
    return docs


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


def get_dataset_url(dataset: str) -> str:
    search_modality = None
    for _modality, _demo_datasets in AVAILABLE_DATASETS.items():
        if any([dataset == _demo_dataset.name for _demo_dataset in _demo_datasets]):
            search_modality = _modality

    data_folder = None
    docarray_version = DEMO_DATASET_DOCARRAY_VERSION
    if search_modality == Modalities.IMAGE:
        data_folder = 'jpeg'
    elif search_modality == Modalities.TEXT:
        data_folder = 'text'
    elif search_modality == Modalities.VIDEO:
        data_folder = 'video'

    if search_modality != Modalities.VIDEO:
        model_name = 'ViT-B32'
        return f'{BASE_STORAGE_URL}/{data_folder}/{dataset}.{model_name}-{docarray_version}.bin'
    else:
        return f'{BASE_STORAGE_URL}/{data_folder}/{dataset}-{docarray_version}.bin'


def get_s3_bucket_and_folder_prefix(user_input: UserInput):
    """
    Gets the s3 bucket and folder prefix from the user input.

    :param user_input: The user input

    :return: The s3 bucket and folder prefix
    """
    import boto3.session

    s3_uri = user_input.dataset_path
    if not s3_uri.startswith('s3://'):
        raise ValueError(
            f"Can't process S3 URI {s3_uri} as it assumes it starts with: 's3://'"
        )

    bucket = s3_uri.split('/')[2]
    folder_prefix = '/'.join(s3_uri.split('/')[3:])

    session = boto3.session.Session(
        aws_access_key_id=user_input.aws_access_key_id,
        aws_secret_access_key=user_input.aws_secret_access_key,
    )
    bucket = session.resource('s3').Bucket(bucket)

    return bucket, folder_prefix
