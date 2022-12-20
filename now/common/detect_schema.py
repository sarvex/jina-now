import itertools
import json
import os
from typing import Dict, List

import requests
from docarray import DocumentArray

from now.constants import (
    AVAILABLE_MODALITIES_FOR_FILTER,
    AVAILABLE_MODALITIES_FOR_SEARCH,
    NOT_AVAILABLE_MODALITIES_FOR_FILTER,
    SUPPORTED_FILE_TYPES,
    Modalities,
)
from now.data_loading.data_loading import get_s3_bucket_and_folder_prefix
from now.now_dataclasses import UserInput


def _create_candidate_search_filter_fields(field_name_to_value):
    """
    Creates candidate search fields from the field_names for s3
    and local file path.
    A candidate search field is a field that we can detect its modality
    A candidate filter field is a field that we can't detect its modality,
    or it's modality is different from image, video or audio.

    :param field_name_to_value: dictionary
    """
    search_fields_modalities = {}
    filter_field_modalities = {}
    not_available_file_types_for_filter = list(
        itertools.chain(
            *[
                SUPPORTED_FILE_TYPES[modality]
                for modality in NOT_AVAILABLE_MODALITIES_FOR_FILTER
            ]
        )
    )
    for field_name, field_value in field_name_to_value.items():
        # we determine search modality
        for modality in AVAILABLE_MODALITIES_FOR_SEARCH:
            file_types = SUPPORTED_FILE_TYPES[modality]
            if field_name.split('.')[-1] in file_types:
                search_fields_modalities[field_name] = modality
                break
            elif field_name == 'uri' and field_value.split('.')[-1] in file_types:
                search_fields_modalities[field_name] = modality
                break
            elif field_name == 'text' and field_value:
                search_fields_modalities[field_name] = Modalities.TEXT
                break
        # we determine if it's a filter field
        if (
            field_name == 'uri'
            and field_value.split('.')[-1] not in not_available_file_types_for_filter
        ) or field_name.split('.')[-1] not in not_available_file_types_for_filter:
            filter_field_modalities[field_name] = str(field_value.__class__.__name__)

    if len(search_fields_modalities.keys()) == 0:
        raise ValueError(
            'No searchable fields found, please check documentation https://now.jina.ai'
        )
    return search_fields_modalities, filter_field_modalities


def _extract_field_candidates_docarray(response):
    """
    Downloads the first document in the document array and extracts field names from it
    if tags also exists then it extracts the keys from tags and adds to the field_names
    """
    search_modalities = {}
    filter_modalities = {}
    response = requests.get(response.json()['data']['download'])
    da = DocumentArray.from_dict(response.json())
    if not da[0]._metadata:
        raise RuntimeError(
            'Multi-modal schema is not provided. Please prepare your data following this guide - '
            'https://docarray.jina.ai/datatypes/multimodal/'
        )
    mm_schema = da[0]._metadata['fields']['multi_modal_schema']
    mm_fields = mm_schema['structValue']['fields']
    for field_name, value in mm_fields.items():
        if 'position' not in value['structValue']['fields']:
            raise ValueError(
                'No modalities found in this multi-modal documents. Please follow the steps in the documentation'
                ' to add modalities to your documents https://docarray.jina.ai/datatypes/multimodal/'
            )
        field_pos = value['structValue']['fields']['position']['numberValue']
        if not da[0].chunks[field_pos].modality:
            raise ValueError(
                f'No modality found for {field_name}. Please follow the steps in the documentation'
                f' to add modalities to your documents https://docarray.jina.ai/datatypes/multimodal/'
            )
        modality = da[0].chunks[field_pos].modality.lower()
        if modality not in AVAILABLE_MODALITIES_FOR_SEARCH:
            raise ValueError(
                f'The modality {modality} is not supported for search. Please use '
                f'one of the following modalities: {AVAILABLE_MODALITIES_FOR_SEARCH}'
            )
        # only the available modalities for filter are added to the filter modalities
        if modality in AVAILABLE_MODALITIES_FOR_FILTER:
            filter_modalities[field_name] = modality
        # only the available modalities for search are added to search modalities
        if modality in AVAILABLE_MODALITIES_FOR_SEARCH:
            search_modalities[field_name] = modality

    if da[0].tags:  # if tags exist then we add them as well to the filter modalities
        for el, value in da[0].tags['fields'].items():
            for val_type, val in value.items():
                filter_modalities[el] = val_type

    if len(search_modalities.keys()) == 0:
        raise ValueError(
            'No searchable fields found, please check documentation https://now.jina.ai'
        )
    return search_modalities, filter_modalities


def set_field_names_from_docarray(user_input: UserInput, **kwargs):
    """
    Get the schema from a DocArray

    :param user_input: UserInput object

    Makes a request to hubble API and downloads the first 10 documents
    from the document array and uses the first document to get the schema and sets field_names in user_input
    """
    cookies = {
        'st': user_input.jwt['token'],
    }

    json_data = {
        'name': user_input.dataset_name,
    }
    response = requests.post(
        'https://api.hubble.jina.ai/v2/rpc/docarray.getFirstDocuments',
        cookies=cookies,
        json=json_data,
    )
    if response.json()['code'] == 200:
        (
            user_input.search_fields_modalities,
            user_input.filter_fields_modalities,
        ) = _extract_field_candidates_docarray(response)
    else:
        raise ValueError('DocumentArray does not exist or you do not have access to it')


def _identify_folder_structure(file_paths: List[str], separator: str) -> str:
    """This function identifies the folder structure.
    It works with a local file structure or a remote file structure.

    :param file_paths: list of file paths
    :param separator: separator used in the file paths
    :return: if all the files are in the same folder then returns 'single_folder' else returns 'sub_folders'
    :raises ValueError: if the files don't have the same depth in the file structure
    """
    # check if all files are in the same folder
    depths = [len(path.split(separator)) for path in file_paths]
    if len(set(depths)) != 1:
        raise ValueError(
            "Files have differing depth, please check documentation https://now.jina.ai"
        )
    # check if all files are in the same folder
    if (
        len(set([separator.join(path.split(separator)[:-1]) for path in file_paths]))
        != 1
    ):
        return 'sub_folders'
    return 'single_folder'


def _extract_field_names_single_folder(
    file_paths: List[str], separator: str
) -> Dict[str, str]:
    """This function extracts the file endings in a single folder and returns them as field names.
    It works with a local file structure or a remote file structure.

    :param file_paths: list of relative file paths from data set path
    :param separator: separator used in the file paths
    :return: list of file endings
    """
    file_endings = set(
        ['.' + path.split(separator)[-1].split('.')[-1] for path in file_paths]
    )
    return {file_ending: file_ending for file_ending in file_endings}


def _extract_field_names_sub_folders(
    file_paths: List[str], separator: str, s3_bucket=None
) -> Dict[str, str]:
    """This function extracts the files in sub folders and returns them as field names. Also, it reads json files
    and adds them as key-value pairs to the field names dictionary.
    It works with a local file structure or a remote file structure.

    :param file_paths: list of relative file paths from data set path
    :param separator: separator used in the file paths
    :param s3_bucket: s3 bucket object, only needed if interacting with s3 bucket
    :return: list of file endings
    """
    field_names = {}
    for path in file_paths:
        if path.endswith('.json'):
            if s3_bucket:
                data = json.loads(s3_bucket.Object(path).get()['Body'].read())
            else:
                with open(path) as f:
                    data = json.load(f)
            for el, value in data.items():
                field_names[el] = value
        else:
            file_name = path.split(separator)[-1]
            field_names[file_name] = file_name
    return field_names


def set_field_names_from_s3_bucket(user_input: UserInput, **kwargs):
    """
    Get the schema from a S3 bucket

    :param user_input: UserInput object

    checks if the bucket exists and the format of the folder structure is correct,
    if yes then downloads the first folder and sets its content as field_names in user_input
    """
    bucket, folder_prefix = get_s3_bucket_and_folder_prefix(
        user_input
    )  # user has to provide the folder where folder structure begins

    objects = list(bucket.objects.filter(Prefix=folder_prefix))
    file_paths = [obj.key for obj in objects if not obj.key.endswith('/')]
    folder_structure = _identify_folder_structure(file_paths, '/')
    if folder_structure == 'single_folder':
        field_names = _extract_field_names_single_folder(file_paths, '/')
    elif folder_structure == 'sub_folders':
        first_folder = '/'.join(objects[1].key.split('/')[:-1])
        first_folder_objects = [
            obj.key
            for obj in bucket.objects.filter(Prefix=first_folder)
            if not obj.key.endswith('/')
        ]
        field_names = _extract_field_names_sub_folders(
            first_folder_objects, '/', bucket
        )
    (
        user_input.search_fields_modalities,
        user_input.filter_fields_modalities,
    ) = _create_candidate_search_filter_fields(field_names)


def set_field_names_from_local_folder(user_input: UserInput, **kwargs):
    """
    Get the schema from a local folder

    :param user_input: UserInput object

    checks if the folder exists and the format of the folder structure is correct,
    if yes set the content of the first folder as field_names in user_input
    """
    dataset_path = user_input.dataset_path.strip()
    dataset_path = os.path.expanduser(dataset_path)
    if os.path.isfile(dataset_path):
        raise ValueError(
            'The path provided is not a folder, please check documentation https://now.jina.ai'
        )
    file_paths = []
    for root, dirs, files in os.walk(dataset_path):
        file_paths.extend([os.path.join(root, file) for file in files])
    folder_structure = _identify_folder_structure(file_paths, os.sep)
    if folder_structure == 'single_folder':
        field_names = _extract_field_names_single_folder(file_paths, os.sep)
    elif folder_structure == 'sub_folders':
        first_folder = os.sep.join(file_paths[0].split(os.sep)[:-1])
        first_folder_files = [
            os.path.join(first_folder, file)
            for file in os.listdir(first_folder)
            if os.path.isfile(os.path.join(first_folder, file))
        ]
        field_names = _extract_field_names_sub_folders(first_folder_files, os.sep)
    (
        user_input.search_fields_modalities,
        user_input.filter_fields_modalities,
    ) = _create_candidate_search_filter_fields(field_names)
