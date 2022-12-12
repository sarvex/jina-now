import glob
import json
import os

import requests

from now.constants import SUPPORTED_FILE_TYPES, Modalities
from now.data_loading.data_loading import get_s3_bucket_and_folder_prefix
from now.now_dataclasses import UserInput


def _create_candidate_search_filter_fields(field_name_to_value):
    """
    Creates candidate search fields from the field_names
    A candidate search field is a field that we can detect its modality
    A candidate filter field is a field that we can't detect its modality,
    or it's modality is different from image, video or audio.

    In case of docarray, we assume all fields are potentially searchable

    :param field_name_to_value: dictionary
    """
    search_fields_modalities = {}
    filter_field_modalities = {}
    for field_name, field_value in field_name_to_value.items():
        # we determine search modality
        for modality, modality_types in SUPPORTED_FILE_TYPES.items():
            if field_name.split('.')[-1] in modality_types:
                search_fields_modalities[field_name] = modality
                break
            if field_name == 'uri' and field_value.split('.')[-1] in modality_types:
                search_fields_modalities[field_name] = modality
                break
            if field_name == 'text' and field_value:
                search_fields_modalities[field_name] = Modalities.TEXT
                break
        # we determine if it's a filter field
        if field_name == 'uri':
            if (
                field_value.split('.')[-1]
                not in SUPPORTED_FILE_TYPES[Modalities.IMAGE]
                + SUPPORTED_FILE_TYPES[Modalities.VIDEO]
                + SUPPORTED_FILE_TYPES[Modalities.MUSIC]
            ):
                filter_field_modalities[field_name] = str(
                    field_value.__class__.__name__
                )
            continue
        if (
            field_name.split('.')[-1]
            not in SUPPORTED_FILE_TYPES[Modalities.IMAGE]
            + SUPPORTED_FILE_TYPES[Modalities.VIDEO]
            + SUPPORTED_FILE_TYPES[Modalities.MUSIC]
        ):
            filter_field_modalities[field_name] = str(field_value.__class__.__name__)

    if len(search_fields_modalities.keys()) == 0:
        raise ValueError(
            'No searchable fields found, please check documentation https://now.jina.ai'
        )
    return search_fields_modalities, filter_field_modalities


def _extract_field_names_docarray(response):
    """
    Downloads the first document in the document array and extracts field names from it
    if tags also exists then it extracts the keys from tags and adds to the field_names
    """
    field_names = {}
    response = requests.get(response.json()['data']['download'])
    ignored_fieldnames = ['embedding', 'id', 'mimeType', 'tags']
    for el, value in response.json()[0].items():
        if el not in ignored_fieldnames:
            field_names[el] = value
    if 'tags' in response.json()[0]:
        for el, value in response.json()[0]['tags']['fields'].items():
            field_names[el] = value
    return field_names


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
        field_names = _extract_field_names_docarray(response)
    else:
        raise ValueError('DocumentArray does not exist or you do not have access to it')
    (
        search_fields_modalities,
        filter_fields_modalities,
    ) = _create_candidate_search_filter_fields(field_names)
    user_input.search_fields_modalities = search_fields_modalities
    user_input.filter_fields_modalities = filter_fields_modalities


def _check_contains_files_only_s3_bucket(objects):
    """
    Checks if the bucket contains only files and no subfolders

    :param objects: list of objects in the bucket
    """
    for obj in objects[1:]:
        if obj.key.endswith('/'):
            return False
    return True


def _check_folder_structure_s3_bucket(objects, folder_prefix):
    """
    Checks if the folder contains only subfolders and no files and no subsub folders

    :param objects: list of objects in the bucket
    :param folder_prefix: root folder
    """
    for obj in objects[1:]:  # first is the bucket path
        if obj.key.endswith('/'):
            continue
        current_path = obj.key.split('/')
        root_folder_path = folder_prefix.split('/')
        if (
            len(current_path) - len(root_folder_path) != 1
        ):  # checks if current path is a direct subfolder of
            # root folder
            raise ValueError(
                'File format different than expected, please check documentation https://now.jina.ai'
            )


def _extract_field_names_s3_folder(first_folder_objects):
    """
    Extracts field names from the files in first folder in the bucket also
    checks if folder contains json files and if yes then extracts the keys from the json files
    and add to the field_names

    :param first_folder_objects: list of objects in the first folder
    """
    field_names = {}
    for field in first_folder_objects:
        if field.key.endswith('.json'):
            data = json.loads(field.get()['Body'].read())
            for el, value in data.items():
                field_names[el] = value
        else:
            field_names[field.key.split('/')[-1]] = field.key.split('/')[-1]
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
    if _check_contains_files_only_s3_bucket(objects):
        return

    _check_folder_structure_s3_bucket(objects, folder_prefix)

    first_folder = objects[1].key.split('/')[-2]
    first_folder_objects = list(
        bucket.objects.filter(Prefix=folder_prefix + first_folder)
    )[1:]
    field_names = _extract_field_names_s3_folder(first_folder_objects)
    (
        search_fields_modalities,
        filter_fields_modalities,
    ) = _create_candidate_search_filter_fields(field_names)
    user_input.search_fields_modalities = search_fields_modalities
    user_input.filter_fields_modalities = filter_fields_modalities


def _ensure_distance_folder_root(path, root):
    """Ensure that the path is a subfolder of the root"""
    path = os.path.abspath(path)
    root = os.path.abspath(root)
    return os.path.relpath(path, root).count(os.path.sep) == 1


def _check_contains_files_only_local_folder(dataset_path):
    """
    Checks if the folder contains only files and no subfolders

    :param dataset_path: path to the folder
    """
    for file_or_directory in os.listdir(dataset_path):
        if not os.path.isfile(os.path.join(dataset_path, file_or_directory)):
            return False
    return True


def _check_folder_structure_local_folder(dataset_path):
    """
    Checks if the folder contains only subfolders and no files and no subsub folders

    :param dataset_path: path to the folder
    """
    first_path = ''
    for path in sorted(glob.glob(os.path.join(dataset_path, '**/**'))):
        if not first_path:
            first_path = path

        if not _ensure_distance_folder_root(path, dataset_path):
            raise ValueError(
                'Folder format is not as expected, please check documentation https://now.jina.ai'
            )
    return first_path


def _extract_field_names_local_folder(first_folder):
    """
    Extracts field names from the files in first folder in the bucket also
    checks if folder contains json files and if yes then extracts the keys from the json files
    and add to the field_names

    :param first_folder: list of objects in the first folder
    """
    field_names = {}
    for field_name in os.listdir(first_folder):
        if field_name.endswith('.json'):
            json_f = open(os.path.join(first_folder, field_name))
            data = json.load(json_f)
            for el, value in data.items():
                field_names[el] = value
        else:
            field_names[field_name] = field_name
    return field_names


def set_field_names_from_local_folder(user_input: UserInput, **kwargs):
    """
    Get the schema from a local folder

    :param user_input: UserInput object

    checks if the folder exists and the format of the folder structure is correct,
    if yes set the content of the first folder as field_names in user_input
    """
    dataset_path = user_input.dataset_path.strip()
    if os.path.isfile(dataset_path):
        raise ValueError(
            'The path provided is not a folder, please check documentation https://now.jina.ai'
        )
    if _check_contains_files_only_local_folder(dataset_path):
        return

    first_path = _check_folder_structure_local_folder(dataset_path)
    first_folder = '/'.join(first_path.split('/')[:-1])
    field_names = _extract_field_names_local_folder(first_folder)
    (
        search_fields_modalities,
        filter_fields_modalities,
    ) = _create_candidate_search_filter_fields(field_names)
    user_input.search_fields_modalities = search_fields_modalities
    user_input.filter_fields_modalities = filter_fields_modalities
