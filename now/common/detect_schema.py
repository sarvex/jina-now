import glob
import json
import os

from now.data_loading.utils import _get_s3_bucket_and_folder_prefix
from now.now_dataclasses import UserInput


def _get_schema_docarray(user_input: UserInput, **kwargs):
    from hubble import Client

    if user_input.jwt is None:
        client = Client()
    else:
        client = Client(token=user_input.jwt['token'])

    resp = client.get_artifact_info(name='test_subset_laion')
    if resp.json()['code'] == 200:
        field_names = resp.json()['data']['metaData']['summary'][3]['value']
        ignored_fieldnames = ['embedding', 'id', 'mime_type']
        user_input.field_names = [
            field_name
            for field_name in field_names
            if field_name not in ignored_fieldnames
        ]
    else:
        raise ValueError('DocumentArray doesnt exist or you dont have access to it')


def _get_schema_s3_bucket(user_input: UserInput, **kwargs):
    bucket, folder_prefix = _get_s3_bucket_and_folder_prefix(
        user_input
    )  # user has to provide the folder where folder structure begins

    field_names = []

    all_files = True
    for obj in list(bucket.objects.filter(Prefix=folder_prefix))[
        1:
    ]:  # first is the bucket path
        if obj.key.endswith('/'):
            all_files = False
    if all_files:
        user_input.field_names = []
    else:
        for obj in list(bucket.objects.filter(Prefix=folder_prefix))[
            1:
        ]:  # first is the bucket path
            if obj.key.endswith('/'):
                continue
            if len(obj.key.split('/')) - len(folder_prefix.split('/')) != 1:
                raise ValueError(
                    'File format different than expected, please check documentation.'
                )

        first_folder = list(bucket.objects.filter(Prefix=folder_prefix))[1].key.split(
            '/'
        )[-2]
        for field in list(bucket.objects.filter(Prefix=folder_prefix + first_folder))[
            1:
        ]:
            if field.key.endswith('.json'):
                data = json.loads(field.get()['Body'].read())
                field_names.extend(list(data.keys()))
            else:
                field_names.append(field.key.split('/')[-1])
        user_input.field_names = field_names


def check_path(path, root):
    path = os.path.abspath(path)
    root = os.path.abspath(root)
    return os.path.relpath(path, root).count(os.path.sep) == 1


def _get_schema_local_folder(user_input: UserInput, **kwargs):

    dataset_path = user_input.dataset_path.strip()
    if os.path.isfile(dataset_path):
        return []
    elif os.path.isdir(dataset_path):
        all_files = True
        for file_or_directory in os.listdir(dataset_path):
            if not os.path.isfile(os.path.join(dataset_path, file_or_directory)):
                all_files = False
        if all_files:
            user_input.field_names = []
        else:
            first_path = ''
            for path in sorted(glob.glob(os.path.join(dataset_path, '**/**'))):
                if not first_path:
                    first_path = path

                if not check_path(path, dataset_path):
                    raise ValueError(
                        'Folder format is not as expected, please check documentation'
                    )
            first_folder = '/'.join(first_path.split('/')[:-1])
            field_names = []
            for field_name in os.listdir(first_folder):
                if field_name.endswith('.json'):
                    json_f = open(os.path.join(first_folder, field_name))
                    data = json.load(json_f)
                    field_names.extend(list(data.keys()))
                else:
                    field_names.append(field_name)
            user_input.field_names = field_names
