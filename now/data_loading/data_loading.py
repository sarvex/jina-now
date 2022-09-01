import base64
import json
import os
import uuid
from copy import deepcopy
from pathlib import Path

from docarray import Document, DocumentArray

from now.apps.base.app import JinaNOWApp
from now.constants import DatasetTypes, DemoDatasets
from now.data_loading.utils import _fetch_da_from_url, get_dataset_url
from now.log import yaspin_extended
from now.now_dataclasses import UserInput
from now.utils import sigmap


def load_data(app: JinaNOWApp, user_input: UserInput) -> DocumentArray:
    """Based on the user input, this function will pull the configured DocArray dataset ready for the preprocessing
    executor.

    :param app: chosen JinaNOWApp
    :param user_input: The configured user object. Result from the Jina Now cli dialog.
    :return: The loaded DocumentArray.
    """
    da = None

    if user_input.is_custom_dataset:
        if user_input.custom_dataset_type == DatasetTypes.DOCARRAY:
            print('⬇  Pull DocArray dataset')
            da = _pull_docarray(user_input.dataset_name)
        elif user_input.custom_dataset_type == DatasetTypes.URL:
            print('⬇  Pull DocArray dataset')
            da = _fetch_da_from_url(user_input.dataset_url)
        elif user_input.custom_dataset_type == DatasetTypes.PATH:
            print('💿  Loading files from disk')
            da = _load_from_disk(app, user_input)
            if any([doc.uri.endswith('.json') for doc in da]):
                da = _load_tags_from_json(da, user_input)
        elif user_input.custom_dataset_type == DatasetTypes.S3_BUCKET:
            da = _list_files_from_s3_bucket(app=app, user_input=user_input)
            if any([doc.uri.endswith('.json') for doc in da]):
                da = _load_tags_from_json(da, user_input)
    else:
        print('⬇  Download DocArray dataset')
        url = get_dataset_url(user_input.data, app.output_modality)
        da = _fetch_da_from_url(url)
        da[:, 'embedding'] = None
    if da is None:
        raise ValueError(
            f'Could not load DocArray dataset. Please check your configuration: {user_input}.'
        )
    if 'NOW_CI_RUN' in os.environ:
        if user_input.data == DemoDatasets.BEST_ARTWORKS:
            da = da[:2000]
        elif user_input.data == DemoDatasets.TUMBLR_GIFS_10K:
            da = da[:600]
        else:
            da = da[:1000]
    return da


def _open_json(path: str):
    with open(path, 'r') as f:
        data = json.load(f)
    return data


def _open_s3_json(path: str, user_input: UserInput):
    import boto3

    path_splits = path.split('/')
    bucket = path_splits[2]
    key = '/'.join(path_splits[3:])

    client = boto3.client(
        's3',
        aws_access_key_id=user_input.aws_access_key_id,
        aws_secret_access_key=user_input.aws_secret_access_key,
    )
    response = client.get_object(Bucket=bucket, Key=key)
    return json.loads(response['Body'].read())


def _load_tags_from_json(da: DocumentArray, user_input: UserInput):

    print('Loading tags!')
    dic = {}
    ids_to_delete = []
    files_in_same_folder = []
    for i, d in enumerate(da):
        folder = d.uri.rsplit('/', 1)[0]
        file_extension = d.uri.split('.')[-1]
        if file_extension in [
            wildcard.split('.')[-1] for wildcard in user_input.app.supported_wildcards
        ]:
            if folder not in dic:
                dic[folder] = i
            else:
                files_in_same_folder.append(i)

    if len(files_in_same_folder) > 0:
        print('Files with the same modality are found within the same folder!')
        print(
            'Printing first 5 ids:',
            ', '.join(str(id) for id in files_in_same_folder[:5]),
        )

    for i, d in enumerate(da):
        folder = d.uri.rsplit('/', 1)[0]
        if d.uri.split('.')[-1] not in [
            wildcard.split('.')[-1] for wildcard in user_input.app.supported_wildcards
        ]:
            ids_to_delete.append(i)
        if d.uri.endswith('.json') and folder in dic:
            if user_input.dataset_path.startswith('s3://'):
                data = _open_s3_json(d.uri, user_input)
            else:
                data = _open_json(d.uri)
            for tag, value in data['tags'].items():
                da[dic[folder]].tags[tag] = value['slug']
    if len(ids_to_delete) > 0:
        del da[ids_to_delete]
    return da


def _pull_docarray(dataset_name: str):
    try:
        return DocumentArray.pull(name=dataset_name, show_progress=True)
    except Exception:
        print(
            '💔 oh no, the secret of your docarray is wrong, or it was deleted after 14 days'
        )
        exit(1)


def _load_to_datauri_and_save_into_tags(d: Document) -> Document:
    d.tags['uri'] = d.uri
    return d.convert_uri_to_datauri()


def _load_from_disk(app: JinaNOWApp, user_input: UserInput) -> DocumentArray:
    dataset_path = user_input.dataset_path.strip()
    if os.path.isfile(dataset_path):
        try:
            return DocumentArray.load_binary(dataset_path)
        except Exception as e:
            print(f'Failed to load the binary file provided under path {dataset_path}')
            exit(1)
    elif os.path.isdir(dataset_path):
        with yaspin_extended(
            sigmap=sigmap, text="Loading data from folder", color="green"
        ) as spinner:
            spinner.ok('🏭')
            docs = DocumentArray.from_files(
                [f"{dataset_path}/{wild_card}" for wild_card in app.supported_wildcards]
            )
            docs.apply(_load_to_datauri_and_save_into_tags)
            return docs
    else:
        raise ValueError(
            f'The provided dataset path {dataset_path} does not'
            f' appear to be a valid file or folder on your system.'
        )


def _list_files_from_s3_bucket(app: JinaNOWApp, user_input: UserInput) -> DocumentArray:
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

    docs = []
    with yaspin_extended(
        sigmap=sigmap, text="Listing files from S3 bucket ...", color="green"
    ) as spinner:
        spinner.ok('🏭')
        for obj in list(bucket.objects.filter(Prefix=folder_prefix)):
            if app.supported_wildcards[0] == '**':
                docs.append(Document(uri=f"s3://{bucket.name}/{obj.key}"))
            else:
                for wild_card in app.supported_wildcards:
                    _postfix = wild_card.split('*')[-1]
                    if str(obj.key).endswith(_postfix):
                        docs.append(Document(uri=f"s3://{bucket.name}/{obj.key}"))
                        break

    return DocumentArray(docs)


def _download_from_s3_bucket(app: JinaNOWApp, user_input: UserInput) -> DocumentArray:
    import boto3.session

    s3_uri = user_input.dataset_path
    if not s3_uri.startswith('s3://'):
        raise ValueError(
            f"Can't process S3 URI {s3_uri} as it assumes it starts with: 's3://'"
        )

    data_dir = os.path.expanduser(
        f'~/.cache/jina-now/data/tmp/{base64.b64encode(bytes(s3_uri, "utf-8")).decode("utf-8")}'
    )
    Path(data_dir).mkdir(parents=True, exist_ok=True)

    bucket = s3_uri.split('/')[2]
    folder_prefix = '/'.join(s3_uri.split('/')[3:])

    session = boto3.session.Session(
        aws_access_key_id=user_input.aws_access_key_id,
        aws_secret_access_key=user_input.aws_secret_access_key,
    )
    bucket = session.resource('s3').Bucket(bucket)

    with yaspin_extended(
        sigmap=sigmap, text="Loading data from S3 and creating DocArray", color="green"
    ) as spinner:
        spinner.ok('🏭')

        for obj in list(bucket.objects.filter(Prefix=folder_prefix)):
            if app.supported_wildcards[0] != '**':
                matches_postfix = False
                for wild_card in app.supported_wildcards:
                    _postfix = wild_card.split('*')[-1]
                    if str(obj.key).endswith(_postfix):
                        matches_postfix = True
                        break
                if not matches_postfix:
                    continue
            # create nested directory structure
            path_obj_machine = os.path.join(data_dir, obj.key)
            Path(os.path.dirname(path_obj_machine)).mkdir(parents=True, exist_ok=True)
            # save file with full path locally
            bucket.download_file(obj.key, path_obj_machine)

        docs = DocumentArray.from_files(
            [f"{data_dir}/{wild_card}" for wild_card in app.supported_wildcards]
        )
        docs.apply(_load_to_datauri_and_save_into_tags)
        return docs


def deep_copy_da(da: DocumentArray) -> DocumentArray:
    new_da = DocumentArray()
    for i, d in enumerate(da):
        new_doc = deepcopy(d)
        new_doc.id = str(uuid.uuid4())
        new_da.append(new_doc)
    return new_da
