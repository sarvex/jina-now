import json
import os
import uuid
from collections import defaultdict
from copy import deepcopy

from docarray import Document, DocumentArray

from now.apps.base.app import JinaNOWApp
from now.constants import DatasetTypes
from now.data_loading.es import ElasticsearchExtractor
from now.data_loading.utils import _fetch_da_from_url, get_dataset_url
from now.demo_data import DemoDatasetNames
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
    if user_input.dataset_type == DatasetTypes.DOCARRAY:
        print('⬇  Pull DocArray dataset')
        da = _pull_docarray(user_input.dataset_name)
    elif user_input.dataset_type == DatasetTypes.URL:
        print('⬇  Pull DocArray dataset')
        da = _fetch_da_from_url(user_input.dataset_url)
    elif user_input.dataset_type == DatasetTypes.PATH:
        print('💿  Loading files from disk')
        da = _load_from_disk(app, user_input)
        da = _load_tags_from_json_if_needed(da, user_input)
    elif user_input.dataset_type == DatasetTypes.S3_BUCKET:
        da = _list_files_from_s3_bucket(app=app, user_input=user_input)
        da = _load_tags_from_json_if_needed(da, user_input)
    elif user_input.dataset_type == DatasetTypes.ELASTICSEARCH:
        da = _extract_es_data(user_input)
    elif user_input.dataset_type == DatasetTypes.DEMO:
        print('⬇  Download DocArray dataset')
        url = get_dataset_url(user_input.dataset_name, app.output_modality)
        da = _fetch_da_from_url(url)
    if da is None:
        raise ValueError(
            f'Could not load DocArray dataset. Please check your configuration: {user_input}.'
        )
    if 'NOW_CI_RUN' in os.environ:
        if user_input.dataset_name == DemoDatasetNames.BEST_ARTWORKS:
            da = da[:300]
        elif user_input.dataset_name == DemoDatasetNames.TUMBLR_GIFS_10K:
            da = da[:300]
        else:
            da = da[:300]
    if (
        user_input.dataset_name == DemoDatasetNames.MUSIC_GENRES_MIX
        or user_input.dataset_name == DemoDatasetNames.MUSIC_GENRES_ROCK
    ):
        for doc in da:
            if 'genre_tags' in doc.tags and isinstance(doc.tags['genre_tags'], list):
                doc.tags['genre_tags'] = ' '.join(doc.tags['genre_tags'])
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


def select_ending(files, endings):
    for file in files:
        for ending in endings:
            if file.endswith(ending):
                return file
    return None


def _load_tags_from_json_if_needed(da: DocumentArray, user_input: UserInput):
    if any([doc.uri.endswith('.json') for doc in da]):
        return _load_tags_from_json(da, user_input)
    else:
        return da


def _load_tags_from_json(da, user_input):
    print(
        f'Loading tags! We assume that you have a folder for each document. The folder contains a content '
        f'file (image, text, video, ...) and a json file containing the tags'
    )
    # map folders to all files they contain
    folder_to_files = defaultdict(list)
    for d in da:
        folder = d.uri.rsplit('/', 1)[0]
        folder_to_files[folder].append(d.uri)

    docs = DocumentArray()
    for files in folder_to_files.values():
        tag_file = select_ending(files, ['json'])
        content_file = select_ending(
            files, user_input.app_instance.supported_file_types
        )
        if content_file:
            if tag_file:
                tags = {'tag_uri': tag_file}
            else:
                tags = {}
            docs.append(Document(uri=content_file, tags=tags))
    return docs


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


def match_types(uri, supported_file_types):
    for t in supported_file_types:
        if t == '**' or uri.split('.')[-1] == t:
            return True
    return False


def _extract_es_data(user_input: UserInput) -> DocumentArray:
    query = {
        'query': {'match_all': {}},
        'fields': user_input.es_image_fields + user_input.es_text_fields,
    }
    es_extractor = ElasticsearchExtractor(
        query=query,
        index=user_input.es_index_name,
        connection_str=user_input.es_host_name,
    )
    extracted_docs = es_extractor.extract()
    return extracted_docs


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
            docs = DocumentArray.from_files(f'{dataset_path}/**')
            docs = DocumentArray(
                d
                for d in docs
                if match_types(d.uri, app.supported_file_types + ['json'])
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
            if app.supported_file_types[0] == '**':
                docs.append(Document(uri=f"s3://{bucket.name}/{obj.key}"))
            else:
                for wild_card in app.supported_file_types + ['json']:
                    _postfix = wild_card.split('*')[-1]
                    if str(obj.key).endswith(_postfix):
                        docs.append(Document(uri=f"s3://{bucket.name}/{obj.key}"))
                        break

    return DocumentArray(docs)


def deep_copy_da(da: DocumentArray) -> DocumentArray:
    new_da = DocumentArray()
    for i, d in enumerate(da):
        new_doc = deepcopy(d)
        new_doc.id = str(uuid.uuid4())
        new_da.append(new_doc)
    return new_da
