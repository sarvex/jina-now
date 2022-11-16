from __future__ import annotations, print_function, unicode_literals

import base64
import json
import os
import shutil
import signal
import sys
import tempfile
from collections.abc import MutableMapping
from concurrent.futures import ThreadPoolExecutor
from os.path import expanduser as user
from typing import Dict, List, Optional, Union

import boto3
import hubble
import yaml
from docarray import Document, DocumentArray
from jina.jaml import JAML
from pyfiglet import Figlet

from now.thirdparty.PyInquirer.prompt import prompt


def download_file(path, r_raw):
    with path.open("wb") as f:
        shutil.copyfileobj(r_raw, f)


def download(url, filename):
    import functools
    import pathlib

    import requests
    from tqdm.auto import tqdm

    r = requests.get(url, stream=True, allow_redirects=True)
    if r.status_code != 200:
        r.raise_for_status()  # Will only raise for 4xx codes, so...
        raise RuntimeError(f"Request to {url} returned status code {r.status_code}")
    file_size = int(r.headers.get('Content-Length', 0))

    path = pathlib.Path(filename).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    desc = "(Unknown total file size)" if file_size == 0 else ""
    r.raw.read = functools.partial(
        r.raw.read, decode_content=True
    )  # Decompress if needed

    if any(map(lambda x: x in os.environ, ['NOW_CI_RUN', 'NOW_EXAMPLES'])):
        download_file(path, r.raw)
    else:
        with tqdm.wrapattr(r.raw, "read", total=file_size, desc=desc) as r_raw:
            download_file(path, r_raw)

    return path


def my_handler(signum, frame, spinner):
    with spinner.hidden():
        sys.stdout.write("Program terminated!\n")
    spinner.stop()
    exit(0)


def flow_definition(dirpath) -> Dict:
    with open(dirpath) as f:
        return yaml.safe_load(f.read())


class BetterEnum:
    def __iter__(self):
        return [getattr(self, x) for x in dir(self) if ('__' not in x)].__iter__()


def to_camel_case(text):
    """Convert text to camel case in great coding style"""
    s = text.replace("_", " ")
    s = s.split()
    return ''.join(i.capitalize() for i in s)


sigmap = {signal.SIGINT: my_handler, signal.SIGTERM: my_handler}


def write_env_file(env_file, config):
    config_string = '\n'.join([f'{key}={value}' for key, value in config.items()])
    with open(env_file, 'w+') as fp:
        fp.write(config_string)


def write_flow_file(flow_yaml_content, new_yaml_file_path):
    with open(new_yaml_file_path, 'w') as f:
        JAML.dump(
            flow_yaml_content,
            f,
            indent=2,
            allow_unicode=True,
            Dumper=Dumper,
        )


@hubble.login_required
def jina_auth_login():
    pass


def get_info_hubble(user_input):
    with open(user('~/.jina/config.json')) as fp:
        config_val = json.load(fp)
        user_token = config_val['auth_token']
    client = hubble.Client(token=user_token, max_retries=None, jsonify=True)
    response = client.get_user_info()
    user_input.admin_emails = (
        [response['data']['email']] if 'email' in response['data'] else []
    )
    if not user_input.admin_emails:
        print(
            'Your hubble account is not verified. Please verify your account to deploy your flow as admin.'
        )
    user_input.jwt = {'token': user_token}
    return response['data'], user_token


def print_headline():
    f = Figlet(font='slant')
    print('Welcome to:')
    print(f.renderText('Jina NOW'))
    print('Get your search case up and running - end to end.\n')
    print(
        'You can choose between image and text search. \nJina NOW trains a model, pushes it to Jina Hub '
        'and deploys a Flow and a playground app in the cloud or locally. \nCheck out one of our demos or bring '
        'your own data.\n'
    )
    print('If you want learn more about our framework please visit docs.jina.ai')
    print(
        '💡 Make sure you give enough memory to your Docker daemon. '
        '5GB - 8GB should be okay.'
    )
    print()


def maybe_prompt_user(questions, attribute, **kwargs):
    """
    Checks the `kwargs` for the `attribute` name. If present, the value is returned directly.
    If not, the user is prompted via the cmd-line using the `questions` argument.

    :param questions: A dictionary that is passed to `PyInquirer.prompt`
        See docs: https://github.com/CITGuru/PyInquirer#documentation
    :param attribute: Name of the value to get. Make sure this matches the name in `kwargs`

    :return: A single value of either from `kwargs` or the user cli input.
    """
    if kwargs and attribute in kwargs:
        return kwargs[attribute]
    else:
        answer = prompt(questions)
        return answer[attribute]


def prompt_value(
    name: str,
    prompt_message: str,
    prompt_type: str = 'input',
    choices: Optional[List[Union[Dict, str]]] = None,
    **kwargs: Dict,
):
    qs = {'name': name, 'type': prompt_type, 'message': prompt_message}

    if choices is not None:
        qs['choices'] = choices
        qs['type'] = 'list'
    return maybe_prompt_user(qs, name, **kwargs)


def _maybe_download_from_s3(
    docs: DocumentArray, tmpdir: tempfile.TemporaryDirectory, user_input, max_workers
):
    """Downloads file to local temporary dictionary, saves S3 URI to `tags['uri']` and modifies `uri` attribute of
    document to local path in-place.

    :param doc: document containing URI pointing to the location on S3 bucket
    :param tmpdir: temporary directory in which files will be saved
    :param user_input: User iput which contain aws credentials
    :param max_workers: number of threads to create in the threadpool executor to make execution faster
    """

    def download(bucket, uri):
        path_s3 = '/'.join(uri.split('/')[3:])
        path_local = os.path.join(
            str(tmpdir),
            base64.b64encode(bytes(path_s3, "utf-8")).decode("utf-8"),
        )
        bucket.download_file(
            path_s3,
            path_local,
        )
        return path_local

    def convert_fn(d: Document) -> Document:
        """Downloads files and tags from S3 bucket and updates the content uri and the tags uri to the local path"""
        d.tags['uri'] = d.uri
        session = boto3.session.Session(
            aws_access_key_id=user_input.aws_access_key_id,
            aws_secret_access_key=user_input.aws_secret_access_key,
            region_name=user_input.aws_region_name,
        )
        bucket = session.resource('s3').Bucket(d.uri.split('/')[2])
        d.uri = download(bucket=bucket, uri=d.uri)
        if 'tag_uri' in d.tags:
            d.tags['tag_uri'] = download(bucket, d.tags['tag_uri'])
            with open(d.tags['tag_uri'], 'r') as fp:
                tags = json.load(fp)
                tags = flatten_dict(tags)
                d.tags.update(tags)
            del d.tags['tag_uri']
        return d

    docs_to_download = [doc for doc in docs if doc.uri.startswith('s3://')]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for d in docs_to_download:
            f = executor.submit(convert_fn, d)
            futures.append(f)
        for f in futures:
            f.result()


def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def _get_context_names(contexts, active_context=None):
    names = [c for c in contexts] if contexts is not None else []
    if active_context is not None:
        names.remove(active_context)
        names = [active_context] + names
    return names


def get_flow_id(host):
    return host.split('.wolf.jina.ai')[0].split('-')[-1]


class Dumper(yaml.Dumper):
    def increase_indent(self, flow=False, *args, **kwargs):
        return super().increase_indent(flow=flow, indentless=False)
