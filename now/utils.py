from __future__ import annotations, print_function, unicode_literals

import signal
import sys
from collections.abc import MutableMapping
from dataclasses import dataclass
from inspect import stack
from typing import Any, TypeVar

import boto3
import docarray
import hubble
import yaml
from jina.jaml import JAML
from pyfiglet import Figlet


def my_handler(signum, frame, spinner):
    with spinner.hidden():
        sys.stdout.write("Program terminated!\n")
    spinner.stop()
    exit(0)


class BetterEnum:
    def __iter__(self):
        return [getattr(self, x) for x in dir(self) if ('__' not in x)].__iter__()


def to_camel_case(text):
    """Convert text to camel case in great coding style"""
    s = text.replace("_", " ")
    s = s.split()
    return ''.join(i.capitalize() for i in s)


sigmap = {signal.SIGINT: my_handler, signal.SIGTERM: my_handler}


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
    client = hubble.Client(max_retries=None, jsonify=True)
    response = client.get_user_info()
    user_input.admin_emails = (
        [response['data']['email']] if 'email' in response['data'] else []
    )
    if not user_input.admin_emails:
        print(
            'Your hubble account is not verified. Please verify your account to deploy your flow as admin.'
        )
    user_input.jwt = {'token': client.token}
    user_input.admin_name = response['data']['name']
    return response['data'], client.token


def print_headline():
    f = Figlet(font='slant')
    print('Welcome to:')
    print(f.renderText('Jina NOW'))
    print('Get your search use case up and running - end to end.\n')
    print(
        'You can choose between image and text search. \nJina NOW trains a model, pushes it to Jina AI Cloud '
        'and deploys a Flow and playground app in the cloud or locally. \nCheck out our demos or bring '
        'your own data.\n'
    )
    print('Visit docs.jina.ai to learn more about our framework')
    print(
        '💡 Make sure you give enough memory to your Docker daemon. '
        '5GB - 8GB should be okay.'
    )
    print()


def debug(msg: Any):
    """
    Prints a message along with the details of the caller, ie filename and line number.
    """
    msg = str(msg) if msg is not None else ''
    frameinfo = stack()[1]
    print(f"{frameinfo.filename}:{frameinfo.lineno}: {msg}")


def flatten_dict(d, parent_key='', sep='__'):
    """
    This function converts a nested dictionary into a dictionary of attirbutes using '__' as a separator.
    Example:
        {'a': {'b': {'c': 1, 'd': 2}}} -> {'a__b__c': 1, 'a__b__c': 2}
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            # TODO for now, we just have string values, str(v) should be removed once we support numeric values
            items.append((new_key, str(v)))
    return dict(items)


def get_flow_id(host):
    return host[len('https://') : -len('-http.wolf.jina.ai')]


class Dumper(yaml.Dumper):
    def increase_indent(self, flow=False, *args, **kwargs):
        return super().increase_indent(flow=flow, indentless=False)


def docarray_typing_to_modality_string(T: TypeVar) -> str:
    """E.g. docarray.typing.Image -> image"""
    return T.__name__.lower()


def modality_string_to_docarray_typing(s: str) -> TypeVar:
    """E.g. image -> docarray.typing.Image"""
    return getattr(docarray.typing, s.capitalize())


@dataclass
class AWSProfile:
    aws_access_key_id: str
    aws_secret_access_key: str
    region: str


def get_aws_profile():
    session = boto3.Session()
    credentials = session.get_credentials()
    aws_profile = (
        AWSProfile(credentials.access_key, credentials.secret_key, session.region_name)
        if credentials
        else AWSProfile(None, None, session.region_name)
    )
    return aws_profile


def hide_string_chars(s):
    return ''.join(['*' for _ in range(len(s) - 4)]) + s[len(s) - 4 :] if s else None


def get_chunk_by_field_name(doc, field_name):
    """
    Gets a specific chunk by field name, using its position instead of getting the attribute directly.
    This solves the getattr problem when there are conflicting attributes with Document.

    :param doc: Document to get the chunk from.
    :param field_name: Field needed to extract the position.

    :return: Specific chunk by field.
    """
    try:
        field_position = int(
            doc._metadata['multi_modal_schema'][field_name]['position']
        )
        return doc.chunks[field_position]
    except Exception as e:
        raise e


# Add a custom retry exception
class RetryException(Exception):
    pass


class DemoAvailableException(Exception):
    pass
