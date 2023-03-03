from __future__ import annotations, print_function, unicode_literals

import os
import shutil
import signal
import sys
from collections.abc import MutableMapping
from dataclasses import dataclass
from inspect import stack
from typing import Any, Dict, List, Optional, TypeVar, Union

import boto3
import cowsay
import docarray
import hubble
import yaml
from jina.jaml import JAML
from pyfiglet import Figlet

from now.deployment.deployment import list_all_wolf, status_wolf
from now.thirdparty.PyInquirer.prompt import prompt


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
    return maybe_prompt_user(qs, name, **kwargs)


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
    return ''.join(['*' for _ in range(len(s) - 5)]) + s[len(s) - 4 :] if s else None


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
        print(f'An error occurred: {e}')


def get_flow_status(action, **kwargs):
    choices = []
    # Add all remote Flows that exists with the namespace `nowapi`
    alive_flows = list_all_wolf(status='Serving')
    for flow_details in alive_flows:
        choices.append(flow_details['name'])
    if len(choices) == 0:
        cowsay.cow(f'nothing to {action}')
        return
    else:
        questions = [
            {
                'type': 'list',
                'name': 'cluster',
                'message': f'Which cluster do you want to {action}?',
                'choices': choices,
            }
        ]
        cluster = maybe_prompt_user(questions, 'cluster', **kwargs)

    flow = [x for x in alive_flows if x['name'] == cluster][0]
    flow_id = flow['id']
    _result = status_wolf(flow_id)
    if _result is None:
        print(f'❎ Flow not found in JCloud. Likely, it has been deleted already')
    return _result, flow_id, cluster


# Add a custom retry exception
class RetryException(Exception):
    pass


class DemoAvailableException(Exception):
    pass
