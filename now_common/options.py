"""
This module implements a command-line dialog with the user.
Its goal is to configure a UserInput object with users specifications.
Optionally, values can be passed from the command-line when jina-now is launched. In that case,
the dialog won't ask for the value.
"""
from __future__ import annotations, print_function, unicode_literals

import importlib
import os
from typing import Dict, List

from hubble import AuthenticationRequiredError
from kubernetes import client, config

from now.constants import AVAILABLE_DATASET, Apps, DatasetTypes, Qualities
from now.deployment.deployment import cmd
from now.log import yaspin_extended
from now.now_dataclasses import DialogOptions
from now.thirdparty.PyInquirer import Separator
from now.utils import _get_info_hubble, jina_auth_login, sigmap, to_camel_case

NEW_CLUSTER = {'name': '🐣 create new', 'value': 'new'}

# Make sure you add this dialog option to your app in order of dependency, i.e., if some dialog option depends on other
# then the parent should be called first before the dependant can called.

APP = DialogOptions(
    name='app',
    choices=[
        {
            'name': '📝 ▶ 🏞 text to image search',
            'value': Apps.TEXT_TO_IMAGE,
        },
        {
            'name': '🏞 ▶ 📝 image to text search',
            'value': Apps.IMAGE_TO_TEXT,
        },
        {
            'name': '🏞 ▶ 🏞 image to image search',
            'value': Apps.IMAGE_TO_IMAGE,
        },
        {'name': '📝 ▶ 📝 text to text search', 'value': Apps.TEXT_TO_TEXT},
        {
            'name': '📝 ▶ 🎦 text to video search (gif only at the moment)',
            'value': Apps.TEXT_TO_VIDEO,
        },
        {
            'name': '🥁 ▶ 🥁 music to music search',
            'value': Apps.MUSIC_TO_MUSIC,
        },
    ],
    prompt_message='What sort of search engine would you like to build?',
    prompt_type='list',
    is_terminal_command=True,
    description='What sort of search engine would you like to build?',
    post_func=lambda user_input, **kwargs: _construct_app(user_input, **kwargs),
)

# ------------------------------------ #

QUALITY = DialogOptions(
    name='quality',
    choices=[
        {'name': '🦊 medium (≈3GB mem, 15q/s)', 'value': Qualities.MEDIUM},
        {'name': '🐻 good (≈3GB mem, 2.5q/s)', 'value': Qualities.GOOD},
        {
            'name': '🦄 excellent (≈4GB mem, 0.5q/s)',
            'value': Qualities.EXCELLENT,
        },
    ],
    prompt_message='What quality do you expect?',
    prompt_type='list',
    is_terminal_command=True,
    description='Choose the quality of the model that you would like to finetune',
)


# --------------------------------------------- #

DATA = DialogOptions(
    name='data',
    prompt_message='What dataset do you want to use?',
    choices=lambda user_input, **kwargs: _get_data_choices(user_input, **kwargs),
    prompt_type='list',
    is_terminal_command=True,
    description='What dataset do you want to use?',
    post_func=lambda user_input, **kwargs: _parse_custom_data_from_cli(
        user_input, **kwargs
    ),
)

CUSTOM_DATASET_TYPE = DialogOptions(
    name='custom_dataset_type',
    prompt_message='How do you want to provide input? (format: https://docarray.jina.ai/)',
    choices=[
        {
            'name': 'DocArray name (recommended)',
            'value': DatasetTypes.DOCARRAY,
        },
        {
            'name': 'DocArray URL',
            'value': DatasetTypes.URL,
        },
        {
            'name': 'Local path',
            'value': DatasetTypes.PATH,
        },
        {
            'name': 'Download from S3 bucket',
            'value': DatasetTypes.S3_BUCKET,
        },
    ],
    prompt_type='list',
    is_terminal_command=False,
    depends_on=DATA,
    trigger_option_value='custom',
)

DATASET_NAME = DialogOptions(
    name='dataset_name',
    prompt_message='Please enter your DocArray name:',
    prompt_type='input',
    is_terminal_command=False,
    depends_on=CUSTOM_DATASET_TYPE,
    trigger_option_value=DatasetTypes.DOCARRAY,
)

DATASET_URL = DialogOptions(
    name='dataset_url',
    prompt_message='Please paste in the URL to download your DocArray from:',
    prompt_type='input',
    is_terminal_command=False,
    depends_on=CUSTOM_DATASET_TYPE,
    trigger_option_value=DatasetTypes.URL,
)

DATASET_PATH = DialogOptions(
    name='dataset_path',
    prompt_message='Please enter the path to the local folder:',
    prompt_type='input',
    is_terminal_command=False,
    depends_on=CUSTOM_DATASET_TYPE,
    trigger_option_value=DatasetTypes.PATH,
)

DATASET_PATH_S3 = DialogOptions(
    name='dataset_path',
    prompt_message='Please enter the S3 URI to the folder:',
    prompt_type='input',
    is_terminal_command=False,
    depends_on=CUSTOM_DATASET_TYPE,
    trigger_option_value=DatasetTypes.S3_BUCKET,
)

AWS_ACCESS_KEY_ID = DialogOptions(
    name='aws_access_key_id',
    prompt_message='Please enter the AWS access key ID:',
    prompt_type='input',
    is_terminal_command=False,
    depends_on=CUSTOM_DATASET_TYPE,
    trigger_option_value=DatasetTypes.S3_BUCKET,
)

AWS_SECRET_ACCESS_KEY = DialogOptions(
    name='aws_secret_access_key',
    prompt_message='Please enter the AWS secret access key:',
    prompt_type='input',
    is_terminal_command=False,
    depends_on=CUSTOM_DATASET_TYPE,
    trigger_option_value=DatasetTypes.S3_BUCKET,
)

AWS_REGION_NAME = DialogOptions(
    name='aws_region_name',
    prompt_message='Please enter the AWS region:',
    prompt_type='input',
    is_terminal_command=False,
    depends_on=CUSTOM_DATASET_TYPE,
    trigger_option_value=DatasetTypes.S3_BUCKET,
)

# --------------------------------------------- #

DEPLOYMENT_TYPE = DialogOptions(
    name='deployment_type',
    choices=[
        {
            'name': '⛅️ Jina Cloud',
            'value': 'remote',
        },
        {
            'name': '📍 Local',
            'value': 'local',
        },
    ],
    prompt_message='Where do you want to deploy your search engine?',
    prompt_type='list',
    is_terminal_command=True,
    description='Where do you want to deploy your search engine?',
    post_func=lambda user_input, **kwargs: _jina_auth_login(user_input, **kwargs),
)


LOCAL_CLUSTER = DialogOptions(
    name='cluster',
    choices=lambda user_input, **kwargs: _construct_local_cluster_choices(**kwargs),
    prompt_message='On which cluster do you want to deploy your search engine?',
    prompt_type='list',
    is_terminal_command=False,
    post_func=lambda user_input, **kwargs: _check_requirements_match(
        user_input, **kwargs
    ),
    depends_on=DEPLOYMENT_TYPE,
    trigger_option_value='local',
)

PROCEED = DialogOptions(
    prompt_type='list',
    name='proceed',
    prompt_message=(
        'jina-now is deployed already. Do you want to remove the ' 'current data?'
    ),
    choices=[
        {'name': '⛔ no', 'value': False},
        {'name': '✅ yes', 'value': True},
    ],
    is_terminal_command=False,
    depends_on=LOCAL_CLUSTER,
)

SECURED = DialogOptions(
    name='secured',
    prompt_message='Do you want to secure the flow?',
    choices=[
        {'name': '✅ yes', 'value': True},
        {'name': '⛔ no', 'value': False},
    ],
    prompt_type='list',
    is_terminal_command=False,
    depends_on=DEPLOYMENT_TYPE,
    trigger_option_value='remote',
)

ADDITIONAL_USERS = DialogOptions(
    name='additional_user',
    choices=[
        {'name': '✅ yes', 'value': True},
        {'name': '⛔ no', 'value': False},
    ],
    prompt_message='Do you want to provide additional users access to this flow?',
    prompt_type='list',
    is_terminal_command=False,
    depends_on=SECURED,
    trigger_option_value=True,
)

USER_EMAILS = DialogOptions(
    name='user_emails',
    prompt_message='Please enter the comma separated Email IDs'
    'who will have access to this flow:',
    prompt_type='input',
    is_terminal_command=False,
    depends_on=ADDITIONAL_USERS,
    trigger_option_value=True,
)


def _construct_app(user_input: UserInput, **kwargs) -> None:
    user_input.app_instance = getattr(
        importlib.import_module(f'now.apps.{user_input.app}.app'),
        f'{to_camel_case(user_input.app)}',
    )()


def _check_requirements_match(user_input: UserInput, **kwargs) -> None:
    user_input.app_instance.run_checks(user_input)


def _jina_auth_login(user_input, **kwargs):
    try:
        jina_auth_login()
    except AuthenticationRequiredError:
        with yaspin_extended(
            sigmap=sigmap, text='Log in to JCloud', color='green'
        ) as spinner:
            cmd('jina auth login')
        spinner.ok('🛠️')

    _get_info_hubble(user_input)
    os.environ['JCLOUD_NO_SURVEY'] = '1'


def _get_data_choices(user_input, **kwargs) -> List[Dict[str, str]]:
    app_instance = user_input.app_instance
    return [
        {'name': name, 'value': value}
        for value, name in AVAILABLE_DATASET[app_instance.output_modality]
    ] + [
        Separator(),
        {
            'name': '✨ custom',
            'value': 'custom',
        },
    ]


def _construct_local_cluster_choices(**kwargs):
    active_context = kwargs.get('active_context')
    contexts = kwargs.get('contexts')
    context_names = _get_context_names(contexts, active_context)
    choices = [NEW_CLUSTER]
    # filter contexts with `gke`
    if len(context_names) > 0 and len(context_names[0]) > 0:
        context_names = [
            context
            for context in context_names
            if 'gke' not in context and _cluster_running(context)
        ]
        choices = context_names + choices
    return choices


def _get_context_names(contexts, active_context=None):
    names = [c for c in contexts] if contexts is not None else []
    if active_context is not None:
        names.remove(active_context)
        names = [active_context] + names
    return names


def _cluster_running(cluster):
    config.load_kube_config(context=cluster)
    v1 = client.CoreV1Api()
    try:
        v1.list_namespace()
    except Exception as e:
        return False
    return True


def _parse_custom_data_from_cli(user_input: UserInput, **kwargs) -> None:
    data = user_input.data
    app_instance = user_input.app_instance

    for k, v in enumerate(AVAILABLE_DATASET[app_instance.output_modality]):
        if v[0] == data:
            return

    try:
        data = os.path.expanduser(data)
    except Exception:
        pass
    if os.path.exists(data):
        user_input.custom_dataset_type = DatasetTypes.PATH
        user_input.dataset_path = data
    elif 'http' in data:
        user_input.custom_dataset_type = DatasetTypes.URL
        user_input.dataset_url = data
    else:
        user_input.custom_dataset_type = DatasetTypes.DOCARRAY
        user_input.dataset_name = data


app = [APP, DATA]
data_da = [CUSTOM_DATASET_TYPE, DATASET_NAME, DATASET_PATH, DATASET_URL]
data_s3 = [DATASET_PATH_S3, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME]
cluster = [DEPLOYMENT_TYPE, LOCAL_CLUSTER]
remote_cluster = [SECURED, ADDITIONAL_USERS, USER_EMAILS]


base_options = app + data_da + data_s3 + cluster + remote_cluster
