"""
This module implements a command-line dialog with the user.
Its goal is to configure a UserInput object with users specifications.
Optionally, values can be passed from the command-line when jina-now is launched. In that case,
the dialog won't ask for the value.
"""
from __future__ import annotations, print_function, unicode_literals

import glob
import importlib
import os
import uuid

from hubble import AuthenticationRequiredError
from kubernetes import client, config

from now.constants import Apps, DatasetTypes
from now.data_loading.utils import _get_s3_bucket_and_folder_prefix
from now.deployment.deployment import cmd
from now.log import yaspin_extended
from now.now_dataclasses import DialogOptions, UserInput
from now.utils import (
    _get_context_names,
    get_info_hubble,
    jina_auth_login,
    sigmap,
    to_camel_case,
)

NEW_CLUSTER = {'name': '🐣 create new', 'value': 'new'}
AVAILABLE_SOON = 'will be available in upcoming versions'

# Make sure you add this dialog option to your app in order of dependency, i.e., if some dialog option depends on other
# than the parent should be called first before the dependant can called.


def _create_app_from_output_modality(user_input: UserInput, **kwargs):
    if user_input.output_modality in ['image', 'text']:
        app_name = Apps.IMAGE_TEXT_RETRIEVAL
    elif user_input.output_modality == 'video':
        app_name = Apps.TEXT_TO_VIDEO
    else:
        raise ValueError(f'Invalid output modality: {user_input.output_modality}')
    user_input.app_instance = construct_app(app_name)


def _get_schema_docarray(user_input: UserInput, **kwargs):
    from hubble import Client

    if user_input.jwt is None:
        client = Client()
    else:
        client = Client(token=user_input.jwt['token'])

    resp = client.get_artifact_info(name='test_subset_laion')
    if resp.json()['code'] == 200:
        field_names = resp.json()['data']['metaData']['summary'][3]['value']
        for el in ['embedding', 'id', 'mime_type']:
            if el in field_names:
                field_names.remove(el)
        user_input.field_names = field_names
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
            field_names = os.listdir('/'.join(first_path.split('/')[:-1]))
            user_input.field_names = field_names


OUTPUT_MODALITY = DialogOptions(
    name='output_modality',
    choices=[
        {'name': '📝 text', 'value': 'text'},
        {'name': '🏞 image', 'value': 'image'},
        {'name': '🎦 video', 'value': 'video'},
    ],
    prompt_type='list',
    prompt_message='What modality do you want to index?',
    description='What is the index modality of your search system?',
    is_terminal_command=True,
    post_func=_create_app_from_output_modality,
)


APP_NAME = DialogOptions(
    name='flow_name',
    prompt_message='Choose a name for your application:',
    prompt_type='input',
    is_terminal_command=True,
)


DATASET_TYPE = DialogOptions(
    name='dataset_type',
    prompt_message='How do you want to provide input? (format: https://docarray.jina.ai/)',
    choices=[
        {'name': 'Demo dataset', 'value': DatasetTypes.DEMO},
        {
            'name': 'DocumentArray name (recommended)',
            'value': DatasetTypes.DOCARRAY,
        },
        {
            'name': 'DocumentArray URL',
            'value': DatasetTypes.URL,
        },
        {
            'name': 'Local folder',
            'value': DatasetTypes.PATH,
        },
        {
            'name': 'S3 bucket',
            'value': DatasetTypes.S3_BUCKET,
        },
        {
            'name': 'Elasticsearch',
            'value': DatasetTypes.ELASTICSEARCH,
            'disabled': AVAILABLE_SOON,
        },
    ],
    prompt_type='list',
    is_terminal_command=True,
    post_func=lambda user_input, **kwargs: _jina_auth_login(user_input, **kwargs),
)


DEMO_DATA = DialogOptions(
    name='dataset_name',
    prompt_message='What demo dataset do you want to use?',
    choices=lambda user_input, **kwargs: _get_demo_data_choices(user_input),
    prompt_type='list',
    depends_on=DATASET_TYPE,
    is_terminal_command=True,
    description='Select one of the available demo datasets',
    conditional_check=lambda user_input, **kwargs: user_input.dataset_type
    == DatasetTypes.DEMO,
)


def _get_demo_data_choices(user_input: UserInput):
    if user_input.output_modality:
        ds = user_input.app_instance.demo_datasets[user_input.output_modality]
    else:
        ds = user_input.app_instance.demo_datasets
    return [
        {'name': demo_data.display_name, 'value': demo_data.name} for demo_data in ds
    ]


DOCARRAY_NAME = DialogOptions(
    name='dataset_name',
    prompt_message='Please enter your DocumentArray name:',
    prompt_type='input',
    depends_on=DATASET_TYPE,
    conditional_check=lambda user_input: user_input.dataset_type
    == DatasetTypes.DOCARRAY,
    post_func=lambda user_input, **kwargs: _get_schema_docarray(user_input),
)

DATASET_PATH = DialogOptions(
    name='dataset_path',
    prompt_message='Please enter the path to the local folder:',
    prompt_type='input',
    depends_on=DATASET_TYPE,
    is_terminal_command=True,
    conditional_check=lambda user_input: user_input.dataset_type == DatasetTypes.PATH,
    post_func=lambda user_input, **kwargs: _get_schema_local_folder(user_input),
)

DATASET_PATH_S3 = DialogOptions(
    name='dataset_path',
    prompt_message='Please enter the S3 URI to the folder:',
    prompt_type='input',
    depends_on=DATASET_TYPE,
    conditional_check=lambda user_input: user_input.dataset_type
    == DatasetTypes.S3_BUCKET,
)

AWS_ACCESS_KEY_ID = DialogOptions(
    name='aws_access_key_id',
    prompt_message='Please enter the AWS access key ID:',
    prompt_type='input',
    depends_on=DATASET_TYPE,
    conditional_check=lambda user_input: user_input.dataset_type
    == DatasetTypes.S3_BUCKET,
)

AWS_SECRET_ACCESS_KEY = DialogOptions(
    name='aws_secret_access_key',
    prompt_message='Please enter the AWS secret access key:',
    prompt_type='input',
    depends_on=DATASET_TYPE,
    conditional_check=lambda user_input: user_input.dataset_type
    == DatasetTypes.S3_BUCKET,
)

AWS_REGION_NAME = DialogOptions(
    name='aws_region_name',
    prompt_message='Please enter the AWS region:',
    prompt_type='input',
    depends_on=DATASET_TYPE,
    conditional_check=lambda user_input: user_input.dataset_type
    == DatasetTypes.S3_BUCKET,
    post_func=lambda user_input, **kwargs: _get_schema_s3_bucket(user_input),
)

# --------------------------------------------- #

'''SEARCH_FIELDS = DialogOptions(
    name='search_fields',
    prompt_message='Enter comma-separated search fields:',
    prompt_type='input',
    depends_on=DATASET_TYPE,
    is_terminal_command=True,
    conditional_check=lambda user_input: user_input.dataset_type != DatasetTypes.DEMO,
    post_func=lambda user_input, **kwargs: _parse_search_fields(user_input),
)


def _parse_search_fields(user_input: UserInput):
    user_input.search_fields = [
        field.strip() for field in user_input.search_fields.split(',')
    ]'''


SEARCH_FIELDS = DialogOptions(
    name='search_fields',
    choices=lambda user_input, **kwargs: _get_fields(user_input),
    prompt_message='Please select the search fields:',
    prompt_type='checkbox',
    depends_on=DATASET_TYPE,
    conditional_check=lambda user_input: len(user_input.field_names) > 0
    and user_input.dataset_type != DatasetTypes.DEMO,
    post_func=lambda user_input, **kwargs: _exclude_search_fields(user_input),
)


def _exclude_search_fields(user_input: UserInput):
    s = set(user_input.search_fields)
    user_input.field_names = [x for x in user_input.field_names if x not in s]


FILTER_FIELDS = DialogOptions(
    name='filter_fields',
    choices=lambda user_input, **kwargs: _get_fields(user_input),
    prompt_message='Please select the filter fields',
    prompt_type='checkbox',
    depends_on=DATASET_TYPE,
    conditional_check=lambda user_input: len(user_input.field_names) > 0
    and user_input.dataset_type != DatasetTypes.DEMO,
)


def _get_fields(user_input: UserInput):
    return [{'name': field, 'value': field} for field in user_input.field_names]


ES_INDEX_NAME = DialogOptions(
    name='es_index_name',
    prompt_message='Please enter the name of your Elasticsearch index:',
    prompt_type='input',
    depends_on=DATASET_TYPE,
    conditional_check=lambda user_input: user_input.dataset_type
    == DatasetTypes.ELASTICSEARCH,
)

ES_HOST_NAME = DialogOptions(
    name='es_host_name',
    prompt_message='Please enter the address of your Elasticsearch node:',
    prompt_type='input',
    depends_on=DATASET_TYPE,
    conditional_check=lambda user_input: user_input.dataset_type
    == DatasetTypes.ELASTICSEARCH,
)

ES_ADDITIONAL_ARGS = DialogOptions(
    name='es_additional_args',
    prompt_message='Please enter additional arguments for your Elasticsearch node if there are any:',
    prompt_type='input',
    depends_on=DATASET_TYPE,
    conditional_check=lambda user_input: user_input.dataset_type
    == DatasetTypes.ELASTICSEARCH,
)

# --------------------------------------------- #

DEPLOYMENT_TYPE = DialogOptions(
    name='deployment_type',
    prompt_message='Where do you want to deploy your search engine?',
    prompt_type='list',
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
    is_terminal_command=True,
    description='Option is `local` and `remote`. Select `local` if you want search engine to be deployed on local '
    'cluster. Select `remote` to deploy it on Jina Cloud',
    post_func=lambda user_input, **kwargs: _jina_auth_login(user_input, **kwargs),
)

LOCAL_CLUSTER = DialogOptions(
    name='cluster',
    prompt_message='On which cluster do you want to deploy your search engine?',
    prompt_type='list',
    choices=lambda user_input, **kwargs: _construct_local_cluster_choices(
        user_input, **kwargs
    ),
    depends_on=DEPLOYMENT_TYPE,
    is_terminal_command=True,
    description='Reference an existing `local` cluster or select `new` to create a new one. '
    'Use this only when the `--deployment-type=local`',
    conditional_check=lambda user_inp: user_inp.deployment_type == 'local',
    post_func=lambda user_input, **kwargs: user_input.app_instance.run_checks(
        user_input
    ),
)

PROCEED = DialogOptions(
    name='proceed',
    prompt_message='jina-now is deployed already. Do you want to remove the current data?',
    prompt_type='list',
    choices=[
        {'name': '⛔ no', 'value': False},
        {'name': '✅ yes', 'value': True},
    ],
    depends_on=LOCAL_CLUSTER,
    conditional_check=lambda user_input: _check_if_namespace_exist(),
)

SECURED = DialogOptions(
    name='secured',
    prompt_message='Do you want to secure the flow?',
    prompt_type='list',
    choices=[
        {'name': '⛔ no', 'value': False},
        {'name': '✅ yes', 'value': True},
    ],
    depends_on=DEPLOYMENT_TYPE,
    is_terminal_command=True,
    conditional_check=lambda user_inp: user_inp.deployment_type == 'remote',
)


API_KEY = DialogOptions(
    name='api_key',
    prompt_message='Do you want to generate an api_key to access this deployment?',
    prompt_type='list',
    choices=[
        {'name': '✅ yes', 'value': uuid.uuid4().hex},
        {'name': '⛔ no', 'value': False},
    ],
    depends_on=SECURED,
    is_terminal_command=True,
    description='Pass an api_key to access the flow once the deployment is complete. ',
    conditional_check=lambda user_inp: str(user_inp.secured).lower() == 'true',
    post_func=lambda user_input, **kwargs: _set_value_to_none(user_input),
)

ADDITIONAL_USERS = DialogOptions(
    name='additional_user',
    prompt_message='Do you want to provide additional users access to this flow?',
    prompt_type='list',
    choices=[
        {'name': '✅ yes', 'value': True},
        {'name': '⛔ no', 'value': False},
    ],
    depends_on=SECURED,
    conditional_check=lambda user_inp: str(user_inp.secured).lower() == 'true',
)

USER_EMAILS = DialogOptions(
    name='user_emails',
    prompt_message='Please enter the comma separated Email IDs '
    'who will have access to this flow.\nAdditionally, you can also specify comma separated domain name'
    ' such that all users from that domain can access this flow. E.g. `jina.ai`\n',
    prompt_type='input',
    depends_on=ADDITIONAL_USERS,
    conditional_check=lambda user_inp: user_inp.additional_user,
    post_func=lambda user_input, **kwargs: _add_additional_users(user_input, **kwargs),
)


def _set_value_to_none(user_input):
    if not user_input.api_key:
        user_input.api_key = None


def _add_additional_users(user_input: UserInput, **kwargs):
    user_input.user_emails = (
        [email.strip() for email in kwargs['user_emails'].split(',')]
        if kwargs['user_emails']
        else []
    )


def _check_if_namespace_exist():
    config.load_kube_config()
    v1 = client.CoreV1Api()
    return 'nowapi' in [item.metadata.name for item in v1.list_namespace().items]


def construct_app(app_name: str):
    return getattr(
        importlib.import_module(f'now.app.{app_name}.app'),
        f'{to_camel_case(app_name)}',
    )()


def _jina_auth_login(user_input, **kwargs):
    if user_input.deployment_type != 'remote' or user_input.jwt is not None:
        return

    if user_input.dataset_type != DatasetTypes.DOCARRAY or user_input.jwt is not None:
        return

    try:
        jina_auth_login()
    except AuthenticationRequiredError:
        with yaspin_extended(
            sigmap=sigmap, text='Log in to JCloud', color='green'
        ) as spinner:
            cmd('jina auth login')
        spinner.ok('🛠️')

    get_info_hubble(user_input)
    os.environ['JCLOUD_NO_SURVEY'] = '1'


def _construct_local_cluster_choices(user_input, **kwargs):
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


def _cluster_running(cluster):
    config.load_kube_config(context=cluster)
    v1 = client.CoreV1Api()
    try:
        v1.list_namespace()
    except Exception as e:
        return False
    return True


app_config = [OUTPUT_MODALITY, APP_NAME]
data_type = [DATASET_TYPE]
data_fields = [SEARCH_FIELDS, FILTER_FIELDS]
data_demo = [DEMO_DATA]
data_da = [DOCARRAY_NAME, DATASET_PATH]
data_s3 = [DATASET_PATH_S3, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME]
data_es = [
    ES_HOST_NAME,
    ES_INDEX_NAME,
    ES_ADDITIONAL_ARGS,
]
cluster = [DEPLOYMENT_TYPE, LOCAL_CLUSTER]
remote_cluster = [SECURED, API_KEY, ADDITIONAL_USERS, USER_EMAILS]

base_options = (
    data_type
    + data_demo
    + data_da
    + data_s3
    + data_es
    + data_fields
    + cluster
    + remote_cluster
    + app_config
)
