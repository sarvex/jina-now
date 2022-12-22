import random
import sys
import uuid
from copy import deepcopy
from time import sleep
from typing import Dict, Optional

import requests
from docarray import DocumentArray
from jina.clients import Client
from tqdm import tqdm

from now.admin.update_api_keys import update_api_keys
from now.app.base.app import JinaNOWApp
from now.common.testing import handle_test_mode
from now.constants import ACCESS_PATHS
from now.data_loading.data_loading import load_data
from now.deployment.flow import deploy_flow
from now.log import time_profiler
from now.now_dataclasses import UserInput
from now.utils import add_env_variables_to_flow, get_flow_id


@time_profiler
def run(
    app_instance: JinaNOWApp,
    user_input: UserInput,
    kubectl_path: str,
    **kwargs,
):
    """
    TODO: Write docs

    :param app_instance:
    :param user_input:
    :param kubectl_path:
    :param ns:
    :return:
    """
    dataset = load_data(app_instance, user_input)

    env_dict = app_instance.setup(
        dataset=dataset, user_input=user_input, kubectl_path=kubectl_path
    )

    handle_test_mode(env_dict)
    add_env_variables_to_flow(app_instance, env_dict)
    (
        client,
        gateway_host,
        gateway_port,
        gateway_host_internal,
        gateway_port_internal,
    ) = deploy_flow(
        deployment_type=user_input.deployment_type,
        flow_yaml=app_instance.flow_yaml,
        env_dict=env_dict,
        kubectl_path=kubectl_path,
    )

    # TODO at the moment the scheduler is not working. So we index the data right away
    # if (
    #     user_input.deployment_type == 'remote'
    #     and user_input.dataset_type == DatasetTypes.S3_BUCKET
    #     and 'NOW_CI_RUN' not in os.environ
    # ):
    #     # schedule the trigger which will syn the bucket with the indexer once a day
    #     trigger_scheduler(user_input, gateway_host_internal)
    # else:
    # index the data right away
    index_docs(user_input, dataset, client)

    return (
        gateway_host,
        gateway_port,
        gateway_host_internal,
        gateway_port_internal,
    )


def trigger_scheduler(user_input, host):
    """
    This function will trigger the scheduler which will sync the bucket with the indexer once a day
    """
    print('Triggering scheduler to index data from S3 bucket')
    # check if the api_key exists. If not then create a new one
    if user_input.secured and not user_input.api_key:
        user_input.api_key = uuid.uuid4().hex
        # Also call the bff to update the api key
        for i in range(
            100
        ):  # increase the probability that all replicas get the new key
            update_api_keys(user_input.deployment_type, user_input.api_key, host)

    user_input_dict = user_input.__dict__
    user_input_dict.pop('app_instance')  # Not needed

    scheduler_params = {
        'flow_id': get_flow_id(host),
        'api_key': user_input.api_key,
        'user_input': user_input_dict,
    }
    cookies = {'st': user_input.jwt['token']}
    try:
        response = requests.post(
            'https://storefrontapi.nowrun.jina.ai/api/v1/schedule_sync',
            json=scheduler_params,
            cookies=cookies,
        )
        response.raise_for_status()
        print(
            'Scheduler triggered successfully. Scheduler will sync data from S3 bucket once a day.'
        )
    except Exception as e:
        print(f'Error while scheduling indexing: {e}')
        print(f'Indexing will not be scheduled. Please contact Jina AI support.')


def index_docs(user_input, dataset, client):
    """
    Index the data right away
    """
    print(f"▶ indexing {len(dataset)} documents in batches")
    params = {
        'user_input': user_input.__dict__,
        'access_paths': ACCESS_PATHS,
    }
    if user_input.secured:
        params['jwt'] = user_input.jwt
    call_flow(
        client=client,
        dataset=dataset,
        max_request_size=user_input.app_instance.max_request_size,
        parameters=deepcopy(params),
        return_results=False,
    )
    print('⭐ Success - your data is indexed')


@time_profiler
def call_flow(
    client: Client,
    dataset: DocumentArray,
    max_request_size: int,
    endpoint: str = '/index',
    parameters: Optional[Dict] = None,
    return_results: Optional[bool] = False,
):
    request_size = estimate_request_size(dataset, max_request_size)

    # Pop app_instance from parameters to be passed to the flow
    parameters['user_input'].pop('app_instance', None)
    task_config = parameters['user_input'].pop('task_config', None)
    if task_config:
        parameters['user_input']['indexer_scope'] = task_config.indexer_scope

    # this is a hack for the current core/ wolf issue
    # since we get errors while indexing, we retry
    # TODO: remove this once the issue is fixed
    batches = list(dataset.batch(request_size * 100))
    for current_batch_nr, batch in enumerate(tqdm(batches)):
        for try_nr in range(5):
            try:
                response = client.post(
                    on=endpoint,
                    request_size=request_size,
                    inputs=batch,
                    show_progress=True,
                    parameters=parameters,
                    return_results=return_results,
                    continue_on_error=True,
                )
                break
            except Exception as e:
                if try_nr == 4:
                    # if we tried 5 times and still failed, raise the error
                    raise e
                print(f'batch {current_batch_nr}, try {try_nr}', e)
                sleep(5 * (try_nr + 1))  # sleep for 5, 10, 15, 20 seconds
                continue

    if return_results and response:
        return DocumentArray.from_json(response.to_json())


def estimate_request_size(index, max_request_size):
    if len(index) > 30:
        sample = random.sample(index, 30)
    else:
        sample = index
    size = sum([sys.getsizeof(x.content) for x in sample]) / 30
    max_size = 50_000
    request_size = max(min(max_request_size, int(max_size / size)), 1)
    return request_size
