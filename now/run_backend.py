import os
import random
import sys
import threading
import time
import uuid
from copy import deepcopy
from typing import Dict, Optional

import requests
from docarray import DocumentArray
from jina.clients import Client

from now.admin.update_api_keys import update_api_keys
from now.app.base.app import JinaNOWApp
from now.constants import ACCESS_PATHS, DatasetTypes
from now.data_loading.create_dataclass import create_dataclass
from now.data_loading.data_loading import load_data
from now.deployment.flow import deploy_flow
from now.log import time_profiler
from now.now_dataclasses import UserInput
from now.utils import get_flow_id


@time_profiler
def run(
    app_instance: JinaNOWApp,
    user_input: UserInput,
    **kwargs,
):
    """
    This function will run the backend of the app. Specifically, it will:
    - Load the data
    - Set up the flow dynamically and get the environment variables
    - Deploy the flow
    - Index the data
    :param app_instance: The app instance
    :param user_input: The user input
    :param kwargs: Additional arguments
    :return:
    """
    print_callback = kwargs.get('print_callback', print)
    if user_input.dataset_type in [DatasetTypes.DEMO, DatasetTypes.DOCARRAY]:
        user_input.field_names_to_dataclass_fields = {
            field: field for field in user_input.index_fields
        }
        data_class = None
    else:
        data_class, user_input.field_names_to_dataclass_fields = create_dataclass(
            user_input=user_input
        )

    dataset = load_data(user_input, data_class, print_callback)
    print_callback('Data loaded. Deploying the flow...')

    # Set up the app specific flow
    app_instance.setup(user_input=user_input)

    client, gateway_port, gateway_host_internal = deploy_flow(
        flow_yaml=app_instance.flow_yaml
    )

    # TODO at the moment the scheduler is not working. So we index the data right away
    # if (
    #     user_input.deployment_type == 'remote'
    #     and user_input.dataset_type == DatasetTypes.S3_BUCKET
    #     and 'NOW_CI_RUN' not in os.environ
    # ):
    #     # schedule the trigger which will sync the bucket with the indexer once a day
    #     trigger_scheduler(user_input, gateway_host_internal)
    # else:
    # index the data right away
    print_callback('Flow deployed. Indexing the data...')
    index_docs(user_input, dataset, client, print_callback, **kwargs)

    return (
        gateway_port,
        gateway_host_internal,
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
            update_api_keys(user_input.api_key, host)

    scheduler_params = {
        'flow_id': get_flow_id(host),
        'api_key': user_input.api_key,
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


def index_docs(user_input, dataset, client, print_callback, **kwargs):
    """
    Index the data right away
    """
    print_callback(f"▶ indexing {len(dataset)} documents")
    params = {'access_paths': ACCESS_PATHS}
    if user_input.secured:
        params['jwt'] = user_input.jwt
    call_flow(
        client=client,
        dataset=dataset,
        max_request_size=user_input.app_instance.max_request_size,
        parameters=deepcopy(params),
        return_results=False,
        **kwargs,
    )
    print_callback('⭐ Success - your data is indexed')


@time_profiler
def call_flow(
    client: Client,
    dataset: DocumentArray,
    max_request_size: int,
    endpoint: str = '/index',
    parameters: Optional[Dict] = None,
    return_results: Optional[bool] = False,
    **kwargs,
):
    request_size = estimate_request_size(dataset, max_request_size)

    response = call_flow_with_retry(
        client=client,
        on=endpoint,
        request_size=request_size,
        inputs=dataset,
        show_progress=True,
        parameters=parameters,
        continue_on_error=True,
        prefetch=100,
        on_error=kwargs.get('on_error', None),
        on_always=kwargs.get('on_always', None),
        num_retries=kwargs.get('num_retries', 10),
        sleep_interval=kwargs.get('sleep_interval', 5),
    )
    if return_results:
        return response


def estimate_request_size(index, max_request_size):
    if len(index) > 30:
        sample = random.sample(index, 30)
    else:
        sample = index
    size = sum([sys.getsizeof(x.content) for x in sample]) / 30
    max_size = 50_000
    request_size = max(min(max_request_size, int(max_size / size)), 1)
    return request_size


def call_flow_with_retry(
    client: Client,
    on: str,
    request_size: int,
    inputs: DocumentArray,
    show_progress: bool,
    parameters: Optional[Dict] = None,
    continue_on_error: bool = True,
    prefetch: int = 100,
    on_error=None,
    on_always=None,
    num_retries: int = 10,
    sleep_interval: int = 5,
):
    print_if_ci = lambda msg: print(msg) if 'NOW_CI_RUN' in os.environ else None

    if len(inputs) == 0:
        print('No documents to index')
        return

    from jina.types.request import Request

    init_inputs_len = len(inputs)
    on_done_len = 0
    on_done_lock = threading.Lock()

    def on_done(r: Request):
        nonlocal inputs, on_done_len
        on_done_len += len(r.data.docs)
        if on_done_len != 0 and on_done_len % 100 == 0:
            print_if_ci(
                f'Completed indexing {on_done_len} docs. current requestid: {r.header.request_id}'
            )
        with on_done_lock:
            for doc in r.data.docs:
                try:
                    del inputs[doc.id]
                except Exception as e:
                    print_if_ci(f'Error while removing {e}')

    def _on_error(r: Request):
        print_if_ci(f'Got an error while indexing requestid: {r.header.request_id}')

    def stream_requests_until_done(docs: DocumentArray):
        return client.post(
            on=on,
            inputs=docs,
            request_size=request_size,
            show_progress=show_progress,
            parameters=parameters,
            continue_on_error=continue_on_error,
            prefetch=prefetch,
            on_done=on_done,
            on_error=on_error if on_error else _on_error,
            on_always=on_always,
        )

    def sleep_before_retry():
        print(
            f'Sleeping for {sleep_interval} seconds, before retrying {len(inputs)} docs'
        )
        time.sleep(sleep_interval)

    for _ in range(num_retries):
        try:
            stream_requests_until_done(inputs)
            if len(inputs) == 0 or on_done_len == init_inputs_len:
                print_if_ci('All docs indexed successfully')
                return
            else:
                # Retry indexing docs that reached on_error
                sleep_before_retry()
        except Exception as e:
            # Retry if there is an exception (usually network errors)
            print_if_ci(f'Exception while indexing: {e}')
            sleep_before_retry()
