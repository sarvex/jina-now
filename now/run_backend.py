import os
import sys
import time
import uuid
from copy import deepcopy
from typing import Dict, Optional

import requests
from docarray import DocumentArray
from jina.clients import Client

from now.admin.update_api_keys import update_api_keys
from now.app.base.app import JinaNOWApp
from now.constants import ACCESS_PATHS
from now.data_loading.data_loading import load_data
from now.deployment.flow import deploy_flow
from now.log import time_profiler
from now.now_dataclasses import UserInput
from now.utils.jcloud.helpers import get_flow_id


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

    dataset = load_data(user_input, print_callback)
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
        # increase the probability that all replicas get the new key
        for i in range(100):
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


@time_profiler
def index_docs(user_input, dataset, client, print_callback, **kwargs):
    """
    Index the data right away
    """
    print_callback(f"▶ indexing {len(dataset)} documents")
    params = {'access_paths': ACCESS_PATHS}
    if user_input.secured:
        params['jwt'] = user_input.jwt
    request_size = estimate_request_size(
        dataset, user_input.app_instance.max_request_size
    )

    call_flow_with_retry(
        client=client,
        on='/index',
        request_size=request_size,
        inputs=dataset,
        parameters=deepcopy(params),
        continue_on_error=True,
        prefetch=request_size * 5,
        on_error=kwargs.get('on_error', None),
        on_always=kwargs.get('on_always', None),
        sleep_interval=kwargs.get('sleep_interval', 5),
        num_retries=kwargs.get('num_retries', 10),
    )

    print_callback('⭐ Success - your data is indexed')


def estimate_request_size(index, max_request_size):
    if len(index) == 0:
        return 1

    # We assume that it is homogeneous multimodal DocumentArray,
    # therefore pick the first document to estimate the size in bytes
    size = sys.getsizeof(index[0].content) + sum(
        [sys.getsizeof(chunk.content) for chunk in index[0].chunks]
    )
    max_size = 1e6  # 1 MB
    request_size = max(min(max_request_size, int(max_size / size)), 1)
    return request_size


def call_flow_with_retry(
    client: Client,
    on: str,
    request_size: int,
    inputs: DocumentArray,
    parameters: Optional[Dict] = None,
    continue_on_error: bool = True,
    prefetch: int = 1000,
    on_error=None,
    on_always=None,
    sleep_interval: int = 3,
    num_retries: int = 10,
):
    print_if_example = (
        print
        if any(map(lambda x: x in os.environ, ['NOW_EXAMPLES', 'NOW_CI_RUN']))
        else lambda x: None
    )

    if len(inputs) == 0:
        print('No documents to index')
        return

    from jina.logging.profile import ProgressBar
    from jina.types.request import Request

    init_inputs_len = len(inputs)
    pbar = ProgressBar(
        description=f'Indexing {init_inputs_len} docs',
        total_length=init_inputs_len,
        message_on_done='Indexing completed',
    )
    completed_req_ids = []

    def on_done(r: Request):
        nonlocal pbar, completed_req_ids
        if len(r.data.docs) == 0:
            print_if_example('No docs in response.')
            return

        for doc in r.data.docs:
            completed_req_ids.append(doc.id)  # update the completion list
        pbar.update(advance=len(r.data.docs))  # update the progress bar

        if (
            len(completed_req_ids) // len(r.data.docs)
        ) % 5 == 0:  # print after every 5 inserts
            print_if_example(f'Indexed {len(completed_req_ids)} docs.')

    def _on_error(r: Request):
        print_if_example(
            'Got an executor error while indexing. '
            'Should trigger a retry using Jina\'s inbuilt mechanism.'
        )

    def stream_requests_until_done(docs: DocumentArray):
        return client.post(
            on=on,
            inputs=docs,
            request_size=request_size,
            show_progress=False,
            parameters=parameters,
            continue_on_error=continue_on_error,
            prefetch=prefetch,
            on_done=on_done,
            on_error=on_error if on_error else _on_error,
            on_always=on_always,
        )

    def sleep_before_retry():
        print_if_example(
            f'Sleeping for {sleep_interval} seconds, '
            f'before retrying {len(inputs)} docs'
        )
        time.sleep(sleep_interval)

    with pbar:
        for _ in range(num_retries):
            # Remove the docs which are already indexed before retrying
            for doc_id in completed_req_ids:
                try:
                    del inputs[doc_id]
                except Exception as e:  # noqa
                    print_if_example(f'Error while removing {e}')

            # Index the remaining docs
            try:
                stream_requests_until_done(inputs)
                break
            except Exception as e:
                # Retry if there is an exception (usually network errors)
                print_if_example(
                    f'Got a network error. Retry using our custom mechanism: {e}'
                )
                sleep_before_retry()
