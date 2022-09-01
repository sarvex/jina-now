import dataclasses
import os
import pathlib
import random
import sys
from time import sleep
from typing import Dict, Optional

from docarray import DocumentArray
from jina.clients import Client

from now.apps.base.app import JinaNOWApp
from now.data_loading.data_loading import load_data
from now.deployment.flow import deploy_flow
from now.log import time_profiler
from now.now_dataclasses import UserInput

cur_dir = pathlib.Path(__file__).parent.resolve()


@time_profiler
def run(app_instance: JinaNOWApp, user_input: UserInput, kubectl_path: str):
    """
    TODO: Write docs

    :param user_input:
    :param kubectl_path:
    :return:
    """
    dataset = load_data(app_instance, user_input)

    env_dict = app_instance.setup(
        dataset=dataset, user_input=user_input, kubectl_path=kubectl_path
    )

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
        ns='nowapi',
        kubectl_path=kubectl_path,
        secured=user_input.secured,
        admin_emails=user_input.admin_emails,
        user_emails=user_input.user_emails,
    )

    print(f"▶ indexing {len(dataset)} documents")
    params = {
        'user_input': dataclasses.asdict(user_input),
        'traversal_paths': app_instance.index_query_access_paths,
        'access_paths': app_instance.index_query_access_paths,
    }
    if user_input.secured:
        params['jwt'] = user_input.jwt
    call_index(client=client, dataset=dataset, parameters=params, return_results=False)
    print('⭐ Success - your data is indexed')

    return gateway_host, gateway_port, gateway_host_internal, gateway_port_internal


@time_profiler
def call_index(
    client: Client,
    dataset: DocumentArray,
    parameters: Optional[Dict] = None,
    return_results: Optional[bool] = False,
):
    request_size = estimate_request_size(dataset)

    # double check that flow is up and running - should be done by wolf/core in the future
    while True:
        try:
            client.post('/index', inputs=DocumentArray(), parameters=parameters)
            break
        except Exception as e:
            if 'NOW_CI_RUN' in os.environ:
                import traceback

                print(e)
                print(traceback.format_exc())
            sleep(1)

    response = client.post(
        '/index',
        request_size=request_size,
        inputs=dataset,
        show_progress=True,
        parameters=parameters,
    )

    if return_results and response:
        return DocumentArray.from_json(response.to_json())


def estimate_request_size(index):
    if len(index) > 30:
        sample = random.sample(index, 30)
    else:
        sample = index
    size = sum([sys.getsizeof(x.content) for x in sample]) / 30
    max_size = 50_000
    max_request_size = 32
    request_size = max(min(max_request_size, int(max_size / size)), 1)
    return request_size
