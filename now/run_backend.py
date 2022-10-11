import os
import pathlib
import random
import sys
from copy import deepcopy
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
def run(
    app_instance: JinaNOWApp,
    user_input: UserInput,
    kubectl_path: str,
    ns: str = 'nowapi',
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
        ns=ns,
        kubectl_path=kubectl_path,
    )

    print(f"▶ indexing {len(dataset)} documents")
    params = {
        'user_input': user_input.__dict__,
        'traversal_paths': app_instance.index_query_access_paths,
        'access_paths': app_instance.index_query_access_paths,
    }
    if user_input.secured:
        params['jwt'] = user_input.jwt
    call_flow(
        client=client,
        dataset=dataset,
        max_request_size=app_instance.max_request_size,
        parameters=deepcopy(params),
        return_results=False,
    )
    print('⭐ Success - your data is indexed')

    return (
        gateway_host,
        gateway_port,
        gateway_host_internal,
        gateway_port_internal,
    )


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

    # double check that flow is up and running - should be done by wolf/core in the future
    while True:
        try:
            client.post(on=endpoint, inputs=DocumentArray(), parameters=parameters)
            break
        except Exception as e:
            if 'NOW_CI_RUN' in os.environ:
                import traceback

                print(e)
                print(traceback.format_exc())
            sleep(1)

    response = client.post(
        on=endpoint,
        request_size=request_size,
        inputs=dataset,
        show_progress=True,
        parameters=parameters,
        return_results=return_results,
    )

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
