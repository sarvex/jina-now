import os

import cowsay
import requests
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Column, Table

from now import run_backend
from now.constants import DEMO_NS, FLOW_STATUS, DatasetTypes
from now.deployment.deployment import cmd, list_all_wolf, status_wolf, terminate_wolf
from now.dialog import configure_user_input
from now.utils import maybe_prompt_user


def stop_now(**kwargs):
    choices = []
    # Add all remote Flows that exists with the namespace `nowapi`
    alive_flows = list_all_wolf(status=FLOW_STATUS)
    for flow_details in alive_flows:
        choices.append(flow_details['name'])
    if len(choices) == 0:
        cowsay.cow('nothing to stop')
        return
    else:
        questions = [
            {
                'type': 'list',
                'name': 'cluster',
                'message': 'Which cluster do you want to delete?',
                'choices': choices,
            }
        ]
        cluster = maybe_prompt_user(questions, 'cluster', **kwargs)

    flow = [x for x in alive_flows if x['name'] == cluster][0]
    flow_id = flow['id']
    _result = status_wolf(flow_id)
    if _result is None:
        print(f'❎ Flow not found in JCloud. Likely, it has been deleted already')
    if _result is not None and _result['status']['phase'] == FLOW_STATUS:
        terminate_wolf(flow_id)
        from hubble import Client

        cookies = {'st': Client().token}
        requests.delete(
            f'https://storefrontapi.nowrun.jina.ai/api/v1/schedule_sync/{flow_id}',
            cookies=cookies,
        )
    cowsay.cow(f'remote Flow `{cluster}` removed')


def start_now(**kwargs):
    user_input = configure_user_input(**kwargs)
    app_instance = user_input.app_instance
    # Only if the deployment is remote and the demo examples is available for the selected app
    # Should not be triggered for CI tests
    if app_instance.is_demo_available(user_input):
        gateway_host_internal = f'grpcs://{DEMO_NS.format(user_input.dataset_name.split("/")[-1])}.dev.jina.ai'
    else:
        (
            gateway_port,
            gateway_host_internal,
        ) = run_backend.run(app_instance, user_input, **kwargs)
    if 'NOW_CI_RUN' in os.environ:
        bff_playground_host = 'http://localhost'
        bff_port = '8080'
        playground_port = '30080'
    else:
        bff_playground_host = 'https://nowrun.jina.ai'
        bff_port = '80'
        playground_port = '80'
    # TODO: add separate BFF endpoints in print output
    bff_url = (
        bff_playground_host
        + ('' if str(bff_port) == '80' else f':{bff_port}')
        + f'/api/v1/search-app/docs'
    )
    playground_url = bff_playground_host + (
        f'/?host='
        + gateway_host_internal
        + (
            f'&data={user_input.dataset_name.split("/")[-1] if user_input.dataset_type == DatasetTypes.DEMO else "custom"}'
        )
        + (f'&secured={user_input.secured}' if user_input.secured else '')
    )
    print()
    my_table = Table(
        'Attribute',
        Column(header="Value", overflow="fold"),
        show_header=False,
        box=box.SIMPLE,
        highlight=True,
    )
    my_table.add_row('Api docs', bff_url)
    if user_input.secured and user_input.api_key:
        my_table.add_row('API Key', user_input.api_key)
    my_table.add_row('Playground', playground_url)
    console = Console()
    console.print(
        Panel(
            my_table,
            title=f':tada: Search app is NOW ready!',
            expand=False,
        )
    )
    return {
        'bff': bff_url,
        'playground': playground_url,
        'bff_playground_host': bff_playground_host,
        'bff_port': bff_port,
        'playground_port': playground_port,
        'host': gateway_host_internal,
        'secured': user_input.secured,
    }


def fetch_logs_now(**kwargs):
    choices = []
    # Add all remote Flows that exists with the namespace `nowapi`
    alive_flows = list_all_wolf(status=FLOW_STATUS)
    for flow_details in alive_flows:
        choices.append(flow_details['name'])
    if len(choices) == 0:
        cowsay.cow('nothing to log')
        return
    else:
        questions = [
            {
                'type': 'list',
                'name': 'cluster',
                'message': 'Which cluster do you want to check logs for?',
                'choices': choices,
            }
        ]
        cluster = maybe_prompt_user(questions, 'cluster', **kwargs)

    flow = [x for x in alive_flows if x['name'] == cluster][0]
    flow_id = flow['id']
    _result = status_wolf(flow_id)
    if _result is None:
        print(f'❎ Flow not found in JCloud. Likely, it has been deleted already')

    if _result is not None and _result['status']['phase'] == FLOW_STATUS:
        namespace = _result["spec"]["jcloud"]["namespace"]

    stdout, stderr = cmd(f"kubectl get pods -n {namespace}")

    pods = []
    for i, line in enumerate(stdout.decode().split("\n")):
        if i == 0:
            continue
        cols = line.split()
        if len(cols) > 0:
            pod_name = cols[0]
            pods.append(pod_name)

    questions = [
        {
            'type': 'list',
            'name': 'pod',
            'message': 'Which pod do you want to check logs for?',
            'choices': pods,
        }
    ]
    pod = maybe_prompt_user(questions, 'pod', **kwargs)

    container = "gateway" if "gateway" in pod else "executor"
    cmd(f"kubectl logs {pod} -n {namespace} -c {container}", std_output=True)
