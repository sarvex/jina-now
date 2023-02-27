import cowsay
import requests
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Column, Table

from now import run_backend
from now.constants import DEMO_NS, FLOW_STATUS
from now.deployment.deployment import cmd, terminate_wolf
from now.dialog import configure_user_input
from now.utils import get_flow_status, maybe_prompt_user


def stop_now(**kwargs):
    _result, flow_id, cluster = get_flow_status(action='delete', **kwargs)
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
        gateway_host_grpc = f'grpcs://{DEMO_NS.format(user_input.dataset_name.split("/")[-1])}.dev.jina.ai'
    else:
        (
            gateway_port,
            gateway_host_grpc,
        ) = run_backend.run(app_instance, user_input, **kwargs)
    gateway_host_http = gateway_host_grpc.replace('grpc', 'http')
    bff_url = f'{gateway_host_http}/api/v1/search-app/docs'
    playground_url = f'{gateway_host_http}/playground'

    print()
    my_table = Table(
        'Attribute',
        Column(header="Value", overflow="fold"),
        show_header=False,
        box=box.SIMPLE,
        highlight=True,
    )
    my_table.add_row('Host (HTTPS)', gateway_host_http)
    my_table.add_row('Host (GRPCS)', gateway_host_grpc)
    my_table.add_row('API docs', bff_url)
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
        'host_http': gateway_host_http,
        'host_grpc': gateway_host_grpc,
        'secured': user_input.secured,
    }


def fetch_logs_now(**kwargs):
    _result, flow_id, cluster = get_flow_status(action='log', **kwargs)

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
