import os

import cowsay
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Column, Table

from now import run_backend, run_bff_playground
from now.cloud_manager import setup_cluster
from now.constants import DOCKER_BFF_PLAYGROUND_TAG, SURVEY_LINK, DatasetTypes
from now.deployment.deployment import cmd, list_all_wolf, status_wolf, terminate_wolf
from now.dialog import configure_app, configure_user_input
from now.log import yaspin_extended
from now.system_information import get_system_state
from now.utils import _get_context_names, maybe_prompt_user, sigmap


def stop_now(app_instance, contexts, active_context, **kwargs):
    choices = _get_context_names(contexts, active_context)
    # Add all remote Flows that exists with the namespace `nowapi`
    alive_flows = list_all_wolf()
    for flow_details in alive_flows:
        choices.append(flow_details['gateway'])
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
    if cluster == 'kind-jina-now':
        delete_cluster = maybe_prompt_user(
            [
                {
                    'type': 'list',
                    'name': 'delete-cluster',
                    'message': 'Do you want to delete the entire cluster or just the namespace?',
                    'choices': [
                        {'name': '⛔ no, keep the cluster', 'value': False},
                        {'name': '✅ yes, delete everything', 'value': True},
                    ],
                }
            ],
            attribute='delete-cluster',
            **kwargs,
        )
        if delete_cluster:
            with yaspin_extended(
                sigmap=sigmap, text=f"Remove local cluster {cluster}", color="green"
            ) as spinner:
                cmd(f'{kwargs["kind_path"]} delete clusters jina-now')
                spinner.ok('💀')
            cowsay.cow('local jina NOW cluster removed')
        else:
            with yaspin_extended(
                sigmap=sigmap,
                text=f"Remove namespace nowapi NOW from {cluster}",
                color="green",
            ) as spinner:
                cmd(f'{kwargs["kubectl_path"]} delete ns nowapi')
                spinner.ok('💀')
            cowsay.cow(f'nowapi namespace removed from {cluster}')
    elif 'wolf.jina.ai' in cluster:
        flow = [x for x in alive_flows if x['gateway'] == cluster][0]
        flow_id = flow['name_id'].replace('nowapi-', '')
        _result = status_wolf(flow_id)
        if _result is None:
            print(f'❎ Flow not found in JCloud. Likely, it has been deleted already')
        if _result is not None and _result['status'] == 'ALIVE':
            terminate_wolf(flow_id)
        cowsay.cow(f'remote Flow `{cluster}` removed')
    else:
        with yaspin_extended(
            sigmap=sigmap, text=f"Remove jina NOW from {cluster}", color="green"
        ) as spinner:
            cmd(f'{kwargs["kubectl_path"]} delete ns nowapi')
            spinner.ok('💀')
        cowsay.cow(f'nowapi namespace removed from {cluster}')
    app_instance.cleanup(app_config=dict())


def get_task(kwargs):
    for x in ['cli', 'now']:
        if x in kwargs:
            return kwargs[x]
    raise Exception('kwargs do not contain a task')


def start_now(app_instance, **kwargs):
    user_input = configure_user_input(app_instance, **kwargs)

    # Only if the deployment is remote and the demo examples is available for the selected app
    # Should not be triggered for CI tests
    if app_instance.is_demo_available(user_input):
        gateway_host = 'remote'
        gateway_host_internal = f'grpcs://now-example-{app_instance.app_name}-{user_input.data}.dev.jina.ai'.replace(
            '_', '-'
        )
        gateway_port_internal = None
    else:
        setup_cluster(user_input, **kwargs)
        (
            gateway_host,
            gateway_port,
            gateway_host_internal,
            gateway_port_internal,
        ) = run_backend.run(app_instance, user_input, **kwargs)

    if gateway_host == 'localhost' or 'NOW_CI_RUN' in os.environ:
        # only deploy playground when running locally or when testing
        bff_playground_host, bff_port, playground_port = run_bff_playground.run(
            gateway_host=gateway_host,
            docker_bff_playground_tag=DOCKER_BFF_PLAYGROUND_TAG,
            kubectl_path=kwargs['kubectl_path'],
        )
    else:
        bff_playground_host = 'https://nowrun.jina.ai'
        bff_port = '80'
        playground_port = '80'
    # TODO: add separate BFF endpoints in print output
    bff_url = (
        bff_playground_host
        + ('' if str(bff_port) == '80' else f':{bff_port}')
        + f'/api/v1/{app_instance.input_modality}-to-{app_instance.output_modality}/docs'
    )
    playground_url = (
        bff_playground_host
        + ('' if str(playground_port) == '80' else f':{playground_port}')
        + (
            f'/?host='
            + (gateway_host_internal if gateway_host != 'localhost' else 'gateway')
            + f'&input_modality={app_instance.input_modality}'
            + f'&output_modality={app_instance.output_modality}'
            + (
                f'&data={user_input.dataset_name}'
                if user_input.dataset_type == DatasetTypes.DEMO
                else 'custom'
            )
            + (f'&secured={user_input.secured}' if user_input.secured else '')
        )
        + (f'&port={gateway_port_internal}' if gateway_port_internal else '')
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
    my_table.add_row('Playground', playground_url)
    console = Console()
    console.print(
        Panel(
            my_table,
            title=f':tada: {app_instance.input_modality}-{app_instance.output_modality} app is NOW ready!',
            expand=False,
        )
    )
    return {
        'bff': bff_url,
        'playground': playground_url,
        'input_modality': app_instance.input_modality,
        'output_modality': app_instance.output_modality,
        'host': gateway_host_internal,
        'port': gateway_port_internal,
        'secured': user_input.secured,
    }


def run_k8s(os_type: str = 'linux', arch: str = 'x86_64', **kwargs):
    contexts, active_context = get_system_state(**kwargs)
    task = get_task(kwargs)
    app = configure_app(**kwargs)
    if task == 'start':
        return start_now(
            app,
            contexts=contexts,
            active_context=active_context,
            os_type=os_type,
            arch=arch,
            **kwargs,
        )
    elif task == 'stop':
        return stop_now(app, contexts, active_context, **kwargs)
    elif task == 'survey':
        import webbrowser

        webbrowser.open(SURVEY_LINK, new=0, autoraise=True)
    else:
        raise Exception(f'unknown task, {task}')


if __name__ == '__main__':
    run_k8s(
        app='text_to_image',
        data='bird-species',
        cluster='kind-jina-now',
        deployment_type='local',
        kubectl_path='/usr/local/bin/kubectl',
        kind_path='/Users/sebastianlettner/.cache/jina-now/kind',
        proceed=True,
        now='start',
    )
