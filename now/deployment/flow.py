import math
import os.path
import pathlib
from collections import namedtuple
from time import sleep

from docarray import DocumentArray
from kubernetes import client as k8s_client
from kubernetes import config
from tqdm import tqdm
from yaspin.spinners import Spinners

from now.cloud_manager import is_local_cluster
from now.constants import Modalities
from now.deployment.deployment import apply_replace, cmd
from now.dialog import UserInput
from now.finetuning.settings import FinetuneSettings
from now.log.log import TEST, yaspin_extended
from now.utils import sigmap

cur_dir = pathlib.Path(__file__).parent.resolve()
_ExecutorConfig = namedtuple('_ExecutorConfig', 'name, uses, uses_with')


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]


def wait_for_lb(lb_name, ns):
    config.load_kube_config()
    v1 = k8s_client.CoreV1Api()
    while True:
        try:
            services = v1.list_namespaced_service(namespace=ns)
            ip = [
                s.status.load_balancer.ingress[0].ip
                for s in services.items
                if s.metadata.name == lb_name
            ][0]
            if ip:
                break
        except Exception:
            pass
        sleep(1)
    return ip


def wait_for_all_pods_in_ns(ns, num_pods, max_wait=1800):
    config.load_kube_config()
    v1 = k8s_client.CoreV1Api()
    for i in range(max_wait):
        pods = v1.list_namespaced_pod(ns).items
        not_ready = [
            'x'
            for pod in pods
            if not pod.status
            or not pod.status.container_statuses
            or not len(pod.status.container_statuses) == 1
            or not pod.status.container_statuses[0].ready
        ]
        if len(not_ready) == 0 and num_pods == len(pods):
            return
        sleep(1)


def deploy_k8s(f, ns, num_pods, tmpdir, kubectl_path):
    k8_path = os.path.join(tmpdir, f'k8s/{ns}')
    with yaspin_extended(
        sigmap=sigmap, text="Convert Flow to Kubernetes YAML", color="green"
    ) as spinner:
        f.to_k8s_yaml(k8_path)
        spinner.ok('🔄')

    # create namespace
    cmd(f'{kubectl_path} create namespace {ns}')

    # deploy flow
    with yaspin_extended(
        Spinners.earth,
        sigmap=sigmap,
        text="Deploy Jina Flow (might take a bit)",
    ) as spinner:
        gateway_host_internal = f'gateway.{ns}.svc.cluster.local'
        gateway_port_internal = 8080
        if is_local_cluster(kubectl_path):
            apply_replace(
                f'{cur_dir}/k8s_backend-svc-node.yml',
                {'ns': ns},
                kubectl_path,
            )
            gateway_host = 'localhost'
            gateway_port = 31080
        else:
            apply_replace(f'{cur_dir}/k8s_backend-svc-lb.yml', {'ns': ns}, kubectl_path)
            gateway_host = wait_for_lb('gateway-lb', ns)
            gateway_port = 8080
        cmd(f'{kubectl_path} apply -R -f {k8_path}')
        # wait for flow to come up
        wait_for_all_pods_in_ns(ns, num_pods)
        spinner.ok("🚀")
    # work around - first request hangs
    sleep(3)
    return gateway_host, gateway_port, gateway_host_internal, gateway_port_internal


def get_executor_config(user_input: UserInput) -> _ExecutorConfig:
    """
    Gets the correct Executor running the pre-trained model given the user configuration.

    :param user_input: Configures user input.
    :return: Small data-transfer-object with information about the executor
    """
    hub_prefix = f'jinahub+{"sandbox" if user_input.sandbox else "docker"}'
    if (
        user_input.output_modality == Modalities.IMAGE
        or user_input.output_modality == Modalities.TEXT
    ):
        return _ExecutorConfig(
            name='clip',
            uses=f'{hub_prefix}://CLIPEncoder/v0.2.1',
            uses_with={'pretrained_model_name_or_path': user_input.model_variant},
        )
    elif user_input.output_modality == Modalities.MUSIC:
        return _ExecutorConfig(
            name='openl3clip',
            uses=f'{hub_prefix}://BiModalMusicTextEncoder/v1.0.0',
            uses_with={},
        )


def deploy_flow(
    user_input: UserInput,
    finetune_settings: FinetuneSettings,
    executor_name: str,
    index: DocumentArray,
    tmpdir,
    kubectl_path,
):
    from jina import Flow
    from jina.clients import Client

    ns = 'nowapi'
    f = Flow(
        name=ns,
        port_expose=8080,
        cors=True,
    )
    f = f.add(
        **get_executor_config(user_input)._asdict(),
        env={'JINA_LOG_LEVEL': 'DEBUG'},
    )
    if finetune_settings.perform_finetuning:
        f = f.add(
            name='linear_head',
            uses=f'jinahub{"+sandbox" if user_input.sandbox else "+docker"}://{executor_name}',
            uses_with={
                'final_layer_output_dim': finetune_settings.pre_trained_embedding_size,
                'embedding_size': finetune_settings.finetune_layer_size,
                'bi_modal': finetune_settings.bi_modal,
            },
            env={'JINA_LOG_LEVEL': 'DEBUG'},
        )
    f = f.add(
        name='indexer',
        uses=f'jinahub+docker://SimpleIndexer',
        env={'JINA_LOG_LEVEL': 'DEBUG'},
    )

    if user_input.output_modality == Modalities.IMAGE:
        index = [x for x in index if not x.text]
    elif user_input.output_modality == Modalities.TEXT:
        index = [x for x in index if x.text]
    elif user_input.output_modality == Modalities.MUSIC:
        index = [x for x in index if not x.text]

    (
        gateway_host,
        gateway_port,
        gateway_host_internal,
        gateway_port_internal,
    ) = deploy_k8s(
        f,
        ns,
        2
        + (2 if finetune_settings.perform_finetuning else 1)
        * (0 if user_input.sandbox else 1),
        tmpdir,
        kubectl_path=kubectl_path,
    )
    print(f'▶ indexing {len(index)} documents')
    client = Client(host=gateway_host, port=gateway_port)
    request_size = 64

    progress_bar = (
        x
        for x in tqdm(
            batch(index, request_size), total=math.ceil(len(index) / request_size)
        )
    )

    def on_done(res):
        if not TEST:
            next(progress_bar)

    client.post('/index', request_size=request_size, inputs=index, on_done=on_done)

    print('⭐ Success - your data is indexed')
    return gateway_host, gateway_port, gateway_host_internal, gateway_port_internal
