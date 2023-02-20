import os.path
import tempfile

from jina.clients import Client

from now.deployment.deployment import deploy_wolf
from now.log import time_profiler
from now.utils import write_flow_file


@time_profiler
def deploy_flow(
    flow_yaml: str,
):
    """Deploy a Flow on JCloud, Kubernetes, or using Jina Orchestration"""
    # TODO create tmpdir top level and pass it down
    with tempfile.TemporaryDirectory() as tmpdir:
        # hack we don't know if the flow yaml is a path or a string
        if type(flow_yaml) == dict:
            flow_file = os.path.join(tmpdir, 'flow.yml')
            write_flow_file(flow_yaml, flow_file)
            flow_yaml = flow_file

        flow = deploy_wolf(path=flow_yaml)
        host = flow.endpoints['gateway (grpc)']
        client = Client(host=host)

        # host & port
        gateway_port = None
        gateway_host_internal = host
    return (
        client,
        gateway_port,
        gateway_host_internal,
    )
