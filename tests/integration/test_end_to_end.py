import json
import os
import time
from argparse import Namespace
from os.path import expanduser as user

import pytest
import requests

from now.cli import cli
from now.constants import JC_SECRET
from now.deployment.deployment import terminate_wolf
from now.dialog import NEW_CLUSTER
from now.run_all_k8s import get_remote_flow_details


@pytest.fixture()
def cleanup(deployment_type, dataset):
    start = time.time()
    yield
    if deployment_type == 'remote':
        if dataset == 'best-artworks':
            flow_id = get_remote_flow_details()['flow_id']
            terminate_wolf(flow_id)
    else:
        kwargs = {
            'deployment_type': deployment_type,
            'now': 'stop',
            'cluster': 'kind-jina-now',
        }
        kwargs = Namespace(**kwargs)
        cli(args=kwargs)
    now = time.time() - start
    mins = int(now / 60)
    secs = int(now % 60)
    print(50 * '#')
    print(
        f'Time taken to execute `{deployment_type}` deployment with dataset `{dataset}`: {mins}m {secs}s'
    )
    print(50 * '#')


@pytest.mark.parametrize(
    'output_modality, dataset',
    # [('image', 'bird-species'), ('image', 'best-artworks'), ('text', 'rock-lyrics')],
    [('image', 'best-artworks')],
)  # art, rock-lyrics -> no finetuning, fashion -> finetuning
@pytest.mark.parametrize('quality', ['medium'])
@pytest.mark.parametrize('cluster', [NEW_CLUSTER['value']])
# @pytest.mark.parametrize('deployment_type', ['local', 'remote'])
@pytest.mark.parametrize('deployment_type', ['remote'])
@pytest.mark.parametrize('which_api', ['api', 'api/api'])
def test_backend(
    output_modality: str,
    dataset: str,
    quality: str,
    cluster: str,
    deployment_type: str,
    which_api: str,
    cleanup,
):
    if deployment_type == 'remote' and dataset != 'best-artworks':
        pytest.skip('Too time consuming, hence skipping!')

    os.environ['NOW_CI_RUN'] = 'True'
    # sandbox = dataset == 'best-artworks'
    # deactivate sandbox since it is hanging from time to time
    sandbox = False
    kwargs = {
        'now': 'start',
        'output_modality': output_modality,
        'data': dataset,
        'quality': quality,
        'sandbox': sandbox,
        'cluster': cluster,
        'deployment_type': deployment_type,
        'proceed': True,
    }
    kwargs = Namespace(**kwargs)
    cli(args=kwargs)

    if dataset == 'best-artworks':
        search_text = 'impressionism'
    elif dataset == 'nft-monkey':
        search_text = 'laser eyes'
    else:
        search_text = 'test'

    # Perform end-to-end check via bff
    request_body = {'text': search_text, 'limit': 9}
    if deployment_type == 'local':
        request_body['host'] = 'localhost'
        request_body['port'] = 31080
    # elif deployment_type == 'remote':
    elif deployment_type.startswith('remote'):
        with open(user(JC_SECRET), 'r') as fp:
            flow_details = json.load(fp)
        request_body['host'] = flow_details['gateway']
    # response = requests.post(
    #     f'http://localhost/api/v1/{output_modality}/search', json=request_body
    # )
    response = requests.post(
        f'http://localhost/{which_api}/v1/{output_modality}/search', json=request_body
    )

    assert response.status_code == 200
    assert len(response.json()) == 9
