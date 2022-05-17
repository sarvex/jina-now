from argparse import Namespace

import pytest
import requests

from now.cli import cli
from now.dialog import NEW_CLUSTER
from now.log import log


@pytest.mark.parametrize(
    'output_modality, dataset',
    [('image', 'best-artworks'), ('image', 'bird-species'), ('text', 'rock-lyrics')],
)  # art, rock-lyrics -> no finetuning, fashion -> finetuning
@pytest.mark.parametrize('quality', ['medium'])
@pytest.mark.parametrize('cluster', [NEW_CLUSTER['value']])
@pytest.mark.parametrize('new_cluster_type', ['local'])
def test_backend(
    output_modality: str,
    dataset: str,
    quality: str,
    cluster: str,
    new_cluster_type: str,
):
    log.TEST = True
    # sandbox = dataset == 'best-artworks'
    # deactivate sandbox since it is hanging from time to time
    sandbox = False
    kwargs = {
        'output_modality': output_modality,
        'data': dataset,
        'quality': quality,
        'sandbox': sandbox,
        'cluster': cluster,
        'new_cluster_type': new_cluster_type,
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

    if cluster == 'local':
        server = 'localhost'
        port = 0

    # Perform end-to-end check via bff
    data = {'host': server, 'port': port, 'text': search_text, 'limit': 9}
    response = requests.post(f'localhost/api/v1/{output_modality}/search', json=data)
    assert response.status_code == 200
    # Limit param is not respected and hence 20 matches are returned
    # Therefore, once the limit is implemented in the CustomIndexer,
    # we should change the below value to 9
    assert len(response.json()) == 9
