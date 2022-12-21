"""
This suite tests that the demo datasets are available
in hubble
"""

import os

import pytest
import requests

from now.constants import Modalities
from now.demo_data import AVAILABLE_DATASETS


@pytest.mark.parametrize(
    'modality, ds_name',
    [(m, d.name) for m, ds in AVAILABLE_DATASETS.items() for d in ds],
)
def test_dataset_is_available(
    ds_name: str,
    modality: Modalities,
):
    token = os.environ['WOLF_TOKEN']
    cookies = {'st': token}
    json_data = {'name': ds_name}
    response = requests.post(
        'https://api.hubble.jina.ai/v2/rpc/docarray.getFirstDocuments',
        cookies=cookies,
        json=json_data,
    )
    assert response.json()['code'] == 200
