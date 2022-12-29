"""
This suite tests that the demo datasets are available
in hubble
"""

import os

import pytest
import requests

from now.constants import MODALITIES_MAPPING, Modalities
from now.demo_data import AVAILABLE_DATASETS


@pytest.mark.parametrize(
    'modality, ds_name',
    [
        (m, d.name)
        for m in Modalities()
        for d in AVAILABLE_DATASETS[MODALITIES_MAPPING[m]]
    ],
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
