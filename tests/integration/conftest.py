import logging
import os
from time import sleep

import pytest
import hubble
from pytest_mock import MockerFixture
from urllib3.exceptions import InsecureRequestWarning
from warnings import filterwarnings, catch_warnings
import requests

from now.constants import DatasetTypes
from now.data_loading.data_loading import load_data
from now.demo_data import DemoDatasetNames
from now.deployment.deployment import cmd

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


class HubbleAuthPatch:
    @staticmethod
    async def login():
        pass

    @staticmethod
    def get_auth_token() -> str:
        token = os.environ.get('WOLF_TOKEN')
        if token:
            log.debug(f'Found token in env *** (Len={len(token)})')
            return token
        else:
            raise RuntimeError(
                'WOLF token not found in environment under key `WOLF_TOKEN`'
            )


@pytest.fixture
def with_hubble_login_patch(mocker: MockerFixture) -> None:
    # WOLF token is required for deployment, but not set locally (only in the CI)
    # If you are running this locally, the WOLF_TOKEN env variable will be set using hubble
    # Otherwise, it will be set in the CI.
    if 'WOLF_TOKEN' not in os.environ:
        hubble.login()
        os.environ['WOLF_TOKEN'] = hubble.Auth.get_auth_token()
    mocker.patch(target='finetuner.client.base.hubble.Auth', new=HubbleAuthPatch)


import os

import pytest
from docarray import dataclass, Document, DocumentArray
from jina import Flow

from now.app.text_to_image.app import TextToImage
from now.executor.preprocessor import NOWPreprocessor
from docarray.typing import Text, Image

from now.now_dataclasses import UserInput


@pytest.fixture
def single_modal_data():
    d1 = Document(text='some text', tags={'color': 'red', 'author': 'saba'})
    d2 = Document(text='text some', tags={'color': 'blue', 'author': 'florian'})
    return DocumentArray([d1, d2])


@pytest.fixture
def multi_modal_data(resources_folder_path):
    @dataclass
    class Page:
        main_text: Text
        image: Image
        color: Text

    p1 = Page(
        main_text='main text 1',
        image=os.path.join(resources_folder_path, 'image', '5109112832.jpg'),
        color='red',
    )
    p2 = Page(
        main_text='not main text',
        image=os.path.join(resources_folder_path, 'image', '6785325056.jpg'),
        color='blue',
    )
    pages = [p1, p2]

    return DocumentArray([Document(page) for page in pages])


@pytest.fixture
def preprocess_and_encode(single_modal_data, multi_modal_data):
    search_fields = ['default_field']
    filter_fields = []
    # data_type = 'single_modal'
    # if data_type == 'single_modal':
    #     data = single_modal_data
    # else:
    #     data = multi_modal_data
    app_instance = TextToImage()
    user_input = UserInput()
    user_input.search_fields = search_fields
    user_input.filter_fields = filter_fields
    user_input.filter_fields = filter_fields
    user_input.dataset_type = DatasetTypes.DEMO
    user_input.dataset_name = DemoDatasetNames.BIRD_SPECIES
    data = load_data(app_instance, user_input)
    # data = DocumentArray([data[0], data[-1]])

    f1 = (
        Flow()
        .add(uses=NOWPreprocessor, uses_with={'app': app_instance.app_name})
        .add(
            uses='jinahub+docker://CLIPOnnxEncoder/latest-gpu',
            host='encoderclip-bh-5f4efaff13.wolf.jina.ai',
            port=443,
            tls=True,
            external=True,
            uses_with={'name': 'ViT-B-32::openai'},
        )
    )
    with f1:
        encoded_d = f1.post(
            '/encode',
            data,
            parameters={
                'user_input': user_input.__dict__,
                'access_paths': app_instance.index_query_access_paths(
                    user_input.search_fields
                ),
                'traversal_paths': app_instance.index_query_access_paths(
                    user_input.search_fields
                ),
            },
        )

    assert len(encoded_d) == len(data)
    assert len(search_fields) == len(encoded_d[0].chunks)
    for chunk, field in zip(encoded_d[0].chunks, search_fields):
        assert chunk.embedding.any()
        assert field == chunk.tags['field_name']
    for filter_field in filter_fields:
        assert filter_field in encoded_d[0].tags['filter_fields']
    return encoded_d, user_input
