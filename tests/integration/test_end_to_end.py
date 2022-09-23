import base64
import json
import os
import time
from argparse import Namespace
from os.path import expanduser as user

import hubble
import pytest
import requests
from now_common.options import NEW_CLUSTER

from now.cli import _get_kind_path, _get_kubectl_path, cli
from now.cloud_manager import create_local_cluster
from now.constants import JC_SECRET, Apps, DatasetTypes, DemoDatasets, Modalities
from now.deployment.deployment import cmd, list_all_wolf, terminate_wolf
from now.run_all_k8s import get_remote_flow_details


@pytest.fixture
def test_search_image(resources_folder_path: str):
    with open(
        os.path.join(resources_folder_path, 'image', '5109112832.jpg'), 'rb'
    ) as f:
        binary = f.read()
        img_query = base64.b64encode(binary).decode('utf-8')
    return img_query


@pytest.fixture()
def cleanup(deployment_type, dataset):
    start = time.time()
    yield
    try:
        if deployment_type == 'remote':
            flow_details = get_remote_flow_details()
            if 'flow_id' not in flow_details:
                print('nothing to clean up')
                return
            flow_id = flow_details['flow_id']
            terminate_wolf(flow_id)
        else:
            kwargs = {
                'deployment_type': deployment_type,
                'now': 'stop',
                'cluster': 'kind-jina-now',
                'delete-cluster': True,
            }
            kwargs = Namespace(**kwargs)
            cli(args=kwargs)
    except Exception as e:
        print('no clean up')
        return
    print('cleaned up')
    now = time.time() - start
    mins = int(now / 60)
    secs = int(now % 60)
    print(50 * '#')
    print(
        f'Time taken to execute `{deployment_type}` deployment with dataset `{dataset}`: {mins}m {secs}s'
    )
    print(50 * '#')


# def test_token_exists():
#     list_all_wolf()


@pytest.mark.parametrize(
    'app, input_modality, output_modality, dataset, deployment_type',
    [
        # (
        #     Apps.TEXT_TO_IMAGE,
        #     Modalities.TEXT,
        #     Modalities.IMAGE,
        #     DemoDatasets.BIRD_SPECIES,
        #     'local',
        # ),
        # (
        #     Apps.IMAGE_TO_IMAGE,
        #     Modalities.IMAGE,
        #     Modalities.IMAGE,
        #     DemoDatasets.BEST_ARTWORKS,
        #     'local',
        # ),
        # (
        #     Apps.IMAGE_TO_TEXT,
        #     Modalities.IMAGE,
        #     Modalities.TEXT,
        #     DemoDatasets.ROCK_LYRICS,
        #     'remote',
        # ),
        (
            Apps.TEXT_TO_TEXT,
            Modalities.TEXT,
            Modalities.TEXT,
            DemoDatasets.POP_LYRICS,
            'remote',
        ),
        # (
        #     Apps.TEXT_TO_VIDEO,
        #     Modalities.TEXT,
        #     Modalities.VIDEO,
        #     DemoDatasets.TUMBLR_GIFS_10K,
        #     'local',
        # ),
    ],
)
@pytest.mark.parametrize('quality', ['medium'])
@pytest.mark.timeout(60 * 15)
def test_backend_demo_data(
    app: str,
    dataset: str,
    quality: str,
    deployment_type: str,
    test_search_image,
    cleanup,
    input_modality,
    output_modality,
    with_hubble_login_patch,
):
    cluster = NEW_CLUSTER['value']
    os.environ['NOW_CI_RUN'] = 'True'
    os.environ['JCLOUD_LOGLEVEL'] = 'DEBUG'
    kwargs = {
        'now': 'start',
        'app': app,
        'data': dataset,
        'quality': quality,
        'cluster': cluster,
        'secured': deployment_type == 'remote',
        'additional_user': False,
        'deployment_type': deployment_type,
        'proceed': True,
    }
    # need to create local cluster and namespace to deploy playground and bff for WOLF deployment
    if deployment_type == 'remote':
        kind_path = _get_kind_path()
        create_local_cluster(kind_path, **kwargs)
        kubectl_path = _get_kubectl_path()
        cmd(f'{kubectl_path} create namespace nowapi')
    kwargs = Namespace(**kwargs)
    response = cli(args=kwargs)

    assert_deployment_response(
        app, deployment_type, input_modality, output_modality, response
    )
    assert_deployment_queries(
        app,
        dataset,
        deployment_type,
        input_modality,
        kwargs,
        output_modality,
        test_search_image,
    )


def assert_search(search_url, request_body):
    response = requests.post(
        search_url,
        json=request_body,
    )
    assert (
        response.status_code == 200
    ), f"Received code {response.status_code} with text: {response.json()['message']}"
    assert len(response.json()) == 9


def assert_deployment_queries(
    app,
    dataset,
    deployment_type,
    input_modality,
    kwargs,
    output_modality,
    test_search_image,
):
    url = f'http://localhost:30090/api/v1'
    # normal case
    request_body = get_search_request_body(
        app, dataset, deployment_type, kwargs, test_search_image
    )
    search_url = f'{url}/{input_modality}-to-{output_modality}/search'
    assert_search(search_url, request_body)

    if kwargs.secured:
        # test add email
        request_body = get_default_request_body(deployment_type, kwargs.secured)
        request_body['user_emails'] = ['florian.hoenicke@jina.ai']
        response = requests.post(
            f'{url}/admin/updateUserEmails',
            json=request_body,
        )
        assert response.status_code == 200

        # add api key
        del request_body['user_emails']
        request_body['api_keys'] = ['my_key']
        response = requests.post(
            f'{url}/admin/updateApiKeys',
            json=request_body,
        )
        if response.status_code != 200:
            print(response.text)
            print(response.json()['message'])
            raise Exception(f'Response status is {response.status_code}')
        # the same search should work now
        request_body = get_search_request_body(
            app, dataset, deployment_type, kwargs, test_search_image
        )
        assert_search(search_url, request_body)
        # search with invalid api key
        del request_body['jwt']
        request_body['api_key'] = 'no_key'
        with pytest.raises(Exception):
            assert_search(search_url, request_body)


def get_search_request_body(app, dataset, deployment_type, kwargs, test_search_image):
    request_body = get_default_request_body(deployment_type, kwargs.secured)
    request_body['limit'] = 9
    # Perform end-to-end check via bff
    if app in [Apps.IMAGE_TO_IMAGE, Apps.IMAGE_TO_TEXT]:
        request_body['image'] = test_search_image
    elif app in [Apps.TEXT_TO_IMAGE, Apps.TEXT_TO_TEXT, Apps.TEXT_TO_VIDEO]:
        if dataset == DemoDatasets.BEST_ARTWORKS:
            search_text = 'impressionism'
        elif dataset == DemoDatasets.NFT_MONKEY:
            search_text = 'laser eyes'
        else:
            search_text = 'test'
        request_body['text'] = search_text
    return request_body


def get_default_request_body(deployment_type, secured):
    request_body = {}
    if deployment_type == 'local':
        request_body['host'] = 'gateway'
        request_body['port'] = 8080
    elif deployment_type == 'remote':
        print(f"Getting gateway from flow_details")
        with open(user(JC_SECRET), 'r') as fp:
            flow_details = json.load(fp)
        request_body['host'] = flow_details['gateway']
    if secured:
        if 'WOLF_TOKEN' in os.environ:
            os.environ['JINA_AUTH_TOKEN'] = os.environ['WOLF_TOKEN']
        request_body['jwt'] = {'token': hubble.get_token()}
    return request_body


def assert_deployment_response(
    app, deployment_type, input_modality, output_modality, response
):
    assert (
        response['bff']
        == f'http://localhost:30090/api/v1/{app.replace("_", "-")}/redoc'
    )
    assert response['playground'].startswith('http://localhost:30080/?')
    assert response['input_modality'] == input_modality
    assert response['output_modality'] == output_modality
    if deployment_type == 'local':
        assert response['host'] == 'gateway.nowapi.svc.cluster.local'
    else:
        assert response['host'].startswith('grpcs://')
        assert response['host'].endswith('.wolf.jina.ai')
    assert response['port'] == 8080 or response['port'] is None


@pytest.mark.parametrize('deployment_type', ['remote'])
@pytest.mark.parametrize('dataset', ['custom_s3_bucket'])
def test_backend_custom_data(
    deployment_type: str,
    dataset: str,
    cleanup,
    with_hubble_login_patch,
):
    os.environ['NOW_CI_RUN'] = 'True'
    os.environ['JCLOUD_LOGLEVEL'] = 'DEBUG'
    app = Apps.TEXT_TO_IMAGE
    kwargs = {
        'now': 'start',
        'app': app,
        'data': 'custom',
        'custom_dataset_type': DatasetTypes.S3_BUCKET,
        'dataset_path': os.environ.get('S3_IMAGE_TEST_DATA_PATH'),
        'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY'),
        'aws_region_name': 'eu-west-1',
        'quality': 'medium',
        'cluster': NEW_CLUSTER['value'],
        'deployment_type': deployment_type,
        'proceed': True,
        'secured': False,
    }

    kind_path = _get_kind_path()
    create_local_cluster(kind_path, **kwargs)
    kubectl_path = _get_kubectl_path()
    cmd(f'{kubectl_path} create namespace nowapi')

    kwargs = Namespace(**kwargs)
    response = cli(args=kwargs)

    assert (
        response['bff']
        == f'http://localhost:30090/api/v1/{app.replace("_", "-")}/redoc'
    )
    assert response['playground'].startswith('http://localhost:30080/?')
    assert response['input_modality'] == 'text'
    assert response['output_modality'] == 'image'
    assert response['host'].startswith('grpcs://')
    assert response['host'].endswith('.wolf.jina.ai')
    assert response['port'] == 8080 or response['port'] is None

    request_body = {'text': 'test', 'limit': 9}

    print(f"Getting gateway from flow_details")
    with open(user(JC_SECRET), 'r') as fp:
        flow_details = json.load(fp)
    request_body['host'] = flow_details['gateway']

    response = requests.post(
        f'http://localhost:30090/api/v1/text-to-image/search',
        json=request_body,
    )

    assert (
        response.status_code == 200
    ), f"Received code {response.status_code} with text: {response.json()['message']}"
    response_json = response.json()
    assert len(response_json) == 2
    assert all(
        [resp['uri'].startswith('s3://') for resp in response_json]
    ), f"Received non s3 uris: {[resp['uri'] for resp in response_json]}"
    assert all(
        [
            resp['blob'] is None or resp['blob'] == '' or resp['blob'] == b''
            for resp in response_json
        ]
    ), f"Received blobs: {[resp['blob'] for resp in response_json]}"
