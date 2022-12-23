""" Module holds reusable fixtures """

import os
import time

import hubble
import pytest
from elasticsearch import Elasticsearch

from now.deployment.deployment import cmd


@pytest.fixture()
def resources_folder_path(tests_folder_path) -> str:
    return os.path.join(tests_folder_path, 'resources')


@pytest.fixture()
def tests_folder_path() -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)))


@pytest.fixture()
def setup_qdrant(tests_folder_path):
    docker_file_path = os.path.join(
        tests_folder_path, 'executor/indexer/base/docker-compose.yml'
    )
    cmd(
        f"docker-compose -f {docker_file_path} --project-directory . up  --build -d --remove-orphans"
    )
    yield
    cmd(
        f"docker-compose -f {docker_file_path} --project-directory . down --remove-orphans"
    )


@pytest.fixture(scope='session')
def service_account_file_path() -> str:
    return os.path.join(
        os.path.dirname(os.path.realpath(__file__)), '..', 'service_account.json'
    )


@pytest.fixture(autouse=True, scope='session')
def setup_env():
    os.environ['NOW_CI_RUN'] = 'True'
    # 1 and true are not working in the current core version therefore, we give it another value
    os.environ['JINA_OPTOUT_TELEMETRY'] = 'someValueToDeactivateTelemetry'
    os.environ['JCLOUD_LOGLEVEL'] = 'DEBUG'


@pytest.fixture()
def image_resource_path(resources_folder_path: str) -> str:
    return os.path.join(resources_folder_path, 'image')


@pytest.fixture()
def gif_resource_path(resources_folder_path: str) -> str:
    return os.path.join(resources_folder_path, 'gif')


@pytest.fixture
def get_task_config_path(resources_folder_path: str) -> str:
    return os.path.join(resources_folder_path, 'text+image/config.json')


@pytest.fixture
def admin_email():
    return 'alpha.omega@jina.ai'


@pytest.fixture
def user_email():
    return 'abc.def@jina.ai'


@pytest.fixture
def domain_user_email():
    return 'abc.def@test.ai'


@pytest.fixture
def mock_hubble_user_email(monkeypatch, user_email):
    class MockedClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_user_info(self, *args, **kwargs):
            return {
                'code': 200,
                'data': {'email': user_email},
            }

    monkeypatch.setattr(hubble, 'Client', MockedClient)


@pytest.fixture
def mock_hubble_domain_user_email(monkeypatch, domain_user_email):
    class MockedClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_user_info(self, *args, **kwargs):
            return {
                'code': 200,
                'data': {'email': domain_user_email},
            }

    monkeypatch.setattr(hubble, 'Client', MockedClient)


@pytest.fixture()
def mock_hubble_admin_email(monkeypatch, admin_email):
    class MockedClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_user_info(self, *args, **kwargs):
            return {
                'code': 200,
                'data': {'email': admin_email},
            }

    monkeypatch.setattr(hubble, 'Client', MockedClient)
    # hubble.Client = MockedClient


MAX_RETRIES = 20


@pytest.fixture(scope="session")
def es_connection_params():
    connection_str = 'http://localhost:9200'
    connection_args = {'verify_certs': False}
    return connection_str, connection_args


@pytest.fixture(scope='session')
def setup_service_running(es_connection_params) -> None:
    docker_compose_file = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'resources/elastic/docker-compose.yml',
    )
    cmd(f'docker-compose -f {docker_compose_file} up -d')
    hosts, _ = es_connection_params
    retries = 0
    while retries < MAX_RETRIES:
        try:
            es = Elasticsearch(hosts=hosts)
            if es.ping():
                break
            else:
                retries += 1
                time.sleep(5)
        except Exception:
            print('Elasticsearch is not running')
    if retries >= MAX_RETRIES:
        raise RuntimeError('Elasticsearch is not running')
    yield
    cmd('docker-compose -f tests/resources/elastic/docker-compose.yml down')
