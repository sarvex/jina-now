""" Module holds reusable fixtures """

import os

import hubble
import pytest


@pytest.fixture()
def resources_folder_path() -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources')


@pytest.fixture(scope='session')
def service_account_file_path() -> str:
    return os.path.join(
        os.path.dirname(os.path.realpath(__file__)), '..', 'service_account.json'
    )


@pytest.fixture()
def image_resource_path(resources_folder_path: str) -> str:
    return os.path.join(resources_folder_path, 'image')


@pytest.fixture()
def music_resource_path(resources_folder_path: str) -> str:
    return os.path.join(resources_folder_path, 'music')


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
