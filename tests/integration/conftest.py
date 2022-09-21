import logging
import os
from time import sleep

import pytest
from pytest_mock import MockerFixture
from urllib3.exceptions import InsecureRequestWarning
from warnings import filterwarnings, catch_warnings
import requests

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
    mocker.patch(target='finetuner.client.base.hubble.Auth', new=HubbleAuthPatch)


MAX_RETRIES = 20


@pytest.fixture(scope="session")
def es_connection_params():
    user_name = 'elastic'
    password = 'elastic'
    connection_str = f'https://{user_name}:{password}@localhost:9200'
    connection_args = {'verify_certs': False}
    return connection_str, connection_args


@pytest.mark.docker
@pytest.fixture(scope='session', autouse=True)
def setup_service_running(es_connection_params) -> None:
    os.system('docker-compose -f tests/resources/text+image/docker-compose.yml up -d')
    es_connection_str, _ = es_connection_params
    with catch_warnings():
        filterwarnings('ignore', category=InsecureRequestWarning)
        connection_str = es_connection_str
        retries = 0
        while True:
            try:
                retries += 1
                if retries > MAX_RETRIES:
                    raise RuntimeError(
                        f'Maximal number of retries ({MAX_RETRIES}) reached for '
                        f'connecting to {connection_str}'
                    )
                r = requests.get(connection_str, verify=False)
                if r.status_code in [200, 401]:
                    break
                else:
                    print(r.status_code)
                    sleep(5)
            except Exception:
                if retries > MAX_RETRIES:
                    raise RuntimeError(
                        f'Maximal number of retries ({MAX_RETRIES}) reached for '
                        f'connecting to {connection_str}'
                    )
                sleep(3)
    yield
    os.system('docker-compose -f tests/resources/text+image/docker-compose.yml down')
