import logging
import os

import pytest
from pytest_mock import MockerFixture

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


class HubbleAuthPatch:
    @staticmethod
    async def login():
        pass

    @staticmethod
    def get_auth_token() -> str:
        token = os.environ.get('HUBBLE_AUTH_TOKEN')
        log.debug(f'Found token in env *** (Len={len(token)})')
        if token:
            return token
        else:
            raise RuntimeError(
                'Hubble token not found in environment ' 'under key `HUBBLE_AUTH_TOKEN`'
            )


@pytest.fixture
def with_hubble_login_patch(mocker: MockerFixture) -> None:
    mocker.patch(target='finetuner.client.base.hubble.Auth', new=HubbleAuthPatch)
