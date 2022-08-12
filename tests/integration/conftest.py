import logging
import os

import pytest
from pytest_mock import MockerFixture

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
#
#
# class HubbleAuthPatch:
#     @staticmethod
#     async def login():
#         pass
#
#     @staticmethod
#     def get_auth_token() -> str:
#         token = os.environ.get('WOLF_TOKEN')
#         if token:
#             log.debug(f'Found token in env *** (Len={len(token)})')
#             return token
#         else:
#             raise RuntimeError(
#                 'WOLF token not found in environment under key `WOLF_TOKEN`'
#             )


@pytest.fixture
def with_hubble_login_patch(mocker: MockerFixture) -> None:
    os.environ['JINA_AUTH_TOKEN'] = os.environ.get('WOLF_TOKEN')
    # mocker.patch(target='finetuner.client.base.hubble.Auth', new=HubbleAuthPatch)
