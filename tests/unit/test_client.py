from unittest.mock import MagicMock

import pytest

from now.client import Client
from now.executor.gateway.bff.app.v1.routers import helper


def test_send_request_bff(requests_mock):
    requests_mock.post(
        'https://nowrun.jina.ai/api/v1/search', json={'status_code': 200}
    )

    client = Client(
        jcloud_id='jcloud_id',
        app='app',
        api_key='api_key',
    )
    response = client.send_request_bff(
        endpoint='search',
        text='text',
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_send_request_not_search_endpoint():
    with pytest.raises(NotImplementedError):
        client = Client(
            jcloud_id='jcloud_id',
            app='app',
            api_key='api_key',
        )
        await client.send_request(
            endpoint='not_search',
            text='text',
        )


@pytest.mark.asyncio
async def test_send_requests(mocker):
    mock_async_func = MagicMock()

    async def async_mock_async_func(endpoint, text, *args, **kwargs):
        return mock_async_func(endpoint=endpoint, text=text, *args, **kwargs)

    mock_async_func.return_value = {'status_code': 200}

    helper.jina_client_post = async_mock_async_func

    client = Client(
        jcloud_id='jcloud_id',
        app='app',
        api_key='api_key',
    )
    response = await client.send_request(
        endpoint='search',
        text='text',
    )
    assert response['status_code'] == 200
