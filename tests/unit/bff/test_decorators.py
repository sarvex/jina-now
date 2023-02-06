import asyncio
import time

import pytest
from fastapi import HTTPException

from now.executor.gateway.bff.app.decorators import api_method, async_timed, timed


def test_timed():
    @timed
    def monty():
        """Monty Python!"""
        time.sleep(0.1)

    monty()


def test_async_timed():
    @async_timed
    async def monty():
        """Monty Python!"""
        await asyncio.sleep(0.1)

    asyncio.run(monty())


def test_api_method():
    @api_method
    def monty():
        """Monty Python!"""
        time.sleep(0.1)

    monty()


def test_api_method_error():
    @api_method
    def monty():
        """Monty Python!"""
        time.sleep(0.1)
        raise HTTPException(status_code=500, detail='Unknown error')

    with pytest.raises(HTTPException):
        monty()
