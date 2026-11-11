import os
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from app.api import create_app
from app.config import Settings


@pytest.fixture
def settings():
    url = os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql+asyncpg://payments:payments@localhost:55432/payments_test",
    )
    if not make_url(url).database.endswith("_test"):
        raise RuntimeError("Test database name must end with _test; tests truncate tables")
    return Settings(database_url=url, api_key=uuid4().hex, retry_base_seconds=2)


@pytest.fixture
async def app(settings):
    app = create_app(settings, relay_enabled=False)
    async with app.state.engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox"))
    yield app
    await app.state.engine.dispose()


@pytest.fixture
def sessions(app):
    return app.state.sessions


@pytest.fixture
async def client(app, settings):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app),
        base_url="http://test",
        headers={"X-API-Key": settings.api_key.get_secret_value()},
    ) as client:
        yield client


@pytest.fixture
def payload():
    return {
        "amount": "125.50",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://merchant.example/webhook",
    }
