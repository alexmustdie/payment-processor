import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app import consumer


@pytest.mark.parametrize("body", [{"invalid": True}, "not an object", None])
async def test_invalid_message_rejected_without_requeue(body):
    message = AsyncMock()
    await consumer.handle_payment(body, message)
    message.reject.assert_awaited_once_with()
    message.ack.assert_not_awaited()


async def test_storage_error_nacks_original_message(monkeypatch):
    monkeypatch.setattr(consumer, "process_payment", AsyncMock(side_effect=ConnectionError))
    monkeypatch.setattr(consumer.asyncio, "sleep", AsyncMock())
    message = AsyncMock()
    await consumer.handle_payment({"payment_id": str(uuid4()), "attempt": 1}, message)
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()


async def test_shutdown_cancellation_requeues(monkeypatch):
    monkeypatch.setattr(consumer, "process_payment", AsyncMock(side_effect=asyncio.CancelledError))
    message = AsyncMock()
    with pytest.raises(asyncio.CancelledError):
        await consumer.handle_payment({"payment_id": str(uuid4()), "attempt": 1}, message)
    message.nack.assert_awaited_once_with(requeue=True)
    message.reject.assert_not_awaited()
