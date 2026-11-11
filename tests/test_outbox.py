import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event, select
from sqlalchemy.orm import Session

from app.models import Outbox
from app.outbox import publish_one
from app.payments import create_payment
from app.schemas import PaymentCreate


async def test_publish_failure_keeps_event_pending(sessions, payload):
    await create_payment(sessions, PaymentCreate(**payload), "publish-failure")
    broker = AsyncMock()
    broker.publish.side_effect = ConnectionError
    with pytest.raises(ConnectionError):
        await publish_one(sessions, broker)
    async with sessions() as session:
        assert (await session.scalar(select(Outbox))).published_at is None
    broker.publish.side_effect = None
    assert await publish_one(sessions, broker)
    assert not await publish_one(sessions, broker)
    assert broker.publish.call_args.kwargs["persist"] is True
    assert broker.publish.call_args.kwargs["mandatory"] is True


async def test_relay_does_not_publish_future_retries(sessions):
    async with sessions.begin() as session:
        session.add(
            Outbox(
                routing_key="payments.new",
                payload={},
                available_at=datetime.now(UTC) + timedelta(seconds=30),
            )
        )
    broker = AsyncMock()
    assert not await publish_one(sessions, broker)
    broker.publish.assert_not_awaited()


async def test_concurrent_relays_skip_locked_rows(sessions, payload):
    await create_payment(sessions, PaymentCreate(**payload), "relay-race")
    entered, release = asyncio.Event(), asyncio.Event()

    async def delayed_publish(*args, **kwargs):
        entered.set()
        await release.wait()

    broker = AsyncMock()
    broker.publish.side_effect = delayed_publish
    first = asyncio.create_task(publish_one(sessions, broker))
    await asyncio.wait_for(entered.wait(), timeout=3)
    try:
        assert not await publish_one(sessions, broker)
    finally:
        release.set()
        await first
    broker.publish.assert_awaited_once()


async def test_crash_after_confirm_can_republish_same_event_id(sessions, payload):
    await create_payment(sessions, PaymentCreate(**payload), "confirm-crash")
    broker = AsyncMock()

    def fail_commit(session):
        if any(isinstance(obj, Outbox) for obj in session.dirty):
            raise RuntimeError("simulated commit failure after confirm")

    event.listen(Session, "before_commit", fail_commit)
    try:
        with pytest.raises(RuntimeError, match="simulated"):
            await publish_one(sessions, broker)
    finally:
        event.remove(Session, "before_commit", fail_commit)
    assert await publish_one(sessions, broker)
    assert broker.publish.await_count == 2
    ids = {call.kwargs["message_id"] for call in broker.publish.await_args_list}
    assert len(ids) == 1
