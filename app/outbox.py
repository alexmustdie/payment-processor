import asyncio
import logging

from sqlalchemy import func, select

from app.messaging import DEAD_EXCHANGE, PAYMENTS_EXCHANGE, declare_topology, make_broker
from app.models import Outbox

logger = logging.getLogger(__name__)


async def publish_one(sessions, broker) -> bool:
    # A lock is held through the broker confirm. Concurrent relays skip this row.
    async with sessions.begin() as session:
        event = await session.scalar(
            select(Outbox)
            .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
            .order_by(Outbox.available_at, Outbox.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if event is None:
            return False
        exchange = DEAD_EXCHANGE if event.routing_key == "payments.dead" else PAYMENTS_EXCHANGE
        await broker.publish(
            event.payload,
            exchange=exchange,
            routing_key=event.routing_key,
            message_id=str(event.id),
            persist=True,
            mandatory=True,
            timeout=10,
        )
        event.published_at = func.now()
    return True


async def run_relay(sessions, settings) -> None:
    while True:
        try:
            async with make_broker(settings.rabbitmq_url) as broker:
                await declare_topology(broker)
                while True:
                    if not await publish_one(sessions, broker):
                        await asyncio.sleep(settings.outbox_poll_seconds)
        except Exception as exc:
            # Do not log connection URLs or user-controlled webhook data.
            logger.warning("Outbox relay will reconnect: %s", type(exc).__name__)
            await asyncio.sleep(2)
