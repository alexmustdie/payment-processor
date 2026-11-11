import asyncio
import logging
from typing import Any

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitMessage
from pydantic import ValidationError

from app.config import Settings
from app.db import make_database
from app.messaging import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE, declare_topology, make_broker
from app.processing import InvalidPaymentEvent, process_payment
from app.schemas import PaymentEvent

settings = Settings()
engine, sessions = make_database(settings.database_url)
broker = make_broker(settings.rabbitmq_url)
app = FastStream(broker)
client = httpx.AsyncClient(trust_env=False)
logger = logging.getLogger(__name__)


@app.on_startup
async def startup():
    await broker.connect()
    await declare_topology(broker)


@app.after_shutdown
async def shutdown():
    await client.aclose()
    await engine.dispose()


@broker.subscriber(PAYMENTS_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.REJECT_ON_ERROR)
async def handle_payment(body: Any, message: RabbitMessage):
    try:
        try:
            event = PaymentEvent.model_validate(body)
            await process_payment(sessions, settings, event, client)
        except (ValidationError, InvalidPaymentEvent):
            logger.warning("Invalid payment message rejected")
            await message.reject()
        except Exception as exc:
            logger.warning("Payment infrastructure unavailable: %s", type(exc).__name__)
            await asyncio.sleep(1)
            await message.nack(requeue=True)
        else:
            await message.ack()
    except asyncio.CancelledError:
        # Include cancellation during backoff/ACK, not only during payment processing.
        await message.nack(requeue=True)
        raise
