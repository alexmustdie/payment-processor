import asyncio
import random
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
from sqlalchemy import select

from app.config import MAX_ATTEMPTS
from app.models import Outbox, Payment
from app.schemas import PaymentEvent


class InvalidPaymentEvent(Exception):
    pass


async def emulate_gateway(payment_id: UUID) -> str:
    await asyncio.sleep(random.uniform(2, 5))
    return "succeeded" if random.random() < 0.9 else "failed"


def is_current(payment: Payment, event: PaymentEvent) -> bool:
    return (
        payment.attempts == event.attempt - 1
        and payment.webhook_delivered_at is None
        and payment.dead_lettered_at is None
    )


async def locked_payment(session, event: PaymentEvent) -> Payment:
    payment = await session.scalar(
        select(Payment).where(Payment.id == event.payment_id).with_for_update()
    )
    if payment is None:
        raise InvalidPaymentEvent("Unknown payment")
    if event.attempt > payment.attempts + 1:
        raise InvalidPaymentEvent("Attempt is out of sequence")
    return payment


async def record_failure(sessions, settings, event: PaymentEvent, error: Exception) -> None:
    async with sessions.begin() as session:
        payment = await locked_payment(session, event)
        if not is_current(payment, event):
            return
        now = datetime.now(UTC)
        payment.attempts = event.attempt
        # Exception text may contain webhook URL tokens; retain only the category.
        payment.last_error = type(error).__name__
        if isinstance(error, httpx.HTTPStatusError):
            payment.last_error += f": HTTP {error.response.status_code}"
        if event.attempt == MAX_ATTEMPTS:
            payment.dead_lettered_at = now
            session.add(
                Outbox(
                    routing_key="payments.dead",
                    payload={**event.model_dump(mode="json"), "error": payment.last_error},
                    available_at=now,
                )
            )
        else:
            delay = settings.retry_base_seconds * 2 ** (event.attempt - 1)
            session.add(
                Outbox(
                    routing_key="payments.new",
                    payload=PaymentEvent(
                        payment_id=event.payment_id, attempt=event.attempt + 1
                    ).model_dump(mode="json"),
                    available_at=now + timedelta(seconds=delay),
                )
            )


async def process_payment(sessions, settings, event, client, gateway=emulate_gateway) -> None:
    try:
        async with sessions.begin() as session:
            payment = await locked_payment(session, event)
            if not is_current(payment, event):
                return
            if payment.status == "pending":
                payment.status = await gateway(payment.id)
                payment.processed_at = datetime.now(UTC)
        # Commit the gateway outcome before HTTP. A webhook failure cannot roll it back.
        async with sessions.begin() as session:
            payment = await locked_payment(session, event)
            if not is_current(payment, event):
                return
            event_id = f"payment:{payment.id}:result"
            payload = {
                "event_id": event_id,
                "payment_id": str(payment.id),
                "status": payment.status,
                "amount": format(payment.amount, ".2f"),
                "currency": payment.currency,
                "description": payment.description,
                "metadata": payment.metadata_,
                "created_at": payment.created_at.isoformat(),
                "processed_at": payment.processed_at.isoformat(),
            }
            # HTTPX timeouts limit inactivity, not the entire exchange. Bound both,
            # and do not download a webhook response body that we never use.
            async with asyncio.timeout(settings.webhook_timeout_seconds):
                async with client.stream(
                    "POST",
                    payment.webhook_url,
                    json=payload,
                    headers={"Idempotency-Key": event_id},
                    timeout=settings.webhook_timeout_seconds,
                    follow_redirects=False,
                ) as response:
                    response.raise_for_status()
            payment.attempts = event.attempt
            payment.webhook_delivered_at = datetime.now(UTC)
            payment.last_error = None
    except InvalidPaymentEvent:
        raise
    except Exception as exc:
        # Retry/DLQ insertion and attempt advancement commit together before Rabbit ACK.
        # If the database itself is down, this raises and the consumer requeues the input.
        await record_failure(sessions, settings, event, exc)
