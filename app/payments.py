import hashlib
import json
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.models import Outbox, Payment
from app.schemas import PaymentCreate, PaymentEvent


class IdempotencyConflict(Exception):
    pass


def request_hash(data: PaymentCreate) -> str:
    payload = data.model_dump(mode="json")
    # "10", "10.0" and "10.00" describe the same monetary amount.
    payload["amount"] = format(data.amount.quantize(Decimal("0.01")), "f")
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


async def create_payment(sessions, data: PaymentCreate, key: str) -> Payment:
    fingerprint = request_hash(data)
    payment_id = uuid4()
    async with sessions.begin() as session:
        inserted_id = await session.scalar(
            insert(Payment)
            .values(
                id=payment_id,
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                metadata_=data.metadata,
                webhook_url=str(data.webhook_url),
                idempotency_key=key,
                request_hash=fingerprint,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment.id)
        )
        payment = await session.scalar(select(Payment).where(Payment.idempotency_key == key))
        if payment.request_hash != fingerprint:
            raise IdempotencyConflict
        if inserted_id:
            session.add(
                Outbox(
                    routing_key="payments.new",
                    payload=PaymentEvent(payment_id=payment_id).model_dump(mode="json"),
                )
            )
    return payment
