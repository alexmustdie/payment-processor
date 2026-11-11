import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import event, func, select
from sqlalchemy.orm import Session

from app.models import Outbox, Payment
from app.payments import create_payment
from app.schemas import PaymentCreate


async def test_create_get_and_atomic_outbox(client, sessions, payload):
    response = await client.post(
        "/api/v1/payments", json=payload, headers={"Idempotency-Key": "order-42"}
    )
    assert response.status_code == 202
    created = response.json()
    assert created["status"] == "pending"
    result = await client.get(f"/api/v1/payments/{created['payment_id']}")
    assert result.status_code == 200
    assert result.json()["amount"] == "125.50"
    assert result.json()["metadata"] == {"order_id": 42}
    assert result.json()["processed_at"] is None
    async with sessions() as session:
        outbox = (await session.scalars(select(Outbox))).one()
        assert outbox.payload == {"payment_id": created["payment_id"], "attempt": 1}
        assert outbox.published_at is None


async def test_concurrent_same_key_creates_one_payment_and_event(client, sessions, payload):
    responses = await asyncio.gather(
        *[
            client.post("/api/v1/payments", json=payload, headers={"Idempotency-Key": "race"})
            for _ in range(20)
        ]
    )
    assert all(r.status_code == 202 for r in responses)
    assert len({r.json()["payment_id"] for r in responses}) == 1
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_idempotency_conflict_and_decimal_normalization(client, payload):
    headers = {"Idempotency-Key": "normalized"}
    first = await client.post("/api/v1/payments", json={**payload, "amount": "10"}, headers=headers)
    same = await client.post(
        "/api/v1/payments", json={**payload, "amount": "10.00"}, headers=headers
    )
    conflict = await client.post("/api/v1/payments", json=payload, headers=headers)
    assert first.status_code == same.status_code == 202
    assert first.json() == same.json()
    assert conflict.status_code == 409


@pytest.mark.parametrize("path", ["/api/v1/payments/" + str(uuid4()), "/openapi.json", "/docs"])
async def test_auth_required_every_get(client, path):
    client.headers.clear()
    assert (await client.get(path)).status_code == 401


async def test_post_auth_and_key_required(client, payload):
    assert (await client.post("/api/v1/payments", json=payload)).status_code == 422
    for value in ["", "has space", "x" * 256]:
        response = await client.post(
            "/api/v1/payments", json=payload, headers={"Idempotency-Key": value}
        )
        assert response.status_code == 422
    client.headers.clear()
    assert (await client.post("/api/v1/payments", json=payload)).status_code == 401
    assert (
        await client.post("/api/v1/payments", json=payload, headers={"X-API-Key": "wrong"})
    ).status_code == 401


@pytest.mark.parametrize(
    "field,value",
    [
        ("amount", "0"),
        ("amount", "-1"),
        ("amount", "0.001"),
        ("amount", "10000000000000000"),
        ("amount", "NaN"),
        ("currency", "GBP"),
        ("webhook_url", "ftp://example.com/a"),
        ("webhook_url", "https://name:password@example.com"),
        ("description", "x" * 2001),
        ("extra", "unknown"),
    ],
)
async def test_invalid_payload(client, payload, field, value):
    response = await client.post(
        "/api/v1/payments",
        json={**payload, field: value},
        headers={"Idempotency-Key": "invalid"},
    )
    assert response.status_code == 422


async def test_unknown_and_invalid_uuid(client):
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404
    assert (await client.get("/api/v1/payments/not-a-uuid")).status_code == 422


async def test_payment_rolls_back_if_outbox_insert_fails(sessions, payload):
    def fail_outbox(session, *_):
        if any(isinstance(obj, Outbox) for obj in session.new):
            raise RuntimeError("simulated outbox failure")

    event.listen(Session, "before_flush", fail_outbox)
    try:
        with pytest.raises(RuntimeError, match="simulated"):
            await create_payment(sessions, PaymentCreate(**payload), "rollback")
    finally:
        event.remove(Session, "before_flush", fail_outbox)
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0
