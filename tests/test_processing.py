import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from app.models import Outbox, Payment
from app.payments import create_payment
from app.processing import InvalidPaymentEvent, process_payment
from app.schemas import PaymentCreate, PaymentEvent


async def make_payment(sessions, payload):
    return await create_payment(sessions, PaymentCreate(**payload), uuid4().hex)


@pytest.mark.parametrize("gateway_status", ["succeeded", "failed"])
async def test_gateway_result_and_duplicate_event(sessions, settings, payload, gateway_status):
    payment = await make_payment(sessions, payload)
    gateway = AsyncMock(return_value=gateway_status)
    requests = []

    def receiver(request):
        requests.append(request)
        return httpx.Response(204)

    event = PaymentEvent(payment_id=payment.id)
    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as client:
        await process_payment(sessions, settings, event, client, gateway)
        await process_payment(sessions, settings, event, client, gateway)
    gateway.assert_awaited_once()
    assert len(requests) == 1
    body = json.loads(requests[0].content)
    assert body["status"] == gateway_status
    assert body["amount"] == "125.50"
    assert requests[0].headers["Idempotency-Key"] == body["event_id"]
    async with sessions() as session:
        result = await session.get(Payment, payment.id)
        assert result.attempts == 1
        assert result.webhook_delivered_at is not None


@pytest.mark.parametrize("final_success", [True, False])
async def test_retries_backoff_dlq_and_gateway_not_repeated(
    sessions, settings, payload, final_success
):
    payment = await make_payment(sessions, payload)
    gateway = AsyncMock(return_value="succeeded")
    calls = []

    def receiver(request):
        calls.append(request)
        return httpx.Response(204 if final_success and len(calls) == 3 else 503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as client:
        for attempt in range(1, 4):
            started = datetime.now(UTC)
            event = PaymentEvent(payment_id=payment.id, attempt=attempt)
            await process_payment(sessions, settings, event, client, gateway)
            await process_payment(sessions, settings, event, client, gateway)  # duplicate
            async with sessions() as session:
                result = await session.get(Payment, payment.id)
                assert result.attempts == attempt
                events = list((await session.scalars(select(Outbox))).all())
                if attempt < 3:
                    retry = next(e for e in events if e.payload.get("attempt") == attempt + 1)
                    delay = (retry.available_at - started).total_seconds()
                    assert 2**attempt <= delay < 2**attempt + 2
                else:
                    assert bool(result.webhook_delivered_at) == final_success
                    assert bool(result.dead_lettered_at) != final_success
                    assert sum(e.routing_key == "payments.dead" for e in events) == (
                        0 if final_success else 1
                    )
    gateway.assert_awaited_once()
    assert len(calls) == 3
    assert len({request.headers["Idempotency-Key"] for request in calls}) == 1
    assert len({request.content for request in calls}) == 1


async def test_crash_after_gateway_commit_resumes_webhook_only(sessions, settings, payload):
    payment = await make_payment(sessions, payload)
    gateway = AsyncMock(return_value="succeeded")
    deliveries = []

    def receiver(request):
        deliveries.append(request)
        if len(deliveries) == 1:
            raise asyncio.CancelledError  # client may have received before process died
        return httpx.Response(204)

    event = PaymentEvent(payment_id=payment.id)
    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as client:
        with pytest.raises(asyncio.CancelledError):
            await process_payment(sessions, settings, event, client, gateway)
        await process_payment(sessions, settings, event, client, gateway)
    gateway.assert_awaited_once()
    assert len(deliveries) == 2
    assert deliveries[0].content == deliveries[1].content


async def test_gateway_exceptions_eventually_dead_letter(sessions, settings, payload):
    payment = await make_payment(sessions, payload)
    gateway = AsyncMock(side_effect=TimeoutError)
    client = AsyncMock()
    for attempt in range(1, 4):
        await process_payment(
            sessions,
            settings,
            PaymentEvent(payment_id=payment.id, attempt=attempt),
            client,
            gateway,
        )
    assert gateway.await_count == 3
    client.stream.assert_not_called()
    async with sessions() as session:
        result = await session.get(Payment, payment.id)
        assert result.status == "pending"
        assert result.dead_lettered_at is not None
        assert result.last_error == "TimeoutError"


@pytest.mark.parametrize("error_kind", ["timeout", "redirect"])
async def test_webhook_transport_errors_and_redirects_retry(
    sessions, settings, payload, error_kind
):
    payment = await make_payment(sessions, payload)

    def receiver(request):
        if error_kind == "timeout":
            raise httpx.ReadTimeout("URL and secret should not be stored", request=request)
        return httpx.Response(302, headers={"Location": "https://another.example/"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as client:
        await process_payment(
            sessions,
            settings,
            PaymentEvent(payment_id=payment.id),
            client,
            AsyncMock(return_value="succeeded"),
        )
    async with sessions() as session:
        result = await session.get(Payment, payment.id)
        assert result.attempts == 1
        assert result.webhook_delivered_at is None
        assert "secret" not in result.last_error


async def test_unknown_or_out_of_order_event_rejected(sessions, settings, payload):
    with pytest.raises(InvalidPaymentEvent):
        await process_payment(sessions, settings, PaymentEvent(payment_id=uuid4()), AsyncMock())
    payment = await make_payment(sessions, payload)
    with pytest.raises(InvalidPaymentEvent):
        await process_payment(
            sessions, settings, PaymentEvent(payment_id=payment.id, attempt=3), AsyncMock()
        )


async def test_concurrent_duplicate_processing_calls_gateway_and_webhook_once(
    sessions, settings, payload
):
    payment = await make_payment(sessions, payload)
    gateway = AsyncMock(return_value="succeeded")
    requests = []

    def receiver(request):
        requests.append(request)
        return httpx.Response(204)

    event = PaymentEvent(payment_id=payment.id)
    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as client:
        await asyncio.gather(
            process_payment(sessions, settings, event, client, gateway),
            process_payment(sessions, settings, event, client, gateway),
        )
    gateway.assert_awaited_once()
    assert len(requests) == 1


async def test_total_webhook_deadline_records_retry(sessions, settings, payload):
    payment = await make_payment(sessions, payload)
    settings = settings.model_copy(update={"webhook_timeout_seconds": 0.03})

    async def receiver(request):
        # This transport ignores HTTPX's I/O timeout: the outer deadline must cancel it.
        await asyncio.Event().wait()

    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as client:
        async with asyncio.timeout(2):
            await process_payment(
                sessions,
                settings,
                PaymentEvent(payment_id=payment.id),
                client,
                AsyncMock(return_value="succeeded"),
            )
    async with sessions() as session:
        result = await session.get(Payment, payment.id)
        assert result.status == "succeeded"
        assert result.webhook_delivered_at is None
        assert result.attempts == 1
        assert result.last_error == "TimeoutError"
        events = (await session.scalars(select(Outbox))).all()
        assert any(event.payload["attempt"] == 2 for event in events)


async def test_webhook_response_body_is_not_downloaded(sessions, settings, payload):
    payment = await make_payment(sessions, payload)

    class UnusedBody(httpx.AsyncByteStream):
        read = False
        closed = False

        async def __aiter__(self):
            self.read = True
            yield b"unneeded response body"
            await asyncio.Event().wait()

        async def aclose(self):
            self.closed = True

    body = UnusedBody()
    transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=body))
    async with httpx.AsyncClient(transport=transport) as client:
        async with asyncio.timeout(2):
            await process_payment(
                sessions,
                settings,
                PaymentEvent(payment_id=payment.id),
                client,
                AsyncMock(return_value="succeeded"),
            )
    assert not body.read
    assert body.closed
    async with sessions() as session:
        result = await session.get(Payment, payment.id)
        assert result.webhook_delivered_at is not None
