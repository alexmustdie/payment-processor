"""End-to-end checks against the local Compose stack; --faults restarts its services."""

import argparse
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from time import monotonic
from uuid import uuid4

import httpx
from dotenv import dotenv_values
from sqlalchemy import select

from app.config import Settings
from app.db import make_database
from app.messaging import PAYMENTS_EXCHANGE, make_broker
from app.models import Outbox

ROOT = Path(__file__).resolve().parents[1]


async def compose(*args):
    process = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        *args,
        cwd=ROOT,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, error = await process.communicate()
    if process.returncode:
        raise RuntimeError(error.decode())


async def wait_payment(api, payment_id, *, deadline_seconds=60):
    deadline = monotonic() + deadline_seconds
    while monotonic() < deadline:
        response = await api.get(f"/api/v1/payments/{payment_id}")
        response.raise_for_status()
        payment = response.json()
        if payment["webhook_delivered_at"] or payment["dead_lettered_at"]:
            return payment
        await asyncio.sleep(0.2)
    raise AssertionError(f"Payment did not finish: {payment_id}")


async def wait_http(client, path):
    async with asyncio.timeout(45):
        while True:
            try:
                response = await client.get(path)
                if response.is_success:
                    return
                if response.status_code == 401:
                    raise RuntimeError("Readiness authentication failed; check .env")
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.2)


async def main(faults):
    env = {**dotenv_values(ROOT / ".env"), **os.environ}
    api_port = env.get("API_PORT", "18080")
    hook_port = env.get("WEBHOOK_PORT", "18081")
    settings = Settings()
    async with (
        httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{api_port}",
            headers={"X-API-Key": settings.api_key.get_secret_value()},
            timeout=10,
        ) as api,
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{hook_port}") as hooks,
        httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{env.get('RABBITMQ_UI_PORT', '15673')}",
            auth=("payments", "payments"),
        ) as management,
    ):
        await asyncio.gather(
            wait_http(api, "/openapi.json"),
            wait_http(hooks, "/events/readiness"),
            wait_http(management, "/api/overview"),
        )

        async def create(mode="ok"):
            key = uuid4().hex
            body = {
                "amount": "123.45",
                "currency": "RUB",
                "description": "Local smoke test",
                "metadata": {"run": key},
                "webhook_url": f"http://webhook:8001/{mode}",
            }
            response = await api.post(
                "/api/v1/payments", json=body, headers={"Idempotency-Key": key}
            )
            assert response.status_code == 202, response.text
            return response.json()["payment_id"], body, key

        async def assert_in_dlq(payment_id):
            deadline = monotonic() + 15
            while monotonic() < deadline:
                response = await management.post(
                    "/api/queues/%2F/payments.dead/get",
                    json={"count": 500, "ackmode": "ack_requeue_true", "encoding": "auto"},
                )
                response.raise_for_status()
                if any(payment_id in item["payload"] for item in response.json()):
                    return
                await asyncio.sleep(0.3)
            raise AssertionError("Expected event not found in DLQ")

        for mode, count in [("ok", 1), ("flaky", 3), ("fail", 3)]:
            payment_id, body, key = await create(mode)
            duplicate = await api.post(
                "/api/v1/payments", json=body, headers={"Idempotency-Key": key}
            )
            assert duplicate.status_code == 202
            assert duplicate.json()["payment_id"] == payment_id
            conflict = await api.post(
                "/api/v1/payments",
                json={**body, "amount": "124.00"},
                headers={"Idempotency-Key": key},
            )
            assert conflict.status_code == 409
            payment = await wait_payment(api, payment_id)
            deliveries = (await hooks.get(f"/events/{payment_id}")).json()
            assert len(deliveries) == count
            assert payment["attempts"] == count
            assert payment["status"] in {"succeeded", "failed"}
            assert bool(payment["dead_lettered_at"]) == (mode == "fail")
            assert bool(payment["webhook_delivered_at"]) == (mode != "fail")
            assert len({json.dumps(item["payload"], sort_keys=True) for item in deliveries}) == 1
            if count == 3:
                times = [datetime.fromisoformat(item["received_at"]) for item in deliveries]
                assert (times[1] - times[0]).total_seconds() >= settings.retry_base_seconds
                assert (times[2] - times[1]).total_seconds() >= 2 * settings.retry_base_seconds
            if mode == "fail":
                await assert_in_dlq(payment_id)
            async with make_broker(settings.rabbitmq_url) as broker:
                await broker.publish(
                    {"payment_id": payment_id, "attempt": 1},
                    exchange=PAYMENTS_EXCHANGE,
                    routing_key="payments.new",
                    persist=True,
                )
            await asyncio.sleep(0.5)
            assert len((await hooks.get(f"/events/{payment_id}")).json()) == count
            print(
                f"PASS {mode}: API, idempotency, webhook, retries, duplicate delivery", flush=True
            )

        poison_id = uuid4().hex
        async with make_broker(settings.rabbitmq_url) as broker:
            await broker.publish(
                {"invalid": poison_id},
                exchange=PAYMENTS_EXCHANGE,
                routing_key="payments.new",
                persist=True,
            )
        await assert_in_dlq(poison_id)
        print("PASS invalid message routed to DLQ", flush=True)

        if faults:
            await compose("stop", "rabbitmq")
            try:
                payment_id, _, _ = await create()
                pending = (await api.get(f"/api/v1/payments/{payment_id}")).json()
                assert pending["status"] == "pending"
                engine, sessions = make_database(settings.database_url)
                try:
                    async with sessions() as session:
                        rows = list((await session.scalars(select(Outbox))).all())
                        assert any(
                            e.payload.get("payment_id") == payment_id and e.published_at is None
                            for e in rows
                        )
                finally:
                    await engine.dispose()
            finally:
                await compose("start", "rabbitmq")
            result = await wait_payment(api, payment_id, deadline_seconds=90)
            assert result["webhook_delivered_at"]
            print("PASS broker outage: 202 accepted, durable outbox, recovery", flush=True)

            payment_id, _, _ = await create("slow-once")
            deadline = monotonic() + 15
            while monotonic() < deadline:
                response = await hooks.get(f"/events/{payment_id}")
                response.raise_for_status()
                if response.json():
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("Consumer did not reach the webhook before SIGKILL")
            before = (await api.get(f"/api/v1/payments/{payment_id}")).json()
            assert before["processed_at"] and before["webhook_delivered_at"] is None
            # A real SIGKILL tests redelivery without graceful ACK/cleanup.
            await compose("kill", "-s", "SIGKILL", "consumer")
            await compose("start", "consumer")
            result = await wait_payment(api, payment_id)
            assert result["webhook_delivered_at"]
            deliveries = (await hooks.get(f"/events/{payment_id}")).json()
            assert len(deliveries) == 2
            assert deliveries[0]["payload"] == deliveries[1]["payload"]
            assert result["processed_at"] == before["processed_at"]
            assert result["attempts"] == 1
            print(
                "PASS consumer SIGKILL: webhook redelivered, gateway result preserved", flush=True
            )

    print("All end-to-end checks passed.", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--faults", action="store_true", help="Restart this project's broker/consumer"
    )
    asyncio.run(main(parser.parse_args().faults))
