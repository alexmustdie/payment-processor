"""Local demonstration receiver. In-memory data is cleared on restart."""

import asyncio
from collections import defaultdict
from datetime import UTC, datetime

from fastapi import FastAPI, Response

app = FastAPI()
received = defaultdict(list)


@app.post("/{mode}")
async def webhook(mode: str, body: dict):
    payment_id = body["payment_id"]
    received[payment_id].append({"received_at": datetime.now(UTC).isoformat(), "payload": body})
    count = len(received[payment_id])
    if mode == "slow-once" and count == 1:
        await asyncio.sleep(30)
    if mode == "fail" or (mode == "flaky" and count < 3):
        return Response(status_code=503)
    return Response(status_code=204)


@app.get("/events/{payment_id}")
async def events(payment_id: str):
    return received[payment_id]
