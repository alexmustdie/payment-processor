import asyncio
import json
import secrets
from contextlib import asynccontextmanager, suppress
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Security
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import Response
from fastapi.security import APIKeyHeader

from app.config import Settings
from app.db import make_database
from app.models import Payment
from app.outbox import run_relay
from app.payments import IdempotencyConflict, create_payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetails


def create_app(settings: Settings | None = None, *, relay_enabled: bool = True) -> FastAPI:
    settings = settings or Settings()
    engine, sessions = make_database(settings.database_url)

    @asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(run_relay(sessions, settings)) if relay_enabled else None
        try:
            yield
        finally:
            if task:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            await engine.dispose()

    api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

    async def authenticate(key: Annotated[str | None, Security(api_key_header)]):
        expected = settings.api_key.get_secret_value()
        if not key or not secrets.compare_digest(key.encode(), expected.encode()):
            raise HTTPException(status_code=401, detail="Invalid or missing API key")

    app = FastAPI(
        title="Payment processor",
        version="0.1.0",
        lifespan=lifespan,
        dependencies=[Depends(authenticate)],
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.engine = engine
    app.state.sessions = sessions

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        # Raw input/context may contain non-finite numbers, invalid Unicode or secrets.
        return Response(
            status_code=422,
            media_type="application/json",
            content=json.dumps(
                {
                    "detail": [
                        {"loc": error["loc"], "msg": error["msg"], "type": error["type"]}
                        for error in exc.errors()
                    ]
                },
                ensure_ascii=True,
                allow_nan=False,
            ),
        )

    @app.get("/openapi.json", include_in_schema=False)
    async def schema():
        return app.openapi()

    @app.get("/docs", include_in_schema=False)
    async def docs():
        return get_swagger_ui_html(openapi_url="/openapi.json", title=app.title)

    @app.post("/api/v1/payments", status_code=202, response_model=PaymentAccepted)
    async def post_payment(
        body: PaymentCreate,
        idempotency_key: Annotated[
            str, Header(min_length=1, max_length=255, pattern=r"^[\x21-\x7E]+$")
        ],
    ):
        try:
            payment = await create_payment(sessions, body, idempotency_key)
        except IdempotencyConflict as exc:
            raise HTTPException(409, "Idempotency-Key was used with a different request") from exc
        return PaymentAccepted(
            payment_id=payment.id, status=payment.status, created_at=payment.created_at
        )

    @app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetails)
    async def get_payment(payment_id: UUID):
        async with sessions() as session:
            payment = await session.get(Payment, payment_id)
            if payment is None:
                raise HTTPException(404, "Payment not found")
            return PaymentDetails(
                payment_id=payment.id,
                metadata=payment.metadata_,
                **{
                    name: getattr(payment, name)
                    for name in PaymentDetails.model_fields
                    if name not in {"payment_id", "metadata"}
                },
            )

    return app
