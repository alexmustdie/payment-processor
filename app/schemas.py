from datetime import datetime
from decimal import Decimal
from math import isfinite
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator

Currency = Literal["RUB", "USD", "EUR"]
PaymentStatus = Literal["pending", "succeeded", "failed"]


def validate_storage_value(value: Any) -> None:
    """Reject JSON values PostgreSQL text/JSONB cannot store, at any nesting level."""
    if isinstance(value, str):
        if "\x00" in value:
            raise ValueError("Strings must not contain NUL characters")
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError("Strings must contain valid Unicode") from exc
    elif isinstance(value, float) and not isfinite(value):
        raise ValueError("JSON numbers must be finite")
    elif isinstance(value, dict):
        for key, item in value.items():
            validate_storage_value(key)
            validate_storage_value(item)
    elif isinstance(value, list):
        for item in value:
            validate_storage_value(item)


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Currency
    description: str = Field(default="", max_length=2000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @model_validator(mode="before")
    @classmethod
    def storage_compatible_input(cls, value: Any) -> Any:
        validate_storage_value(value)
        return value

    @field_validator("webhook_url")
    @classmethod
    def no_url_credentials(cls, value: HttpUrl) -> HttpUrl:
        if value.username or value.password or value.fragment:
            raise ValueError("Webhook URL must not contain credentials or a fragment")
        return value


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetails(PaymentAccepted):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str
    idempotency_key: str
    processed_at: datetime | None
    attempts: int
    webhook_delivered_at: datetime | None
    dead_lettered_at: datetime | None
    last_error: str | None


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payment_id: UUID
    attempt: Annotated[int, Field(ge=1, le=3)] = 1
