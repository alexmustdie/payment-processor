from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:55432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5673/"
    api_key: SecretStr = Field(min_length=16)
    outbox_poll_seconds: float = Field(default=0.5, gt=0)
    retry_base_seconds: float = Field(default=2, gt=0)
    webhook_timeout_seconds: float = Field(default=5, gt=0)


MAX_ATTEMPTS = 3
