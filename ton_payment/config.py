from functools import lru_cache
from typing import Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL


class Settings(BaseSettings):
    """Application settings loaded from environment and .env file."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_echo: bool = False

    api_token: SecretStr

    webhook_url: str
    webhook_max_attempts: int = 3
    webhook_retry_delay_seconds: float = 2.0

    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    worker_poll_interval: float = 1.0
    run_workers: bool = True

    toncenter_api_key: SecretStr
    toncenter_network: Literal["MAINNET", "TESTNET"] = "TESTNET"
    ton_wallet_mnemonic: SecretStr
    ton_wallet_address: str

    ton_wallet_version: Literal["v4r2", "v5r1"] = "v5r1"
    ton_network_global_id: int = -239

    valid_for_seconds: int = 60
    grace_period_seconds: int = 180

    outbox_poll_interval: float = 1.0
    seqno_poll_interval: float = 2.0
    tracker_sse_interval: float = 10.0
    tracker_poll_interval: float = 10.0
    tracker_expire_interval: float = 10.0

    debug: bool = False
    title: str = "TonPayment"
    description: str = "Ton Gateway"

    @property
    def database_url(self) -> URL:
        return URL.create(
            "postgresql+asyncpg",
            username=self.postgres_user,
            password=self.postgres_password,
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


settings = get_settings()
