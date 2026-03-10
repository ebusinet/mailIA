from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://mailia:mailia@postgres:5432/mailia"

    # Elasticsearch
    elasticsearch_url: str = "http://elasticsearch:9200"

    # Redis
    redis_url: str = "redis://redis:6379/0"
    celery_broker_url: str = "redis://redis:6379/1"
    redis_password: str = "mailia_redis"

    # Tika
    tika_url: str = "http://tika:9998"

    # Security
    secret_key: str = "change-me"
    encryption_key: str = "change-me"
    access_token_expire_minutes: int = 60 * 24  # 24h
    algorithm: str = "HS256"

    # Telegram
    telegram_bot_token: str = ""

    # AI defaults
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    mcp_sse_url: str = ""  # e.g. https://mailia.expert-presta.com/mcp/sse

    # App
    app_url: str = "https://mailia.expert-presta.com"
    app_name: str = "MailIA"
    log_level: str = "info"

    model_config = {"env_file": ".env", "extra": "ignore"}


@lru_cache
def get_settings() -> Settings:
    return Settings()
