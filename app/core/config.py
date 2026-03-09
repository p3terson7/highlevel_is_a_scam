from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "lead-conversion-sms-agent"
    env: str = "dev"
    log_level: str = "INFO"
    auto_create_tables: bool = True

    database_url: str = "postgresql+psycopg://postgres:postgres@postgres:5432/leads_db"
    redis_url: str = "redis://redis:6379/0"

    twilio_account_sid: str = ""
    twilio_auth_token: str = ""
    twilio_from_number: str = ""
    public_base_url: str = ""

    openai_api_key: str = ""
    openai_model: str = "gpt-4.1-mini"
    ai_provider_mode: str = "auto"

    meta_verify_token: str = "meta-verify-token"
    meta_access_token: str = ""
    meta_graph_api_version: str = "v22.0"
    linkedin_verify_token: str = "linkedin-verify-token"
    admin_token: str = "change-me"
    enable_demo_seed: bool = False

    rq_eager: bool = False
    rate_limit_count: int = 30
    rate_limit_window_minutes: int = 1
    after_hours_followup_minutes: int = 720
    request_timeout_seconds: int = 20


@lru_cache
def get_settings() -> Settings:
    return Settings()
