import logging
from functools import lru_cache

from pydantic import field_validator
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
    sms_provider_mode: str = "auto"
    settings_encryption_keys: str = ""

    openai_api_key: str = ""
    openai_model: str = "gpt-5.4-mini"
    ai_provider_mode: str = "auto"

    admin_token: str = ""
    enable_demo_seed: bool = False
    allow_unsigned_twilio_webhooks: bool = False
    allow_unsigned_crm_webhooks: bool = False

    # Deployment fallbacks for client-scoped webhook credentials. The legacy
    # Zapier key remains an inbound-only alias during migration.
    crm_webhook_secret: str = ""
    zapier_booking_webhook_secret: str = ""
    zapier_webhook_secret: str = ""

    rq_eager: bool = False
    # Aggregate authenticated Twilio admission is separate from the per-lead
    # conversation limiter below. It bounds lead creation and paid downstream
    # work when an attacker rotates sender numbers.
    twilio_inbound_tenant_limit: int = 120
    twilio_inbound_account_limit: int = 600
    twilio_inbound_window_seconds: int = 60
    rate_limit_count: int = 100
    rate_limit_window_minutes: int = 1
    automated_sms_delay_seconds: int = 20
    after_hours_followup_minutes: int = 720
    request_timeout_seconds: int = 20
    request_body_max_bytes: int = 1024 * 1024
    message_media_storage_dir: str = "storage/message_media"
    message_media_max_bytes: int = 25 * 1024 * 1024

    ui_react_island_enabled: bool = False
    ui_react_app_shell_enabled: bool = True
    ui_legacy_shell_enabled: bool = False
    ui_secure_cookies: bool | None = None
    enable_legacy_portal_token_login: bool = False

    @field_validator("ui_secure_cookies", mode="before")
    @classmethod
    def _parse_ui_secure_cookies(cls, value: object) -> object:
        if isinstance(value, str) and value.strip().lower() in {"", "auto"}:
            return None
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()


def validate_security_settings(settings: Settings) -> None:
    admin_token = settings.admin_token.strip()
    if len(admin_token) < 32 or admin_token.lower() in {"change-me", "changeme", "admin", "password"}:
        raise RuntimeError(
            "ADMIN_TOKEN must be set to a non-default secret of at least 32 characters before the application starts"
        )

    sms_provider_mode = settings.sms_provider_mode.strip().lower()
    if sms_provider_mode not in {"auto", "mock", "twilio"}:
        raise RuntimeError("SMS_PROVIDER_MODE must be one of: auto, mock, twilio")
    if sms_provider_mode == "mock":
        logging.getLogger(__name__).warning(
            "sms_mock_mode_explicitly_enabled",
            extra={
                "environment": settings.env,
                "warning": "SMS messages will be recorded but will not be delivered by Twilio",
            },
        )

    if settings.allow_unsigned_crm_webhooks and settings.env.strip().lower() not in {
        "dev",
        "development",
        "local",
        "test",
    }:
        raise RuntimeError(
            "ALLOW_UNSIGNED_CRM_WEBHOOKS may only be enabled in a local development environment"
        )

    if settings.twilio_inbound_tenant_limit <= 0:
        raise RuntimeError("TWILIO_INBOUND_TENANT_LIMIT must be greater than zero")
    if settings.twilio_inbound_account_limit <= 0:
        raise RuntimeError("TWILIO_INBOUND_ACCOUNT_LIMIT must be greater than zero")
    if settings.twilio_inbound_window_seconds <= 0:
        raise RuntimeError("TWILIO_INBOUND_WINDOW_SECONDS must be greater than zero")

    if not 0 <= settings.automated_sms_delay_seconds <= 300:
        raise RuntimeError("AUTOMATED_SMS_DELAY_SECONDS must be between 0 and 300")

    if (
        settings.env.strip().lower() in {"prod", "production"}
        and settings.ui_secure_cookies is False
    ):
        raise RuntimeError("UI_SECURE_COOKIES cannot be disabled in production")
