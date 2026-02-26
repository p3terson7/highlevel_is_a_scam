from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

import yaml
from twilio.rest import Client as TwilioClient

from app.core.config import Settings
from app.core.logging import get_logger
from app.db.models import Client

logger = get_logger(__name__)


class SMSProvider(Protocol):
    def send_sms(self, to_number: str, body: str) -> str:
        ...


class TwilioSMSProvider:
    def __init__(self, account_sid: str, auth_token: str, from_number: str) -> None:
        self._client = TwilioClient(account_sid, auth_token)
        self._from_number = from_number

    def send_sms(self, to_number: str, body: str) -> str:
        message = self._client.messages.create(body=body, from_=self._from_number, to=to_number)
        return str(message.sid)


class LoggingSMSProvider:
    def send_sms(self, to_number: str, body: str) -> str:
        sid = f"MOCK-{uuid4().hex[:16]}"
        logger.info("sms_mock_send", extra={"to": to_number, "sid": sid, "body": body})
        return sid


class SMSService:
    def __init__(self, provider: SMSProvider, templates: dict[str, str]) -> None:
        self._provider = provider
        self._templates = templates

    def render_template(
        self,
        client: Client,
        template_key: str,
        context: dict[str, Any] | None = None,
    ) -> str:
        merged = {**self._templates, **(client.template_overrides or {})}
        template = merged.get(template_key, "")
        values: dict[str, Any] = {
            "business_name": client.business_name,
            "booking_url": client.booking_url,
            "consent_text": client.consent_text,
            "handoff_number": client.fallback_handoff_number,
        }
        if context:
            values.update(context)
        return template.format(**values).strip()

    def send_message(self, to_number: str, body: str) -> str:
        return self._provider.send_sms(to_number=to_number, body=body)


@lru_cache
def load_default_templates() -> dict[str, str]:
    path = Path(__file__).resolve().parents[1] / "templates" / "default_messages.yml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    return {str(k): str(v) for k, v in raw.items()}


def build_sms_service(settings: Settings, runtime_overrides: dict[str, str] | None = None) -> SMSService:
    account_sid = (runtime_overrides or {}).get("twilio_account_sid", settings.twilio_account_sid)
    auth_token = (runtime_overrides or {}).get("twilio_auth_token", settings.twilio_auth_token)
    from_number = (runtime_overrides or {}).get("twilio_from_number", settings.twilio_from_number)

    provider: SMSProvider
    if account_sid and auth_token and from_number:
        provider = TwilioSMSProvider(
            account_sid=account_sid,
            auth_token=auth_token,
            from_number=from_number,
        )
    else:
        provider = LoggingSMSProvider()

    return SMSService(provider=provider, templates=load_default_templates())
