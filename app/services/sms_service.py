from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

import yaml
from twilio.base.exceptions import TwilioRestException
from twilio.rest import Client as TwilioClient

from app.core.config import Settings
from app.core.logging import get_logger
from app.db.models import Client
from app.services.i18n import client_language, normalize_language

logger = get_logger(__name__)


class SMSDeliveryError(RuntimeError):
    def __init__(self, detail: str, *, provider_status: int | None = None, provider_code: str | None = None) -> None:
        super().__init__(detail)
        self.provider_status = provider_status
        self.provider_code = provider_code


class SMSProvider(Protocol):
    def send_sms(self, to_number: str, body: str, media_urls: list[str] | None = None) -> str:
        ...


class TwilioSMSProvider:
    def __init__(self, account_sid: str, auth_token: str, from_number: str) -> None:
        self._client = TwilioClient(account_sid, auth_token)
        self._from_number = from_number

    def send_sms(self, to_number: str, body: str, media_urls: list[str] | None = None) -> str:
        payload: dict[str, Any] = {"from_": self._from_number, "to": to_number}
        if body:
            payload["body"] = body
        if media_urls:
            payload["media_url"] = media_urls
        message = self._client.messages.create(**payload)
        return str(message.sid)


@lru_cache(maxsize=16)
def _cached_twilio_provider(account_sid: str, auth_token: str, from_number: str) -> TwilioSMSProvider:
    return TwilioSMSProvider(account_sid=account_sid, auth_token=auth_token, from_number=from_number)


def clear_sms_provider_cache() -> None:
    _cached_twilio_provider.cache_clear()


class LoggingSMSProvider:
    def send_sms(self, to_number: str, body: str, media_urls: list[str] | None = None) -> str:
        sid = f"MOCK-{uuid4().hex[:16]}"
        logger.info("sms_mock_send", extra={"to": to_number, "sid": sid, "body": body, "media_urls": media_urls or []})
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
        language = normalize_language((context or {}).get("language") or client_language(client))
        localized_key = f"{language}:{template_key}"
        template = merged.get(localized_key) or merged.get(template_key, "")
        values: dict[str, Any] = {
            "business_name": client.business_name,
            "booking_url": client.booking_url,
            "consent_text": client.consent_text,
            "handoff_number": client.fallback_handoff_number,
        }
        if context:
            values.update(context)
        return template.format(**values).strip()

    def send_message(self, to_number: str, body: str, media_urls: list[str] | None = None) -> str:
        try:
            return self._provider.send_sms(to_number=to_number, body=body, media_urls=media_urls)
        except SMSDeliveryError:
            raise
        except Exception as exc:
            detail, provider_status, provider_code = _delivery_error_details(exc)
            logger.warning(
                "sms_delivery_failed",
                extra={
                    "provider_status": provider_status,
                    "provider_code": provider_code,
                    "error_type": type(exc).__name__,
                },
            )
            raise SMSDeliveryError(
                detail,
                provider_status=provider_status,
                provider_code=provider_code,
            ) from exc


@lru_cache
def load_default_templates() -> dict[str, str]:
    template_dir = Path(__file__).resolve().parents[1] / "templates"
    path = template_dir / "default_messages.yml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    templates = {str(k): str(v) for k, v in raw.items()} if isinstance(raw, dict) else {}
    fr_path = template_dir / "default_messages.fr.yml"
    if fr_path.exists():
        raw_fr = yaml.safe_load(fr_path.read_text(encoding="utf-8"))
        if isinstance(raw_fr, dict):
            templates.update({f"fr:{k}": str(v) for k, v in raw_fr.items()})
    return templates


def build_sms_service(settings: Settings, runtime_overrides: dict[str, str] | None = None) -> SMSService:
    account_sid = (runtime_overrides or {}).get("twilio_account_sid", settings.twilio_account_sid)
    auth_token = (runtime_overrides or {}).get("twilio_auth_token", settings.twilio_auth_token)
    from_number = (runtime_overrides or {}).get("twilio_from_number", settings.twilio_from_number)

    provider: SMSProvider
    if account_sid and auth_token and from_number:
        provider = _cached_twilio_provider(
            account_sid=account_sid,
            auth_token=auth_token,
            from_number=from_number,
        )
    else:
        provider = LoggingSMSProvider()

    return SMSService(provider=provider, templates=load_default_templates())


def build_mock_sms_service() -> SMSService:
    return SMSService(provider=LoggingSMSProvider(), templates=load_default_templates())


def _delivery_error_details(exc: Exception) -> tuple[str, int | None, str | None]:
    if isinstance(exc, TwilioRestException):
        provider_status = getattr(exc, "status", None)
        provider_code = str(getattr(exc, "code", "") or "") or None
        if provider_status in {401, 403}:
            return (
                "SMS provider authentication failed. Check the Twilio Account SID/Auth Token for this client or runtime config.",
                provider_status,
                provider_code,
            )
        message = str(getattr(exc, "msg", "") or "").strip()
        if not message:
            message = "Twilio rejected the message."
        return f"SMS provider rejected the message: {message}", provider_status, provider_code
    return "SMS provider failed to send the message. Check the SMS provider configuration and logs.", None, None
