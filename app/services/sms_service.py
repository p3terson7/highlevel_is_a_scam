from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

import yaml
from twilio.base.exceptions import TwilioRestException
from twilio.http.http_client import TwilioHttpClient
from twilio.rest import Client as TwilioClient

from app.core.config import Settings
from app.core.logging import get_logger
from app.db.models import Client
from app.services.i18n import client_language, normalize_language
from app.services.sms_delivery import (
    twilio_status_callback_url,
    with_initial_delivery_status,
)

logger = get_logger(__name__)

_PRODUCTION_ENVS = {"prod", "production"}


class SMSDeliveryError(RuntimeError):
    def __init__(self, detail: str, *, provider_status: int | None = None, provider_code: str | None = None) -> None:
        super().__init__(detail)
        self.provider_status = provider_status
        self.provider_code = provider_code


@dataclass(frozen=True)
class SMSFailureDisposition:
    """Whether a failed provider call is known not to have sent a message."""

    ambiguous: bool
    safe_to_retry: bool
    reason: str
    provider_status: int | None = None
    provider_code: str | None = None


def classify_sms_delivery_failure(exc: Exception) -> SMSFailureDisposition:
    """Classify provider rejections separately from unknown delivery outcomes.

    A returned 4xx response is definitive: Twilio rejected the request, so the
    same durable outbound operation may be retried explicitly. Transport/read
    failures and server-side failures do not prove that Twilio rejected the
    message and must never be retried automatically.
    """

    if isinstance(exc, SMSDeliveryError):
        try:
            provider_status = (
                int(exc.provider_status)
                if exc.provider_status is not None
                else None
            )
        except (TypeError, ValueError):
            provider_status = None
        provider_code = exc.provider_code
        if provider_status is not None and 400 <= provider_status < 500:
            return SMSFailureDisposition(
                ambiguous=False,
                safe_to_retry=True,
                reason="delivery_rejected",
                provider_status=provider_status,
                provider_code=provider_code,
            )
        return SMSFailureDisposition(
            ambiguous=True,
            safe_to_retry=False,
            reason="delivery_result_unknown",
            provider_status=provider_status,
            provider_code=provider_code,
        )
    return SMSFailureDisposition(
        ambiguous=True,
        safe_to_retry=False,
        reason="delivery_result_unknown",
    )


class SMSProvider(Protocol):
    def send_sms(self, to_number: str, body: str, media_urls: list[str] | None = None) -> str:
        ...


class TwilioSMSProvider:
    def __init__(
        self,
        account_sid: str,
        auth_token: str,
        from_number: str,
        status_callback_url: str = "",
        timeout_seconds: float = 20,
    ) -> None:
        http_client = TwilioHttpClient(
            timeout=max(float(timeout_seconds), 1.0),
            # POST /Messages is not safely retryable without an application-level
            # idempotency reservation. Keep SDK transport retries disabled.
            max_retries=0,
        )
        self._client = TwilioClient(account_sid, auth_token, http_client=http_client)
        self._from_number = from_number
        self._status_callback_url = status_callback_url

    def send_sms(self, to_number: str, body: str, media_urls: list[str] | None = None) -> str:
        payload: dict[str, Any] = {"from_": self._from_number, "to": to_number}
        if body:
            payload["body"] = body
        if media_urls:
            payload["media_url"] = media_urls
        if self._status_callback_url:
            payload["status_callback"] = self._status_callback_url
        message = self._client.messages.create(**payload)
        return str(message.sid)


@lru_cache(maxsize=16)
def _cached_twilio_provider(
    account_sid: str,
    auth_token: str,
    from_number: str,
    status_callback_url: str,
    timeout_seconds: float,
) -> TwilioSMSProvider:
    return TwilioSMSProvider(
        account_sid=account_sid,
        auth_token=auth_token,
        from_number=from_number,
        status_callback_url=status_callback_url,
        timeout_seconds=timeout_seconds,
    )


def clear_sms_provider_cache() -> None:
    _cached_twilio_provider.cache_clear()
    _warn_mock_provider_once.cache_clear()
    _warn_unavailable_provider_once.cache_clear()


class LoggingSMSProvider:
    def send_sms(self, to_number: str, body: str, media_urls: list[str] | None = None) -> str:
        sid = f"MOCK-{uuid4().hex[:16]}"
        logger.info(
            "sms_mock_send",
            extra={
                "sid": sid,
                "body_length": len(body or ""),
                "media_count": len(media_urls or []),
                "recipient_redacted": bool(to_number),
            },
        )
        return sid


class UnavailableSMSProvider:
    def __init__(self, reason: str) -> None:
        self._reason = reason

    def send_sms(self, to_number: str, body: str, media_urls: list[str] | None = None) -> str:
        _ = to_number, body, media_urls
        raise SMSDeliveryError(self._reason)


@lru_cache(maxsize=8)
def _warn_mock_provider_once(environment: str, mode: str) -> None:
    logger.warning(
        "sms_mock_provider_enabled",
        extra={
            "environment": environment,
            "mode": mode,
            "warning": "SMS messages will not be delivered by Twilio",
        },
    )


@lru_cache(maxsize=16)
def _warn_unavailable_provider_once(environment: str, mode: str, missing_fields: tuple[str, ...]) -> None:
    logger.error(
        "sms_provider_unavailable",
        extra={
            "environment": environment,
            "mode": mode,
            "missing_fields": list(missing_fields),
        },
    )


class SMSService:
    def __init__(self, provider: SMSProvider, templates: dict[str, str], *, provider_kind: str = "mock", status_callback_url: str = "") -> None:
        self._provider = provider
        self._templates = templates
        self.provider_kind = provider_kind
        self.status_callback_url = status_callback_url

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

    def with_delivery_status(self, raw_payload: dict[str, Any] | None, provider_sid: str) -> dict[str, Any]:
        return with_initial_delivery_status(
            raw_payload,
            provider_sid=provider_sid,
            provider=self.provider_kind,
            callback_url=self.status_callback_url,
        )


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
    account_sid = str((runtime_overrides or {}).get("twilio_account_sid", settings.twilio_account_sid) or "").strip()
    auth_token = str((runtime_overrides or {}).get("twilio_auth_token", settings.twilio_auth_token) or "").strip()
    from_number = str((runtime_overrides or {}).get("twilio_from_number", settings.twilio_from_number) or "").strip()
    status_callback_url = twilio_status_callback_url(settings, runtime_overrides)
    mode = settings.sms_provider_mode.strip().lower()
    environment = settings.env.strip().lower()
    missing_fields = tuple(
        field
        for field, value in (
            ("twilio_account_sid", account_sid),
            ("twilio_auth_token", auth_token),
            ("twilio_from_number", from_number),
        )
        if not value
    )

    provider: SMSProvider
    provider_kind: str
    if mode == "mock":
        provider_kind = "mock"
        _warn_mock_provider_once(environment, mode)
        provider = LoggingSMSProvider()
    elif not missing_fields and mode in {"auto", "twilio"}:
        provider_kind = "twilio"
        provider = _cached_twilio_provider(
            account_sid=account_sid,
            auth_token=auth_token,
            from_number=from_number,
            status_callback_url=status_callback_url,
            timeout_seconds=max(float(settings.request_timeout_seconds), 1.0),
        )
    elif mode == "auto" and environment not in _PRODUCTION_ENVS:
        provider_kind = "mock"
        provider = LoggingSMSProvider()
    else:
        provider_kind = "unavailable"
        _warn_unavailable_provider_once(environment, mode or "invalid", missing_fields)
        provider = UnavailableSMSProvider(
            "SMS provider is not configured. Configure the Twilio Account SID, Auth Token, and From Number."
        )

    return SMSService(
        provider=provider,
        templates=load_default_templates(),
        provider_kind=provider_kind,
        status_callback_url=status_callback_url if provider_kind == "twilio" else "",
    )


def build_mock_sms_service() -> SMSService:
    return SMSService(provider=LoggingSMSProvider(), templates=load_default_templates(), provider_kind="mock")


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
