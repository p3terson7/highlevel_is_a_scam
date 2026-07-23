from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from app.core.config import Settings
from app.db.models import Message, MessageDirection

DELIVERY_KEY = "delivery"

_WARNING_STATUSES = {"canceled", "failed", "undelivered"}
_OK_STATUSES = {"delivered"}
_PENDING_STATUSES = {"accepted", "queued", "scheduled", "sending"}
_SENT_STATUSES = {"sent"}
_SIMULATED_PREFIXES = ("MOCK-", "SANDBOX-", "DEMO-", "PRECISCAN-DEMO-", "STACKLEADS-DEMO-")
_ALLOWED_CALLBACK_STATUSES = _WARNING_STATUSES | _OK_STATUSES | _PENDING_STATUSES | _SENT_STATUSES
_TERMINAL_STATUSES = _WARNING_STATUSES | _OK_STATUSES | {"simulated"}
_STATUS_RANK = {
    "sent_to_provider": 0,
    "unverified": 0,
    "accepted": 1,
    "scheduled": 2,
    "queued": 3,
    "sending": 4,
    "sent": 5,
    "delivered": 6,
    "canceled": 6,
    "failed": 6,
    "undelivered": 6,
}


@dataclass(frozen=True)
class DeliveryCallbackResult:
    delivery: dict[str, Any]
    applied: bool
    reason: str


def twilio_status_callback_url(settings: Settings, runtime_overrides: Mapping[str, str] | None = None) -> str:
    base = ""
    if runtime_overrides:
        base = str(runtime_overrides.get("public_base_url") or "").strip()
    if not base:
        base = str(settings.public_base_url or "").strip()
    if not base or base.startswith("http://localhost") or base.startswith("http://127.0.0.1"):
        return ""
    return f"{base.rstrip('/')}/sms/status-callback"


def is_simulated_provider_sid(provider_sid: str) -> bool:
    sid = str(provider_sid or "").strip().upper()
    return bool(sid) and sid.startswith(_SIMULATED_PREFIXES)


def delivery_record_for_send(
    *,
    provider_sid: str,
    provider: str,
    callback_url: str = "",
    now: datetime | None = None,
) -> dict[str, Any]:
    timestamp = _iso(now)
    if is_simulated_provider_sid(provider_sid) or provider == "mock":
        return {
            "channel": "sms",
            "provider": "mock",
            "provider_message_sid": provider_sid,
            "provider_status": "simulated",
            "status": "simulated",
            "severity": "info",
            "label": "Simulated SMS",
            "label_fr": "SMS simulé",
            "description": "Stored in the CRM only. This message was not sent through Twilio.",
            "description_fr": "Stocké dans le CRM seulement. Ce message n'a pas été envoyé par Twilio.",
            "callback_configured": False,
            "updated_at": timestamp,
        }

    callback_configured = bool(callback_url)
    return {
        "channel": "sms",
        "provider": provider or "twilio",
        "provider_message_sid": provider_sid,
        "provider_status": "sent_to_provider",
        "status": "sent_to_provider" if callback_configured else "unverified",
        "severity": "info",
        "label": "Sent - awaiting delivery" if callback_configured else "Sent - delivery unverified",
        "label_fr": "Envoyé - livraison en attente" if callback_configured else "Envoyé - livraison non vérifiée",
        "description": (
            "Twilio accepted the message. Waiting for carrier delivery status."
            if callback_configured
            else "Twilio accepted the message, but no public status callback URL is configured for delivery confirmation."
        ),
        "description_fr": (
            "Twilio a accepté le message. En attente du statut de livraison de l'opérateur."
            if callback_configured
            else "Twilio a accepté le message, mais aucune URL publique de rappel n'est configurée pour confirmer la livraison."
        ),
        "callback_configured": callback_configured,
        "callback_url": callback_url,
        "updated_at": timestamp,
    }


def with_initial_delivery_status(
    raw_payload: Mapping[str, Any] | None,
    *,
    provider_sid: str,
    provider: str,
    callback_url: str = "",
    now: datetime | None = None,
) -> dict[str, Any]:
    payload = dict(raw_payload or {})
    payload[DELIVERY_KEY] = delivery_record_for_send(
        provider_sid=provider_sid,
        provider=provider,
        callback_url=callback_url,
        now=now,
    )
    return payload


def delivery_status_for_message(message: Message) -> dict[str, Any] | None:
    if message.direction != MessageDirection.OUTBOUND:
        return None
    raw_payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
    delivery = raw_payload.get(DELIVERY_KEY)
    if isinstance(delivery, dict):
        normalized = _normalize_delivery_record(delivery)
    else:
        normalized = delivery_record_for_send(
            provider_sid=message.provider_message_sid,
            provider="mock" if is_simulated_provider_sid(message.provider_message_sid) else "twilio",
            callback_url="",
            now=message.created_at,
        )
    return normalized


def apply_twilio_delivery_callback(
    message: Message,
    *,
    payload: Mapping[str, Any],
    now: datetime | None = None,
) -> DeliveryCallbackResult:
    raw_payload = dict(message.raw_payload or {})
    previous = raw_payload.get(DELIVERY_KEY) if isinstance(raw_payload.get(DELIVERY_KEY), dict) else {}
    provider_status = str(payload.get("MessageStatus") or payload.get("SmsStatus") or "").strip().lower()
    previous_status = str(previous.get("provider_status") or previous.get("status") or "").strip().lower()
    current_delivery = _normalize_delivery_record(previous) if previous else (delivery_status_for_message(message) or {})

    if provider_status not in _ALLOWED_CALLBACK_STATUSES:
        return DeliveryCallbackResult(delivery=current_delivery, applied=False, reason="unsupported_status")
    if provider_status == previous_status:
        enriched = _enrich_warning_error_detail(
            current_delivery,
            payload=payload,
            now=now,
        )
        if enriched is not None:
            raw_payload[DELIVERY_KEY] = enriched
            message.raw_payload = raw_payload
            return DeliveryCallbackResult(
                delivery=enriched,
                applied=True,
                reason="error_detail_enriched",
            )
        return DeliveryCallbackResult(delivery=current_delivery, applied=False, reason="duplicate")
    if previous_status in _TERMINAL_STATUSES:
        return DeliveryCallbackResult(delivery=current_delivery, applied=False, reason="terminal_status")
    if _STATUS_RANK.get(provider_status, -1) < _STATUS_RANK.get(previous_status, 0):
        return DeliveryCallbackResult(delivery=current_delivery, applied=False, reason="status_regression")

    error_code = _bounded_text(payload.get("ErrorCode"), limit=64)
    error_message = _bounded_text(payload.get("ErrorMessage"), limit=500)
    normalized = _status_record(
        provider_status=provider_status,
        provider_message_sid=message.provider_message_sid,
        error_code=error_code,
        error_message=error_message,
        callback_configured=True,
        callback_received=True,
        now=now,
    )
    if previous:
        history = list(previous.get("history") or [])
        history.append(
            {
                "provider_status": previous.get("provider_status") or previous.get("status") or "",
                "status": previous.get("status") or "",
                "updated_at": previous.get("updated_at") or "",
            }
        )
        normalized["history"] = history[-8:]
    normalized["raw_callback"] = _bounded_callback_payload(payload)
    raw_payload[DELIVERY_KEY] = normalized
    message.raw_payload = raw_payload
    return DeliveryCallbackResult(delivery=normalized, applied=True, reason="applied")


def _enrich_warning_error_detail(
    current: Mapping[str, Any],
    *,
    payload: Mapping[str, Any],
    now: datetime | None,
) -> dict[str, Any] | None:
    """Fill provider error fields that were absent from an earlier callback.

    Twilio can repeat the same terminal status with more complete error fields.
    Existing non-empty values remain authoritative so replays cannot rewrite a
    recorded failure, while a byte-for-byte duplicate remains a no-op.
    """

    status = str(current.get("provider_status") or current.get("status") or "").strip().lower()
    if status not in _WARNING_STATUSES:
        return None

    current_code = _bounded_text(current.get("error_code"), limit=64)
    current_message = _bounded_text(current.get("error_message"), limit=500)
    incoming_code = _bounded_text(payload.get("ErrorCode"), limit=64)
    incoming_message = _bounded_text(payload.get("ErrorMessage"), limit=500)
    error_code = current_code or incoming_code
    error_message = current_message or incoming_message
    if error_code == current_code and error_message == current_message:
        return None

    enriched = dict(current)
    enriched.update(
        {
            "error_code": error_code,
            "error_message": error_message,
            "description": _description_for_status(
                status,
                error_code=error_code,
                error_message=error_message,
            ),
            "description_fr": _description_for_status_fr(
                status,
                error_code=error_code,
                error_message=error_message,
            ),
            "updated_at": _iso(now),
        }
    )
    previous_callback = current.get("raw_callback")
    enriched["raw_callback"] = {
        **(dict(previous_callback) if isinstance(previous_callback, Mapping) else {}),
        **_bounded_callback_payload(payload),
    }
    return enriched


def _bounded_callback_payload(payload: Mapping[str, Any]) -> dict[str, str]:
    return {
        key: _bounded_text(payload.get(key), limit=500)
        for key in (
            "AccountSid",
            "MessageSid",
            "SmsSid",
            "MessageStatus",
            "SmsStatus",
            "ErrorCode",
            "ErrorMessage",
            "To",
            "From",
        )
        if key in payload
    }


def _normalize_delivery_record(raw: Mapping[str, Any]) -> dict[str, Any]:
    provider_status = str(raw.get("provider_status") or raw.get("status") or "").strip().lower()
    if provider_status and provider_status not in {"sent_to_provider", "simulated", "unverified"}:
        refreshed = _status_record(
            provider_status=provider_status,
            provider_message_sid=str(raw.get("provider_message_sid") or ""),
            error_code=str(raw.get("error_code") or ""),
            error_message=str(raw.get("error_message") or ""),
            callback_configured=bool(raw.get("callback_configured")),
            callback_received=bool(raw.get("callback_received")),
            now=None,
        )
        refreshed.update({key: value for key, value in raw.items() if key not in refreshed or value})
        return refreshed
    normalized = dict(raw)
    normalized.setdefault("severity", "info")
    normalized.setdefault("label", _label_for_status(str(normalized.get("status") or provider_status or "unverified")))
    normalized.setdefault("label_fr", _label_for_status_fr(str(normalized.get("status") or provider_status or "unverified")))
    normalized.setdefault("description", "")
    normalized.setdefault("description_fr", "")
    return normalized


def _status_record(
    *,
    provider_status: str,
    provider_message_sid: str,
    error_code: str = "",
    error_message: str = "",
    callback_configured: bool = False,
    callback_received: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    status = provider_status or "unknown"
    severity = "warning" if status in _WARNING_STATUSES else "ok" if status in _OK_STATUSES else "info"
    return {
        "channel": "sms",
        "provider": "twilio",
        "provider_message_sid": provider_message_sid,
        "provider_status": provider_status or "unknown",
        "status": status,
        "severity": severity,
        "label": _label_for_status(status),
        "label_fr": _label_for_status_fr(status),
        "description": _description_for_status(status, error_code=error_code, error_message=error_message),
        "description_fr": _description_for_status_fr(status, error_code=error_code, error_message=error_message),
        "error_code": error_code,
        "error_message": error_message,
        "callback_configured": callback_configured,
        "callback_received": callback_received,
        "updated_at": _iso(now),
    }


def _label_for_status(status: str) -> str:
    if status in _OK_STATUSES:
        return "SMS delivered"
    if status in _WARNING_STATUSES:
        return "SMS not delivered"
    if status in _PENDING_STATUSES:
        return "SMS queued"
    if status in _SENT_STATUSES:
        return "Sent to carrier"
    if status == "sent_to_provider":
        return "Sent - awaiting delivery"
    if status == "unverified":
        return "Sent - delivery unverified"
    if status == "simulated":
        return "Simulated SMS"
    return "SMS status unknown"


def _label_for_status_fr(status: str) -> str:
    if status in _OK_STATUSES:
        return "SMS livré"
    if status in _WARNING_STATUSES:
        return "SMS non livré"
    if status in _PENDING_STATUSES:
        return "SMS en file d'attente"
    if status in _SENT_STATUSES:
        return "Envoyé à l'opérateur"
    if status == "sent_to_provider":
        return "Envoyé - livraison en attente"
    if status == "unverified":
        return "Envoyé - livraison non vérifiée"
    if status == "simulated":
        return "SMS simulé"
    return "Statut SMS inconnu"


def _description_for_status(status: str, *, error_code: str = "", error_message: str = "") -> str:
    if status in _OK_STATUSES:
        return "Twilio confirmed the message reached the destination handset or carrier endpoint."
    if status == "undelivered":
        detail = error_message or "The carrier reported that the SMS could not be delivered."
        return f"{detail} This can happen with landlines, business numbers, blocked numbers, or unreachable phones."
    if status == "failed":
        detail = error_message or "Twilio failed before the SMS could be delivered."
        suffix = f" Error code: {error_code}." if error_code else ""
        return f"{detail}{suffix}"
    if status == "canceled":
        return "Twilio canceled the SMS before delivery."
    if status in _PENDING_STATUSES:
        return "The SMS is still moving through Twilio or the carrier network."
    if status in _SENT_STATUSES:
        return "Twilio sent the SMS to the carrier. Final delivery has not been confirmed yet."
    return "No confirmed delivery result is available yet."


def _description_for_status_fr(status: str, *, error_code: str = "", error_message: str = "") -> str:
    if status in _OK_STATUSES:
        return "Twilio a confirmé que le message a atteint le téléphone ou le point de terminaison de l'opérateur."
    if status == "undelivered":
        detail = _translate_twilio_error(error_message) or "L'opérateur indique que le SMS n'a pas pu être livré."
        return f"{detail} Cela peut arriver avec un téléphone fixe, un numéro professionnel, un numéro bloqué ou un téléphone injoignable."
    if status == "failed":
        detail = _translate_twilio_error(error_message) or "Twilio a échoué avant que le SMS puisse être livré."
        suffix = f" Code d'erreur: {error_code}." if error_code else ""
        return f"{detail}{suffix}"
    if status == "canceled":
        return "Twilio a annulé le SMS avant sa livraison."
    if status in _PENDING_STATUSES:
        return "Le SMS circule encore dans le réseau Twilio ou chez l'opérateur."
    if status in _SENT_STATUSES:
        return "Twilio a envoyé le SMS à l'opérateur. La livraison finale n'est pas encore confirmée."
    return "Aucun résultat de livraison confirmé n'est disponible pour le moment."


def _translate_twilio_error(error_message: str) -> str:
    text = str(error_message or "").strip()
    if not text:
        return ""
    known = {
        "unknown destination handset": "Destination inconnue ou téléphone injoignable.",
        "landline or unreachable carrier": "Le numéro semble être un téléphone fixe ou un numéro injoignable.",
    }
    return known.get(text.lower(), text)


def _bounded_text(value: Any, *, limit: int) -> str:
    return str(value or "").strip()[: max(int(limit), 0)]


def _iso(value: datetime | None) -> str:
    return (value or datetime.now(timezone.utc)).isoformat()
