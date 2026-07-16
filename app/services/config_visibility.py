from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping

from app.services.runtime_config import CLIENT_PROVIDER_SECRET_KEYS, RETIRED_CLIENT_PROVIDER_KEYS


_BROWSER_PROVIDER_KEYS = {
    "language",
    "public_base_url",
    "twilio_from_number",
}
_BOOKING_SECRET_KEYS = {"calendly_personal_access_token"}


def browser_safe_provider_config(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return only non-sensitive provider settings needed to populate UI forms."""
    if not isinstance(raw, Mapping):
        return {}
    return {
        key: deepcopy(raw[key])
        for key in _BROWSER_PROVIDER_KEYS
        if key in raw and key not in CLIENT_PROVIDER_SECRET_KEYS and key not in RETIRED_CLIENT_PROVIDER_KEYS
    }


def browser_safe_booking_config(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    """Expose calendar settings without returning provider credentials."""
    if not isinstance(raw, Mapping):
        return {}

    safe: dict[str, Any] = {}
    internal_calendar = raw.get("internal_calendar")
    if isinstance(internal_calendar, Mapping):
        safe["internal_calendar"] = deepcopy(dict(internal_calendar))

    event_type_uri = str(raw.get("calendly_event_type_uri") or "").strip()
    if event_type_uri:
        safe["calendly_event_type_uri"] = event_type_uri
    safe["calendly_personal_access_token_configured"] = any(
        bool(str(raw.get(key) or "").strip()) for key in _BOOKING_SECRET_KEYS
    )
    return safe


def client_provider_configured_flags(raw: Mapping[str, Any] | None) -> dict[str, bool]:
    """Describe configured write-only settings without revealing their values."""
    values = raw if isinstance(raw, Mapping) else {}
    return {
        f"{key}_configured": bool(str(values.get(key) or "").strip())
        for key in sorted(CLIENT_PROVIDER_SECRET_KEYS)
    }
