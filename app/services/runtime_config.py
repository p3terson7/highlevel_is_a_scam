from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.models import Client, RuntimeSetting
from app.services.secret_storage import (
    is_protected_secret,
    protect_secret,
    reveal_secret,
    rotate_protected_secret,
)

GLOBAL_RUNTIME_KEYS = {
    "openai_api_key",
    "openai_model",
    "ai_provider_mode",
}

CLIENT_PROVIDER_KEYS = {
    "twilio_account_sid",
    "twilio_auth_token",
    "twilio_from_number",
    "public_base_url",
    "language",
    "crm_webhook_secret",
    "zapier_booking_webhook_secret",
    # Deprecated inbound-only alias retained for existing clients.
    "zapier_webhook_secret",
    "zapier_booking_webhook_url",
}

# These integrations have been retired. Keep the names centralized so old
# credentials are removed the next time a client configuration is updated and
# can be purged by the accompanying data migration.
RETIRED_CLIENT_PROVIDER_KEYS = {
    "meta_verify_token",
    "meta_access_token",
    "meta_graph_api_version",
    "linkedin_verify_token",
}

RUNTIME_KEYS = GLOBAL_RUNTIME_KEYS | CLIENT_PROVIDER_KEYS

SECRET_KEYS = {
    "twilio_account_sid",
    "twilio_auth_token",
    "openai_api_key",
    "crm_webhook_secret",
    "zapier_booking_webhook_secret",
    "zapier_webhook_secret",
    "zapier_booking_webhook_url",
}

GLOBAL_SECRET_KEYS = SECRET_KEYS & GLOBAL_RUNTIME_KEYS
CLIENT_PROVIDER_SECRET_KEYS = SECRET_KEYS & CLIENT_PROVIDER_KEYS


def load_runtime_overrides(db: Session) -> dict[str, str]:
    rows = db.scalars(select(RuntimeSetting)).all()
    output: dict[str, str] = {}
    for row in rows:
        if row.key in GLOBAL_RUNTIME_KEYS:
            output[row.key] = (
                reveal_secret(row.value) if row.key in GLOBAL_SECRET_KEYS else row.value
            )
    return output


def get_effective_runtime_value(
    settings: Settings,
    overrides: Mapping[str, str] | None,
    key: str,
) -> str:
    if overrides and key in overrides and overrides[key] != "":
        return overrides[key]
    return str(getattr(settings, key, ""))


def get_effective_runtime_map(
    settings: Settings,
    overrides: Mapping[str, str] | None,
) -> dict[str, str]:
    effective = {key: get_effective_runtime_value(settings, overrides, key) for key in GLOBAL_RUNTIME_KEYS}
    for key in CLIENT_PROVIDER_KEYS:
        # Client-scoped values may override these defaults, but deployment-level
        # Twilio/public URL settings remain a supported fallback. Keys without a
        # Settings field (for example Zapier secrets) resolve to an empty value.
        effective[key] = get_effective_runtime_value(settings, overrides, key)
    return effective


def upsert_runtime_values(db: Session, values: Mapping[str, str]) -> None:
    for key, value in values.items():
        if key not in GLOBAL_RUNTIME_KEYS:
            continue
        existing = db.scalar(select(RuntimeSetting).where(RuntimeSetting.key == key))
        if existing is None:
            stored_value = protect_secret(value) if key in GLOBAL_SECRET_KEYS else value
            existing = RuntimeSetting(key=key, value=stored_value)
            db.add(existing)
        else:
            existing.value = protect_secret(value) if key in GLOBAL_SECRET_KEYS else value


def client_runtime_overrides(client: Client | None) -> dict[str, str]:
    if client is None or not isinstance(client.provider_config, dict):
        return {}

    output: dict[str, str] = {}
    for key in CLIENT_PROVIDER_KEYS:
        raw_value: Any = client.provider_config.get(key)
        if raw_value is None:
            continue
        text = str(raw_value).strip()
        if text:
            output[key] = reveal_secret(text) if key in CLIENT_PROVIDER_SECRET_KEYS else text
    return output


def get_effective_runtime_map_for_client(
    *,
    settings: Settings,
    overrides: Mapping[str, str] | None,
    client: Client | None,
) -> dict[str, str]:
    effective = get_effective_runtime_map(settings=settings, overrides=overrides)
    client_values = client_runtime_overrides(client)
    effective.update(client_values)

    # A client-scoped inbound credential must win over deployment fallbacks,
    # including the deprecated key used before inbound/outbound secrets split.
    client_inbound_secret = client_values.get("crm_webhook_secret") or client_values.get(
        "zapier_webhook_secret"
    )
    if client_inbound_secret:
        effective["crm_webhook_secret"] = client_inbound_secret
    elif not effective.get("crm_webhook_secret"):
        effective["crm_webhook_secret"] = effective.get("zapier_webhook_secret", "")
    return effective


def normalize_client_provider_config(raw: Mapping[str, Any] | None) -> dict[str, str]:
    if not isinstance(raw, Mapping):
        return {}
    output: dict[str, str] = {}
    for key in CLIENT_PROVIDER_KEYS:
        value = raw.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            output[key] = protect_secret(text) if key in CLIENT_PROVIDER_SECRET_KEYS else text
    return output


def protect_existing_persisted_secrets(db: Session) -> int:
    """Encrypt legacy plaintext settings in place after migrations have run."""

    changed = 0
    for row in db.scalars(select(RuntimeSetting)).all():
        if row.key not in GLOBAL_SECRET_KEYS or not row.value:
            continue
        if is_protected_secret(row.value):
            rotated = rotate_protected_secret(row.value)
            if rotated != row.value:
                row.value = rotated
                changed += 1
            continue
        row.value = protect_secret(row.value)
        changed += 1

    for client in db.scalars(select(Client)).all():
        provider_config = dict(client.provider_config or {})
        provider_changed = False
        for key in CLIENT_PROVIDER_SECRET_KEYS:
            value = provider_config.get(key)
            if not value:
                continue
            if is_protected_secret(value):
                rotated = rotate_protected_secret(value)
                if rotated != value:
                    provider_config[key] = rotated
                    provider_changed = True
                    changed += 1
                continue
            provider_config[key] = protect_secret(value)
            provider_changed = True
            changed += 1
        if provider_changed:
            client.provider_config = provider_config

        booking_config = dict(client.booking_config or {})
        calendly_token = booking_config.get("calendly_personal_access_token")
        if calendly_token and is_protected_secret(calendly_token):
            rotated = rotate_protected_secret(calendly_token)
            if rotated != calendly_token:
                booking_config["calendly_personal_access_token"] = rotated
                client.booking_config = booking_config
                changed += 1
        elif calendly_token:
            booking_config["calendly_personal_access_token"] = protect_secret(calendly_token)
            client.booking_config = booking_config
            changed += 1

    if changed:
        db.commit()
    return changed
