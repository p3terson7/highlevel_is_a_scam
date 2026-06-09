from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.models import Client, RuntimeSetting

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
    "meta_verify_token",
    "meta_access_token",
    "meta_graph_api_version",
    "linkedin_verify_token",
    "zapier_webhook_secret",
}

RUNTIME_KEYS = GLOBAL_RUNTIME_KEYS | CLIENT_PROVIDER_KEYS

SECRET_KEYS = {
    "twilio_account_sid",
    "twilio_auth_token",
    "openai_api_key",
    "meta_access_token",
    "zapier_webhook_secret",
}

GLOBAL_SECRET_KEYS = SECRET_KEYS & GLOBAL_RUNTIME_KEYS
CLIENT_PROVIDER_SECRET_KEYS = SECRET_KEYS & CLIENT_PROVIDER_KEYS


def load_runtime_overrides(db: Session) -> dict[str, str]:
    rows = db.scalars(select(RuntimeSetting)).all()
    output: dict[str, str] = {}
    for row in rows:
        if row.key in GLOBAL_RUNTIME_KEYS:
            output[row.key] = row.value
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
        effective[key] = ""
    effective["meta_graph_api_version"] = str(getattr(settings, "meta_graph_api_version", "v22.0") or "v22.0")
    return effective


def upsert_runtime_values(db: Session, values: Mapping[str, str]) -> None:
    for key, value in values.items():
        if key not in GLOBAL_RUNTIME_KEYS:
            continue
        existing = db.scalar(select(RuntimeSetting).where(RuntimeSetting.key == key))
        if existing is None:
            existing = RuntimeSetting(key=key, value=value)
            db.add(existing)
        else:
            existing.value = value


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
            output[key] = text
    return output


def get_effective_runtime_map_for_client(
    *,
    settings: Settings,
    overrides: Mapping[str, str] | None,
    client: Client | None,
) -> dict[str, str]:
    effective = get_effective_runtime_map(settings=settings, overrides=overrides)
    effective.update(client_runtime_overrides(client))
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
            output[key] = text
    return output
