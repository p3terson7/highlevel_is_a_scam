from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.models import Client, RuntimeSetting

RUNTIME_KEYS = {
    "twilio_account_sid",
    "twilio_auth_token",
    "twilio_from_number",
    "public_base_url",
    "openai_api_key",
    "openai_model",
    "ai_provider_mode",
    "meta_verify_token",
    "meta_access_token",
    "meta_graph_api_version",
    "linkedin_verify_token",
}

SECRET_KEYS = {
    "twilio_account_sid",
    "twilio_auth_token",
    "openai_api_key",
    "meta_access_token",
}


def load_runtime_overrides(db: Session) -> dict[str, str]:
    rows = db.scalars(select(RuntimeSetting)).all()
    output: dict[str, str] = {}
    for row in rows:
        if row.key in RUNTIME_KEYS:
            output[row.key] = row.value
    return output


def get_effective_runtime_value(
    settings: Settings,
    overrides: Mapping[str, str] | None,
    key: str,
) -> str:
    if overrides and key in overrides and overrides[key] != "":
        return overrides[key]
    return str(getattr(settings, key))


def get_effective_runtime_map(
    settings: Settings,
    overrides: Mapping[str, str] | None,
) -> dict[str, str]:
    return {key: get_effective_runtime_value(settings, overrides, key) for key in RUNTIME_KEYS}


def upsert_runtime_values(db: Session, values: Mapping[str, str]) -> None:
    for key, value in values.items():
        if key not in RUNTIME_KEYS:
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
    for key in RUNTIME_KEYS:
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
