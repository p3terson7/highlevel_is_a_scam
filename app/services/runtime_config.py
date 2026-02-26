from __future__ import annotations

from collections.abc import Mapping

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.models import RuntimeSetting

RUNTIME_KEYS = {
    "twilio_account_sid",
    "twilio_auth_token",
    "twilio_from_number",
    "openai_api_key",
    "openai_model",
    "meta_verify_token",
    "linkedin_verify_token",
}

SECRET_KEYS = {
    "twilio_account_sid",
    "twilio_auth_token",
    "openai_api_key",
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
