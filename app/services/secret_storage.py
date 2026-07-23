from __future__ import annotations

import base64
import hashlib
from collections.abc import Mapping
from typing import Any

from cryptography.fernet import Fernet, InvalidToken, MultiFernet

from app.core.config import Settings, get_settings


_PREFIX = "fernet:v1:"


class SecretStorageError(RuntimeError):
    pass


def is_protected_secret(value: Any) -> bool:
    return str(value or "").strip().startswith(_PREFIX)


def validate_secret_storage_settings(settings: Settings) -> None:
    configured = str(settings.settings_encryption_keys or "").strip()
    if settings.env.strip().lower() in {"prod", "production"} and not configured:
        raise SecretStorageError(
            "SETTINGS_ENCRYPTION_KEYS is required in production so ADMIN_TOKEN can rotate independently"
        )
    _cipher(settings)


def protect_secret(value: Any, *, settings: Settings | None = None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    cipher = _cipher(settings or get_settings())
    if cipher is None:
        # Local unit tests and one-off scripts may run without any application
        # secret. Production startup already requires ADMIN_TOKEN.
        return text
    if text.startswith(_PREFIX):
        text = reveal_secret(text, settings=settings)
    token = cipher.encrypt(text.encode("utf-8")).decode("ascii")
    return f"{_PREFIX}{token}"


def reveal_secret(value: Any, *, settings: Settings | None = None) -> str:
    text = str(value or "").strip()
    if not text or not text.startswith(_PREFIX):
        # Backward-compatible read path for rows written before encryption was
        # introduced. The next settings update rewrites them encrypted.
        return text
    cipher = _cipher(settings or get_settings())
    if cipher is None:
        raise SecretStorageError("Encrypted settings cannot be read without an application encryption key")
    try:
        return cipher.decrypt(text[len(_PREFIX) :].encode("ascii")).decode("utf-8")
    except (InvalidToken, UnicodeError, ValueError) as exc:
        raise SecretStorageError(
            "Stored secret could not be decrypted; verify SETTINGS_ENCRYPTION_KEYS before rotating keys"
        ) from exc


def rotate_protected_secret(value: Any, *, settings: Settings | None = None) -> str:
    """Re-encrypt a legacy/secondary-key token with the configured primary key."""

    text = str(value or "").strip()
    if not text or not text.startswith(_PREFIX):
        return protect_secret(text, settings=settings)
    resolved_settings = settings or get_settings()
    fernets = _fernets(resolved_settings)
    if not fernets:
        raise SecretStorageError("Encrypted settings cannot be rotated without an encryption key")
    try:
        token = text[len(_PREFIX) :].encode("ascii")
        fernets[0].decrypt(token)
        return text
    except (InvalidToken, UnicodeError, ValueError):
        plaintext = reveal_secret(text, settings=resolved_settings)
        return f"{_PREFIX}{fernets[0].encrypt(plaintext.encode('utf-8')).decode('ascii')}"


def protect_mapping(
    raw: Mapping[str, Any] | None,
    *,
    secret_keys: set[str],
    settings: Settings | None = None,
) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        return {}
    resolved_settings = settings or get_settings()
    return {
        str(key): protect_secret(value, settings=resolved_settings) if key in secret_keys else value
        for key, value in raw.items()
    }


def reveal_mapping(
    raw: Mapping[str, Any] | None,
    *,
    secret_keys: set[str],
    settings: Settings | None = None,
) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        return {}
    resolved_settings = settings or get_settings()
    return {
        str(key): reveal_secret(value, settings=resolved_settings) if key in secret_keys else value
        for key, value in raw.items()
    }


def _cipher(settings: Settings) -> MultiFernet | None:
    fernets = _fernets(settings)
    return MultiFernet(fernets) if fernets else None


def _fernets(settings: Settings) -> list[Fernet]:
    configured = str(getattr(settings, "settings_encryption_keys", "") or "").strip()
    keys = [item.strip() for item in configured.split(",") if item.strip()]
    admin_token = str(settings.admin_token or "").strip()
    if admin_token:
        # Keep the original ADMIN_TOKEN-derived key as a read-only transition
        # key. Startup rotates those tokens to the explicit primary key, after
        # which ADMIN_TOKEN can change independently.
        digest = hashlib.sha256(f"leadops-settings-v1:{admin_token}".encode("utf-8")).digest()
        legacy_key = base64.urlsafe_b64encode(digest).decode("ascii")
        if legacy_key not in keys:
            keys.append(legacy_key)
    if not keys:
        return []
    try:
        return [Fernet(key.encode("ascii")) for key in keys]
    except (ValueError, TypeError, UnicodeError) as exc:
        raise SecretStorageError(
            "SETTINGS_ENCRYPTION_KEYS must contain comma-separated Fernet keys"
        ) from exc
