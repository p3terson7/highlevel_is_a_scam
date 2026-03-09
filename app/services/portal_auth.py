from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass

from app.core.config import Settings

_PBKDF2_ITERATIONS = 390_000
_PORTAL_SESSION_TTL_SECONDS = 60 * 60 * 24 * 7


@dataclass(frozen=True)
class PortalTokenPayload:
    client_id: int
    client_key: str
    email: str
    exp: int


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64decode(raw: str) -> bytes:
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode(f"{raw}{padding}".encode("utf-8"))


def _secret_key(settings: Settings) -> bytes:
    base = settings.admin_token or settings.app_name or "lead-ops"
    return hashlib.sha256(f"portal-auth::{base}".encode("utf-8")).digest()


def hash_portal_password(password: str) -> str:
    cleaned = password.strip()
    if len(cleaned) < 8:
        raise ValueError("Portal password must be at least 8 characters")

    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        cleaned.encode("utf-8"),
        salt.encode("utf-8"),
        _PBKDF2_ITERATIONS,
    )
    return f"pbkdf2_sha256${_PBKDF2_ITERATIONS}${salt}${_b64encode(digest)}"


def verify_portal_password(password: str, stored_hash: str) -> bool:
    try:
        scheme, iterations_raw, salt, expected_raw = stored_hash.split("$", maxsplit=3)
        iterations = int(iterations_raw)
    except ValueError:
        return False

    if scheme != "pbkdf2_sha256":
        return False

    candidate = hashlib.pbkdf2_hmac(
        "sha256",
        password.strip().encode("utf-8"),
        salt.encode("utf-8"),
        iterations,
    )
    expected = _b64decode(expected_raw)
    return hmac.compare_digest(candidate, expected)


def issue_portal_token(*, settings: Settings, client_id: int, client_key: str, email: str) -> str:
    payload = {
        "client_id": client_id,
        "client_key": client_key,
        "email": email.strip().lower(),
        "exp": int(time.time()) + _PORTAL_SESSION_TTL_SECONDS,
    }
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload_raw = _b64encode(payload_bytes)
    signature = hmac.new(_secret_key(settings), payload_raw.encode("utf-8"), hashlib.sha256).digest()
    return f"{payload_raw}.{_b64encode(signature)}"


def verify_portal_token(settings: Settings, token: str) -> PortalTokenPayload | None:
    try:
        payload_raw, signature_raw = token.split(".", maxsplit=1)
    except ValueError:
        return None

    expected_signature = hmac.new(
        _secret_key(settings),
        payload_raw.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    if not hmac.compare_digest(expected_signature, _b64decode(signature_raw)):
        return None

    try:
        payload = json.loads(_b64decode(payload_raw).decode("utf-8"))
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return None

    exp = int(payload.get("exp", 0))
    if exp <= int(time.time()):
        return None

    client_id = int(payload.get("client_id", 0))
    client_key = str(payload.get("client_key", "")).strip()
    email = str(payload.get("email", "")).strip().lower()
    if not client_id or not client_key or not email:
        return None

    return PortalTokenPayload(
        client_id=client_id,
        client_key=client_key,
        email=email,
        exp=exp,
    )
