from __future__ import annotations

import base64
import binascii
from contextvars import ContextVar, Token
from dataclasses import dataclass
import hashlib
import hmac
import json
import secrets
import time

from app.core.config import Settings


UI_SESSION_COOKIE = "leadops_session"
UI_CSRF_COOKIE = "leadops_csrf"
UI_CSRF_HEADER = "X-CSRF-Token"
_ADMIN_TTL_SECONDS = 8 * 60 * 60
_CLIENT_TTL_SECONDS = 7 * 24 * 60 * 60
_MAX_TOKEN_LENGTH = 4096
_LOCAL_ENVIRONMENTS = {"dev", "development", "local", "test", "testing"}
_current_session_token: ContextVar[str] = ContextVar("ui_session_token", default="")


@dataclass(frozen=True)
class UISessionPayload:
    role: str
    exp: int
    csrf_hash: str
    client_id: int = 0
    client_key: str = ""
    email: str = ""
    auth_version: str = ""


def new_csrf_token() -> str:
    return secrets.token_urlsafe(32)


def ui_session_cookies_secure(settings: Settings) -> bool:
    """Use secure cookies by default everywhere except explicit local/test environments."""

    if settings.ui_secure_cookies is not None:
        return settings.ui_secure_cookies
    return settings.env.strip().lower() not in _LOCAL_ENVIRONMENTS


def issue_ui_session_token(
    *,
    settings: Settings,
    role: str,
    csrf_token: str,
    client_id: int = 0,
    client_key: str = "",
    email: str = "",
    auth_version: str = "",
) -> str:
    normalized_role = str(role or "").strip().lower()
    if normalized_role not in {"admin", "client"}:
        raise ValueError("Unsupported UI session role")
    if not csrf_token:
        raise ValueError("CSRF token is required")
    if normalized_role == "client" and (
        not client_id or not str(client_key).strip() or not str(email).strip()
    ):
        raise ValueError("Client UI sessions require client identity")
    ttl = _ADMIN_TTL_SECONDS if normalized_role == "admin" else _CLIENT_TTL_SECONDS
    payload = {
        "role": normalized_role,
        "exp": int(time.time()) + ttl,
        "csrf_hash": _csrf_hash(csrf_token),
        "nonce": secrets.token_hex(12),
        "client_id": int(client_id or 0),
        "client_key": str(client_key or "").strip(),
        "email": str(email or "").strip().lower(),
        "auth_version": str(auth_version or "").strip(),
    }
    encoded = _b64encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signature = hmac.new(_signing_key(settings), encoded.encode("ascii"), hashlib.sha256).digest()
    return f"{encoded}.{_b64encode(signature)}"


def verify_ui_session_token(settings: Settings, token: str) -> UISessionPayload | None:
    if not isinstance(token, str) or not token or len(token) > _MAX_TOKEN_LENGTH:
        return None
    try:
        encoded, supplied_raw = token.split(".", maxsplit=1)
        supplied = _b64decode(supplied_raw)
    except (ValueError, TypeError, binascii.Error):
        return None
    expected = hmac.new(_signing_key(settings), encoded.encode("ascii"), hashlib.sha256).digest()
    if not hmac.compare_digest(expected, supplied):
        return None
    try:
        payload = json.loads(_b64decode(encoded).decode("utf-8"))
        role = str(payload.get("role") or "").strip().lower()
        exp = int(payload.get("exp") or 0)
        client_id = int(payload.get("client_id") or 0)
    except (ValueError, TypeError, OverflowError, binascii.Error, UnicodeError, json.JSONDecodeError):
        return None
    csrf_hash = str(payload.get("csrf_hash") or "").strip()
    if role not in {"admin", "client"} or exp <= int(time.time()) or len(csrf_hash) != 64:
        return None
    client_key = str(payload.get("client_key") or "").strip()
    email = str(payload.get("email") or "").strip().lower()
    auth_version = str(payload.get("auth_version") or "").strip()
    if role == "client" and (not client_id or not client_key or not email or not auth_version):
        return None
    return UISessionPayload(
        role=role,
        exp=exp,
        csrf_hash=csrf_hash,
        client_id=client_id,
        client_key=client_key,
        email=email,
        auth_version=auth_version,
    )


def csrf_matches_session(payload: UISessionPayload, csrf_token: str) -> bool:
    token_hash = _csrf_hash(str(csrf_token or ""))
    return hmac.compare_digest(payload.csrf_hash, token_hash)


def set_current_ui_session_token(value: str) -> Token[str]:
    return _current_session_token.set(str(value or ""))


def reset_current_ui_session_token(token: Token[str]) -> None:
    _current_session_token.reset(token)


def current_ui_session_token() -> str:
    return _current_session_token.get()


def _csrf_hash(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _signing_key(settings: Settings) -> bytes:
    return hashlib.sha256(f"leadops-ui-session-v1::{settings.admin_token}".encode("utf-8")).digest()


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64decode(raw: str) -> bytes:
    padding = "=" * (-len(raw) % 4)
    return base64.b64decode(f"{raw}{padding}".encode("ascii"), altchars=b"-_", validate=True)
