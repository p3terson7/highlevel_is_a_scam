import re
import secrets
from typing import Mapping
from urllib.parse import urlsplit, urlunsplit

from fastapi import Request
from twilio.request_validator import RequestValidator


def verify_admin_token(provided_token: str | None, expected_token: str) -> bool:
    provided = str(provided_token or "")
    expected = str(expected_token or "")
    return bool(provided and expected and secrets.compare_digest(provided, expected))


def _canonical_public_base_url(raw_url: str) -> str:
    raw = str(raw_url or "").strip()
    if not raw or any(ord(character) < 32 for character in raw):
        return ""
    try:
        parsed = urlsplit(raw)
        _ = parsed.port
    except ValueError:
        return ""
    if (
        parsed.scheme.lower() not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        return ""
    path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme.lower(), parsed.netloc, path, "", ""))


def _public_request_url(request: Request, public_base_url: str = "") -> str:
    configured_base = _canonical_public_base_url(public_base_url)
    if public_base_url and not configured_base:
        return ""
    if configured_base:
        base = configured_base
    else:
        # Uvicorn/Starlette may already have applied proxy headers from a trusted
        # proxy. Do not consume raw X-Forwarded-* values here: accepting them from
        # any caller lets an untrusted request choose the URL being authenticated.
        base = _canonical_public_base_url(f"{request.url.scheme}://{request.url.netloc}")
        if not base:
            return ""
    query_string = request.url.query
    url = f"{base}{request.url.path}"
    return f"{url}?{query_string}" if query_string else url


def _phone_digits(raw_value: str) -> str:
    raw = str(raw_value or "").strip()
    if any(character.isalpha() for character in raw):
        return ""
    digits = re.sub(r"\D", "", raw)
    return digits if len(digits) >= 7 else ""


def verify_twilio_tenant_binding(
    form_data: Mapping[str, str],
    *,
    expected_account_sid: str,
    expected_number: str,
    number_field: str,
    require_account: bool = False,
) -> bool:
    """Bind a signed callback to the tenant account and receiving/sending number.

    Account SIDs may intentionally be shared by several tenants. The per-tenant
    number check keeps those tenants distinct without requiring account-SID
    uniqueness. Messaging-service/alphanumeric senders cannot be compared as
    phone numbers, so the signed account binding remains the enforcement point.
    """

    expected_sid = str(expected_account_sid or "").strip()
    provided_sid = str(form_data.get("AccountSid") or "").strip()
    if require_account and not expected_sid:
        return False
    if expected_sid and (not provided_sid or not secrets.compare_digest(provided_sid, expected_sid)):
        return False

    expected_digits = _phone_digits(expected_number)
    if expected_digits:
        provided_digits = _phone_digits(str(form_data.get(number_field) or ""))
        if not provided_digits or not secrets.compare_digest(provided_digits, expected_digits):
            return False
    return True


def verify_twilio_signature(
    request: Request,
    form_data: Mapping[str, str],
    auth_token: str,
    *,
    public_base_url: str = "",
) -> bool:
    if not auth_token:
        return False

    signature = request.headers.get("X-Twilio-Signature", "")
    validator = RequestValidator(auth_token)
    url = _public_request_url(request, public_base_url=public_base_url)
    if not url:
        return False
    return bool(validator.validate(url, dict(form_data), signature))
