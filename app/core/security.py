from typing import Mapping

from fastapi import Request
from twilio.request_validator import RequestValidator


def verify_twilio_signature(
    request: Request,
    form_data: Mapping[str, str],
    auth_token: str,
) -> bool:
    if not auth_token:
        return True

    signature = request.headers.get("X-Twilio-Signature", "")
    validator = RequestValidator(auth_token)
    url = str(request.url)
    return bool(validator.validate(url, dict(form_data), signature))


def verify_meta_challenge(
    mode: str | None,
    verify_token: str | None,
    challenge: str | None,
    expected_verify_token: str,
) -> str | None:
    if mode == "subscribe" and verify_token == expected_verify_token and challenge:
        return challenge
    return None


def verify_meta_signature(_: Request, __: str) -> bool:
    """
    Placeholder for Meta signature verification.
    Production integration should verify X-Hub-Signature-256.
    """
    return True


def verify_linkedin_signature(_: Request, __: str) -> bool:
    """
    Placeholder for LinkedIn signature verification.
    Production integration should validate LinkedIn-specific signing headers.
    """
    return True
