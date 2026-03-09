from starlette.requests import Request

from app.core.security import verify_twilio_signature


def test_verify_twilio_signature_uses_forwarded_public_url(monkeypatch):
    captured: dict[str, object] = {}

    class FakeValidator:
        def __init__(self, auth_token: str) -> None:
            captured["auth_token"] = auth_token

        def validate(self, url: str, form_data: dict[str, str], signature: str) -> bool:
            captured["url"] = url
            captured["form_data"] = form_data
            captured["signature"] = signature
            return True

    monkeypatch.setattr("app.core.security.RequestValidator", FakeValidator)

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/sms/inbound/test-client-key",
            "query_string": b"",
            "headers": [
                (b"host", b"localhost:8000"),
                (b"x-forwarded-proto", b"https"),
                (b"x-forwarded-host", b"demo.ngrok-free.app"),
                (b"x-twilio-signature", b"sig-123"),
            ],
        }
    )

    result = verify_twilio_signature(request, {"From": "+15552223333"}, "secret-token")

    assert result is True
    assert captured["auth_token"] == "secret-token"
    assert captured["signature"] == "sig-123"
    assert captured["form_data"] == {"From": "+15552223333"}
    assert captured["url"] == "https://demo.ngrok-free.app/sms/inbound/test-client-key"
