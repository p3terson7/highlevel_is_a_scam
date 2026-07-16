import logging

import pytest

from app.core.config import Settings, validate_security_settings
from app.services.sms_service import (
    SMSDeliveryError,
    TwilioSMSProvider,
    build_sms_service,
    clear_sms_provider_cache,
)


def test_twilio_provider_configures_finite_timeout_and_disables_transport_retries(monkeypatch):
    captured: dict[str, object] = {}

    class FakeHTTPClient:
        def __init__(self, *, timeout, max_retries):
            captured["timeout"] = timeout
            captured["max_retries"] = max_retries

    class FakeMessages:
        def create(self, **payload):
            captured["payload"] = payload
            return type("Message", (), {"sid": "SM-TIMEOUT"})()

    class FakeTwilioClient:
        def __init__(self, account_sid, auth_token, *, http_client):
            captured["account_sid"] = account_sid
            captured["auth_token"] = auth_token
            captured["http_client"] = http_client
            self.messages = FakeMessages()

    monkeypatch.setattr("app.services.sms_service.TwilioHttpClient", FakeHTTPClient)
    monkeypatch.setattr("app.services.sms_service.TwilioClient", FakeTwilioClient)

    provider = TwilioSMSProvider(
        account_sid="AC123",
        auth_token="secret",
        from_number="+15550001111",
        timeout_seconds=7,
    )

    assert provider.send_sms("+15550002222", "hello") == "SM-TIMEOUT"
    assert captured["timeout"] == 7.0
    assert captured["max_retries"] == 0
    assert captured["payload"] == {
        "from_": "+15550001111",
        "to": "+15550002222",
        "body": "hello",
    }


def test_production_auto_mode_fails_closed_when_twilio_is_incomplete():
    clear_sms_provider_cache()
    service = build_sms_service(
        Settings(
            env="production",
            sms_provider_mode="auto",
            twilio_account_sid="",
            twilio_auth_token="",
            twilio_from_number="",
        )
    )

    assert service.provider_kind == "unavailable"
    with pytest.raises(SMSDeliveryError, match="SMS provider is not configured"):
        service.send_message("+15550001111", "private customer text")


def test_explicit_mock_mode_logs_only_redacted_metadata(caplog):
    clear_sms_provider_cache()
    phone = "+15550001111"
    body = "private customer text"
    media_url = "https://crm.example/media/private-token"

    with caplog.at_level(logging.INFO):
        service = build_sms_service(
            Settings(env="production", sms_provider_mode="mock")
        )
        provider_sid = service.send_message(phone, body, [media_url])

    assert provider_sid.startswith("MOCK-")
    assert service.provider_kind == "mock"
    assert phone not in caplog.text
    assert body not in caplog.text
    assert media_url not in caplog.text
    send_record = next(record for record in caplog.records if record.getMessage() == "sms_mock_send")
    assert send_record.body_length == len(body)
    assert send_record.media_count == 1
    assert send_record.recipient_redacted is True


def test_sms_provider_mode_is_validated_and_explicit_mock_warns(caplog):
    with pytest.raises(RuntimeError, match="SMS_PROVIDER_MODE"):
        validate_security_settings(
            Settings(
                admin_token="a-secure-random-admin-token-over-32-chars",
                sms_provider_mode="invalid",
            )
        )

    with caplog.at_level(logging.WARNING):
        validate_security_settings(
            Settings(
                env="production",
                admin_token="a-secure-random-admin-token-over-32-chars",
                sms_provider_mode="mock",
            )
        )
    assert "sms_mock_mode_explicitly_enabled" in caplog.text
    assert "will not be delivered" in caplog.records[-1].warning
