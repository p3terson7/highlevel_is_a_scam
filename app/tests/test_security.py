import pytest
from sqlalchemy import select
from starlette.requests import Request
from twilio.request_validator import RequestValidator

from app.api.routes_sms import _twilio_request_is_valid
from app.core.config import Settings, validate_security_settings
from app.core.request_limits import RequestBodyLimitMiddleware
from app.core.security import (
    verify_admin_token,
    verify_twilio_signature,
    verify_twilio_tenant_binding,
)
from app.db.models import Client
from app.db.session import get_session_factory
from app.services.portal_auth import (
    hash_portal_password,
    issue_portal_token,
    verify_portal_token,
)
from app.services.ui_session_auth import ui_session_cookies_secure


def test_verify_twilio_signature_uses_configured_public_url_and_ignores_untrusted_forwarded_headers(monkeypatch):
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
                (b"x-forwarded-proto", b"http"),
                (b"x-forwarded-host", b"attacker.invalid"),
                (b"x-twilio-signature", b"sig-123"),
            ],
        }
    )

    result = verify_twilio_signature(
        request,
        {"From": "+15552223333"},
        "secret-token",
        public_base_url="https://demo.ngrok-free.app",
    )

    assert result is True
    assert captured["auth_token"] == "secret-token"
    assert captured["signature"] == "sig-123"
    assert captured["form_data"] == {"From": "+15552223333"}
    assert captured["url"] == "https://demo.ngrok-free.app/sms/inbound/test-client-key"


def test_verify_twilio_signature_preserves_official_validator_behavior_behind_proxy():
    auth_token = "twilio-auth-token"
    public_url = "https://crm.example/sms/inbound/test-client-key"
    payload = {
        "AccountSid": "AC123",
        "From": "+15552223333",
        "To": "+15550001111",
        "Body": "hello",
        "MessageSid": "SM123",
    }
    signature = RequestValidator(auth_token).compute_signature(public_url, payload)
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "scheme": "http",
            "path": "/sms/inbound/test-client-key",
            "query_string": b"",
            "headers": [
                (b"host", b"internal-api:8000"),
                (b"x-forwarded-proto", b"http"),
                (b"x-forwarded-host", b"attacker.invalid"),
                (b"x-twilio-signature", signature.encode()),
            ],
        }
    )

    assert verify_twilio_signature(
        request,
        payload,
        auth_token,
        public_base_url="https://crm.example",
    ) is True
    assert verify_twilio_signature(
        request,
        payload,
        auth_token,
        public_base_url="https://other.example",
    ) is False


def test_twilio_tenant_binding_supports_shared_accounts_but_enforces_each_number():
    first_tenant_payload = {"AccountSid": "AC-SHARED", "To": "+1 (555) 000-1111"}
    second_tenant_payload = {"AccountSid": "AC-SHARED", "To": "+1 (555) 000-2222"}

    assert verify_twilio_tenant_binding(
        first_tenant_payload,
        expected_account_sid="AC-SHARED",
        expected_number="+15550001111",
        number_field="To",
        require_account=True,
    ) is True
    assert verify_twilio_tenant_binding(
        second_tenant_payload,
        expected_account_sid="AC-SHARED",
        expected_number="+15550002222",
        number_field="To",
        require_account=True,
    ) is True
    assert verify_twilio_tenant_binding(
        first_tenant_payload,
        expected_account_sid="AC-SHARED",
        expected_number="+15550002222",
        number_field="To",
        require_account=True,
    ) is False
    assert verify_twilio_tenant_binding(
        {"AccountSid": "AC-WRONG", "To": "+15550001111"},
        expected_account_sid="AC-SHARED",
        expected_number="+15550001111",
        number_field="To",
        require_account=True,
    ) is False


def test_signed_inbound_request_must_match_effective_tenant_account_and_number():
    auth_token = "tenant-auth-token"
    public_url = "https://crm.example/sms/inbound/client-a"
    settings = Settings(env="production")

    def signed_request(payload):
        signature = RequestValidator(auth_token).compute_signature(public_url, payload)
        return Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/sms/inbound/client-a",
                "query_string": b"",
                "headers": [
                    (b"host", b"internal-api"),
                    (b"x-twilio-signature", signature.encode()),
                ],
            }
        )

    valid_payload = {
        "AccountSid": "AC-TENANT",
        "To": "+15550001111",
        "From": "+15550002222",
        "MessageSid": "SM-INBOUND",
    }
    assert _twilio_request_is_valid(
        request=signed_request(valid_payload),
        payload=valid_payload,
        auth_token=auth_token,
        settings=settings,
        public_base_url="https://crm.example",
        expected_account_sid="AC-TENANT",
        expected_number="+15550001111",
    ) is True

    wrong_number_payload = {**valid_payload, "To": "+15550009999"}
    assert _twilio_request_is_valid(
        request=signed_request(wrong_number_payload),
        payload=wrong_number_payload,
        auth_token=auth_token,
        settings=settings,
        public_base_url="https://crm.example",
        expected_account_sid="AC-TENANT",
        expected_number="+15550001111",
    ) is False

    missing_account_payload = {key: value for key, value in valid_payload.items() if key != "AccountSid"}
    assert _twilio_request_is_valid(
        request=signed_request(missing_account_payload),
        payload=missing_account_payload,
        auth_token=auth_token,
        settings=settings,
        public_base_url="https://crm.example",
        expected_account_sid="AC-TENANT",
        expected_number="+15550001111",
    ) is False


@pytest.mark.parametrize(
    "public_base_url",
    [
        "https://user:secret@crm.example",
        "javascript://crm.example",
        "https://crm.example?host=other.example",
        "https://crm.example\nX-Forwarded-Host: attacker.invalid",
    ],
)
def test_twilio_signature_rejects_invalid_canonical_public_base(public_base_url):
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/sms/status-callback",
            "query_string": b"",
            "headers": [(b"host", b"crm.example"), (b"x-twilio-signature", b"anything")],
        }
    )

    assert verify_twilio_signature(
        request,
        {},
        "secret-token",
        public_base_url=public_base_url,
    ) is False


def test_twilio_signature_fails_closed_without_token_outside_local_development():
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/sms/inbound/test-client-key",
            "query_string": b"",
            "headers": [(b"host", b"crm.example")],
        }
    )

    assert verify_twilio_signature(request, {}, "") is False
    assert _twilio_request_is_valid(
        request=request,
        payload={},
        auth_token="",
        settings=Settings(env="production"),
    ) is False
    assert _twilio_request_is_valid(
        request=request,
        payload={},
        auth_token="",
        settings=Settings(env="dev"),
    ) is False
    assert _twilio_request_is_valid(
        request=request,
        payload={},
        auth_token="",
        settings=Settings(env="dev", allow_unsigned_twilio_webhooks=True),
    ) is True


@pytest.mark.parametrize("admin_token", ["", "change-me", "changeme", "admin", "password", "a" * 31])
def test_insecure_admin_tokens_fail_startup_validation(admin_token):
    with pytest.raises(RuntimeError, match="ADMIN_TOKEN"):
        validate_security_settings(Settings(admin_token=admin_token))


def test_long_admin_token_passes_startup_validation():
    validate_security_settings(Settings(admin_token="a-secure-random-admin-token-over-32-chars"))


def test_unsigned_crm_webhook_opt_in_is_rejected_outside_local_environments():
    with pytest.raises(RuntimeError, match="ALLOW_UNSIGNED_CRM_WEBHOOKS"):
        validate_security_settings(
            Settings(
                env="production",
                admin_token="a-secure-random-admin-token-over-32-chars",
                allow_unsigned_crm_webhooks=True,
            )
        )


@pytest.mark.parametrize("environment", ["dev", "development", "local", "test", "testing"])
def test_ui_cookies_default_to_insecure_only_in_local_environments(environment):
    assert ui_session_cookies_secure(Settings(env=environment)) is False


@pytest.mark.parametrize("environment", ["staging", "preview", "prod", "production"])
def test_ui_cookies_default_to_secure_outside_local_environments(environment):
    assert ui_session_cookies_secure(Settings(env=environment)) is True


def test_ui_cookie_security_can_be_overridden_but_not_disabled_in_production():
    assert ui_session_cookies_secure(Settings(env="dev", ui_secure_cookies=True)) is True
    assert ui_session_cookies_secure(Settings(env="staging", ui_secure_cookies=False)) is False
    with pytest.raises(RuntimeError, match="UI_SECURE_COOKIES"):
        validate_security_settings(
            Settings(
                env="production",
                admin_token="a-secure-random-admin-token-over-32-chars",
                ui_secure_cookies=False,
            )
        )


def test_admin_token_verification_rejects_missing_values():
    assert verify_admin_token("secure-token", "secure-token") is True
    assert verify_admin_token("", "") is False
    assert verify_admin_token(None, "") is False
    assert verify_admin_token("wrong", "secure-token") is False


@pytest.mark.parametrize(
    "token",
    [
        "not-a-token",
        "payload.a",
        "payload.%%%%",
        "payload.signature.extra",
        "x" * 4097,
    ],
)
def test_malformed_portal_tokens_are_rejected_without_raising(token):
    settings = Settings(admin_token="portal-test-secret")

    assert verify_portal_token(settings, token) is None


def test_valid_portal_token_still_round_trips():
    settings = Settings(admin_token="portal-test-secret")
    token = issue_portal_token(
        settings=settings,
        client_id=42,
        client_key="client-42",
        email="Owner@Example.com",
    )

    payload = verify_portal_token(settings, token)

    assert payload is not None
    assert payload.client_id == 42
    assert payload.client_key == "client-42"
    assert payload.email == "owner@example.com"
    assert verify_portal_token(settings, f"{token}.") is None


def _configure_portal_client(test_context, *, email: str, password: str) -> None:
    session_factory = get_session_factory()
    with session_factory() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.portal_email = email
        client.portal_password_hash = hash_portal_password(password)
        client.portal_enabled = True
        db.commit()


def test_browser_client_login_is_cookie_only(test_context):
    _configure_portal_client(
        test_context,
        email="browser@example.com",
        password="browser-password",
    )

    login = test_context.client.post(
        "/ui/api/login/client",
        json={"email": "browser@example.com", "password": "browser-password"},
    )

    assert login.status_code == 200
    assert "token" not in login.json()
    assert login.json()["session"]["role"] == "client"
    set_cookie = login.headers.get("set-cookie", "")
    assert "leadops_session=" in set_cookie
    assert "HttpOnly" in set_cookie
    assert "leadops_csrf=" in set_cookie
    session = test_context.client.get("/ui/api/session")
    assert session.status_code == 200
    assert session.json()["role"] == "client"


def test_enabled_portal_email_is_unique_across_tenants(test_context):
    headers = {"X-Admin-Token": "test-admin-token-32-characters-long!"}
    first = test_context.client.post(
        "/admin/clients",
        headers=headers,
        json={
            "client_key": "portal-email-first",
            "business_name": "Portal Email First",
            "portal_email": "owner@example.com",
            "portal_password": "first-portal-password",
            "portal_enabled": True,
        },
    )
    assert first.status_code == 201

    conflict = test_context.client.post(
        "/admin/clients",
        headers=headers,
        json={
            "client_key": "portal-email-conflict",
            "business_name": "Portal Email Conflict",
            "portal_email": " Owner@Example.com ",
            "portal_password": "second-portal-password",
            "portal_enabled": True,
        },
    )
    assert conflict.status_code == 409

    disabled = test_context.client.post(
        "/admin/clients",
        headers=headers,
        json={
            "client_key": "portal-email-disabled",
            "business_name": "Portal Email Disabled",
            "portal_email": "owner@example.com",
            "portal_password": "disabled-portal-password",
            "portal_enabled": False,
        },
    )
    assert disabled.status_code == 201

    enable_conflict = test_context.client.patch(
        "/admin/clients/portal-email-disabled",
        headers=headers,
        json={"portal_enabled": True},
    )
    assert enable_conflict.status_code == 409


def test_legacy_portal_token_login_is_explicitly_gated(test_context):
    _configure_portal_client(
        test_context,
        email="legacy@example.com",
        password="legacy-password",
    )
    from app.core.deps import get_app_settings
    from app.main import app

    disabled_settings = get_app_settings().model_copy(
        update={"enable_legacy_portal_token_login": False}
    )
    app.dependency_overrides[get_app_settings] = lambda: disabled_settings
    try:
        disabled = test_context.client.post(
            "/ui/api/login/client/token",
            json={"email": "legacy@example.com", "password": "legacy-password"},
        )
    finally:
        app.dependency_overrides.pop(get_app_settings, None)

    assert disabled.status_code == 404

    enabled = test_context.client.post(
        "/ui/api/login/client/token",
        json={"email": "legacy@example.com", "password": "legacy-password"},
    )
    assert enabled.status_code == 200
    assert verify_portal_token(get_app_settings(), enabled.json()["token"]) is not None
    assert "leadops_session=" not in enabled.headers.get("set-cookie", "")
    assert enabled.headers["Cache-Control"] == "no-store"


def test_portal_bearer_cannot_suppress_csrf_for_admin_cookie(test_context):
    _configure_portal_client(
        test_context,
        email="mixed-auth@example.com",
        password="mixed-auth-password",
    )
    token_login = test_context.client.post(
        "/ui/api/login/client/token",
        json={"email": "mixed-auth@example.com", "password": "mixed-auth-password"},
    )
    assert token_login.status_code == 200
    portal_token = token_login.json()["token"]

    # A bearer-only server client remains compatible and does not need browser CSRF.
    bearer_write = test_context.client.patch(
        f"/ui/api/owner/{test_context.client_key}/ai-context",
        headers={"X-Portal-Token": portal_token},
        json={"ai_context": "Bearer-only update"},
    )
    assert bearer_write.status_code == 200

    admin_login = test_context.client.post(
        "/ui/api/login/admin",
        json={"admin_token": "test-admin-token-32-characters-long!"},
    )
    assert admin_login.status_code == 200

    mixed_without_csrf = test_context.client.put(
        "/admin/runtime-config",
        headers={"X-Portal-Token": portal_token},
        json={},
    )
    assert mixed_without_csrf.status_code == 403
    assert mixed_without_csrf.json()["detail"] == "Invalid CSRF token"

    csrf_token = test_context.client.cookies.get("leadops_csrf")
    mixed_with_csrf = test_context.client.put(
        "/admin/runtime-config",
        headers={
            "X-Portal-Token": portal_token,
            "X-CSRF-Token": csrf_token,
        },
        json={},
    )
    assert mixed_with_csrf.status_code == 200


def test_malformed_portal_token_returns_unauthorized(test_context):
    response = test_context.client.get(
        "/ui/api/session",
        headers={"X-Portal-Token": "payload.a"},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid portal session"


def test_portal_login_rate_limits_failed_password_hash_work(test_context, monkeypatch):
    session_factory = get_session_factory()
    with session_factory() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.portal_email = "limited@example.com"
        client.portal_password_hash = hash_portal_password("correct-password")
        client.portal_enabled = True
        db.commit()

    from app.api.ui import session_routes

    verify_calls = 0
    original_verify = session_routes.verify_portal_password

    def counting_verify(password: str, encoded: str) -> bool:
        nonlocal verify_calls
        verify_calls += 1
        return original_verify(password, encoded)

    monkeypatch.setattr(session_routes, "verify_portal_password", counting_verify)

    for _ in range(5):
        response = test_context.client.post(
            "/ui/api/login/client",
            json={"email": "limited@example.com", "password": "wrong-password"},
        )
        assert response.status_code == 401

    limited = test_context.client.post(
        "/ui/api/login/client",
        json={"email": "limited@example.com", "password": "wrong-password"},
    )
    assert limited.status_code == 429
    assert limited.headers["Retry-After"] == "900"
    assert verify_calls == 5


def test_portal_password_change_revokes_existing_session(test_context):
    session_factory = get_session_factory()
    with session_factory() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.portal_email = "rotate@example.com"
        client.portal_password_hash = hash_portal_password("first-password")
        client.portal_enabled = True
        db.commit()

    login = test_context.client.post(
        "/ui/api/login/client/token",
        json={"email": "rotate@example.com", "password": "first-password"},
    )
    assert login.status_code == 200
    old_token = login.json()["token"]
    assert test_context.client.get(
        "/ui/api/session",
        headers={"X-Portal-Token": old_token},
    ).status_code == 200

    changed = test_context.client.patch(
        f"/admin/clients/{test_context.client_key}",
        headers={"X-Admin-Token": "test-admin-token-32-characters-long!"},
        json={"portal_password": "second-password", "portal_enabled": True},
    )
    assert changed.status_code == 200
    stale = test_context.client.get(
        "/ui/api/session",
        headers={"X-Portal-Token": old_token},
    )
    assert stale.status_code == 401
    assert stale.json()["detail"] == "Portal session is stale"


def test_global_request_body_limit_rejects_oversized_json_before_validation(test_context):
    response = test_context.client.post(
        "/ui/api/login/client",
        content=b'{' + b'"padding":"' + (b"x" * (1024 * 1024)) + b'"}',
        headers={"Content-Type": "application/json"},
    )

    assert response.status_code == 413
    assert response.json()["detail"] == "Request body too large"


def test_manual_media_upload_uses_the_dedicated_larger_body_limit():
    assert RequestBodyLimitMiddleware._is_media_upload(
        "/ui/api/conversations/42/messages/manual-media"
    )
    assert not RequestBodyLimitMiddleware._is_media_upload(
        "/ui/api/conversations/42/messages/manual"
    )


def test_admin_browser_session_uses_httponly_cookie_and_requires_csrf(test_context):
    login = test_context.client.post(
        "/ui/api/login/admin",
        json={"admin_token": "test-admin-token-32-characters-long!"},
    )

    assert login.status_code == 200
    assert "token" not in login.json()
    set_cookie = login.headers.get("set-cookie", "")
    assert "leadops_session=" in set_cookie
    assert "HttpOnly" in set_cookie
    assert "leadops_csrf=" in set_cookie
    assert "SameSite=strict" in set_cookie

    session = test_context.client.get("/ui/api/session")
    assert session.status_code == 200
    assert session.json()["role"] == "admin"

    missing_csrf = test_context.client.post("/ui/api/logout")
    assert missing_csrf.status_code == 403
    assert missing_csrf.json()["detail"] == "Invalid CSRF token"

    invalid_header_does_not_bypass_csrf = test_context.client.post(
        "/ui/api/logout",
        headers={"X-Admin-Token": "invalid"},
    )
    assert invalid_header_does_not_bypass_csrf.status_code == 403

    csrf_token = test_context.client.cookies.get("leadops_csrf")
    logout = test_context.client.post(
        "/ui/api/logout",
        headers={"X-CSRF-Token": csrf_token},
    )
    assert logout.status_code == 200
    assert test_context.client.get("/ui/api/session").status_code == 401
