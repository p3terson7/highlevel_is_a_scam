from sqlalchemy import select

from app.db.models import Client, RuntimeSetting
from app.db.session import get_session_factory
from app.core.config import Settings
from app.services.runtime_config import get_effective_runtime_map_for_client
from app.services.secret_storage import reveal_secret


def test_admin_runtime_and_client_update_flow(test_context):
    headers = {"X-Admin-Token": "test-admin-token-32-characters-long!"}

    update_runtime = test_context.client.put(
        "/admin/runtime-config",
        headers=headers,
        json={
            "openai_api_key": "sk-test",
            "openai_model": "gpt-4.1-mini",
            "ai_provider_mode": "heuristic",
        },
    )
    assert update_runtime.status_code == 200
    payload = update_runtime.json()
    assert payload["updated_keys"] == ["ai_provider_mode", "openai_api_key", "openai_model"]
    assert "openai_api_key" in payload["secret_keys_updated"]

    status_resp = test_context.client.get("/admin/runtime-config/status", headers=headers)
    assert status_resp.status_code == 200
    status_payload = status_resp.json()
    assert status_payload["openai_api_key_configured"] is True
    assert "openai_api_key" not in status_payload
    assert status_payload["ai_provider_mode"] == "heuristic"
    assert "twilio_account_sid" not in status_payload
    assert "meta_access_token" not in status_payload
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        stored = db.scalar(select(RuntimeSetting).where(RuntimeSetting.key == "openai_api_key"))
        assert stored is not None
        assert stored.value.startswith("fernet:v1:")
        assert reveal_secret(stored.value) == "sk-test"

    patch_client = test_context.client.patch(
        f"/admin/clients/{test_context.client_key}",
        headers=headers,
        json={
            "tone": "professional and concise",
            "booking_url": "https://example.com/new-booking",
            "ai_context": "Focus on qualified pipeline over lead volume. Never promise guaranteed results.",
            "template_overrides": {"initial_sms": "Hello from override"},
        },
    )
    assert patch_client.status_code == 200
    client_payload = patch_client.json()
    assert client_payload["tone"] == "professional and concise"
    assert client_payload["booking_url"] == "https://example.com/new-booking"
    assert "qualified pipeline" in client_payload["ai_context"]
    assert client_payload["template_overrides"]["initial_sms"] == "Hello from override"


def test_client_provider_config_merges_and_ui_runtime_uses_client_scope(test_context):
    headers = {"X-Admin-Token": "test-admin-token-32-characters-long!"}

    first_patch = test_context.client.patch(
        f"/admin/clients/{test_context.client_key}",
        headers=headers,
        json={
            "provider_config": {
                "openai_api_key": "sk-client-owned",
                "openai_model": "gpt-4.1-mini",
                "twilio_from_number": "+15554443333",
                "twilio_account_sid": "AC_CLIENT_SID",
                "twilio_auth_token": "client-secret",
                "meta_access_token": "meta-client-token",
                "linkedin_verify_token": "linkedin-client-token",
                "zapier_webhook_secret": "zapier-client-secret",
                "crm_webhook_secret": "crm-inbound-secret",
                "zapier_booking_webhook_secret": "zapier-outbound-secret",
                "zapier_booking_webhook_url": "https://hooks.zapier.com/hooks/catch/test/booking/",
            }
        },
    )
    assert first_patch.status_code == 200
    first_payload = first_patch.json()
    assert "openai_api_key" not in first_payload["provider_config"]
    assert "openai_model" not in first_payload["provider_config"]
    assert first_payload["provider_config"]["twilio_from_number"] == "+15554443333"
    assert "twilio_account_sid" not in first_payload["provider_config"]
    assert "twilio_auth_token" not in first_payload["provider_config"]
    assert "zapier_webhook_secret" not in first_payload["provider_config"]
    assert "crm_webhook_secret" not in first_payload["provider_config"]
    assert "zapier_booking_webhook_secret" not in first_payload["provider_config"]
    assert "zapier_booking_webhook_url" not in first_payload["provider_config"]

    second_patch = test_context.client.patch(
        f"/admin/clients/{test_context.client_key}",
        headers=headers,
        json={"provider_config": {"public_base_url": "https://client.example"}},
    )
    assert second_patch.status_code == 200
    second_payload = second_patch.json()
    assert "openai_api_key" not in second_payload["provider_config"]
    assert "openai_model" not in second_payload["provider_config"]
    assert second_payload["provider_config"]["twilio_from_number"] == "+15554443333"
    assert second_payload["provider_config"]["public_base_url"] == "https://client.example"
    assert "zapier_booking_webhook_url" not in second_payload["provider_config"]

    ui_client = test_context.client.get(
        f"/ui/api/clients/{test_context.client_key}",
        headers=headers,
    )
    assert ui_client.status_code == 200
    ui_payload = ui_client.json()
    assert ui_payload["provider_runtime"]["source"] == "client"
    assert ui_payload["provider_runtime"]["twilio_configured"] is True
    assert ui_payload["provider_runtime"]["twilio_from_number"] == "+15554443333"
    assert ui_payload["provider_runtime"]["twilio_account_sid_configured"] is True
    assert ui_payload["provider_runtime"]["twilio_auth_token_configured"] is True
    assert ui_payload["provider_runtime"]["zapier_webhook_secret_configured"] is True
    assert ui_payload["provider_runtime"]["crm_webhook_secret_configured"] is True
    assert ui_payload["provider_runtime"]["zapier_booking_webhook_secret_configured"] is True
    assert ui_payload["provider_runtime"]["zapier_booking_webhook_url_configured"] is True
    assert ui_payload["provider_runtime"]["openai_model"]
    assert "twilio_auth_token" not in ui_payload["client"]["provider_config"]

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        assert client.provider_config["twilio_auth_token"].startswith("fernet:v1:")
        assert reveal_secret(client.provider_config["twilio_auth_token"]) == "client-secret"
        assert client.provider_config["zapier_webhook_secret"].startswith("fernet:v1:")
        assert reveal_secret(client.provider_config["zapier_webhook_secret"]) == "zapier-client-secret"
        assert client.provider_config["crm_webhook_secret"].startswith("fernet:v1:")
        assert reveal_secret(client.provider_config["crm_webhook_secret"]) == "crm-inbound-secret"
        assert client.provider_config["zapier_booking_webhook_secret"].startswith("fernet:v1:")
        assert reveal_secret(client.provider_config["zapier_booking_webhook_secret"]) == "zapier-outbound-secret"
        assert "meta_access_token" not in client.provider_config
        assert "linkedin_verify_token" not in client.provider_config


def test_client_provider_credentials_can_be_explicitly_removed(test_context):
    headers = {"X-Admin-Token": "test-admin-token-32-characters-long!"}
    client_url = f"/admin/clients/{test_context.client_key}"
    configured = test_context.client.patch(
        client_url,
        headers=headers,
        json={
            "provider_config": {
                "twilio_account_sid": "AC_REMOVE",
                "twilio_auth_token": "remove-token",
                "twilio_from_number": "+15554440000",
                "zapier_webhook_secret": "remove-secret",
                "crm_webhook_secret": "remove-inbound-secret",
                "zapier_booking_webhook_secret": "remove-outbound-secret",
                "zapier_booking_webhook_url": "https://hooks.zapier.com/remove",
                "public_base_url": "https://keep.example",
            }
        },
    )
    assert configured.status_code == 200

    removed = test_context.client.patch(
        client_url,
        headers=headers,
        json={
            "provider_config_clear_keys": [
                "twilio_account_sid",
                "twilio_auth_token",
                "twilio_from_number",
                "zapier_webhook_secret",
                "crm_webhook_secret",
                "zapier_booking_webhook_secret",
                "zapier_booking_webhook_url",
            ]
        },
    )
    assert removed.status_code == 200
    assert removed.json()["provider_config"]["public_base_url"] == "https://keep.example"

    rejected = test_context.client.patch(
        client_url,
        headers=headers,
        json={"provider_config_clear_keys": ["openai_api_key"]},
    )
    assert rejected.status_code == 400

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        assert client.provider_config == {"public_base_url": "https://keep.example"}


def test_client_booking_patch_preserves_credentials_without_returning_them(test_context):
    headers = {"X-Admin-Token": "test-admin-token-32-characters-long!"}
    client_url = f"/admin/clients/{test_context.client_key}"

    configured = test_context.client.patch(
        client_url,
        headers=headers,
        json={
            "booking_config": {
                "calendly_personal_access_token": "calendly-secret",
                "calendly_event_type_uri": "https://api.calendly.com/event_types/demo",
                "internal_calendar": {"slot_minutes": 30, "horizon_days": 14},
            }
        },
    )
    assert configured.status_code == 200
    configured_payload = configured.json()["booking_config"]
    assert "calendly_personal_access_token" not in configured_payload
    assert configured_payload["calendly_personal_access_token_configured"] is True

    calendar_patch = test_context.client.patch(
        client_url,
        headers=headers,
        json={"booking_config": {"internal_calendar": {"slot_minutes": 45}}},
    )
    assert calendar_patch.status_code == 200
    assert calendar_patch.json()["booking_config"]["internal_calendar"]["slot_minutes"] == 45

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        assert client.booking_config["calendly_personal_access_token"].startswith("fernet:v1:")
        assert reveal_secret(client.booking_config["calendly_personal_access_token"]) == "calendly-secret"
        assert client.booking_config["calendly_event_type_uri"].endswith("/demo")
        assert client.booking_config["internal_calendar"]["horizon_days"] == 14
        assert client.booking_config["internal_calendar"]["slot_minutes"] == 45


def test_client_runtime_uses_deployment_twilio_fallback_and_partial_overrides():
    settings = Settings(
        twilio_account_sid="AC-global",
        twilio_auth_token="global-auth-token",
        twilio_from_number="+15550001111",
        public_base_url="https://global.example",
    )
    client = Client(
        client_key="partial-provider-client",
        business_name="Partial Provider Client",
        provider_config={
            "twilio_from_number": "+15550002222",
            "public_base_url": "https://client.example",
        },
    )

    effective = get_effective_runtime_map_for_client(
        settings=settings,
        overrides={},
        client=client,
    )

    assert effective["twilio_account_sid"] == "AC-global"
    assert effective["twilio_auth_token"] == "global-auth-token"
    assert effective["twilio_from_number"] == "+15550002222"
    assert effective["public_base_url"] == "https://client.example"


def test_client_legacy_inbound_secret_overrides_deployment_fallback_during_migration():
    settings = Settings(
        crm_webhook_secret="global-inbound-secret",
        zapier_booking_webhook_secret="global-outbound-secret",
    )
    client = Client(
        client_key="legacy-inbound-client",
        business_name="Legacy Inbound Client",
        provider_config={"zapier_webhook_secret": "client-legacy-inbound-secret"},
    )

    effective = get_effective_runtime_map_for_client(
        settings=settings,
        overrides={},
        client=client,
    )

    assert effective["crm_webhook_secret"] == "client-legacy-inbound-secret"
    assert effective["zapier_booking_webhook_secret"] == "global-outbound-secret"
