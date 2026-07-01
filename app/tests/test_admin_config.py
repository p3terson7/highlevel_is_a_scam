def test_admin_runtime_and_client_update_flow(test_context):
    headers = {"X-Admin-Token": "test-admin-token"}

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
    assert status_payload["openai_api_key"] == "sk-test"
    assert status_payload["ai_provider_mode"] == "heuristic"
    assert "twilio_account_sid" not in status_payload
    assert "meta_access_token" not in status_payload

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
    headers = {"X-Admin-Token": "test-admin-token"}

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
                "zapier_booking_webhook_url": "https://hooks.zapier.com/hooks/catch/test/booking/",
            }
        },
    )
    assert first_patch.status_code == 200
    first_payload = first_patch.json()
    assert "openai_api_key" not in first_payload["provider_config"]
    assert "openai_model" not in first_payload["provider_config"]
    assert first_payload["provider_config"]["twilio_from_number"] == "+15554443333"
    assert first_payload["provider_config"]["zapier_webhook_secret"] == "zapier-client-secret"
    assert first_payload["provider_config"]["zapier_booking_webhook_url"] == "https://hooks.zapier.com/hooks/catch/test/booking/"

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
    assert second_payload["provider_config"]["zapier_booking_webhook_url"] == "https://hooks.zapier.com/hooks/catch/test/booking/"

    ui_client = test_context.client.get(
        f"/ui/api/clients/{test_context.client_key}",
        headers=headers,
    )
    assert ui_client.status_code == 200
    ui_payload = ui_client.json()
    assert ui_payload["provider_runtime"]["source"] == "client"
    assert ui_payload["provider_runtime"]["twilio_configured"] is True
    assert ui_payload["provider_runtime"]["twilio_from_number"] == "+15554443333"
    assert ui_payload["provider_runtime"]["openai_model"]
