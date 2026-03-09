def test_admin_runtime_and_client_update_flow(test_context):
    headers = {"X-Admin-Token": "test-admin-token"}

    update_runtime = test_context.client.put(
        "/admin/runtime-config",
        headers=headers,
        json={
            "twilio_account_sid": "AC_TEST_SID",
            "twilio_auth_token": "secret-token",
            "twilio_from_number": "+15550001234",
            "public_base_url": "https://demo.ngrok-free.app",
            "openai_api_key": "sk-test",
            "openai_model": "gpt-4.1-mini",
            "ai_provider_mode": "heuristic",
            "meta_verify_token": "meta-token-test",
            "meta_access_token": "meta-access-test",
            "meta_graph_api_version": "v22.0",
            "linkedin_verify_token": "linkedin-token-test",
        },
    )
    assert update_runtime.status_code == 200
    payload = update_runtime.json()
    assert "twilio_account_sid" in payload["updated_keys"]
    assert "openai_api_key" in payload["secret_keys_updated"]

    status_resp = test_context.client.get("/admin/runtime-config/status", headers=headers)
    assert status_resp.status_code == 200
    status_payload = status_resp.json()
    assert status_payload["twilio_account_sid_configured"] is True
    assert status_payload["twilio_auth_token_configured"] is True
    assert status_payload["openai_api_key_configured"] is True
    assert status_payload["twilio_account_sid"] == "AC_TEST_SID"
    assert status_payload["twilio_auth_token"] == "secret-token"
    assert status_payload["public_base_url"] == "https://demo.ngrok-free.app"
    assert status_payload["openai_api_key"] == "sk-test"
    assert status_payload["ai_provider_mode"] == "heuristic"
    assert status_payload["meta_verify_token"] == "meta-token-test"
    assert status_payload["meta_access_token"] == "meta-access-test"
    assert status_payload["meta_graph_api_version"] == "v22.0"
    assert status_payload["linkedin_verify_token"] == "linkedin-token-test"

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
                "twilio_from_number": "+15554443333",
            }
        },
    )
    assert first_patch.status_code == 200
    first_payload = first_patch.json()
    assert first_payload["provider_config"]["openai_api_key"] == "sk-client-owned"
    assert first_payload["provider_config"]["twilio_from_number"] == "+15554443333"

    second_patch = test_context.client.patch(
        f"/admin/clients/{test_context.client_key}",
        headers=headers,
        json={"provider_config": {"openai_model": "gpt-4.1-mini"}},
    )
    assert second_patch.status_code == 200
    second_payload = second_patch.json()
    assert second_payload["provider_config"]["openai_api_key"] == "sk-client-owned"
    assert second_payload["provider_config"]["openai_model"] == "gpt-4.1-mini"
    assert second_payload["provider_config"]["twilio_from_number"] == "+15554443333"

    ui_client = test_context.client.get(
        f"/ui/api/clients/{test_context.client_key}",
        headers=headers,
    )
    assert ui_client.status_code == 200
    ui_payload = ui_client.json()
    assert ui_payload["provider_runtime"]["source"] == "client"
    assert ui_payload["provider_runtime"]["twilio_from_number"] == "+15554443333"
    assert ui_payload["provider_runtime"]["openai_model"] == "gpt-4.1-mini"
