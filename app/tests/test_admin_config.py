def test_admin_runtime_and_client_update_flow(test_context):
    headers = {"X-Admin-Token": "test-admin-token"}

    update_runtime = test_context.client.put(
        "/admin/runtime-config",
        headers=headers,
        json={
            "twilio_account_sid": "AC_TEST_SID",
            "twilio_auth_token": "secret-token",
            "twilio_from_number": "+15550001234",
            "openai_api_key": "sk-test",
            "openai_model": "gpt-4.1-mini",
            "meta_verify_token": "meta-token-test",
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
    # Endpoint should not return secret raw values.
    assert "twilio_account_sid" not in status_payload
    assert "openai_api_key" not in status_payload

    patch_client = test_context.client.patch(
        f"/admin/clients/{test_context.client_key}",
        headers=headers,
        json={
            "tone": "professional and concise",
            "booking_url": "https://example.com/new-booking",
            "template_overrides": {"initial_sms": "Hello from override"},
        },
    )
    assert patch_client.status_code == 200
    client_payload = patch_client.json()
    assert client_payload["tone"] == "professional and concise"
    assert client_payload["booking_url"] == "https://example.com/new-booking"
    assert client_payload["template_overrides"]["initial_sms"] == "Hello from override"
