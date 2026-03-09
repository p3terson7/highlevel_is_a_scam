from __future__ import annotations

from sqlalchemy import select

from app.db.models import Client
from app.db.session import get_session_factory


def _admin_headers() -> dict[str, str]:
    return {"X-Admin-Token": "test-admin-token"}


def _portal_headers(token: str) -> dict[str, str]:
    return {"X-Portal-Token": token}


def test_ui_shell_and_session_endpoint(test_context):
    page = test_context.client.get("/ui")
    assert page.status_code == 200
    assert "Lead Ops Console" in page.text
    assert "Operator workspace" in page.text
    assert "Conversations" in page.text

    session = test_context.client.get("/ui/api/session", headers=_admin_headers())
    assert session.status_code == 200
    payload = session.json()
    assert payload["status"] == "ok"
    assert payload["role"] == "admin"
    assert payload["can_seed_demo"] is True


def test_dashboard_omits_stringified_none_for_ai_error(test_context):
    ai_test = test_context.client.post(
        "/admin/test/ai",
        headers=_admin_headers(),
        json={
            "client_key": test_context.client_key,
            "inbound_text": "Can I book a meeting?",
            "lead_name": "UI Test Lead",
            "lead_city": "Austin",
        },
    )
    assert ai_test.status_code == 200

    dashboard = test_context.client.get("/ui/api/dashboard", headers=_admin_headers())
    assert dashboard.status_code == 200
    runtime = dashboard.json()["runtime"]
    assert runtime["openai_model"]
    assert all(value != "None" for value in runtime.values() if isinstance(value, str))


def test_demo_seed_populates_inbox_and_client_detail(test_context):
    seed = test_context.client.post("/ui/api/seed-demo?reset=true", headers=_admin_headers())
    assert seed.status_code == 200
    seed_payload = seed.json()
    assert seed_payload["clients_created"] == 3
    assert seed_payload["leads_created"] == 21
    assert seed_payload["messages_created"] > 0

    conversations = test_context.client.get("/ui/api/conversations", headers=_admin_headers())
    assert conversations.status_code == 200
    inbox_payload = conversations.json()
    assert inbox_payload["total"] >= 20
    assert any(item["state"] == "BOOKING_SENT" for item in inbox_payload["items"])
    assert any("After-hours pending" in item["tags"] for item in inbox_payload["items"])

    handoff = test_context.client.get(
        "/ui/api/conversations?client_key=demo-roofing&state=HANDOFF",
        headers=_admin_headers(),
    )
    assert handoff.status_code == 200
    handoff_payload = handoff.json()
    assert handoff_payload["total"] == 1
    assert "Needs handoff" in handoff_payload["items"][0]["tags"]

    client_detail = test_context.client.get("/ui/api/clients/demo-roofing", headers=_admin_headers())
    assert client_detail.status_code == 200
    client_payload = client_detail.json()
    assert client_payload["client"]["business_name"] == "Northwind Roofing Co."
    assert len(client_payload["recent_conversations"]) > 0
    assert len(client_payload["recent_logs"]) > 0



def test_conversation_thread_notes_and_actions(test_context):
    test_context.client.post("/ui/api/seed-demo?reset=true", headers=_admin_headers())

    qualifying = test_context.client.get(
        "/ui/api/conversations?client_key=demo-medspa&state=QUALIFYING",
        headers=_admin_headers(),
    )
    qualifying_payload = qualifying.json()
    lead_id = qualifying_payload["items"][0]["lead_id"]

    thread = test_context.client.get(f"/ui/api/conversations/{lead_id}/thread", headers=_admin_headers())
    assert thread.status_code == 200
    thread_payload = thread.json()
    assert thread_payload["lead"]["display_name"]
    assert thread_payload["lead"]["summary"]
    assert isinstance(thread_payload["lead"]["summary_lines"], list)
    assert len(thread_payload["messages"]) >= 3
    assert len(thread_payload["state_transitions"]) >= 2
    assert thread_payload["client"]["business_name"] == "Harbor MedSpa Studio"

    note = test_context.client.post(
        f"/ui/api/conversations/{lead_id}/notes",
        headers=_admin_headers(),
        json={"note": "Priority lead for front desk follow-up."},
    )
    assert note.status_code == 200
    assert note.json()["note"]["body"] == "Priority lead for front desk follow-up."

    booking = test_context.client.post(
        f"/ui/api/conversations/{lead_id}/actions/booking-link",
        headers=_admin_headers(),
        json={},
    )
    assert booking.status_code == 200
    booking_payload = booking.json()
    assert booking_payload["state"] == "BOOKING_SENT"
    assert test_context.fake_sms.sent
    assert "https://demo.harbor-medspa.example/consult" in test_context.fake_sms.sent[-1]["body"]

    handoff_target = test_context.client.get(
        "/ui/api/conversations?client_key=demo-legal&state=QUALIFYING",
        headers=_admin_headers(),
    ).json()["items"][0]["lead_id"]
    handoff = test_context.client.post(
        f"/ui/api/conversations/{handoff_target}/actions/handoff",
        headers=_admin_headers(),
        json={"note": "Counsel should review this personally."},
    )
    assert handoff.status_code == 200
    assert handoff.json()["state"] == "HANDOFF"

    handoff_thread = test_context.client.get(
        f"/ui/api/conversations/{handoff_target}/thread",
        headers=_admin_headers(),
    ).json()
    assert handoff_thread["lead"]["current_state"] == "HANDOFF"
    assert any(item["event_type"] == "admin_marked_handoff" for item in handoff_thread["audit_events"])


def test_zapier_results_console_endpoint_returns_recent_events(test_context):
    zapier_payload = {
        "id": "zap-ui-001",
        "full_name": "Zap UI Lead",
        "phone_number": "+15551114444",
        "email": "zap-ui@example.com",
        "city": "Austin",
    }
    webhook_response = test_context.client.post(
        f"/webhooks/zapier/{test_context.client_key}",
        json=zapier_payload,
    )
    assert webhook_response.status_code == 202

    response = test_context.client.get(
        f"/ui/api/clients/{test_context.client_key}/zapier-results",
        headers=_admin_headers(),
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["client_key"] == test_context.client_key
    assert payload["webhook_url"] == f"/webhooks/zapier/{test_context.client_key}"
    assert any(item["event_type"] == "zapier_webhook_received" for item in payload["items"])


def test_owner_workspace_can_start_test_contact_and_send_manual_message(test_context):
    runtime_update = test_context.client.put(
        "/admin/runtime-config",
        headers=_admin_headers(),
        json={"public_base_url": "https://owner-demo.ngrok-free.app"},
    )
    assert runtime_update.status_code == 200

    owner = test_context.client.get(
        f"/ui/api/owner/{test_context.client_key}",
        headers=_admin_headers(),
    )
    assert owner.status_code == 200
    owner_payload = owner.json()
    assert owner_payload["client"]["business_name"] == "Acme Solar"
    assert owner_payload["delivery_mode"] == "mock"
    assert owner_payload["client"]["twilio_inbound_path"] == "/sms/inbound/test-client-key"

    start = test_context.client.post(
        f"/ui/api/owner/{test_context.client_key}/test-contact",
        headers=_admin_headers(),
        json={
            "full_name": "Peter Test",
            "phone": "+1 (555) 222-3333",
            "email": "peter@example.com",
            "city": "Austin",
            "use_initial_template": True,
        },
    )
    assert start.status_code == 200
    start_payload = start.json()
    assert start_payload["state"] == "GREETED"
    assert start_payload["delivery_mode"] == "mock"
    assert "Acme Solar" in test_context.fake_sms.sent[-1]["body"]

    manual = test_context.client.post(
        f"/ui/api/conversations/{start_payload['lead_id']}/messages/manual",
        headers=_admin_headers(),
        json={"body": "Checking in personally before we get you scheduled."},
    )
    assert manual.status_code == 200
    manual_payload = manual.json()
    assert manual_payload["state"] == "GREETED"
    assert "Checking in personally" in test_context.fake_sms.sent[-1]["body"]

    thread = test_context.client.get(
        f"/ui/api/conversations/{start_payload['lead_id']}/thread",
        headers=_admin_headers(),
    )
    assert thread.status_code == 200
    thread_payload = thread.json()
    outbound_bodies = [message["body"] for message in thread_payload["messages"] if message["direction"] == "OUTBOUND"]
    assert any("Acme Solar" in body for body in outbound_bodies)
    assert any("Checking in personally" in body for body in outbound_bodies)


def test_owner_portal_can_view_and_update_ai_context(test_context):
    seed = test_context.client.post("/ui/api/seed-demo?reset=true", headers=_admin_headers())
    assert seed.status_code == 200

    login = test_context.client.post(
        "/ui/api/login/client",
        json={"email": "owner@demo-roofing.demo", "password": "demo-portal-2026"},
    )
    assert login.status_code == 200
    token = login.json()["token"]

    owner_workspace = test_context.client.get(
        "/ui/api/owner/demo-roofing",
        headers=_portal_headers(token),
    )
    assert owner_workspace.status_code == 200
    payload = owner_workspace.json()
    assert payload["client"]["client_key"] == "demo-roofing"
    assert "ai_context" in payload["client"]

    update = test_context.client.patch(
        "/ui/api/owner/demo-roofing/ai-context",
        headers=_portal_headers(token),
        json={
            "ai_context": "Speak like a seasoned roofing operator. Ask one practical question at a time.",
            "faq_context": "We install and repair asphalt shingle roofs in Chicago suburbs.",
        },
    )
    assert update.status_code == 200
    update_payload = update.json()
    assert "seasoned roofing operator" in update_payload["ai_context"]
    assert "Chicago suburbs" in update_payload["faq_context"]

    updated_workspace = test_context.client.get(
        "/ui/api/owner/demo-roofing",
        headers=_portal_headers(token),
    )
    assert updated_workspace.status_code == 200
    updated_payload = updated_workspace.json()
    assert "seasoned roofing operator" in updated_payload["client"]["ai_context"]
    assert "Chicago suburbs" in updated_payload["client"]["faq_context"]


def test_client_portal_login_is_scoped_to_own_leads_and_can_delete_conversation(test_context):
    seed = test_context.client.post("/ui/api/seed-demo?reset=true", headers=_admin_headers())
    assert seed.status_code == 200

    login = test_context.client.post(
        "/ui/api/login/client",
        json={"email": "owner@demo-roofing.demo", "password": "demo-portal-2026"},
    )
    assert login.status_code == 200
    token = login.json()["token"]

    session = test_context.client.get("/ui/api/session", headers=_portal_headers(token))
    assert session.status_code == 200
    session_payload = session.json()
    assert session_payload["role"] == "client"
    assert session_payload["client_key"] == "demo-roofing"

    conversations = test_context.client.get("/ui/api/conversations", headers=_portal_headers(token))
    assert conversations.status_code == 200
    items = conversations.json()["items"]
    assert items
    assert all(item["client_key"] == "demo-roofing" for item in items)

    foreign_lead_id = test_context.client.get(
        "/ui/api/conversations?client_key=demo-legal",
        headers=_admin_headers(),
    ).json()["items"][0]["lead_id"]
    foreign_thread = test_context.client.get(
        f"/ui/api/conversations/{foreign_lead_id}/thread",
        headers=_portal_headers(token),
    )
    assert foreign_thread.status_code == 404

    delete_lead_id = items[0]["lead_id"]
    delete_response = test_context.client.delete(
        f"/ui/api/conversations/{delete_lead_id}",
        headers=_portal_headers(token),
    )
    assert delete_response.status_code == 200
    assert delete_response.json()["deleted_lead_id"] == delete_lead_id

    missing_thread = test_context.client.get(
        f"/ui/api/conversations/{delete_lead_id}/thread",
        headers=_portal_headers(token),
    )
    assert missing_thread.status_code == 404

    refreshed = test_context.client.get("/ui/api/conversations", headers=_portal_headers(token))
    assert refreshed.status_code == 200
    refreshed_ids = {item["lead_id"] for item in refreshed.json()["items"]}
    assert delete_lead_id not in refreshed_ids


def test_seed_demo_backfills_missing_portal_credentials(test_context):
    seed = test_context.client.post("/ui/api/seed-demo?reset=true", headers=_admin_headers())
    assert seed.status_code == 200

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == "demo-roofing"))
        client.portal_email = ""
        client.portal_display_name = ""
        client.portal_password_hash = ""
        client.portal_enabled = False
        db.commit()

    repair = test_context.client.post("/ui/api/seed-demo", headers=_admin_headers())
    assert repair.status_code == 200
    repair_payload = repair.json()
    assert repair_payload["seeded"] is False
    assert repair_payload["portal_clients_updated"] >= 1
    assert "demo-roofing" in repair_payload["portal_client_keys"]

    login = test_context.client.post(
        "/ui/api/login/client",
        json={"email": "owner@demo-roofing.demo", "password": "demo-portal-2026"},
    )
    assert login.status_code == 200
