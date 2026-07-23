from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from sqlalchemy import select

from app.api.ui import shell as ui_shell
from app.api.ui import sandbox_routes
from app.db.models import (
    AuditLog,
    CalendarBooking,
    Client,
    ConversationState,
    KnowledgeChunk,
    KnowledgeSource,
    Lead,
    LeadSource,
    Message,
    MessageAttachment,
    MessageDirection,
    OutboundRequest,
)
from app.db.session import get_session_factory
from app.services import zapier_booking
from app.services.llm_agent import LLMAgent


def _admin_headers() -> dict[str, str]:
    return {"X-Admin-Token": "test-admin-token-32-characters-long!"}


def _portal_headers(token: str) -> dict[str, str]:
    return {"X-Portal-Token": token}


@pytest.fixture(autouse=True)
def _clear_ui_rollout_flags(monkeypatch) -> None:
    for flag in (
        "UI_REACT_ISLAND_ENABLED",
        "UI_REACT_APP_SHELL_ENABLED",
        "UI_LEGACY_SHELL_ENABLED",
    ):
        monkeypatch.delenv(flag, raising=False)


def test_ui_shell_and_session_endpoint(test_context):
    page = test_context.client.get("/ui")
    assert page.status_code == 200
    assert "Lead Ops Console" in page.text
    assert (
        '<div id="react-root" data-react-app-shell="true"></div>' in page.text
        or "Operator workspace" in page.text
    )
    assert page.headers["x-content-type-options"] == "nosniff"
    assert page.headers["x-frame-options"] == "DENY"
    assert "frame-ancestors 'none'" in page.headers["content-security-policy"]

    session = test_context.client.get("/ui/api/session", headers=_admin_headers())
    assert session.status_code == 200
    payload = session.json()
    assert payload["status"] == "ok"
    assert payload["role"] == "admin"
    assert payload["can_seed_demo"] is True


def test_ui_home_and_deep_links_render_shell(test_context):
    for path in ["/", "/dashboard", "/inbox", "/pipeline", "/records", "/calendar", "/settings", "/ui", "/ui/dashboard"]:
        page = test_context.client.get(path)
        assert page.status_code == 200
        assert "Lead Ops Console" in page.text
        assert (
            '<div id="react-root" data-react-app-shell="true"></div>' in page.text
            or "Operator workspace" in page.text
        )

    missing_api = test_context.client.get("/ui/api/not-a-real-endpoint")
    missing_page = test_context.client.get("/not-a-real-page", headers={"Accept": "text/html"})
    missing_ui_page = test_context.client.get("/ui/not-a-real-page", headers={"Accept": "text/html"})
    assert missing_api.status_code == 404
    assert missing_page.status_code == 404
    assert missing_ui_page.status_code == 404
    assert "Lead Ops Console" not in missing_api.text
    assert "Lead Ops Console" not in missing_page.text


def test_ui_shell_fails_closed_when_react_build_is_missing(test_context, monkeypatch):
    monkeypatch.setenv("UI_REACT_APP_SHELL_ENABLED", "true")
    missing_dist = ui_shell._REPO_DIR / ".missing-frontend-dist-for-test"
    monkeypatch.setattr(ui_shell, "_FRONTEND_DIST_DIR", missing_dist)
    monkeypatch.setattr(ui_shell, "_FRONTEND_MANIFEST_FILE", missing_dist / ".vite" / "manifest.json")

    page = test_context.client.get("/ui")

    assert page.status_code == 503
    assert "React frontend build unavailable" in page.text
    assert 'data-react-island="' not in page.text
    assert "/ui/react-assets/" not in page.text
    assert page.headers["cache-control"] == "no-store, max-age=0"


def test_ui_shell_defaults_to_react_when_build_exists(test_context, tmp_path, monkeypatch):
    dist_dir = tmp_path / "dist"
    asset_dir = dist_dir / "assets"
    manifest_dir = dist_dir / ".vite"
    asset_dir.mkdir(parents=True)
    manifest_dir.mkdir()
    (asset_dir / "app-shell-default.js").write_text("console.log('react app shell default');", encoding="utf-8")
    (asset_dir / "app-shell-default.css").write_text("#react-root { min-height: 100vh; }", encoding="utf-8")
    (manifest_dir / "manifest.json").write_text(
        json.dumps(
            {
                "src/main.tsx": {
                    "file": "assets/app-shell-default.js",
                    "css": ["assets/app-shell-default.css"],
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(ui_shell, "_FRONTEND_DIST_DIR", dist_dir)
    monkeypatch.setattr(ui_shell, "_FRONTEND_MANIFEST_FILE", manifest_dir / "manifest.json")

    page = test_context.client.get("/ui")

    assert page.status_code == 200
    assert "Operator workspace" not in page.text
    assert '<div id="react-root" data-react-app-shell="true"></div>' in page.text
    assert "/ui/react-assets/assets/app-shell-default.js" in page.text
    assert "/ui/react-assets/assets/app-shell-default.css" in page.text
    assert "ui-core.js" not in page.text


def test_ui_shell_can_force_legacy_shell_when_build_exists(test_context, tmp_path, monkeypatch):
    dist_dir = tmp_path / "dist"
    asset_dir = dist_dir / "assets"
    manifest_dir = dist_dir / ".vite"
    asset_dir.mkdir(parents=True)
    manifest_dir.mkdir()
    (asset_dir / "app-shell-default.js").write_text("console.log('react app shell default');", encoding="utf-8")
    (manifest_dir / "manifest.json").write_text(
        json.dumps({"src/main.tsx": {"file": "assets/app-shell-default.js"}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("UI_REACT_APP_SHELL_ENABLED", "true")
    monkeypatch.setenv("UI_LEGACY_SHELL_ENABLED", "true")
    monkeypatch.setattr(ui_shell, "_FRONTEND_DIST_DIR", dist_dir)
    monkeypatch.setattr(ui_shell, "_FRONTEND_MANIFEST_FILE", manifest_dir / "manifest.json")

    page = test_context.client.get("/ui")

    assert page.status_code == 200
    assert "Operator workspace" in page.text
    assert '<div id="react-root" data-react-app-shell="true"></div>' not in page.text
    assert "/ui/react-assets/" not in page.text
    assert 'for="clientProviderZapierSecret">CRM intake webhook secret</label>' in page.text
    assert 'for="clientProviderZapierBookingSecret">Zapier booking signing secret</label>' in page.text
    assert "Zapier webhook secret" not in page.text


def test_ui_shell_mounts_react_island_when_enabled(test_context, tmp_path, monkeypatch):
    dist_dir = tmp_path / "dist"
    asset_dir = dist_dir / "assets"
    manifest_dir = dist_dir / ".vite"
    asset_dir.mkdir(parents=True)
    manifest_dir.mkdir()
    (asset_dir / "app-abc123.js").write_text("console.log('react island');", encoding="utf-8")
    (asset_dir / "app-abc123.css").write_text("#react-root { display: contents; }", encoding="utf-8")
    (manifest_dir / "manifest.json").write_text(
        json.dumps(
            {
                "src/main.tsx": {
                    "file": "assets/app-abc123.js",
                    "css": ["assets/app-abc123.css"],
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("UI_REACT_ISLAND_ENABLED", "true")
    monkeypatch.setenv("UI_REACT_APP_SHELL_ENABLED", "false")
    monkeypatch.setattr(ui_shell, "_FRONTEND_DIST_DIR", dist_dir)
    monkeypatch.setattr(ui_shell, "_FRONTEND_MANIFEST_FILE", manifest_dir / "manifest.json")

    page = test_context.client.get("/ui")

    assert page.status_code == 200
    for island in ["dashboard", "clients", "inbox", "pipeline", "records", "calendar", "tasks", "logs", "settings", "test-lab"]:
        assert f'class="react-island-root react-{island}-root" data-react-island="{island}"' in page.text
    assert '<link rel="stylesheet" href="/ui/react-assets/assets/app-abc123.css" />' in page.text
    assert '<script type="module" src="/ui/react-assets/assets/app-abc123.js"></script>' in page.text

    script = test_context.client.get("/ui/react-assets/assets/app-abc123.js")
    assert script.status_code == 200
    assert "react island" in script.text
    assert "immutable" in script.headers["cache-control"]

    style = test_context.client.get("/ui/react-assets/assets/app-abc123.css")
    assert style.status_code == 200
    assert "#react-root" in style.text
    assert "immutable" in style.headers["cache-control"]


def test_ui_shell_mounts_full_react_app_shell_when_enabled(test_context, tmp_path, monkeypatch):
    dist_dir = tmp_path / "dist"
    asset_dir = dist_dir / "assets"
    manifest_dir = dist_dir / ".vite"
    asset_dir.mkdir(parents=True)
    manifest_dir.mkdir()
    (asset_dir / "app-shell-abc123.js").write_text("console.log('react app shell');", encoding="utf-8")
    (asset_dir / "app-shell-abc123.css").write_text("#react-root { min-height: 100vh; }", encoding="utf-8")
    (manifest_dir / "manifest.json").write_text(
        json.dumps(
            {
                "src/main.tsx": {
                    "file": "assets/app-shell-abc123.js",
                    "css": ["assets/app-shell-abc123.css"],
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("UI_REACT_APP_SHELL_ENABLED", "true")
    monkeypatch.setattr(ui_shell, "_FRONTEND_DIST_DIR", dist_dir)
    monkeypatch.setattr(ui_shell, "_FRONTEND_MANIFEST_FILE", manifest_dir / "manifest.json")

    page = test_context.client.get("/ui")

    assert page.status_code == 200
    assert '<body data-theme="dark">' in page.text
    assert '<div class="app-background" aria-hidden="true"></div>' in page.text
    assert '<div id="react-root" data-react-app-shell="true"></div>' in page.text
    assert '<link rel="stylesheet" href="/ui/assets/ui.css" />' in page.text
    assert '<link rel="stylesheet" href="/ui/react-assets/assets/app-shell-abc123.css" />' in page.text
    assert '<script type="module" src="/ui/react-assets/assets/app-shell-abc123.js"></script>' in page.text
    assert 'data-react-island="' not in page.text
    assert "ui-core.js" not in page.text
    assert "Operator workspace" not in page.text


def test_ui_shell_full_app_flag_takes_precedence_over_island_flag(test_context, tmp_path, monkeypatch):
    dist_dir = tmp_path / "dist"
    asset_dir = dist_dir / "assets"
    manifest_dir = dist_dir / ".vite"
    asset_dir.mkdir(parents=True)
    manifest_dir.mkdir()
    (asset_dir / "app-shell.js").write_text("console.log('react app shell');", encoding="utf-8")
    (manifest_dir / "manifest.json").write_text(
        json.dumps({"src/main.tsx": {"file": "assets/app-shell.js"}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("UI_REACT_ISLAND_ENABLED", "true")
    monkeypatch.setenv("UI_REACT_APP_SHELL_ENABLED", "true")
    monkeypatch.setattr(ui_shell, "_FRONTEND_DIST_DIR", dist_dir)
    monkeypatch.setattr(ui_shell, "_FRONTEND_MANIFEST_FILE", manifest_dir / "manifest.json")

    page = test_context.client.get("/ui")

    assert page.status_code == 200
    assert '<div id="react-root" data-react-app-shell="true"></div>' in page.text
    assert 'data-react-island="' not in page.text


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


def test_dashboard_returns_breakdowns_for_admin_and_client_scope(test_context):
    seed = test_context.client.post("/ui/api/seed-demo?reset=true", headers=_admin_headers())
    assert seed.status_code == 200

    admin_dashboard = test_context.client.get("/ui/api/dashboard", headers=_admin_headers())
    assert admin_dashboard.status_code == 200
    admin_payload = admin_dashboard.json()
    assert admin_payload["scope"]["role"] == "admin"
    assert admin_payload["source_breakdown"]
    assert admin_payload["stage_breakdown"]
    assert len(admin_payload["lead_trend"]) == 6
    assert "new_last_24_hours" in admin_payload["stats"]
    assert isinstance(admin_payload["recent_leads"], list)

    scoped_dashboard = test_context.client.get(
        "/ui/api/dashboard?client_key=demo-roofing",
        headers=_admin_headers(),
    )
    assert scoped_dashboard.status_code == 200
    scoped_payload = scoped_dashboard.json()
    assert scoped_payload["scope"]["role"] == "admin"
    assert scoped_payload["scope"]["client_key"] == "demo-roofing"
    assert scoped_payload["stats"]["clients_total"] == 1
    assert all(item["client_key"] == "demo-roofing" for item in scoped_payload["recent_leads"])
    assert all(item["client_name"] == "Northwind Roofing Co." for item in scoped_payload["upcoming"]["tasks"])
    assert all(item["client_name"] == "Northwind Roofing Co." for item in scoped_payload["upcoming"]["meetings"])

    login = test_context.client.post(
        "/ui/api/login/client/token",
        json={"email": "owner@demo-roofing.demo", "password": "demo-portal-2026"},
    )
    assert login.status_code == 200
    token = login.json()["token"]

    client_dashboard = test_context.client.get("/ui/api/dashboard", headers=_portal_headers(token))
    assert client_dashboard.status_code == 200
    client_payload = client_dashboard.json()
    assert client_payload["scope"]["role"] == "client"
    assert client_payload["scope"]["client_key"] == "demo-roofing"
    assert client_payload["stats"]["clients_total"] == 1
    assert isinstance(client_payload["recent_leads"], list)
    assert all(item["client_name"] in {"", "Northwind Roofing Co."} for item in client_payload["upcoming"]["meetings"])


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


def test_seed_showcase_for_selected_existing_client(test_context):
    seed = test_context.client.post(
        f"/ui/api/seed-showcase/{test_context.client_key}?reset=true",
        headers=_admin_headers(),
    )
    assert seed.status_code == 200
    payload = seed.json()
    assert payload["seeded"] is True
    assert payload["client_key"] == test_context.client_key
    assert payload["leads_created"] > 0
    assert payload["messages_created"] > 0

    conversations = test_context.client.get(
        f"/ui/api/conversations?client_key={test_context.client_key}",
        headers=_admin_headers(),
    )
    assert conversations.status_code == 200
    convo_payload = conversations.json()
    assert convo_payload["total"] >= payload["leads_created"]

    crm_leads = test_context.client.get(
        f"/ui/api/crm/leads?client_key={test_context.client_key}",
        headers=_admin_headers(),
    )
    assert crm_leads.status_code == 200
    crm_payload = crm_leads.json()
    assert crm_payload["total"] >= payload["leads_created"]
    assert any(item["conversation_state"] == "BOOKING_SENT" for item in crm_payload["items"])


def test_ui_can_start_custom_test_lab_sandbox_thread(test_context):
    response = test_context.client.post(
        f"/ui/api/owner/{test_context.client_key}/sandbox/start",
        headers=_admin_headers(),
        json={
            "mode": "gpt_only",
            "full_name": "Strategy Call Lead",
            "phone": "+15554443333",
            "email": "strategy@example.com",
            "city": "Toronto",
            "form_answers": [
                {"question": "Timeline", "answer": "Within 2 weeks"},
                {"question": "Project scope", "answer": "Retail existing conditions"},
                {"question": "Locations scope", "answer": "One building"},
            ],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["mode"] == "gpt_only"
    assert payload["delivery_mode"] == "sandbox"
    assert payload["phone"] == "+15554443333"
    assert test_context.fake_sms.sent == []

    thread = test_context.client.get(
        f"/ui/api/conversations/{payload['lead_id']}/thread",
        headers=_admin_headers(),
    )
    assert thread.status_code == 200
    thread_payload = thread.json()
    assert thread_payload["lead"]["full_name"] == "Strategy Call Lead"
    assert thread_payload["lead"]["source"] == "manual"
    assert thread_payload["lead"]["form_answers"]["when_to_start"] == "Within 2 weeks"
    assert thread_payload["lead"]["form_answers"]["project_scope"] == "Retail existing conditions"
    assert thread_payload["messages"]
    assert thread_payload["messages"][0]["direction"] == "OUTBOUND"
    assert any(item["event_type"] == "ui_sandbox_initial_ai_sms" for item in thread_payload["audit_events"])


def test_test_lab_opening_reply_uses_selected_clients_website_knowledge(
    test_context,
    monkeypatch,
):
    captured_prompts: list[dict] = []
    now = datetime.now(timezone.utc)
    session_factory = get_session_factory()
    with session_factory() as db:
        selected_client = db.scalar(
            select(Client).where(Client.client_key == test_context.client_key)
        )
        assert selected_client is not None
        selected_client.knowledge_profile_context = (
            "Selected tenant profile: precision metrology specialists."
        )
        other_client = Client(
            client_key="other-knowledge-tenant",
            business_name="Other Knowledge Tenant",
            knowledge_profile_context="Other tenant profile must never appear.",
        )
        db.add(other_client)
        db.flush()

        selected_source = KnowledgeSource(
            client_id=selected_client.id,
            url="https://selected.example/metrology",
            normalized_url="https://selected.example/metrology",
            final_url="https://selected.example/metrology",
            title="Selected precision metrology",
            status="ok",
            extracted_text="Selected-source fact: zirconium turbine metrology fixtures are supported.",
            text_excerpt="Selected-source fact: zirconium turbine metrology fixtures are supported.",
            last_success_at=now,
        )
        other_source = KnowledgeSource(
            client_id=other_client.id,
            url="https://other.example/metrology",
            normalized_url="https://other.example/metrology",
            final_url="https://other.example/metrology",
            title="Other tenant metrology",
            status="ok",
            extracted_text="Other-tenant secret: zirconium turbine metrology fixtures are forbidden.",
            text_excerpt="Other-tenant secret: zirconium turbine metrology fixtures are forbidden.",
            last_success_at=now,
        )
        db.add_all([selected_source, other_source])
        db.flush()
        db.add_all(
            [
                KnowledgeChunk(
                    client_id=selected_client.id,
                    source_id=selected_source.id,
                    chunk_index=0,
                    content=selected_source.extracted_text,
                    search_text=selected_source.extracted_text.casefold(),
                ),
                KnowledgeChunk(
                    client_id=other_client.id,
                    source_id=other_source.id,
                    chunk_index=0,
                    content=other_source.extracted_text,
                    search_text=other_source.extracted_text.casefold(),
                ),
            ]
        )
        selected_client_id = selected_client.id
        selected_source_id = selected_source.id
        db.commit()

    class OpeningKnowledgeProvider:
        name = "test-lab-opening-knowledge"

        def generate_json(self, system_prompt: str, user_prompt: str):
            _ = system_prompt
            prompt = json.loads(user_prompt)
            captured_prompts.append(prompt)
            return {
                "reply_text": "Yes, our precision metrology team can help.",
                "next_state": "QUALIFYING",
                "collected_fields": prompt["qualification_memory"],
                "next_question_key": None,
                "action": "none",
                "uses_knowledge_context": True,
                "tool_call": {"name": "none", "args": {}},
            }

    monkeypatch.setattr(
        sandbox_routes,
        "build_llm_agent",
        lambda *args, **kwargs: LLMAgent(provider=OpeningKnowledgeProvider()),
    )

    response = test_context.client.post(
        f"/ui/api/owner/{test_context.client_key}/sandbox/start",
        headers=_admin_headers(),
        json={
            "mode": "gpt_only",
            "full_name": "Knowledge Sandbox Lead",
            "form_answers": [
                {
                    "question": "Project scope",
                    "answer": "Zirconium turbine metrology fixture",
                }
            ],
        },
    )

    assert response.status_code == 200
    assert len(captured_prompts) == 1
    opening_prompt = captured_prompts[0]
    assert "selected tenant profile" in opening_prompt["business_profile_context"].lower()
    assert "selected-source fact" in opening_prompt["knowledge_context"].lower()
    assert "other tenant" not in opening_prompt["business_profile_context"].lower()
    assert "other-tenant secret" not in opening_prompt["knowledge_context"].lower()
    assert "ai sandbox" in opening_prompt["latest_inbound_message"].lower()
    assert "meta lead ads" not in opening_prompt["latest_inbound_message"].lower()

    with session_factory() as db:
        lead = db.get(Lead, response.json()["lead_id"])
        assert lead is not None
        assert lead.client_id == selected_client_id
        outbound = db.scalar(
            select(Message)
            .where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.OUTBOUND,
            )
            .order_by(Message.id.desc())
        )
        assert outbound is not None
        agent_trace = (outbound.raw_payload or {})["agent"]
        assert agent_trace["uses_knowledge_context"] is True
        assert agent_trace["knowledge_retrieval"]["selected_sources"][0][
            "source_id"
        ] == selected_source_id
        assert "url" not in agent_trace["knowledge_retrieval"]["selected_sources"][0]


def test_test_lab_future_modes_are_explicitly_disabled(test_context):
    response = test_context.client.post(
        f"/ui/api/owner/{test_context.client_key}/sandbox/start",
        headers=_admin_headers(),
        json={
            "mode": "gpt_twilio",
            "full_name": "Future Mode Lead",
            "form_answers": [{"question": "Timeline", "answer": "Tomorrow"}],
        },
    )

    assert response.status_code == 400
    assert "GPT + Zapier" in response.json()["detail"]


def test_ai_sandbox_gpt_zapier_mode_posts_booking_payload(test_context, monkeypatch):
    webhook_url = "https://hooks.zapier.com/hooks/catch/test/sandbox-booking/"
    captured: dict = {}
    session_factory = get_session_factory()
    with session_factory() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.provider_config = {"zapier_booking_webhook_url": webhook_url}
        db.commit()

    def fake_post_json(*, url: str, payload: dict, timeout_seconds: int) -> httpx.Response:
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout_seconds"] = timeout_seconds
        return httpx.Response(200, json={"ok": True}, request=httpx.Request("POST", url))

    monkeypatch.setattr(zapier_booking, "_post_json", fake_post_json)
    monkeypatch.setattr(sandbox_routes, "build_llm_agent", lambda *args, **kwargs: test_context.fake_llm)

    start = test_context.client.post(
        f"/ui/api/owner/{test_context.client_key}/sandbox/start",
        headers=_admin_headers(),
        json={
            "mode": "gpt_zapier",
            "full_name": "Zapier Sandbox Lead",
            "email": "zapier-sandbox@example.com",
            "form_answers": [{"question": "Monthly lead volume", "answer": "80-100"}],
        },
    )
    assert start.status_code == 200
    start_payload = start.json()
    assert start_payload["mode"] == "gpt_zapier"
    assert start_payload["zapier_booking_webhook"]["status"] == "waiting_for_booking"

    offer = test_context.client.post(
        f"/ui/api/conversations/{start_payload['lead_id']}/sandbox/messages",
        headers=_admin_headers(),
        json={"body": "Can I book this week?"},
    )
    assert offer.status_code == 200
    offer_payload = offer.json()
    assert offer_payload["zapier_booking_webhook"]["status"] == "waiting_for_booking"
    assert offer_payload["booking_debug"]["selected_slots"]
    assert "request" in offer_payload["booking_debug"]
    assert "planner" in offer_payload["booking_debug"]

    booked = test_context.client.post(
        f"/ui/api/conversations/{start_payload['lead_id']}/sandbox/messages",
        headers=_admin_headers(),
        json={"body": "1"},
    )
    assert booked.status_code == 200
    booked_payload = booked.json()
    assert booked_payload["state"] == "BOOKED"
    assert captured["url"] == webhook_url
    payload = captured["payload"]
    assert payload["event_type"] == "calendar_booking.created"
    assert payload["trigger"] == "sms_ai_calendar_booking_created"
    assert payload["lead"]["email"] == "zapier-sandbox@example.com"
    assert payload["form_answers"]["monthly_lead_volume"] == "80-100"
    assert payload["form"]["answers"][0] == {
        "question": "Monthly lead volume",
        "key": "monthly_lead_volume",
        "answer": "80-100",
        "value": "80-100",
    }
    assert payload["meeting"]["start_at"] == "2026-03-09T15:00:00+00:00"
    assert payload["meeting"]["title"] == "Acme Solar meeting - Zapier Sandbox Lead"
    assert payload["calendar_event"]["summary"] == "Acme Solar meeting - Zapier Sandbox Lead"
    assert payload["calendar_event"]["start_datetime"] == "2026-03-09T15:00:00+00:00"
    assert payload["calendar_event"]["end_datetime"] == "2026-03-09T15:30:00+00:00"
    assert payload["email_confirmation"]["to"] == "zapier-sandbox@example.com"
    assert booked_payload["zapier_booking_webhook"]["status"] == "sent"
    assert booked_payload["zapier_booking_webhook"]["payload"] is None

    with session_factory() as db:
        sent = db.scalar(
            select(AuditLog)
            .where(
                AuditLog.lead_id == start_payload["lead_id"],
                AuditLog.event_type == "zapier_booking_webhook_sent",
            )
            .limit(1)
        )
        assert sent is not None


def test_ai_sandbox_gpt_only_mode_does_not_post_zapier_booking(test_context, monkeypatch):
    calls = 0
    session_factory = get_session_factory()
    with session_factory() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.provider_config = {"zapier_booking_webhook_url": "https://hooks.zapier.com/hooks/catch/test/disabled/"}
        db.commit()

    def fake_post_json(*, url: str, payload: dict, timeout_seconds: int) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr(zapier_booking, "_post_json", fake_post_json)
    monkeypatch.setattr(sandbox_routes, "build_llm_agent", lambda *args, **kwargs: test_context.fake_llm)

    start = test_context.client.post(
        f"/ui/api/owner/{test_context.client_key}/sandbox/start",
        headers=_admin_headers(),
        json={
            "mode": "gpt_only",
            "full_name": "GPT Only Lead",
            "email": "gpt-only@example.com",
            "form_answers": [{"question": "Timeline", "answer": "This week"}],
        },
    )
    assert start.status_code == 200
    lead_id = start.json()["lead_id"]

    offer = test_context.client.post(
        f"/ui/api/conversations/{lead_id}/sandbox/messages",
        headers=_admin_headers(),
        json={"body": "Can I book this week?"},
    )
    assert offer.status_code == 200
    assert offer.json()["booking_debug"]["selected_slots"]
    booked = test_context.client.post(
        f"/ui/api/conversations/{lead_id}/sandbox/messages",
        headers=_admin_headers(),
        json={"body": "1"},
    )

    assert booked.status_code == 200
    assert booked.json()["state"] == "BOOKED"
    assert booked.json()["zapier_booking_webhook"]["status"] == "disabled"
    assert calls == 0


def test_ai_sandbox_runs_agent_thread_without_sms_provider(test_context):
    start = test_context.client.post(
        f"/ui/api/owner/{test_context.client_key}/sandbox/start",
        headers=_admin_headers(),
        json={
            "mode": "gpt_only",
            "full_name": "Peter Sandbox",
            "form_answers": [{"question": "Project scope", "answer": "Revit models for retail spaces"}],
        },
    )

    assert start.status_code == 200
    start_payload = start.json()
    assert start_payload["delivery_mode"] == "sandbox"
    assert start_payload["twilio_bypassed"] is True
    assert start_payload["lead_id"]
    assert test_context.fake_sms.sent == []

    turn = test_context.client.post(
        f"/ui/api/conversations/{start_payload['lead_id']}/sandbox/messages",
        headers=_admin_headers(),
        json={"body": "Do you handle Revit models for retail spaces?"},
    )

    assert turn.status_code == 200
    turn_payload = turn.json()
    assert turn_payload["delivery_mode"] == "sandbox"
    assert turn_payload["twilio_bypassed"] is True
    assert turn_payload["reply"]["provider_message_sid"].startswith("MOCK-")
    assert test_context.fake_sms.sent == []

    thread = test_context.client.get(
        f"/ui/api/conversations/{start_payload['lead_id']}/thread",
        headers=_admin_headers(),
    )
    assert thread.status_code == 200
    thread_payload = thread.json()
    assert "sandbox" in thread_payload["lead"]["tags"]
    assert [message["direction"] for message in thread_payload["messages"]] == ["OUTBOUND", "INBOUND", "OUTBOUND"]
    assert any("Revit models" in message["body"] for message in thread_payload["messages"])


def test_client_portal_can_launch_test_lead_without_admin_token(test_context):
    seed = test_context.client.post("/ui/api/seed-demo?reset=true", headers=_admin_headers())
    assert seed.status_code == 200

    login = test_context.client.post(
        "/ui/api/login/client/token",
        json={"email": "owner@demo-roofing.demo", "password": "demo-portal-2026"},
    )
    assert login.status_code == 200
    token = login.json()["token"]

    response = test_context.client.post(
        "/ui/api/owner/demo-roofing/sandbox/start",
        headers=_portal_headers(token),
        json={
            "mode": "gpt_only",
            "full_name": "Portal Sandbox",
            "phone": "+15556667777",
            "form_answers": [{"question": "Timeline", "answer": "This week"}],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["phone"] == "+15556667777"
    assert payload["delivery_mode"] == "sandbox"
    assert test_context.fake_sms.sent == []


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

    booking_headers = {**_admin_headers(), "Idempotency-Key": "booking-link-ui-test-001"}
    booking_sent_before = len(test_context.fake_sms.sent)
    booking = test_context.client.post(
        f"/ui/api/conversations/{lead_id}/actions/booking-link",
        headers=booking_headers,
        json={},
    )
    assert booking.status_code == 200
    booking_payload = booking.json()
    assert booking_payload["state"] == "BOOKING_SENT"
    assert test_context.fake_sms.sent
    assert "https://demo.harbor-medspa.example/consult" in test_context.fake_sms.sent[-1]["body"]
    booking_retry = test_context.client.post(
        f"/ui/api/conversations/{lead_id}/actions/booking-link",
        headers=booking_headers,
        json={},
    )
    assert booking_retry.status_code == 200
    assert booking_retry.json() == booking_payload
    assert len(test_context.fake_sms.sent) == booking_sent_before + 1

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


def test_conversation_agent_control_pause_resume_and_manual_takeover(test_context):
    test_context.client.post("/ui/api/seed-demo?reset=true", headers=_admin_headers())

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).join(Client).where(Client.client_key == "demo-roofing"))
        assert lead is not None
        lead_id = lead.id

    pause = test_context.client.patch(
        f"/ui/api/conversations/{lead_id}/agent-control",
        headers=_admin_headers(),
        json={"paused": True, "reason": "operator_testing", "note": "Owner is taking over."},
    )
    assert pause.status_code == 200
    assert pause.json()["agent_control"]["paused"] is True
    assert pause.json()["agent_control"]["reason"] == "operator_testing"

    thread = test_context.client.get(f"/ui/api/conversations/{lead_id}/thread", headers=_admin_headers()).json()
    assert thread["lead"]["agent_control"]["paused"] is True
    assert any(item["event_type"] == "agent_paused" for item in thread["audit_events"])

    resume = test_context.client.patch(
        f"/ui/api/conversations/{lead_id}/agent-control",
        headers=_admin_headers(),
        json={"paused": False, "reason": "operator_done"},
    )
    assert resume.status_code == 200
    assert resume.json()["agent_control"]["paused"] is False

    manual = test_context.client.post(
        f"/ui/api/conversations/{lead_id}/messages/manual",
        headers=_admin_headers(),
        json={"body": "I can take this one from here.", "pause_agent": True},
    )
    assert manual.status_code == 200
    assert manual.json()["agent_control"]["paused"] is True
    assert manual.json()["agent_control"]["reason"] == "manual_reply_takeover"
    assert test_context.fake_sms.sent[-1]["body"] == "I can take this one from here."


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


def test_calendar_endpoint_returns_internal_bookings(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.booking_mode = "internal"
        client.booking_config = {
            "internal_calendar": {
                "slot_minutes": 30,
                "notice_minutes": 0,
                "horizon_days": 14,
                "availability": [{"day": day, "start": "09:00", "end": "17:00", "enabled": True} for day in range(5)],
            }
        }
        lead = Lead(
            client_id=client.id,
            source=LeadSource.META,
            full_name="Calendar API Lead",
            phone="+15553334444",
            email="calendar-api@example.com",
            city="Austin",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.flush()
        start_at = datetime.now(timezone.utc) + timedelta(days=1)
        db.add(
            CalendarBooking(
                client_id=client.id,
                lead_id=lead.id,
                provider="internal",
                source="sms_ai",
                status="scheduled",
                start_at=start_at,
                end_at=start_at + timedelta(minutes=30),
                timezone=client.timezone,
                title="Lead call",
                notes="",
            )
        )
        db.commit()

    response = test_context.client.get(
        f"/ui/api/clients/{test_context.client_key}/calendar",
        headers=_admin_headers(),
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["booking_mode"] == "internal"
    assert payload["total"] >= 1
    assert any(item["lead_name"] == "Calendar API Lead" for item in payload["items"])


def test_manual_lead_creation_adds_pipeline_record(test_context):
    response = test_context.client.post(
        "/ui/api/crm/leads",
        headers=_admin_headers(),
        json={
            "client_key": test_context.client_key,
            "full_name": "Manual Pipeline Lead",
            "phone": "+1 (555) 222-1010",
            "email": "manual@example.com",
            "city": "Toronto",
            "crm_stage": "Qualified",
            "notes": "Created by an operator.",
        },
    )

    assert response.status_code == 200
    lead_payload = response.json()["lead"]
    assert lead_payload["display_name"] == "Manual Pipeline Lead"
    assert lead_payload["crm_stage"] == "Qualified"

    leads = test_context.client.get(
        f"/ui/api/crm/leads?client_key={test_context.client_key}",
        headers=_admin_headers(),
    )
    assert leads.status_code == 200
    assert lead_payload["lead_id"] in {item["lead_id"] for item in leads.json()["items"]}

    inbox = test_context.client.get(
        f"/ui/api/conversations?client_key={test_context.client_key}",
        headers=_admin_headers(),
    )
    assert inbox.status_code == 200
    assert lead_payload["lead_id"] in {item["lead_id"] for item in inbox.json()["items"]}


def test_manual_calendar_meeting_lifecycle(test_context):
    lead_create = test_context.client.post(
        "/ui/api/crm/leads",
        headers=_admin_headers(),
        json={
            "client_key": test_context.client_key,
            "full_name": "Manual Meeting Lead",
            "phone": "+1 (555) 222-2020",
        },
    )
    assert lead_create.status_code == 200
    lead_id = lead_create.json()["lead"]["lead_id"]

    meeting_create = test_context.client.post(
        f"/ui/api/clients/{test_context.client_key}/calendar/meetings",
        headers=_admin_headers(),
        json={
            "lead_id": lead_id,
            "start_at": "2026-06-15T10:30",
            "duration_minutes": 45,
            "timezone": "America/Toronto",
            "title": "Manual discovery call",
            "notes": "Bring pricing context.",
        },
    )
    assert meeting_create.status_code == 200
    meeting = meeting_create.json()["meeting"]
    assert meeting["lead_id"] == lead_id
    assert meeting["status"] == "scheduled"
    assert meeting["source"] == "manual"
    assert meeting["title"] == "Manual discovery call"
    assert meeting_create.json()["zapier_booking_webhook"]["status"] == "skipped"
    assert meeting_create.json()["zapier_booking_webhook"]["reason"] == "not_configured"
    session_factory = get_session_factory()
    with session_factory() as db:
        audit = db.scalar(
            select(AuditLog)
            .where(
                AuditLog.lead_id == lead_id,
                AuditLog.event_type == "manual_calendar_booking_created",
            )
            .limit(1)
        )
        assert audit is not None
        assert audit.decision["options"] == {
            "create_conference_link": True,
            "send_email_invite": True,
            "include_meeting_link": True,
            "send_sms_reminders": True,
            "zapier_pending": True,
        }

    completed = test_context.client.patch(
        f"/ui/api/calendar/meetings/{meeting['id']}",
        headers=_admin_headers(),
        json={"status": "completed"},
    )
    assert completed.status_code == 200
    assert completed.json()["meeting"]["status"] == "completed"

    calendar = test_context.client.get(
        f"/ui/api/clients/{test_context.client_key}/calendar",
        headers=_admin_headers(),
    )
    assert calendar.status_code == 200
    assert any(item["id"] == meeting["id"] and item["status"] == "completed" for item in calendar.json()["items"])

    detail = test_context.client.get(f"/ui/api/crm/leads/{lead_id}", headers=_admin_headers())
    assert detail.status_code == 200
    assert detail.json()["lead"]["crm_stage"] == "Meeting Completed"

    deleted = test_context.client.delete(
        f"/ui/api/calendar/meetings/{meeting['id']}",
        headers=_admin_headers(),
    )
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True

    after_delete = test_context.client.get(
        f"/ui/api/clients/{test_context.client_key}/calendar",
        headers=_admin_headers(),
    )
    assert after_delete.status_code == 200
    assert meeting["id"] not in {item["id"] for item in after_delete.json()["items"]}


def test_manual_calendar_meeting_can_create_lead_inline(test_context):
    meeting_create = test_context.client.post(
        f"/ui/api/clients/{test_context.client_key}/calendar/meetings",
        headers=_admin_headers(),
        json={
            "new_lead": {
                "full_name": "Inline Calendar Lead",
                "phone": "+1 (555) 222-3030",
                "email": "inline@example.com",
                "city": "Ottawa",
            },
            "start_at": "2026-06-16T14:00",
            "duration_minutes": 30,
            "timezone": "America/Toronto",
            "title": "Inline intro call",
            "notes": "Created during meeting scheduling.",
        },
    )

    assert meeting_create.status_code == 200
    meeting = meeting_create.json()["meeting"]
    assert meeting["lead_name"] == "Inline Calendar Lead"
    assert meeting["lead_id"]

    leads = test_context.client.get(
        f"/ui/api/crm/leads?client_key={test_context.client_key}",
        headers=_admin_headers(),
    )
    assert leads.status_code == 200
    assert meeting["lead_id"] in {item["lead_id"] for item in leads.json()["items"]}


def test_owner_calendar_settings_endpoint_updates_client_booking_config(test_context):
    response = test_context.client.patch(
        f"/ui/api/owner/{test_context.client_key}/calendar",
        headers=_admin_headers(),
        json={
            "slot_minutes": 45,
            "notice_minutes": 90,
            "horizon_days": 21,
            "availability": [
                {"day": 0, "start": "09:00", "end": "17:00", "enabled": True},
                {"day": 1, "start": "09:00", "end": "17:00", "enabled": True},
            ],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["booking_mode"] == "internal"
    assert payload["internal_calendar"]["slot_minutes"] == 45

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        assert client.booking_mode == "internal"
        internal = (client.booking_config or {}).get("internal_calendar", {})
        assert internal.get("slot_minutes") == 45
        assert isinstance(internal.get("availability"), list)


def test_owner_workspace_can_send_manual_message_with_client_provider(test_context):
    runtime_update = test_context.client.patch(
        f"/admin/clients/{test_context.client_key}",
        headers=_admin_headers(),
        json={"provider_config": {"public_base_url": "https://owner-demo.ngrok-free.app"}},
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

    session_factory = get_session_factory()
    with session_factory() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        lead = Lead(
            client_id=client.id,
            source=LeadSource.MANUAL,
            full_name="Peter Test",
            phone="+15552223333",
            email="peter@example.com",
            city="Austin",
            form_answers={"timeline": "This week"},
            raw_payload={"created_from": "manual_test"},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.commit()
        lead_id = lead.id

    idempotent_headers = {**_admin_headers(), "Idempotency-Key": "manual-message-ui-test-001"}
    invalid_key = "manual-message-ui-invalid-001"
    invalid = test_context.client.post(
        f"/ui/api/conversations/{lead_id}/messages/manual",
        headers={**_admin_headers(), "Idempotency-Key": invalid_key},
        json={"body": "   "},
    )
    assert invalid.status_code == 400
    with session_factory() as db:
        assert db.scalar(select(OutboundRequest).where(OutboundRequest.idempotency_key == invalid_key)) is None

    sent_before = len(test_context.fake_sms.sent)
    manual = test_context.client.post(
        f"/ui/api/conversations/{lead_id}/messages/manual",
        headers=idempotent_headers,
        json={"body": "Checking in personally before we get you scheduled."},
    )
    assert manual.status_code == 200
    manual_payload = manual.json()
    assert manual_payload["state"] == "GREETED"
    assert "Checking in personally" in test_context.fake_sms.sent[-1]["body"]

    repeated = test_context.client.post(
        f"/ui/api/conversations/{lead_id}/messages/manual",
        headers=idempotent_headers,
        json={"body": "Checking in personally before we get you scheduled."},
    )
    assert repeated.status_code == 200
    assert repeated.json() == manual_payload
    assert len(test_context.fake_sms.sent) == sent_before + 1

    conflicting = test_context.client.post(
        f"/ui/api/conversations/{lead_id}/messages/manual",
        headers=idempotent_headers,
        json={"body": "A different message must not reuse the same key."},
    )
    assert conflicting.status_code == 409
    assert len(test_context.fake_sms.sent) == sent_before + 1

    thread = test_context.client.get(
        f"/ui/api/conversations/{lead_id}/thread",
        headers=_admin_headers(),
    )
    assert thread.status_code == 200
    thread_payload = thread.json()
    outbound_bodies = [message["body"] for message in thread_payload["messages"] if message["direction"] == "OUTBOUND"]
    assert any("Checking in personally" in body for body in outbound_bodies)


def test_owner_workspace_can_send_manual_media_message(test_context):
    runtime_update = test_context.client.patch(
        f"/admin/clients/{test_context.client_key}",
        headers=_admin_headers(),
        json={"provider_config": {"public_base_url": "https://owner-demo.ngrok-free.app"}},
    )
    assert runtime_update.status_code == 200

    session_factory = get_session_factory()
    with session_factory() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        lead = Lead(
            client_id=client.id,
            source=LeadSource.MANUAL,
            full_name="Media Contact",
            phone="+15552224444",
            email="media@example.com",
            city="Montreal",
            form_answers={},
            raw_payload={"created_from": "manual_media_test"},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.commit()
        lead_id = lead.id

    media_headers = {**_admin_headers(), "Idempotency-Key": "manual-media-ui-test-001"}
    media_sent_before = len(test_context.fake_sms.sent)
    response = test_context.client.post(
        f"/ui/api/conversations/{lead_id}/messages/manual-media",
        headers=media_headers,
        data={"body": "Voici la photo de la pièce."},
        files={"media": ("piece.jpg", b"\xff\xd8\xfffake-image-bytes", "image/jpeg")},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["state"] == "GREETED"
    assert payload["attachments"][0]["media_kind"] == "image"
    assert payload["attachments"][0]["url"].startswith("/media/public/")
    assert test_context.fake_sms.sent[-1]["media_urls"][0].startswith("https://owner-demo.ngrok-free.app/media/public/")

    repeated = test_context.client.post(
        f"/ui/api/conversations/{lead_id}/messages/manual-media",
        headers=media_headers,
        data={"body": "Voici la photo de la pièce."},
        files={"media": ("piece.jpg", b"\xff\xd8\xfffake-image-bytes", "image/jpeg")},
    )
    assert repeated.status_code == 200
    assert repeated.json() == payload
    assert len(test_context.fake_sms.sent) == media_sent_before + 1

    media_response = test_context.client.get(payload["attachments"][0]["url"])
    assert media_response.status_code == 200
    assert media_response.content == b"\xff\xd8\xfffake-image-bytes"
    assert media_response.headers["content-type"].startswith("image/jpeg")
    assert media_response.headers["cache-control"] == "private, no-store, max-age=0"

    thread = test_context.client.get(
        f"/ui/api/conversations/{lead_id}/thread",
        headers=_admin_headers(),
    )
    assert thread.status_code == 200
    thread_payload = thread.json()
    outbound = [message for message in thread_payload["messages"] if message["direction"] == "OUTBOUND"]
    assert outbound[-1]["attachments"][0]["filename"] == "piece.jpg"
    assert outbound[-1]["attachments"][0]["media_kind"] == "image"

    with session_factory() as db:
        attachment = db.scalar(select(MessageAttachment).where(MessageAttachment.lead_id == lead_id))
        assert attachment is not None
        assert attachment.content_type == "image/jpeg"
        assert attachment.public_expires_at is not None
        attachment.public_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        db.commit()

    expired = test_context.client.get(payload["attachments"][0]["url"])
    assert expired.status_code == 404


def test_owner_portal_can_view_and_update_ai_context(test_context):
    seed = test_context.client.post("/ui/api/seed-demo?reset=true", headers=_admin_headers())
    assert seed.status_code == 200

    login = test_context.client.post(
        "/ui/api/login/client/token",
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


def test_client_portal_login_is_scoped_to_own_leads_and_cannot_delete_conversation(test_context):
    seed = test_context.client.post("/ui/api/seed-demo?reset=true", headers=_admin_headers())
    assert seed.status_code == 200

    login = test_context.client.post(
        "/ui/api/login/client/token",
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
    assert delete_response.status_code == 403
    assert delete_response.json()["detail"] == "Admin access required"

    existing_thread = test_context.client.get(
        f"/ui/api/conversations/{delete_lead_id}/thread",
        headers=_portal_headers(token),
    )
    assert existing_thread.status_code == 200

    refreshed = test_context.client.get("/ui/api/conversations", headers=_portal_headers(token))
    assert refreshed.status_code == 200
    refreshed_ids = {item["lead_id"] for item in refreshed.json()["items"]}
    assert delete_lead_id in refreshed_ids


def test_client_portal_can_archive_and_restore_conversation(test_context):
    seed = test_context.client.post("/ui/api/seed-demo?reset=true", headers=_admin_headers())
    assert seed.status_code == 200

    login = test_context.client.post(
        "/ui/api/login/client/token",
        json={"email": "owner@demo-roofing.demo", "password": "demo-portal-2026"},
    )
    assert login.status_code == 200
    token = login.json()["token"]

    conversations = test_context.client.get("/ui/api/conversations", headers=_portal_headers(token))
    assert conversations.status_code == 200
    lead_id = conversations.json()["items"][0]["lead_id"]

    archive = test_context.client.patch(
        f"/ui/api/conversations/{lead_id}/archive",
        headers=_portal_headers(token),
        json={"archived": True},
    )
    assert archive.status_code == 200
    assert archive.json()["archived"] is True

    archived_thread = test_context.client.get(
        f"/ui/api/conversations/{lead_id}/thread",
        headers=_portal_headers(token),
    )
    assert archived_thread.status_code == 200
    archived_payload = archived_thread.json()
    assert "archived" in archived_payload["lead"]["tags"]
    assert any(event["event_type"] == "conversation_archived" for event in archived_payload["audit_events"])

    archived_detail = test_context.client.get(
        f"/ui/api/crm/leads/{lead_id}",
        headers=_portal_headers(token),
    )
    assert archived_detail.status_code == 200
    assert "archived" in archived_detail.json()["tags"]

    active_crm_leads = test_context.client.get("/ui/api/crm/leads", headers=_portal_headers(token))
    assert active_crm_leads.status_code == 200
    assert lead_id not in {item["lead_id"] for item in active_crm_leads.json()["items"]}

    archived_crm_leads = test_context.client.get("/ui/api/crm/leads?archived=true", headers=_portal_headers(token))
    assert archived_crm_leads.status_code == 200
    assert lead_id in {item["lead_id"] for item in archived_crm_leads.json()["items"]}

    restore = test_context.client.patch(
        f"/ui/api/conversations/{lead_id}/archive",
        headers=_portal_headers(token),
        json={"archived": False},
    )
    assert restore.status_code == 200
    assert restore.json()["archived"] is False

    restored_thread = test_context.client.get(
        f"/ui/api/conversations/{lead_id}/thread",
        headers=_portal_headers(token),
    )
    assert restored_thread.status_code == 200
    restored_payload = restored_thread.json()
    assert "archived" not in restored_payload["lead"]["tags"]
    assert any(event["event_type"] == "conversation_unarchived" for event in restored_payload["audit_events"])

    restored_crm_leads = test_context.client.get("/ui/api/crm/leads", headers=_portal_headers(token))
    assert restored_crm_leads.status_code == 200
    assert lead_id in {item["lead_id"] for item in restored_crm_leads.json()["items"]}


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


def test_crm_lead_detail_and_tasks_endpoints(test_context):
    seed = test_context.client.post("/ui/api/seed-demo?reset=true", headers=_admin_headers())
    assert seed.status_code == 200

    leads = test_context.client.get(
        "/ui/api/crm/leads?client_key=demo-roofing",
        headers=_admin_headers(),
    )
    assert leads.status_code == 200
    leads_payload = leads.json()
    assert leads_payload["total"] > 0
    assert "stages" in leads_payload
    lead_id = leads_payload["items"][0]["lead_id"]

    detail = test_context.client.get(
        f"/ui/api/crm/leads/{lead_id}",
        headers=_admin_headers(),
    )
    assert detail.status_code == 200
    detail_payload = detail.json()
    assert detail_payload["lead"]["crm_stage"]
    assert "timeline" in detail_payload

    stage_update = test_context.client.patch(
        f"/ui/api/crm/leads/{lead_id}/stage",
        headers=_admin_headers(),
        json={"stage": "Meeting Completed"},
    )
    assert stage_update.status_code == 200
    assert stage_update.json()["crm_stage"] == "Meeting Completed"

    tag_add = test_context.client.post(
        f"/ui/api/crm/leads/{lead_id}/tags",
        headers=_admin_headers(),
        json={"tag": "High Budget"},
    )
    assert tag_add.status_code == 200
    assert "high budget" in tag_add.json()["tags"]

    note_add = test_context.client.post(
        f"/ui/api/crm/leads/{lead_id}/notes",
        headers=_admin_headers(),
        json={"note": "CRM follow-up note from admin."},
    )
    assert note_add.status_code == 200

    task_create = test_context.client.post(
        f"/ui/api/crm/leads/{lead_id}/tasks",
        headers=_admin_headers(),
        json={
            "title": "Review after meeting",
            "description": "Send recap and next steps.",
            "due_date": "2026-03-12",
        },
    )
    assert task_create.status_code == 200
    task_payload = task_create.json()["task"]
    assert task_payload["status"] == "open"
    task_id = task_payload["id"]

    task_done = test_context.client.patch(
        f"/ui/api/crm/tasks/{task_id}",
        headers=_admin_headers(),
        json={"status": "done"},
    )
    assert task_done.status_code == 200
    assert task_done.json()["task"]["status"] == "done"

    refreshed_detail = test_context.client.get(
        f"/ui/api/crm/leads/{lead_id}",
        headers=_admin_headers(),
    )
    assert refreshed_detail.status_code == 200
    refreshed_payload = refreshed_detail.json()
    assert refreshed_payload["lead"]["crm_stage"] == "Meeting Completed"
    assert any(tag == "high budget" for tag in refreshed_payload["tags"])
    assert any("CRM follow-up note" in note["body"] for note in refreshed_payload["notes"])
    assert any(task["id"] == task_id and task["status"] == "done" for task in refreshed_payload["tasks"])
    assert any(item["type"] == "crm_stage" for item in refreshed_payload["timeline"])

    task_list = test_context.client.get(
        "/ui/api/crm/tasks?client_key=demo-roofing&status=done",
        headers=_admin_headers(),
    )
    assert task_list.status_code == 200
    assert any(item["id"] == task_id for item in task_list.json()["items"])


def test_crm_endpoints_are_scoped_for_client_portal(test_context):
    seed = test_context.client.post("/ui/api/seed-demo?reset=true", headers=_admin_headers())
    assert seed.status_code == 200

    login = test_context.client.post(
        "/ui/api/login/client/token",
        json={"email": "owner@demo-roofing.demo", "password": "demo-portal-2026"},
    )
    assert login.status_code == 200
    token = login.json()["token"]

    crm_leads = test_context.client.get("/ui/api/crm/leads", headers=_portal_headers(token))
    assert crm_leads.status_code == 200
    items = crm_leads.json()["items"]
    assert items
    assert all(item["client_key"] == "demo-roofing" for item in items)

    own_lead_id = items[0]["lead_id"]
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.get(Lead, own_lead_id)
        assert lead is not None
        db.add(
            AuditLog(
                client_id=lead.client_id,
                lead_id=lead.id,
                event_type="provider_internal_error",
                decision={"api_key": "must-not-reach-portal", "provider_payload": {"debug": True}},
            )
        )
        state_row = db.scalar(select(ConversationState).where(ConversationState.lead_id == lead.id).limit(1))
        assert state_row is not None
        state_row.metadata_json = {
            "source": "ui",
            "actor_role": "system",
            "provider_secret": "must-not-reach-portal-timeline",
        }
        db.commit()

    own_detail = test_context.client.get(
        f"/ui/api/crm/leads/{own_lead_id}",
        headers=_portal_headers(token),
    )
    assert own_detail.status_code == 200
    serialized = json.dumps(own_detail.json())
    assert "provider_internal_error" not in serialized
    assert "must-not-reach-portal" not in serialized

    own_thread = test_context.client.get(
        f"/ui/api/conversations/{own_lead_id}/thread",
        headers=_portal_headers(token),
    )
    assert own_thread.status_code == 200
    thread_serialized = json.dumps(own_thread.json())
    assert "must-not-reach-portal-timeline" not in thread_serialized
    assert "provider_secret" not in thread_serialized

    foreign_lead_id = test_context.client.get(
        "/ui/api/crm/leads?client_key=demo-legal",
        headers=_admin_headers(),
    ).json()["items"][0]["lead_id"]
    forbidden = test_context.client.get(
        f"/ui/api/crm/leads/{foreign_lead_id}",
        headers=_portal_headers(token),
    )
    assert forbidden.status_code == 404
