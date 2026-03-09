from sqlalchemy import select

from app.db.models import AuditLog, Lead, Message, MessageDirection
from app.db.session import get_session_factory


def test_meta_webhook_intake_and_initial_sms(test_context):
    payload = {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "leadgen_id": "meta-lead-001",
                            "field_data": [
                                {"name": "full_name", "values": ["Jane Prospect"]},
                                {"name": "phone_number", "values": ["+1 (555) 123-4567"]},
                                {"name": "email", "values": ["jane@example.com"]},
                                {"name": "city", "values": ["Austin"]},
                            ],
                        }
                    }
                ]
            }
        ]
    }

    response = test_context.client.post(f"/webhooks/meta/{test_context.client_key}", json=payload)

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(
            select(Lead).where(Lead.external_lead_id == "meta-lead-001", Lead.client_id == 1)
        )
        assert lead is not None
        assert lead.phone == "+15551234567"

        outbound_messages = db.scalars(
            select(Message).where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.OUTBOUND,
            )
        ).all()
        assert len(outbound_messages) >= 1


def test_meta_webhook_fetches_leadgen_details_and_sends_ai_initial_sms(test_context, monkeypatch):
    captured: dict[str, object] = {}

    class FakeMetaResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "field_data": [
                    {"name": "full_name", "values": ["Morgan Prospect"]},
                    {"name": "phone_number", "values": ["(555) 222-9999"]},
                    {"name": "email", "values": ["morgan@example.com"]},
                    {"name": "city", "values": ["Houston"]},
                ],
                "created_time": "2026-03-04T10:00:00+0000",
            }

    def fake_meta_get(url: str, params: dict | None = None, timeout: int = 20):
        captured["url"] = url
        captured["params"] = params or {}
        captured["timeout"] = timeout
        return FakeMetaResponse()

    monkeypatch.setattr("app.services.lead_intake.httpx.get", fake_meta_get)

    headers = {"X-Admin-Token": "test-admin-token"}
    runtime_update = test_context.client.put(
        "/admin/runtime-config",
        headers=headers,
        json={
            "meta_access_token": "meta-token-for-tests",
            "meta_graph_api_version": "v22.0",
        },
    )
    assert runtime_update.status_code == 200

    payload = {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "leadgen_id": "meta-lead-graph-001",
                        }
                    }
                ]
            }
        ]
    }

    response = test_context.client.post(f"/webhooks/meta/{test_context.client_key}", json=payload)
    assert response.status_code == 202
    assert response.json()["status"] == "accepted"

    assert captured["url"] == "https://graph.facebook.com/v22.0/meta-lead-graph-001"
    assert captured["params"] == {
        "fields": "field_data,created_time",
        "access_token": "meta-token-for-tests",
    }

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(
            select(Lead).where(Lead.external_lead_id == "meta-lead-graph-001", Lead.client_id == 1)
        )
        assert lead is not None
        assert lead.phone == "+15552229999"
        assert lead.email == "morgan@example.com"
        assert lead.conversation_state.value == "QUALIFYING"

        outbound_messages = db.scalars(
            select(Message).where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.OUTBOUND,
            )
        ).all()
        assert len(outbound_messages) >= 1


def test_zapier_webhook_accepts_flat_payload_and_processes_meta_flow(test_context):
    payload = {
        "id": "zap-lead-001",
        "full_name": "Zap Prospect",
        "phone_number": "+1 (555) 888-1212",
        "email": "zap@example.com",
        "city": "Dallas",
    }
    response = test_context.client.post(f"/webhooks/zapier/{test_context.client_key}", json=payload)

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    assert response.json()["source"] == "zapier"

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        zapier_log = db.scalar(
            select(AuditLog).where(
                AuditLog.client_id == 1,
                AuditLog.event_type == "zapier_webhook_received",
            )
        )
        assert zapier_log is not None

        lead = db.scalar(
            select(Lead).where(Lead.external_lead_id == "zap-lead-001", Lead.client_id == 1)
        )
        assert lead is not None
        assert lead.phone == "+15558881212"
        assert lead.email == "zap@example.com"

        outbound_messages = db.scalars(
            select(Message).where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.OUTBOUND,
            )
        ).all()
        assert len(outbound_messages) >= 1


def test_zapier_webhook_parses_blob_payload_into_context_fields(test_context):
    payload = {
        "": (
            'Full Name : "Peter Lead", '
            'Email : "lead@lead.com", '
            'Phone : "+14387253890", '
            'Business Type : "Software & Technology", '
            'Biggest Marketing Challenge : "Getting more leads, Converting leads into clients", '
            'Running Ads? : "Yes, Google Ads", '
            'When to start : "Immediately"'
        )
    }
    response = test_context.client.post(f"/webhooks/zapier/{test_context.client_key}", json=payload)

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(
            select(Lead).where(Lead.phone == "+14387253890", Lead.client_id == 1)
        )
        assert lead is not None
        assert lead.full_name == "Peter Lead"
        assert lead.email == "lead@lead.com"
        assert lead.form_answers.get("business_type") == "Software & Technology"
        assert lead.form_answers.get("running_ads") == "Yes, Google Ads"
        assert lead.form_answers.get("when_to_start") == "Immediately"
        assert "Getting more leads" in (lead.form_answers.get("biggest_marketing_challenge") or "")

        outbound_messages = db.scalars(
            select(Message).where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.OUTBOUND,
            )
        ).all()
        assert len(outbound_messages) >= 1
