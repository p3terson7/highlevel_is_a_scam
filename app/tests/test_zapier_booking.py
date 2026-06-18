from __future__ import annotations

import httpx
from sqlalchemy import select

from app.db.models import AuditLog, Client, ConversationStateEnum, Lead, LeadSource
from app.db.session import get_session_factory
from app.services import zapier_booking


def _admin_headers() -> dict[str, str]:
    return {"X-Admin-Token": "test-admin-token"}


def test_manual_calendar_booking_posts_configured_zapier_payload(test_context, monkeypatch):
    webhook_url = "https://hooks.zapier.com/hooks/catch/test/booking/"
    captured: dict = {}
    session_factory = get_session_factory()

    with session_factory() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.provider_config = {"zapier_booking_webhook_url": webhook_url}
        lead = Lead(
            client_id=client.id,
            external_lead_id="zapier-lead-1",
            source=LeadSource.META,
            full_name="Zapier Payload Lead",
            phone="+15553334444",
            email="zapier.lead@example.com",
            city="Toronto",
            form_answers={
                "service_interest": "AI lead follow-up",
                "monthly_lead_volume": "80-100",
            },
            raw_payload={"source": "test"},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.QUALIFYING,
            crm_stage="Qualified",
        )
        db.add(lead)
        db.commit()
        lead_id = lead.id

    def fake_post_json(*, url: str, payload: dict, timeout_seconds: int) -> httpx.Response:
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout_seconds"] = timeout_seconds
        return httpx.Response(200, json={"ok": True}, request=httpx.Request("POST", url))

    monkeypatch.setattr(zapier_booking, "_post_json", fake_post_json)

    response = test_context.client.post(
        f"/ui/api/clients/{test_context.client_key}/calendar/meetings",
        headers=_admin_headers(),
        json={
            "lead_id": lead_id,
            "start_at": "2026-06-15T10:30",
            "duration_minutes": 45,
            "timezone": "America/Toronto",
            "title": "Strategy call",
            "notes": "Review lead routing.",
        },
    )

    assert response.status_code == 200
    assert response.json()["zapier_booking_webhook"]["status"] == "sent"
    assert response.json()["zapier_booking_webhook"]["dedupe_key"].startswith("calendar_booking:")
    assert captured["url"] == webhook_url
    payload = captured["payload"]
    assert payload["event_type"] == "calendar_booking.created"
    assert payload["schema_version"] == "2026-06-17"
    assert payload["trigger"] == "manual_calendar_booking_created"
    assert payload["client"]["client_key"] == test_context.client_key
    assert payload["lead"]["full_name"] == "Zapier Payload Lead"
    assert payload["lead"]["email"] == "zapier.lead@example.com"
    assert payload["form_answers"]["monthly_lead_volume"] == "80-100"
    assert payload["form"]["answers_map"]["monthly_lead_volume"] == "80-100"
    assert payload["form"]["question_answer_map"]["Monthly Lead Volume"] == "80-100"
    assert payload["meeting"]["title"] == "Acme Solar meeting - Zapier Payload Lead"
    assert payload["meeting"]["internal_title"] == "Strategy call"
    assert payload["meeting"]["date"] == "2026-06-15"
    assert payload["meeting"]["start_time"] == "10:30"
    assert payload["meeting"]["end_time"] == "11:15"
    assert payload["meeting"]["start_at_local"] == "2026-06-15T10:30:00-04:00"
    assert payload["meeting"]["end_at_local"] == "2026-06-15T11:15:00-04:00"
    assert payload["meeting"]["zapier_start_datetime"] == "2026-06-15T10:30:00-04:00"
    assert payload["meeting"]["zapier_end_datetime"] == "2026-06-15T11:15:00-04:00"
    assert payload["meeting"]["duration_minutes"] == 45
    assert payload["calendar_event"]["summary"] == "Acme Solar meeting - Zapier Payload Lead"
    assert payload["calendar_event"]["start_datetime"] == "2026-06-15T10:30:00-04:00"
    assert payload["calendar_event"]["end_datetime"] == "2026-06-15T11:15:00-04:00"
    assert payload["calendar_event"]["attendee_emails"] == ["zapier.lead@example.com"]
    assert "Notes: Review lead routing." in payload["calendar_event"]["description"]
    assert "Monthly Lead Volume: 80-100" in payload["calendar_event"]["description"]
    assert payload["calendar_event"]["description_text"] == payload["calendar_event"]["description"]
    assert "<strong>Meeting</strong><ul>" in payload["calendar_event"]["description_html"]
    assert "<li><strong>Internal notes:</strong> Review lead routing.</li>" in payload["calendar_event"]["description_html"]
    assert "Review lead routing." in payload["calendar_event"]["description_html"]
    assert "Monthly Lead Volume" in payload["calendar_event"]["description_html"]
    assert payload["email_confirmation"]["to"] == "zapier.lead@example.com"
    assert payload["email_confirmation"]["subject"] == "Acme Solar: meeting confirmed with Zapier Payload Lead"
    assert "Notes: Review lead routing." in payload["email_confirmation"]["body_text"]
    assert "Your meeting with Acme Solar is booked." in payload["email_confirmation"]["body_text"]
    assert "Meeting confirmed" in payload["email_confirmation"]["body_html"]
    assert "background:#f5f5f7" in payload["email_confirmation"]["body_html"]
    assert "background:#007aff" in payload["email_confirmation"]["body_html"]
    assert (
        payload["zapier_mapping_hints"]["google_calendar"]["start_date_time"]
        == "calendar_event.start_datetime"
    )

    with session_factory() as db:
        sent = db.scalar(
            select(AuditLog)
            .where(
                AuditLog.lead_id == lead_id,
                AuditLog.event_type == "zapier_booking_webhook_sent",
            )
            .limit(1)
        )
        assert sent is not None
        assert sent.decision["status_code"] == 200
        assert sent.decision["payload"]["lead"]["email"] == "zapier.lead@example.com"
        assert (
            sent.decision["payload"]["zapier_mapping_hints"]["important"]
            == payload["zapier_mapping_hints"]["important"]
        )


def test_zapier_booking_webhook_skips_clients_without_url(test_context, monkeypatch):
    calls = 0
    session_factory = get_session_factory()

    def fake_post_json(*, url: str, payload: dict, timeout_seconds: int) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr(zapier_booking, "_post_json", fake_post_json)

    with session_factory() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        lead = Lead(
            client_id=client.id,
            source=LeadSource.META,
            full_name="No Webhook Lead",
            phone="+15550000001",
            email="no-webhook@example.com",
            city="",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.commit()

        result = zapier_booking.notify_zapier_booking_webhook(
            db=db,
            client=client,
            lead=lead,
            calendar_booking={"provider": "internal", "booking": {"booking_id": 123}},
            trigger="test",
        )

    assert result["status"] == "skipped"
    assert result["reason"] == "not_configured"
    assert calls == 0
