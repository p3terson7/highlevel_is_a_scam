from __future__ import annotations

import hashlib
import hmac
from types import SimpleNamespace

import httpx
from sqlalchemy import select

from app.db.models import AuditLog, Client, ConversationStateEnum, Lead, LeadSource, OutboundRequest
from app.db.session import get_session_factory
from app.services import zapier_booking


def _admin_headers() -> dict[str, str]:
    return {"X-Admin-Token": "test-admin-token-32-characters-long!"}


def test_outbound_booking_signing_uses_only_dedicated_secret(monkeypatch):
    payload = {"event_id": "event-123", "meeting": {"dedupe_key": "booking-123"}}
    monkeypatch.setattr(zapier_booking.time, "time", lambda: 1_700_000_000)
    monkeypatch.setattr(
        zapier_booking,
        "get_settings",
        lambda: SimpleNamespace(zapier_booking_webhook_secret=""),
    )

    client = SimpleNamespace(
        provider_config={
            "crm_webhook_secret": "inbound-secret",
            "zapier_webhook_secret": "legacy-inbound-secret",
            "zapier_booking_webhook_secret": "outbound-secret",
        }
    )
    headers = zapier_booking._outbound_signature_headers(client=client, payload=payload)
    signed = b"1700000000." + zapier_booking._canonical_json(payload)
    expected = hmac.new(b"outbound-secret", signed, hashlib.sha256).hexdigest()

    assert headers == {
        "X-LeadOps-Event-Id": "event-123",
        "X-LeadOps-Timestamp": "1700000000",
        "X-LeadOps-Signature": f"sha256={expected}",
    }

    legacy_only = SimpleNamespace(provider_config={"zapier_webhook_secret": "legacy-secret"})
    assert zapier_booking._outbound_signature_headers(client=legacy_only, payload=payload) == {}

    monkeypatch.setattr(
        zapier_booking,
        "get_settings",
        lambda: SimpleNamespace(zapier_booking_webhook_secret="deployment-outbound-secret"),
    )
    fallback_headers = zapier_booking._outbound_signature_headers(
        client=legacy_only,
        payload=payload,
    )
    fallback_expected = hmac.new(
        b"deployment-outbound-secret",
        signed,
        hashlib.sha256,
    ).hexdigest()
    assert fallback_headers["X-LeadOps-Signature"] == f"sha256={fallback_expected}"


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
        assert sent.decision["event_id"] == payload["event_id"]
        assert "payload" not in sent.decision


def test_manual_calendar_booking_payload_respects_french_workspace_language(test_context, monkeypatch):
    webhook_url = "https://hooks.zapier.com/hooks/catch/test/booking-fr/"
    captured: dict = {}
    session_factory = get_session_factory()

    with session_factory() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.provider_config = {"zapier_booking_webhook_url": webhook_url, "language": "fr"}
        lead = Lead(
            client_id=client.id,
            external_lead_id="zapier-lead-fr-1",
            source=LeadSource.META,
            full_name="Camille Tremblay",
            phone="+15145550123",
            email="camille@example.com",
            city="Montréal",
            form_answers={"objectif": "Réserver une consultation"},
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
            "title": "Appel stratégique",
            "notes": "Valider les prochaines étapes.",
        },
    )

    assert response.status_code == 200
    payload = captured["payload"]
    assert payload["meeting"]["title"] == "Acme Solar rencontre - Camille Tremblay"
    assert payload["meeting"]["time_range_label"] == "lundi 15 juin 2026 de 10 h 30 à 11 h 15 EDT"
    assert payload["calendar_event"]["location"] == "Appel de consultation"
    assert "RENCONTRE" in payload["calendar_event"]["description"]
    assert "Quand: lundi 15 juin 2026 de 10 h 30 à 11 h 15 EDT" in payload["calendar_event"]["description"]
    assert "RÉPONSES DU FORMULAIRE" in payload["calendar_event"]["description"]
    assert "Meeting confirmed" not in payload["email_confirmation"]["body_html"]
    assert "Rencontre confirmée" in payload["email_confirmation"]["body_html"]
    assert payload["email_confirmation"]["subject"] == "Acme Solar: rencontre confirmée avec Camille Tremblay"
    assert "Votre rencontre avec Acme Solar est réservée." in payload["email_confirmation"]["body_text"]
    assert "When:" not in payload["email_confirmation"]["body_text"]


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


def test_zapier_booking_url_validation_rejects_ssrf_targets():
    rejected = [
        "http://hooks.zapier.com/hooks/catch/1/2/",
        "https://127.0.0.1/hooks/catch/1/2/",
        "https://169.254.169.254/latest/meta-data/",
        "https://hooks.zapier.com.evil.example/hooks/catch/1/2/",
        "https://user:password@hooks.zapier.com/hooks/catch/1/2/",
        "https://hooks.zapier.com:8443/hooks/catch/1/2/",
        "https://hooks.zapier.com/other/path",
    ]

    assert zapier_booking._valid_webhook_url(
        "https://hooks.zapier.com/hooks/catch/123/abc/"
    )
    assert all(not zapier_booking._valid_webhook_url(url) for url in rejected)


def test_zapier_booking_delivery_is_durably_deduplicated(test_context, monkeypatch):
    session_factory = get_session_factory()
    calls = 0

    def fake_post_json(*, url: str, payload: dict, timeout_seconds: int) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr(zapier_booking, "_post_json", fake_post_json)
    with session_factory() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.provider_config = {
            "zapier_booking_webhook_url": "https://hooks.zapier.com/hooks/catch/test/dedupe/"
        }
        lead = Lead(
            client_id=client.id,
            source=LeadSource.META,
            full_name="Dedupe Lead",
            phone="+15550009999",
            email="dedupe@example.com",
            city="",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.commit()

        calendar_booking = {"provider": "internal", "booking": {"booking_id": 987}}
        first = zapier_booking.notify_zapier_booking_webhook(
            db=db,
            client=client,
            lead=lead,
            calendar_booking=calendar_booking,
            trigger="test",
        )
        second = zapier_booking.notify_zapier_booking_webhook(
            db=db,
            client=client,
            lead=lead,
            calendar_booking=calendar_booking,
            trigger="test",
        )
        reservation = db.scalar(
            select(OutboundRequest).where(
                OutboundRequest.client_id == client.id,
                OutboundRequest.request_kind == "zapier_booking_webhook",
            )
        )

    assert first["status"] == "sent"
    assert second == {
        "status": "skipped",
        "reason": "already_sent",
        "dedupe_key": first["dedupe_key"],
        "event_id": first["event_id"],
    }
    assert calls == 1
    assert reservation is not None
    assert reservation.status == "completed"
    assert "delivery_payload" not in reservation.response_json
