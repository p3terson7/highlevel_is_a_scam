from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from app.db.models import AuditLog, Client, Lead, LeadSource
from app.db.session import get_session_factory
from app.scripts.lead_attribution_report import main


def _client_id() -> int:
    with get_session_factory()() as db:
        return db.query(Client.id).filter(Client.client_key == "test-client-key").scalar()


def test_report_returns_all_retained_leads_in_created_at_order(test_context, capsys):
    client_id = _client_id()
    newer = datetime(2026, 8, 10, 15, 20, tzinfo=timezone.utc)
    older = newer - timedelta(days=2)
    with get_session_factory()() as db:
        first_inserted = Lead(
            client_id=client_id,
            source=LeadSource.MANUAL,
            full_name="Later Person",
            phone="+15555550101",
            email="later@example.com",
            city="",
            form_answers={},
            raw_payload={},
            consented=False,
            created_at=newer,
            updated_at=newer,
        )
        second_inserted = Lead(
            client_id=client_id,
            source=LeadSource.LINKEDIN,
            full_name="Earlier Person",
            phone="",
            email="earlier@example.com",
            city="",
            form_answers={},
            raw_payload={},
            consented=False,
            created_at=older,
            updated_at=older,
        )
        db.add_all([first_inserted, second_inserted])
        db.commit()
        later_id = first_inserted.id
        earlier_id = second_inserted.id

    assert main(["--client-key", test_context.client_key]) == 0
    report = json.loads(capsys.readouterr().out)

    assert report["reported_lead_count"] == 2
    assert [lead["lead_id"] for lead in report["leads"]] == [earlier_id, later_id]
    assert report["leads"][0]["stored_source"] == "linkedin"
    assert report["first_created_at_utc"] == "2026-08-08T15:20:00Z"
    assert report["last_created_at_utc"] == "2026-08-10T15:20:00Z"


def test_report_includes_attribution_and_history_without_contact_pii(test_context, capsys):
    client_id = _client_id()
    created_at = datetime(2026, 8, 10, 15, 20, tzinfo=timezone.utc)
    with get_session_factory()() as db:
        lead = Lead(
            client_id=client_id,
            source=LeadSource.MANUAL,
            full_name="Secret Name",
            phone="+15555550102",
            email="secret@example.com",
            city="Secret City",
            form_answers={"private_question": "private answer"},
            raw_payload={
                "tracking": {
                    "utm_source": "chatgpt.com",
                    "utm_campaign": "precision",
                    "email": "must-not-print@example.com",
                },
                "source_page_url": (
                    "https://user:password@3dpreciscan.com/contact"
                    "?utm_source=chatgpt.com&email=must-not-print@example.com#private"
                ),
                "referrer": "https://3dpreciscan.com/realisations?customer=private",
                "raw_website_payload": {"source": "website"},
                "consent_evidence": {
                    "granted": False,
                    "status": "not_provided",
                    "source_fields": [],
                },
                "private": "must not print",
            },
            consented=False,
            created_at=created_at,
            updated_at=created_at,
        )
        db.add(lead)
        db.flush()
        db.add(
            AuditLog(
                client_id=client_id,
                lead_id=lead.id,
                event_type="lead_normalized",
                decision={
                    "source": "manual",
                    "created": True,
                    "should_send_initial_sms": False,
                    "external_lead_id": "private-external-id",
                },
                created_at=created_at,
            )
        )
        db.commit()
        lead_id = lead.id

    assert main(["--client-key", test_context.client_key]) == 0
    output = capsys.readouterr().out
    report = json.loads(output)
    item = next(lead for lead in report["leads"] if lead["lead_id"] == lead_id)

    assert item["created_at_utc"] == "2026-08-10T15:20:00Z"
    assert item["ingestion_kind"] == "website_form"
    assert item["phone_present"] is True
    assert item["email_present"] is True
    assert item["attribution"] == {
        "tracking": {"utm_campaign": "precision", "utm_source": "chatgpt.com"},
        "source_page_url": "https://3dpreciscan.com/contact?utm_source=chatgpt.com",
        "referrer": "https://3dpreciscan.com/realisations",
        "submitted_source": "website",
        "payload_source": None,
    }
    assert item["normalization_history"] == [
        {
            "timestamp_utc": "2026-08-10T15:20:00Z",
            "source": "manual",
            "created": True,
            "should_send_initial_sms": False,
        }
    ]
    for private_value in (
        "Secret Name",
        "+15555550102",
        "secret@example.com",
        "Secret City",
        "private answer",
        "must-not-print@example.com",
        "password",
        "private-external-id",
    ):
        assert private_value not in output


def test_latest_uses_created_at_then_id_and_empty_scope_is_valid(test_context, capsys):
    assert main(["--client-key", test_context.client_key]) == 0
    empty_report = json.loads(capsys.readouterr().out)
    assert empty_report["reported_lead_count"] == 0
    assert empty_report["first_created_at_utc"] is None
    assert empty_report["leads"] == []

    client_id = _client_id()
    same_time = datetime(2026, 8, 10, 15, 20, tzinfo=timezone.utc)
    with get_session_factory()() as db:
        for source in (LeadSource.META, LeadSource.LINKEDIN):
            db.add(
                Lead(
                    client_id=client_id,
                    source=source,
                    full_name="",
                    phone="",
                    email=f"{source.value}@example.com",
                    city="",
                    form_answers={},
                    raw_payload="malformed",
                    consented=False,
                    created_at=same_time,
                    updated_at=same_time,
                )
            )
        db.commit()
        newest_id = db.query(Lead.id).order_by(Lead.id.desc()).first()[0]

    assert main(["--client-key", test_context.client_key, "--latest"]) == 0
    latest_report = json.loads(capsys.readouterr().out)
    assert latest_report["reported_lead_count"] == 1
    assert latest_report["leads"][0]["lead_id"] == newest_id
    assert latest_report["leads"][0]["attribution"]["tracking"] == {}
    assert latest_report["leads"][0]["consent"]["evidence"] == {}
