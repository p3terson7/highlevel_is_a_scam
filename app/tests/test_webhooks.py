from sqlalchemy import select

from app.db.models import AuditLog, Lead, LeadSource, Message, MessageDirection
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
        assert lead.crm_stage == "Contacted"

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
    client_update = test_context.client.patch(
        f"/admin/clients/{test_context.client_key}",
        headers=headers,
        json={
            "provider_config": {
                "meta_access_token": "meta-token-for-tests",
                "meta_graph_api_version": "v22.0",
            }
        },
    )
    assert client_update.status_code == 200

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
        assert lead.crm_stage == "Contacted"

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
        assert lead.crm_stage == "Contacted"

        outbound_messages = db.scalars(
            select(Message).where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.OUTBOUND,
            )
        ).all()
        assert len(outbound_messages) >= 1


def test_zapier_webhook_uses_client_scoped_secret_when_configured(test_context):
    headers = {"X-Admin-Token": "test-admin-token"}
    patch = test_context.client.patch(
        f"/admin/clients/{test_context.client_key}",
        headers=headers,
        json={"provider_config": {"zapier_webhook_secret": "zapier-secret-123"}},
    )
    assert patch.status_code == 200

    payload = {
        "id": "zap-secret-lead-001",
        "full_name": "Secret Zap Prospect",
        "phone_number": "+1 (555) 888-1313",
    }

    rejected = test_context.client.post(f"/webhooks/zapier/{test_context.client_key}", json=payload)
    assert rejected.status_code == 403

    accepted = test_context.client.post(
        f"/webhooks/zapier/{test_context.client_key}",
        headers={"X-Zapier-Webhook-Secret": "zapier-secret-123"},
        json=payload,
    )
    assert accepted.status_code == 202


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
        assert {
            "question": "Business Type",
            "answer": "Software & Technology",
        } in lead.raw_payload["submitted_form_answers"]
        assert lead.crm_stage == "Contacted"


def test_website_form_webhook_uses_linkedin_utm_as_source(test_context):
    payload = {
        "source_page_url": "https://3dpreciscan.com/soumission?utm_source=linkedin&utm_medium=paid_social",
        "referrer": "https://www.linkedin.com/",
        "lead": {
            "full_name": "LinkedIn Website Lead",
            "phone": "+1 (555) 333-4444",
            "email": "linkedin.website@example.com",
        },
        "form_answers": {
            "type_client": "Company",
            "service_interest": "Scan 3D",
            "timeline": "2 semaines",
            "message": "Besoin d'une soumission pour une pièce industrielle.",
        },
        "tracking": {
            "utm_source": "linkedin",
            "utm_medium": "paid_social",
            "utm_campaign": "scan-3d-linkedin",
            "utm_content": "soumission-button",
        },
    }

    response = test_context.client.post(f"/webhooks/form/{test_context.client_key}", json=payload)

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    assert response.json()["source"] == "linkedin"

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(
            select(Lead).where(Lead.email == "linkedin.website@example.com", Lead.client_id == 1)
        )
        assert lead is not None
        assert lead.source == LeadSource.LINKEDIN
        assert lead.phone == "+15553334444"
        assert lead.form_answers["utm_source"] == "linkedin"
        assert lead.form_answers["utm_campaign"] == "scan-3d-linkedin"
        assert lead.form_answers["service_interest"] == "Scan 3D"
        assert lead.crm_stage == "Contacted"

        webhook_log = db.scalar(
            select(AuditLog).where(
                AuditLog.client_id == 1,
                AuditLog.event_type == "website_form_webhook_received",
            )
        )
        assert webhook_log is not None
        assert webhook_log.decision["queued_source"] == "linkedin"

        outbound_messages = db.scalars(
            select(Message).where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.OUTBOUND,
            )
        ).all()
        assert len(outbound_messages) >= 1


def test_website_form_webhook_parses_zapier_key_value_blob(test_context):
    payload = {
        "": (
            "source = linkedin\n\n"
            "lead.id = urn:li:adFormResponse:4719df71-8cfa-49e5-8db6-85464ebea38a-6\n"
            "lead.full_name = Peter TestLead\n"
            "lead.first_name = Peter\n"
            "lead.last_name = TestLead\n"
            "lead.email = peterlead@email.com\n"
            "lead.phone = 4381231212\n\n"
            "form_answers.email = peterlead@email.com\n"
            "form_answers.phone = 4381231212\n"
            "form_answers.first_name = Peter\n"
            "form_answers.last_name = TestLead\n"
            "form_answers.besoin_principal = Inspection dimensionnelle / Conformité\n"
            "form_answers.type_piece_equipement = Pièce plastique moulée / thermoformée\n"
            "form_answers.echeance = Urgent : moins de 7 jours\n"
            "form_answers.form_name = LeadGen\n"
            "form_answers.linkedin_lead_id = 1022600223/4719df71-8cfa-49e5-8db6-85464ebea38a-6\n\n"
            "tracking.utm_source = linkedin\n"
            "tracking.utm_medium = lead_gen_form\n"
            "tracking.utm_campaign = LeadGen\n"
            "tracking.utm_content = LeadGen\n"
            "tracking.ad_id = "
        )
    }

    response = test_context.client.post(f"/webhooks/form/{test_context.client_key}", json=payload)

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    assert response.json()["source"] == "linkedin"

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.email == "peterlead@email.com", Lead.client_id == 1))
        assert lead is not None
        assert lead.source == LeadSource.LINKEDIN
        assert lead.full_name == "Peter TestLead"
        assert lead.phone == "+14381231212"
        assert lead.external_lead_id == "urn:li:adFormResponse:4719df71-8cfa-49e5-8db6-85464ebea38a-6"
        assert lead.form_answers["besoin_principal"] == "Inspection dimensionnelle / Conformité"
        assert lead.form_answers["type_piece_equipement"] == "Pièce plastique moulée / thermoformée"
        assert lead.form_answers["echeance"] == "Urgent : moins de 7 jours"
        assert lead.form_answers["utm_source"] == "linkedin"
        assert lead.crm_stage == "Contacted"
