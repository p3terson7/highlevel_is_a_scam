import hashlib
import hmac
import json
import time
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from app.api.routes_webhooks import (
    _check_webhook_rate,
    _reset_webhook_admission_state,
    _verify_webhook_authentication,
)
from app.core.config import Settings
from app.core.deps import get_app_settings
from app.db.models import (
    AuditLog,
    InboundWebhookEvent,
    Lead,
    LeadSource,
    Message,
    MessageDirection,
)
from app.db.session import get_session_factory


def _sms_consent() -> dict[str, object]:
    return {
        "sms": True,
        "method": "explicit_checkbox",
        "captured_at": "2026-07-13T12:00:00Z",
        "form": "pytest",
        "text": "I agree to receive text messages about this request.",
    }


def test_retired_meta_and_linkedin_webhooks_fail_closed(test_context):
    responses = [
        test_context.client.get(
            f"/webhooks/meta/{test_context.client_key}",
            params={
                "hub.mode": "subscribe",
                "hub.verify_token": "unused-token",
                "hub.challenge": "challenge",
            },
        ),
        test_context.client.post(
            f"/webhooks/meta/{test_context.client_key}",
            json={"entry": [{"changes": [{"value": {"leadgen_id": "ignored"}}]}]},
        ),
        test_context.client.post(
            f"/webhooks/linkedin/{test_context.client_key}",
            json={"elements": [{"id": "ignored"}]},
        ),
    ]

    assert all(response.status_code == 410 for response in responses)
    assert "retired" in responses[0].json()["detail"].lower()
    assert "retired" in responses[1].json()["detail"].lower()
    assert "retired" in responses[2].json()["detail"].lower()

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        retired_logs = db.scalars(
            select(AuditLog).where(
                AuditLog.event_type.in_({"meta_webhook_received", "linkedin_webhook_received"})
            )
        ).all()
        assert retired_logs == []


def test_zapier_webhook_accepts_flat_payload_and_processes_meta_flow(test_context):
    payload = {
        "id": "zap-lead-001",
        "full_name": "Zap Prospect",
        "phone_number": "+1 (555) 888-1212",
        "email": "zap@example.com",
        "city": "Dallas",
        "consent": _sms_consent(),
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


def test_zapier_webhook_preserves_explicit_linkedin_attribution(test_context):
    payload = {
        "id": "zap-linkedin-lead-001",
        "source": "linkedin",
        "full_name": "LinkedIn Zap Prospect",
        "phone_number": "+1 (555) 888-1414",
        "email": "linkedin.zap@example.com",
        "city": "Toronto",
    }

    response = test_context.client.post(f"/webhooks/zapier/{test_context.client_key}", json=payload)

    assert response.status_code == 202
    assert response.json()["queued_source"] == "linkedin"

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(
            select(Lead).where(Lead.external_lead_id == "zap-linkedin-lead-001", Lead.client_id == 1)
        )
        assert lead is not None
        assert lead.source == LeadSource.LINKEDIN
        assert lead.phone == "+15558881414"
        webhook_log = db.scalar(
            select(AuditLog).where(
                AuditLog.client_id == 1,
                AuditLog.event_type == "zapier_webhook_received",
            ).order_by(AuditLog.id.desc())
        )
        assert webhook_log is not None
        assert webhook_log.decision["queued_source"] == "linkedin"


def test_zapier_webhook_detects_nested_and_utm_linkedin_attribution(test_context):
    payloads = [
        {
            "id": "zap-linkedin-utm-001",
            "source": "zapier",
            "utm_source": "linkedin",
            "full_name": "LinkedIn UTM Prospect",
            "phone_number": "+1 (555) 888-1515",
        },
        {
            "lead": {
                "id": "zap-linkedin-nested-001",
                "tracking": {"utm_source": "linkedin_ads"},
                "full_name": "Nested LinkedIn Prospect",
                "phone_number": "+1 (555) 888-1616",
            }
        },
        {
            "id": "zap-linkedin-answers-001",
            "form_answers": {"utm_source": "linkedin"},
            "full_name": "LinkedIn Answers Prospect",
            "phone_number": "+1 (555) 888-1717",
        },
        {
            "": (
                'Lead ID : "zap-linkedin-blob-001", '
                'Full Name : "LinkedIn Blob Prospect", '
                'Phone : "+1 (555) 888-1818", '
                'Source : "linkedin"'
            )
        },
    ]

    for payload in payloads:
        response = test_context.client.post(f"/webhooks/zapier/{test_context.client_key}", json=payload)
        assert response.status_code == 202
        assert response.json()["queued_source"] == "linkedin"

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        leads = db.scalars(
            select(Lead).where(
                Lead.external_lead_id.in_(
                    {
                        "zap-linkedin-utm-001",
                        "zap-linkedin-nested-001",
                        "zap-linkedin-answers-001",
                        "zap-linkedin-blob-001",
                    }
                ),
                Lead.client_id == 1,
            )
        ).all()
        assert len(leads) == 4
        assert all(lead.source == LeadSource.LINKEDIN for lead in leads)


def test_zapier_webhook_uses_client_scoped_secret_when_configured(test_context):
    headers = {"X-Admin-Token": "test-admin-token-32-characters-long!"}
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

    query_secret_rejected = test_context.client.post(
        f"/webhooks/zapier/{test_context.client_key}",
        params={"webhook_secret": "zapier-secret-123"},
        json=payload,
    )
    assert query_secret_rejected.status_code == 403

    accepted = test_context.client.post(
        f"/webhooks/zapier/{test_context.client_key}",
        headers={"X-Zapier-Webhook-Secret": "zapier-secret-123"},
        json=payload,
    )
    assert accepted.status_code == 202


def test_webhook_hmac_authentication_replay_and_consent_evidence(test_context):
    admin_headers = {"X-Admin-Token": "test-admin-token-32-characters-long!"}
    secret = "webhook-hmac-secret-123"
    patch = test_context.client.patch(
        f"/admin/clients/{test_context.client_key}",
        headers=admin_headers,
        json={"provider_config": {"crm_webhook_secret": secret}},
    )
    assert patch.status_code == 200

    payload = {
        "id": "hmac-consented-lead-001",
        "full_name": "HMAC Lead",
        "phone_number": "+1 (555) 888-1919",
        "email": "hmac@example.com",
        "consent": _sms_consent(),
    }
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    timestamp = str(int(time.time()))
    signature = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    signed_headers = {
        "Content-Type": "application/json",
        "X-CRM-Webhook-Timestamp": timestamp,
        "X-CRM-Webhook-Signature": f"sha256={signature}",
    }

    accepted = test_context.client.post(
        f"/webhooks/zapier/{test_context.client_key}", content=body, headers=signed_headers
    )
    duplicate = test_context.client.post(
        f"/webhooks/zapier/{test_context.client_key}", content=body, headers=signed_headers
    )

    assert accepted.status_code == 202
    assert duplicate.status_code == 202
    assert duplicate.json()["duplicate"] is True

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.external_lead_id == "hmac-consented-lead-001"))
        assert lead is not None
        assert lead.consented is True
        assert lead.raw_payload["consent_evidence"]["granted"] is True
        assert lead.raw_payload["consent_evidence"]["method"] == "explicit_checkbox"
        outbound_messages = db.scalars(
            select(Message).where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.OUTBOUND,
            )
        ).all()
        assert len(outbound_messages) == 1
        logs = db.scalars(
            select(AuditLog).where(AuditLog.event_type == "zapier_webhook_received")
        ).all()
        assert len(logs) == 1
        assert logs[0].decision["authentication"] == "hmac-sha256"
        assert "payload" not in logs[0].decision
        inbox_events = db.scalars(
            select(InboundWebhookEvent).where(
                InboundWebhookEvent.client_id == lead.client_id,
                InboundWebhookEvent.endpoint == "zapier",
            )
        ).all()
        assert len(inbox_events) == 1
        assert inbox_events[0].status == "completed"
        assert inbox_events[0].attempt_count == 1
        assert inbox_events[0].payload_json == {}

    stale_timestamp = str(int(time.time()) - 301)
    stale_body = b'{"id":"stale","phone_number":"+15558881920"}'
    stale_signature = hmac.new(
        secret.encode(), stale_timestamp.encode() + b"." + stale_body, hashlib.sha256
    ).hexdigest()
    stale = test_context.client.post(
        f"/webhooks/zapier/{test_context.client_key}",
        content=stale_body,
        headers={
            "Content-Type": "application/json",
            "X-CRM-Webhook-Timestamp": stale_timestamp,
            "X-CRM-Webhook-Signature": f"sha256={stale_signature}",
        },
    )
    assert stale.status_code == 403

    invalid = test_context.client.post(
        f"/webhooks/zapier/{test_context.client_key}",
        content=b'{"id":"invalid","phone_number":"+15558881921"}',
        headers={
            "Content-Type": "application/json",
            "X-CRM-Webhook-Timestamp": str(int(time.time())),
            "X-CRM-Webhook-Signature": "sha256=" + ("0" * 64),
        },
    )
    assert invalid.status_code == 403


def test_production_webhook_without_configured_secret_fails_closed(test_context):
    from app.main import app

    app.dependency_overrides[get_app_settings] = lambda: Settings(
        env="production",
        admin_token="test-admin-token-32-characters-long!",
    )
    try:
        response = test_context.client.post(
            f"/webhooks/form/{test_context.client_key}",
            json={"lead": {"email": "production@example.com"}},
        )
    finally:
        app.dependency_overrides.pop(get_app_settings, None)

    assert response.status_code == 503
    assert "authentication is not configured" in response.json()["detail"]


def test_local_unsigned_webhook_requires_opt_in_and_loopback_origin():
    loopback_request = SimpleNamespace(headers={}, client=SimpleNamespace(host="127.0.0.1"))
    remote_request = SimpleNamespace(headers={}, client=SimpleNamespace(host="192.0.2.10"))

    with pytest.raises(HTTPException) as disabled:
        _verify_webhook_authentication(
            request=loopback_request,
            raw_body=b"{}",
            effective_runtime={},
            settings=Settings(env="local", allow_unsigned_crm_webhooks=False),
        )
    assert disabled.value.status_code == 503

    assert (
        _verify_webhook_authentication(
            request=loopback_request,
            raw_body=b"{}",
            effective_runtime={},
            settings=Settings(env="local", allow_unsigned_crm_webhooks=True),
        )
        == "unsigned-loopback-dev"
    )

    with pytest.raises(HTTPException) as remote:
        _verify_webhook_authentication(
            request=remote_request,
            raw_body=b"{}",
            effective_runtime={},
            settings=Settings(env="local", allow_unsigned_crm_webhooks=True),
        )
    assert remote.value.status_code == 403


def test_inbound_crm_secret_is_independent_from_outbound_zapier_secret():
    request = SimpleNamespace(
        headers={"X-CRM-Webhook-Secret": "outbound-booking-secret"},
        client=SimpleNamespace(host="127.0.0.1"),
    )
    runtime = {
        "crm_webhook_secret": "inbound-crm-secret",
        "zapier_booking_webhook_secret": "outbound-booking-secret",
        "zapier_webhook_secret": "legacy-inbound-secret",
    }
    with pytest.raises(HTTPException) as wrong_secret:
        _verify_webhook_authentication(
            request=request,
            raw_body=b"{}",
            effective_runtime=runtime,
            settings=Settings(env="production"),
        )
    assert wrong_secret.value.status_code == 403

    request.headers["X-CRM-Webhook-Secret"] = "inbound-crm-secret"
    assert (
        _verify_webhook_authentication(
            request=request,
            raw_body=b"{}",
            effective_runtime=runtime,
            settings=Settings(env="production"),
        )
        == "legacy-header"
    )


def test_hmac_timestamp_requires_ascii_digits():
    request = SimpleNamespace(
        headers={
            "X-CRM-Webhook-Timestamp": "١٧٨٣٨٩١٢٠٠",
            "X-CRM-Webhook-Signature": "sha256=" + ("0" * 64),
        }
    )
    with pytest.raises(HTTPException) as exc_info:
        _verify_webhook_authentication(
            request=request,
            raw_body=b"{}",
            effective_runtime={"crm_webhook_secret": "shared-secret"},
            settings=Settings(
                env="production",
                admin_token="test-admin-token-32-characters-long!",
            ),
        )
    assert exc_info.value.status_code == 403


def test_webhook_rejects_empty_oversized_and_excessive_batch_payloads(test_context):
    empty = test_context.client.post(f"/webhooks/zapier/{test_context.client_key}", json={})
    assert empty.status_code == 422
    assert "phone number or email" in empty.json()["detail"]

    too_many = test_context.client.post(
        f"/webhooks/zapier/{test_context.client_key}",
        json={
            "leads": [
                {"id": f"batch-{index}", "phone": f"+1555000{index:04d}"}
                for index in range(11)
            ]
        },
    )
    assert too_many.status_code == 422
    assert "10-lead batch limit" in too_many.json()["detail"]

    oversized = test_context.client.post(
        f"/webhooks/zapier/{test_context.client_key}",
        content=json.dumps({"blob": "a" * (128 * 1024)}).encode(),
        headers={"Content-Type": "application/json"},
    )
    assert oversized.status_code == 413

    field_too_long = test_context.client.post(
        f"/webhooks/zapier/{test_context.client_key}",
        json={"email": "valid@example.com", "notes": "a" * (8 * 1024 + 1)},
    )
    assert field_too_long.status_code == 422
    assert "field value is too long" in field_too_long.json()["detail"]

    duplicate_field = test_context.client.post(
        f"/webhooks/zapier/{test_context.client_key}",
        content=b'{"email":"first@example.com","email":"second@example.com"}',
        headers={"Content-Type": "application/json"},
    )
    assert duplicate_field.status_code == 400

    non_standard_number = test_context.client.post(
        f"/webhooks/zapier/{test_context.client_key}",
        content=b'{"email":"valid@example.com","score":NaN}',
        headers={"Content-Type": "application/json"},
    )
    assert non_standard_number.status_code == 400


def test_webhook_defaults_sms_consent_to_false(test_context):
    response = test_context.client.post(
        f"/webhooks/zapier/{test_context.client_key}",
        json={
            "id": "unconsented-lead-001",
            "full_name": "Email Only Permission",
            "phone_number": "+1 (555) 888-2020",
            "email": "unconsented@example.com",
        },
    )
    assert response.status_code == 202

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.external_lead_id == "unconsented-lead-001"))
        assert lead is not None
        assert lead.consented is False
        assert lead.raw_payload["consent_evidence"] == {
            "granted": False,
            "status": "not_provided",
            "source_fields": [],
        }
        assert lead.crm_stage == "New Lead"
        outbound_messages = db.scalars(
            select(Message).where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.OUTBOUND,
            )
        ).all()
        assert outbound_messages == []


def test_webhook_preserves_omitted_consent_but_honors_explicit_withdrawal(test_context):
    endpoint = f"/webhooks/zapier/{test_context.client_key}"
    identity = {
        "id": "consent-withdrawal-001",
        "full_name": "Consent Withdrawal",
        "phone_number": "+1 (555) 888-2121",
        "email": "withdrawal@example.com",
    }

    granted = test_context.client.post(endpoint, json={**identity, "consent": _sms_consent()})
    omitted = test_context.client.post(endpoint, json={**identity, "city": "Toronto"})
    declined = test_context.client.post(
        endpoint,
        json={
            **identity,
            "consent": {
                "sms": False,
                "method": "explicit_checkbox",
                "text": "I do not agree to receive SMS messages.",
            },
        },
    )

    assert granted.status_code == 202
    assert omitted.status_code == 202
    assert declined.status_code == 202

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.external_lead_id == identity["id"]))
        assert lead is not None
        assert lead.consented is False
        assert lead.raw_payload["consent_evidence"]["status"] == "declined_or_conflicting"
        outbound_messages = db.scalars(
            select(Message).where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.OUTBOUND,
            )
        ).all()
        # The first explicitly authorized message remains recorded; neither an
        # omission nor a later withdrawal creates another automated send.
        assert len(outbound_messages) == 1


def test_webhook_rate_limiter_operates_without_redis():
    _reset_webhook_admission_state()
    try:
        for _ in range(60):
            _check_webhook_rate(client_id=999, endpoint="unit-test", now=100.0)
        with pytest.raises(HTTPException) as exc_info:
            _check_webhook_rate(client_id=999, endpoint="unit-test", now=100.0)
        assert exc_info.value.status_code == 429
        assert exc_info.value.headers == {"Retry-After": "60"}

        _check_webhook_rate(client_id=999, endpoint="unit-test", now=161.0)
    finally:
        _reset_webhook_admission_state()


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
        ),
        "consent": _sms_consent(),
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
        assert "full_name" not in lead.form_answers
        assert "email" not in lead.form_answers
        assert "phone_number" not in lead.form_answers
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
        "consent": _sms_consent(),
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
        assert lead.form_answers["service_interest"] == "Scan 3D"
        assert "email" not in lead.form_answers
        assert "phone_number" not in lead.form_answers
        assert "utm_source" not in lead.form_answers
        assert "utm_campaign" not in lead.form_answers
        assert lead.raw_payload["tracking"]["utm_source"] == "linkedin"
        assert lead.raw_payload["tracking"]["utm_campaign"] == "scan-3d-linkedin"
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
        ),
        "consent": _sms_consent(),
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
        assert "email" not in lead.form_answers
        assert "phone_number" not in lead.form_answers
        assert "full_name" not in lead.form_answers
        assert "last_name" not in lead.form_answers
        assert "form_name" not in lead.form_answers
        assert "linkedin_lead_id" not in lead.form_answers
        assert "utm_source" not in lead.form_answers
        assert lead.raw_payload["tracking"]["utm_source"] == "linkedin"
        assert lead.crm_stage == "Contacted"


def test_website_form_webhook_accepts_zapier_json_blob(test_context):
    payload = {
        "": json.dumps(
            {
                "source": "linkedin",
                "source_page_url": "https://3dpreciscan.com/",
                "lead": {
                    "id": "urn:li:adFormResponse:4719df71-8cfa-49e5-8db6-85464ebea38a-6-json",
                    "first_name": "Peter",
                    "last_name": "TestLead",
                    "full_name": "Peter TestLead",
                    "email": "peterlead-json@email.com",
                    "phone": "4387253890",
                },
                "form_answers": {
                    "besoin_principal": "Inspection dimensionnelle / Conformité",
                    "type_piece_equipement": "Pièce plastique moulée / thermoformée",
                    "echeance": "Urgent : moins de 7 jours",
                },
                "tracking": {
                    "utm_source": "linkedin",
                    "utm_medium": "lead_gen_form",
                    "utm_campaign": "C1",
                    "ad_id": "",
                    "form_name": "LeadGen",
                },
            }
        )
    }

    response = test_context.client.post(f"/webhooks/form/{test_context.client_key}", json=payload)

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    assert response.json()["source"] == "linkedin"

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.email == "peterlead-json@email.com", Lead.client_id == 1))
        assert lead is not None
        assert lead.source == LeadSource.LINKEDIN
        assert lead.full_name == "Peter TestLead"
        assert lead.phone == "+14387253890"
        assert lead.form_answers["besoin_principal"] == "Inspection dimensionnelle / Conformité"
        assert "email" not in lead.form_answers
        assert "phone_number" not in lead.form_answers
        assert "utm_source" not in lead.form_answers
        assert lead.raw_payload["tracking"]["utm_source"] == "linkedin"
