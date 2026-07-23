from datetime import datetime, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from app.api import routes_sms
from app.core.config import Settings
from app.core.deps import get_booking_service, get_llm_agent
from app.db.models import (
    AuditLog,
    CalendarBooking,
    Client,
    ConversationStateEnum,
    InboundWebhookEvent,
    Lead,
    LeadSource,
    Message,
    MessageAttachment,
    MessageDirection,
    OutboundRequest,
)
from app.db.session import get_session_factory
from app.services.booking import BookingService
from app.services.llm_agent import AgentResponse, LLMAgent
from app.services.sms_delivery import with_initial_delivery_status
from app.workers.tasks import process_inbound_media_event_task, recover_webhook_inbox_events


def test_inbound_media_queue_failure_requests_provider_retry(monkeypatch):
    monkeypatch.setattr(
        routes_sms,
        "enqueue_process_inbound_media_event",
        lambda event_id: False,
    )

    with pytest.raises(HTTPException) as exc_info:
        routes_sms._enqueue_inbound_media_or_raise(
            event_id=42,
            settings=Settings(rq_eager=False),
        )

    assert exc_info.value.status_code == 503


def test_webhook_recovery_routes_pending_mms_to_media_worker(test_context, monkeypatch):
    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 000-4499",
            "Body": "",
            "MessageSid": "SM-IN-MEDIA-RECOVERY-001",
            "NumMedia": "1",
            "MediaUrl0": "https://api.twilio.com/2010-04-01/Accounts/AC/Messages/MM/Media/ME",
            "MediaContentType0": "image/jpeg",
        },
    )
    assert response.status_code == 200

    media_event_ids: list[int] = []
    monkeypatch.setattr(
        "app.workers.tasks.enqueue_process_inbound_media_event",
        lambda event_id: media_event_ids.append(event_id) or object(),
    )

    def unexpected_generic_enqueue(event_id):
        raise AssertionError(f"MMS event {event_id} was sent to the generic webhook worker")

    monkeypatch.setattr(
        "app.workers.tasks.enqueue_process_webhook_event",
        unexpected_generic_enqueue,
    )

    assert recover_webhook_inbox_events() == 1
    assert len(media_event_ids) == 1


def test_sms_inbound_booking_turn_sends_slots_via_agent_tool_flow(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = Lead(
            client_id=1,
            external_lead_id="meta-lead-002",
            source=LeadSource.META,
            full_name="John Reply",
            phone="+15557778888",
            email="john@example.com",
            city="Denver",
            form_answers={"interest": "roof replacement"},
            raw_payload={"source": "seed"},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 777-8888",
            "Body": "Can I book this week?",
            "MessageSid": "SM-IN-001",
        },
    )

    assert response.status_code == 200
    assert test_context.fake_llm.calls == 1
    assert test_context.fake_booking.offer_calls == 1
    assert "next available times" in test_context.fake_sms.sent[-1]["body"].lower() or "should work" in test_context.fake_sms.sent[-1]["body"].lower()

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15557778888"))
        assert lead is not None
        assert lead.conversation_state.value == "BOOKING_SENT"
        assert lead.crm_stage == "Qualified"
    assert lead.raw_payload.get("pending_step") == "slot_selection_pending"


def test_sms_inbound_question_only_does_not_force_slots(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = Lead(
            client_id=1,
            external_lead_id="meta-lead-001x",
            source=LeadSource.META,
            full_name="Question First",
            phone="+15551110000",
            email="question@example.com",
            city="Denver",
            form_answers={"interest": "roof replacement"},
            raw_payload={"source": "seed"},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 111-0000",
            "Body": "That’s right",
            "MessageSid": "SM-IN-001X",
        },
    )

    assert response.status_code == 200
    assert "next available times" not in test_context.fake_sms.sent[-1]["body"].lower()


def test_sms_inbound_duplicate_messagesid_is_idempotent(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = Lead(
            client_id=1,
            external_lead_id="meta-lead-dup-001",
            source=LeadSource.META,
            full_name="Dup Lead",
            phone="+15550001122",
            email="dup@example.com",
            city="Denver",
            form_answers={"interest": "roof replacement"},
            raw_payload={"source": "seed"},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.commit()

    payload = {
        "From": "+1 (555) 000-1122",
        "Body": "Can I book this week?",
        "MessageSid": "SM-IN-DUP-001",
    }
    first = test_context.client.post(f"/sms/inbound/{test_context.client_key}", data=payload)
    second = test_context.client.post(f"/sms/inbound/{test_context.client_key}", data=payload)

    assert first.status_code == 200
    assert second.status_code == 200
    assert test_context.fake_llm.calls == 1
    assert len(test_context.fake_sms.sent) == 1

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15550001122"))
        assert lead is not None
        inbound_messages = db.scalars(
            select(Message).where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.INBOUND,
                Message.provider_message_sid == "SM-IN-DUP-001",
            )
        ).all()
        assert len(inbound_messages) == 1


def test_stop_confirmation_is_sent_at_most_once_per_lead(test_context):
    sent_before = len(test_context.fake_sms.sent)

    first = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={"From": "+15550003333", "Body": "STOP", "MessageSid": "SM-STOP-001"},
    )
    second = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={"From": "+15550003333", "Body": "STOP", "MessageSid": "SM-STOP-002"},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert len(test_context.fake_sms.sent) == sent_before + 1

    with get_session_factory()() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15550003333"))
        assert lead is not None and lead.opted_out is True
        audits = db.scalars(
            select(AuditLog)
            .where(AuditLog.lead_id == lead.id, AuditLog.event_type == "compliance_stop")
            .order_by(AuditLog.id)
        ).all()
        assert [audit.decision["reply_status"] for audit in audits] == ["sent", "suppressed"]
        assert audits[-1].decision["suppression_reason"] == "already_opted_out"


def test_start_resubscribes_an_opted_out_lead(test_context):
    sent_before = len(test_context.fake_sms.sent)
    stop = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={"From": "+15550003334", "Body": "STOP", "MessageSid": "SM-STOP-START-001"},
    )
    start = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={"From": "+15550003334", "Body": "START", "MessageSid": "SM-STOP-START-002"},
    )

    assert stop.status_code == 200
    assert start.status_code == 200
    assert len(test_context.fake_sms.sent) == sent_before + 2

    with get_session_factory()() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15550003334"))
        assert lead is not None
        assert lead.opted_out is False
        assert lead.consented is True
        assert lead.conversation_state == ConversationStateEnum.NEW
        assert lead.crm_stage == "New Lead"
        assert lead.raw_payload["consent_evidence"]["method"] == "sms_start_keyword"
        audit = db.scalar(
            select(AuditLog).where(
                AuditLog.lead_id == lead.id,
                AuditLog.event_type == "compliance_start",
            )
        )
        assert audit is not None
        assert audit.decision["reply_status"] == "sent"


def test_help_remains_available_after_opt_out(test_context):
    sent_before = len(test_context.fake_sms.sent)
    test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={"From": "+15550003335", "Body": "STOP", "MessageSid": "SM-STOP-HELP-001"},
    )
    help_response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={"From": "+15550003335", "Body": "HELP", "MessageSid": "SM-STOP-HELP-002"},
    )

    assert help_response.status_code == 200
    assert len(test_context.fake_sms.sent) == sent_before + 2
    with get_session_factory()() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15550003335"))
        assert lead is not None
        assert lead.opted_out is True
        assert lead.consented is False
        help_audit = db.scalar(
            select(AuditLog).where(
                AuditLog.lead_id == lead.id,
                AuditLog.event_type == "compliance_help",
            )
        )
        assert help_audit is not None
        assert help_audit.decision["reply_status"] == "sent"


def test_repeated_stop_does_not_erase_booked_state_restored_by_start(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = Lead(
            client_id=1,
            external_lead_id="booked-opt-out-001",
            source=LeadSource.MANUAL,
            full_name="Booked Opt Out",
            phone="+15550003336",
            email="booked-optout@example.com",
            city="Toronto",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKED,
            crm_stage="Meeting Booked",
        )
        db.add(lead)
        db.commit()

    for sid in ("SM-BOOKED-STOP-001", "SM-BOOKED-STOP-002"):
        test_context.client.post(
            f"/sms/inbound/{test_context.client_key}",
            data={"From": "+15550003336", "Body": "STOP", "MessageSid": sid},
        )
    test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={"From": "+15550003336", "Body": "START", "MessageSid": "SM-BOOKED-START-001"},
    )

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.external_lead_id == "booked-opt-out-001"))
        assert lead is not None
        assert lead.opted_out is False
        assert lead.consented is True
        assert lead.conversation_state == ConversationStateEnum.BOOKED
        assert lead.crm_stage == "Meeting Booked"


def test_help_reply_is_bounded_to_one_per_rate_window(test_context):
    sent_before = len(test_context.fake_sms.sent)

    first = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={"From": "+15550004444", "Body": "HELP", "MessageSid": "SM-HELP-001"},
    )
    second = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={"From": "+15550004444", "Body": "HELP", "MessageSid": "SM-HELP-002"},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert len(test_context.fake_sms.sent) == sent_before + 1

    with get_session_factory()() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15550004444"))
        assert lead is not None
        audits = db.scalars(
            select(AuditLog)
            .where(AuditLog.lead_id == lead.id, AuditLog.event_type == "compliance_help")
            .order_by(AuditLog.id)
        ).all()
        assert [audit.decision["reply_status"] for audit in audits] == ["sent", "suppressed"]
        assert audits[-1].decision["suppression_reason"] == "rate_limited"


def test_admission_limit_runs_before_media_download_and_reply(test_context, monkeypatch):
    media_job_queued = False

    def unexpected_enqueue(event_id):
        nonlocal media_job_queued
        _ = event_id
        media_job_queued = True

    monkeypatch.setattr("app.api.routes_sms.is_rate_limited", lambda **kwargs: True)
    monkeypatch.setattr(
        "app.api.routes_sms.enqueue_process_inbound_media_event",
        unexpected_enqueue,
    )
    sent_before = len(test_context.fake_sms.sent)

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+15550005555",
            "Body": "HELP",
            "MessageSid": "SM-LIMITED-001",
            "NumMedia": "1",
            "MediaUrl0": "https://api.twilio.com/2010-04-01/Accounts/AC/Messages/MM/Media/ME",
            "MediaContentType0": "image/jpeg",
        },
    )

    assert response.status_code == 200
    assert media_job_queued is False
    assert len(test_context.fake_sms.sent) == sent_before
    with get_session_factory()() as db:
        audit = db.scalar(select(AuditLog).where(AuditLog.event_type == "rate_limited"))
        assert audit is not None
        assert audit.decision["admission_stage"] == "before_media_and_reply"


def test_rate_limited_stop_still_opts_out_without_sending_confirmation(test_context, monkeypatch):
    monkeypatch.setattr("app.api.routes_sms.is_rate_limited", lambda **kwargs: True)
    sent_before = len(test_context.fake_sms.sent)

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={"From": "+15550006666", "Body": "STOP", "MessageSid": "SM-LIMITED-STOP-001"},
    )

    assert response.status_code == 200
    assert len(test_context.fake_sms.sent) == sent_before
    with get_session_factory()() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15550006666"))
        assert lead is not None and lead.opted_out is True
        audit = db.scalar(
            select(AuditLog).where(AuditLog.lead_id == lead.id, AuditLog.event_type == "compliance_stop")
        )
        assert audit is not None
        assert audit.decision["reply_status"] == "suppressed"
        assert audit.decision["suppression_reason"] == "rate_limited"


def test_sms_inbound_paused_agent_logs_message_without_reply(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = Lead(
            client_id=1,
            external_lead_id="meta-lead-paused-001",
            source=LeadSource.META,
            full_name="Paused Lead",
            phone="+15550007777",
            email="paused@example.com",
            city="Denver",
            form_answers={"interest": "roof replacement"},
            raw_payload={
                "source": "seed",
                "agent_control": {
                    "paused": True,
                    "mode": "paused",
                    "reason": "operator_testing",
                },
            },
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 000-7777",
            "Body": "Are you still there?",
            "MessageSid": "SM-IN-PAUSED-001",
        },
    )

    assert response.status_code == 200
    assert test_context.fake_llm.calls == 0
    assert test_context.fake_sms.sent == []

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15550007777"))
        assert lead is not None
        inbound = db.scalar(
            select(Message).where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.INBOUND,
                Message.provider_message_sid == "SM-IN-PAUSED-001",
            )
        )
        assert inbound is not None
        audit = db.scalar(select(AuditLog).where(AuditLog.lead_id == lead.id, AuditLog.event_type == "agent_reply_suppressed"))
        assert audit is not None
        assert audit.decision["reason"] == "operator_testing"


def test_sms_status_callback_marks_outbound_delivery_warning(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = Lead(
            client_id=1,
            external_lead_id="meta-lead-delivery-001",
            source=LeadSource.META,
            full_name="Delivery Lead",
            phone="+15550008888",
            email="delivery@example.com",
            city="Denver",
            form_answers={"interest": "roof replacement"},
            raw_payload={"source": "seed"},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.flush()
        db.add(
            Message(
                client_id=1,
                lead_id=lead.id,
                direction=MessageDirection.OUTBOUND,
                body="Hi, can we help?",
                provider_message_sid="SM-DELIVERY-001",
                raw_payload=with_initial_delivery_status(
                    {"source": "test"},
                    provider_sid="SM-DELIVERY-001",
                    provider="twilio",
                    callback_url="http://testserver/sms/status-callback",
                ),
            )
        )
        db.commit()
        lead_id = lead.id

    response = test_context.client.post(
        "/sms/status-callback",
        data={
            "MessageSid": "SM-DELIVERY-001",
            "MessageStatus": "undelivered",
            "ErrorCode": "30005",
            "ErrorMessage": "Unknown destination handset",
            "To": "+15550008888",
            "From": "+15550000000",
        },
    )

    assert response.status_code == 200

    duplicate = test_context.client.post(
        "/sms/status-callback",
        data={
            "MessageSid": "SM-DELIVERY-001",
            "MessageStatus": "undelivered",
            "ErrorCode": "30005",
            "ErrorMessage": "Unknown destination handset",
            "To": "+15550008888",
            "From": "+15550000000",
        },
    )
    assert duplicate.status_code == 200

    stale = test_context.client.post(
        "/sms/status-callback",
        data={
            "MessageSid": "SM-DELIVERY-001",
            "MessageStatus": "queued",
            "To": "+15550008888",
            "From": "+15550000000",
        },
    )
    assert stale.status_code == 200

    with SessionLocal() as db:
        message = db.scalar(select(Message).where(Message.provider_message_sid == "SM-DELIVERY-001"))
        assert message is not None
        delivery = message.raw_payload["delivery"]
        assert delivery["status"] == "undelivered"
        assert delivery["severity"] == "warning"
        assert delivery["error_code"] == "30005"
        assert delivery["error_message"] == "Unknown destination handset"
        assert delivery["label_fr"] == "SMS non livré"
        assert "téléphone injoignable" in delivery["description_fr"]
        lead = db.get(Lead, lead_id)
        assert lead is not None
        assert lead.raw_payload["sms_contactability"]["status"] == "sms_failed"
        audits = db.scalars(
            select(AuditLog).where(AuditLog.lead_id == lead_id, AuditLog.event_type == "sms_delivery_failed")
        ).all()
        assert len(audits) == 1

    thread = test_context.client.get(
        f"/ui/api/conversations/{lead_id}/thread",
        headers={"X-Admin-Token": "test-admin-token-32-characters-long!"},
    )
    assert thread.status_code == 200
    outbound = next(item for item in thread.json()["messages"] if item["provider_message_sid"] == "SM-DELIVERY-001")
    assert outbound["delivery"]["severity"] == "warning"
    assert outbound["delivery"]["label"] == "SMS not delivered"
    assert outbound["delivery"]["label_fr"] == "SMS non livré"


def test_sms_inbound_explicit_human_request_handoffs_without_llm(test_context):
    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 000-4411",
            "Body": "Can someone from your team call me?",
            "MessageSid": "SM-IN-HANDOFF-001",
        },
    )

    assert response.status_code == 200
    assert test_context.fake_llm.calls == 0
    assert len(test_context.fake_sms.sent) == 1
    assert "someone" in test_context.fake_sms.sent[-1]["body"].lower() or "team" in test_context.fake_sms.sent[-1]["body"].lower()

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15550004411"))
        assert lead is not None
        assert lead.conversation_state == ConversationStateEnum.HANDOFF
        assert lead.raw_payload["handoff"]["reason"] == "explicit_human_request"
        audit = db.scalar(select(AuditLog).where(AuditLog.lead_id == lead.id, AuditLog.event_type == "agent_handoff_triggered"))
        assert audit is not None
        assert audit.decision["reason"] == "explicit_human_request"


def test_sms_inbound_media_only_is_stored_and_handed_off(test_context, monkeypatch):
    download_calls: list[str] = []
    queued_event_ids: list[int] = []

    async def fake_download_twilio_media(**kwargs):
        assert kwargs["media_url"] == "https://api.twilio.com/2010-04-01/Accounts/AC/Messages/MM/Media/ME"
        assert kwargs["content_type"] == "image/jpeg"
        download_calls.append(kwargs["media_url"])
        return b"\xff\xd8\xffinbound-image"

    monkeypatch.setattr("app.workers.tasks.download_twilio_media", fake_download_twilio_media)
    monkeypatch.setattr(
        "app.api.routes_sms.enqueue_process_inbound_media_event",
        lambda event_id: queued_event_ids.append(event_id) or False,
    )
    monkeypatch.setattr("app.workers.tasks._acquire_lead_workflow_lock", lambda **kwargs: None)
    monkeypatch.setattr("app.workers.tasks.build_sms_service", lambda *args, **kwargs: test_context.fake_sms)
    monkeypatch.setattr("app.workers.tasks.build_llm_agent", lambda *args, **kwargs: test_context.fake_llm)
    monkeypatch.setattr("app.workers.tasks.build_booking_service", lambda *args, **kwargs: test_context.fake_booking)

    payload = {
        "From": "+1 (555) 000-4455",
        "Body": "",
        "MessageSid": "SM-IN-MEDIA-001",
        "NumMedia": "1",
        "MediaUrl0": "https://api.twilio.com/2010-04-01/Accounts/AC/Messages/MM/Media/ME",
        "MediaContentType0": "image/jpeg",
    }
    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data=payload,
    )
    duplicate = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data=payload,
    )

    assert response.status_code == 200
    assert duplicate.status_code == 200
    assert download_calls == []
    assert len(queued_event_ids) == 2
    assert queued_event_ids[0] == queued_event_ids[1]
    assert test_context.fake_llm.calls == 0
    assert test_context.fake_sms.sent == []

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15550004455"))
        assert lead is not None
        lead_id = lead.id
        message = db.scalar(
            select(Message).where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.INBOUND,
            )
        )
        assert message is not None
        assert message.raw_payload["media_ingestion"]["status"] == "pending"
        assert db.scalar(
            select(MessageAttachment).where(MessageAttachment.message_id == message.id)
        ) is None
        events = db.scalars(
            select(InboundWebhookEvent).where(
                InboundWebhookEvent.client_id == lead.client_id,
                InboundWebhookEvent.endpoint == "sms_inbound_media",
            )
        ).all()
        assert len(events) == 1
        assert events[0].status == "pending"
        event_id = events[0].id

    result = process_inbound_media_event_task(event_id)
    duplicate_result = process_inbound_media_event_task(event_id)

    assert result["status"] == "ok"
    assert duplicate_result["reason"] == "already_processed"
    assert len(download_calls) == 1
    assert test_context.fake_llm.calls == 0
    assert len(test_context.fake_sms.sent) == 1
    assert "attachment" in test_context.fake_sms.sent[-1]["body"].lower()

    with SessionLocal() as db:
        lead = db.get(Lead, lead_id)
        assert lead is not None
        assert lead.conversation_state == ConversationStateEnum.HANDOFF
        assert lead.raw_payload["handoff"]["reason"] == "unsupported_media"
        message = db.scalar(
            select(Message).where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.INBOUND,
            )
        )
        assert message is not None
        assert message.raw_payload["attachments"][0]["media_kind"] == "image"
        attachment = db.scalar(
            select(MessageAttachment).where(MessageAttachment.message_id == message.id)
        )
        assert attachment is not None
        assert attachment.content_type == "image/jpeg"
        assert db.scalar(
            select(InboundWebhookEvent).where(InboundWebhookEvent.id == event_id)
        ).status == "completed"

    thread = test_context.client.get(
        f"/ui/api/conversations/{lead_id}/thread",
        headers={"X-Admin-Token": "test-admin-token-32-characters-long!"},
    )
    assert thread.status_code == 200
    thread_payload = thread.json()
    assert thread_payload["messages"][0]["attachments"][0]["media_kind"] == "image"


def test_inbound_media_worker_enforces_aggregate_byte_cap(test_context, monkeypatch):
    async def fake_download_twilio_media(**kwargs):
        _ = kwargs
        return b"\xff\xd8\xffabc"

    monkeypatch.setattr("app.workers.tasks.download_twilio_media", fake_download_twilio_media)
    monkeypatch.setattr("app.workers.tasks._MAX_INBOUND_MEDIA_AGGREGATE_BYTES", 8)
    monkeypatch.setattr("app.workers.tasks._acquire_lead_workflow_lock", lambda **kwargs: None)
    monkeypatch.setattr(
        "app.workers.tasks.build_sms_service",
        lambda *args, **kwargs: test_context.fake_sms,
    )
    monkeypatch.setattr(
        "app.workers.tasks.build_llm_agent",
        lambda *args, **kwargs: test_context.fake_llm,
    )
    monkeypatch.setattr(
        "app.workers.tasks.build_booking_service",
        lambda *args, **kwargs: test_context.fake_booking,
    )

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 000-4466",
            "Body": "",
            "MessageSid": "SM-IN-MEDIA-CAP-001",
            "NumMedia": "2",
            "MediaUrl0": "https://api.twilio.com/2010-04-01/Accounts/AC/Messages/MM/Media/ME0",
            "MediaContentType0": "image/jpeg",
            "MediaUrl1": "https://api.twilio.com/2010-04-01/Accounts/AC/Messages/MM/Media/ME1",
            "MediaContentType1": "image/jpeg",
        },
    )

    assert response.status_code == 200
    with get_session_factory()() as db:
        event = db.scalar(
            select(InboundWebhookEvent).where(
                InboundWebhookEvent.endpoint == "sms_inbound_media"
            )
        )
        assert event is not None
        event_id = event.id

    result = process_inbound_media_event_task(event_id)

    assert result["status"] == "partial"
    assert result["saved"] == 1
    assert result["failed"] == 1
    with get_session_factory()() as db:
        event = db.get(InboundWebhookEvent, event_id)
        assert event is not None
        message = db.get(Message, result["message_id"])
        assert message is not None
        attachments = db.scalars(
            select(MessageAttachment).where(MessageAttachment.message_id == message.id)
        ).all()
        assert len(attachments) == 1
        assert message.raw_payload["media_ingestion"]["status"] == "partial"
        failure = db.scalar(
            select(AuditLog).where(
                AuditLog.lead_id == message.lead_id,
                AuditLog.event_type == "inbound_media_download_failed",
            )
        )
        assert failure is not None
        assert "aggregate byte limit" in failure.decision["error"]


def test_sms_inbound_selection_books_slot_without_backend_short_circuit_loop(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-004",
            source=LeadSource.META,
            full_name="Calendar Lead",
            phone="+15554443333",
            email="calendar@example.com",
            city="Denver",
            form_answers={"interest": "consultation"},
            raw_payload={"source": "seed", "pending_step": "slot_selection_pending"},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()
        db.add(
            Message(
                client_id=client.id,
                lead_id=lead.id,
                direction=MessageDirection.OUTBOUND,
                body="I found a few times that should work:\n1) Mon Mar 09 at 10:00 AM\n2) Mon Mar 09 at 12:00 PM\nReply with 1 or 2.",
                provider_message_sid="SM-OFFER-EXISTING",
                raw_payload={
                    "booking_offer": {
                        "provider": "calendly",
                        "slots": [
                            {"index": 1, "start_time": "2026-03-09T15:00:00Z", "display_time": "Mon Mar 09 at 10:00 AM"},
                            {"index": 2, "start_time": "2026-03-09T17:00:00Z", "display_time": "Mon Mar 09 at 12:00 PM"},
                        ],
                    }
                },
            )
        )
        db.commit()

    confirm = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 444-3333",
            "Body": "1",
            "MessageSid": "SM-IN-011",
        },
    )
    assert confirm.status_code == 200
    assert test_context.fake_llm.calls == 0
    assert test_context.fake_booking.selection_calls >= 1
    assert "Booked. You are set" in test_context.fake_sms.sent[-1]["body"]

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15554443333"))
        assert lead is not None
        assert lead.conversation_state.value == "BOOKED"
        assert lead.crm_stage == "Meeting Booked"


def test_confirmed_booking_state_survives_confirmation_sms_failure(test_context, monkeypatch):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="booking-confirmation-failure",
            source=LeadSource.META,
            full_name="Booked Without SMS",
            phone="+15554443334",
            email="booking-failure@example.com",
            raw_payload={"pending_step": "slot_selection_pending"},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()
        db.add(
            Message(
                client_id=client.id,
                lead_id=lead.id,
                direction=MessageDirection.OUTBOUND,
                body="1) Mon Mar 09 at 10:00 AM",
                provider_message_sid="SM-OFFER-CONFIRMATION-FAILURE",
                raw_payload={
                    "booking_offer": {
                        "provider": "calendly",
                        "slots": [
                            {
                                "index": 1,
                                "start_time": "2026-03-09T15:00:00Z",
                                "display_time": "Mon Mar 09 at 10:00 AM",
                            }
                        ],
                    }
                },
            )
        )
        db.commit()

    def fail_sms(*args, **kwargs):
        raise RuntimeError("simulated provider timeout")

    monkeypatch.setattr(test_context.fake_sms, "send_message", fail_sms)
    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 444-3334",
            "Body": "1",
            "MessageSid": "SM-IN-CONFIRMATION-FAILURE",
        },
    )

    assert response.status_code == 200
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15554443334"))
        assert lead is not None
        assert lead.conversation_state == ConversationStateEnum.BOOKED
        assert lead.crm_stage == "Meeting Booked"
        assert isinstance(lead.raw_payload.get("calendar_booking"), dict)
        event_types = set(
            db.scalars(select(AuditLog.event_type).where(AuditLog.lead_id == lead.id)).all()
        )
        assert "calendar_booking_created" in event_types
        assert "booking_confirmation_sms_failed" in event_types


def test_calendly_booking_reservation_reuses_completed_provider_result(test_context, monkeypatch):
    SessionLocal = get_session_factory()
    service = BookingService()
    provider_calls = 0

    def fake_request(**kwargs):
        nonlocal provider_calls
        provider_calls += 1
        return {
            "resource": {
                "event": "https://api.calendly.com/scheduled_events/durable-1",
                "uri": "https://api.calendly.com/scheduled_events/durable-1/invitees/1",
            }
        }

    monkeypatch.setattr(service, "_request", fake_request)
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.booking_mode = "calendly"
        client.booking_config = {
            "calendly_personal_access_token": "test-token",
            "calendly_event_type_uri": "https://api.calendly.com/event_types/test",
        }
        lead = Lead(
            client_id=client.id,
            external_lead_id="calendly-durable-reservation",
            source=LeadSource.META,
            full_name="Durable Calendly",
            phone="+15554443335",
            email="durable-calendly@example.com",
            raw_payload={},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.commit()
        slot = {"start_time": "2026-03-09T15:00:00Z"}

        first = service._book_calendly_slot(client=client, lead=lead, slot=slot, db=db)
        second = service._book_calendly_slot(client=client, lead=lead, slot=slot, db=db)

        assert first == second
        assert provider_calls == 1
        reservations = db.scalars(
            select(OutboundRequest).where(
                OutboundRequest.lead_id == lead.id,
                OutboundRequest.request_kind == "calendly_booking_create",
            )
        ).all()
        assert len(reservations) == 1
        assert reservations[0].status == "completed"


def test_sms_inbound_booked_lead_reschedule_requires_confirmation_before_cancel(test_context):
    from app.main import app

    def internal_always_open_config() -> dict:
        return {
            "internal_calendar": {
                "slot_minutes": 30,
                "notice_minutes": 0,
                "horizon_days": 7,
                "availability": [
                    {"day": day, "enabled": True, "start": "00:00", "end": "23:59"}
                    for day in range(7)
                ],
            }
        }

    booking_service = BookingService()
    app.dependency_overrides[get_booking_service] = lambda: booking_service
    SessionLocal = get_session_factory()

    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.booking_mode = "internal"
        client.booking_config = internal_always_open_config()
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-reschedule-confirm",
            source=LeadSource.META,
            full_name="SMS Reschedule Lead",
            phone="+15552224444",
            email="sms-reschedule@example.com",
            city="Denver",
            form_answers={"interest": "consultation"},
            raw_payload={"source": "seed", "pending_step": "slot_selection_pending"},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKED,
            crm_stage="Meeting Booked",
        )
        db.add(lead)
        db.flush()

        first_offer = booking_service.offer_slots(client=client, lead=lead, db=db)
        first_result = booking_service.book_requested_slot(
            client=client,
            lead=lead,
            latest_offer=first_offer.raw_payload["booking_offer"],
            slot_index=1,
            db=db,
        )
        old_booking_id = int(first_result["booking"]["booking_id"])

        second_offer = booking_service.offer_slots(client=client, lead=lead, db=db)
        db.add(
            Message(
                client_id=client.id,
                lead_id=lead.id,
                direction=MessageDirection.OUTBOUND,
                body=second_offer.reply_text,
                provider_message_sid="SM-OFFER-SMS-RESCHEDULE",
                raw_payload=second_offer.raw_payload,
            )
        )
        db.commit()

    request_confirmation = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 222-4444",
            "Body": "1",
            "MessageSid": "SM-IN-RESCHEDULE-1",
        },
    )

    assert request_confirmation.status_code == 200
    assert test_context.fake_llm.calls == 0
    assert "Should I cancel" in test_context.fake_sms.sent[-1]["body"]

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15552224444"))
        assert lead is not None
        assert lead.raw_payload["pending_step"] == "reschedule_confirmation_pending"
        assert "pending_reschedule_confirmation" in lead.raw_payload
        bookings = db.scalars(select(CalendarBooking).where(CalendarBooking.lead_id == lead.id)).all()
        assert len([booking for booking in bookings if booking.status == "scheduled"]) == 1

    confirm = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 222-4444",
            "Body": "yes",
            "MessageSid": "SM-IN-RESCHEDULE-2",
        },
    )

    assert confirm.status_code == 200
    assert "Updated. Your call is now set" in test_context.fake_sms.sent[-1]["body"]
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15552224444"))
        assert lead is not None
        bookings = db.scalars(select(CalendarBooking).where(CalendarBooking.lead_id == lead.id)).all()
        scheduled = [booking for booking in bookings if booking.status == "scheduled"]
        cancelled = [booking for booking in bookings if booking.status == "cancelled"]
        assert len(scheduled) == 1
        assert len(cancelled) == 1
        assert cancelled[0].id == old_booking_id
        assert "pending_reschedule_confirmation" not in lead.raw_payload

    app.dependency_overrides[get_booking_service] = lambda: test_context.fake_booking


def test_sms_inbound_booking_question_uses_agent_not_repeated_slot_menu(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-005",
            source=LeadSource.META,
            full_name="Booking Question Lead",
            phone="+15553332222",
            email="booking-question@example.com",
            city="Denver",
            form_answers={"interest": "consultation"},
            raw_payload={"source": "seed", "pending_step": "slot_selection_pending"},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()
        db.add(
            Message(
                client_id=client.id,
                lead_id=lead.id,
                direction=MessageDirection.OUTBOUND,
                body="I found a few times that should work:\n1) Mon Mar 09 at 10:00 AM\n2) Mon Mar 09 at 12:00 PM\nReply with 1 or 2.",
                provider_message_sid="SM-OFFER-EXISTING",
                raw_payload={
                    "booking_offer": {
                        "provider": "calendly",
                        "slots": [
                            {"index": 1, "start_time": "2026-03-09T15:00:00Z", "display_time": "Mon Mar 09 at 10:00 AM"},
                            {"index": 2, "start_time": "2026-03-09T17:00:00Z", "display_time": "Mon Mar 09 at 12:00 PM"},
                        ],
                    }
                },
            )
        )
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 333-2222",
            "Body": "Do you have availability on Wednesday?",
            "MessageSid": "SM-IN-012",
        },
    )

    assert response.status_code == 200
    assert test_context.fake_llm.calls >= 1
    assert "wednesday options" in test_context.fake_sms.sent[-1]["body"].lower()
    assert "did not catch which slot" not in test_context.fake_sms.sent[-1]["body"].lower()


def test_sms_inbound_requested_day_gets_day_specific_options(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-015",
            source=LeadSource.META,
            full_name="Thursday Lead",
            phone="+15556667777",
            email="thursday@example.com",
            city="Denver",
            form_answers={"interest": "consultation"},
            raw_payload={"source": "seed", "pending_step": "slot_selection_pending"},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()
        db.add(
            Message(
                client_id=client.id,
                lead_id=lead.id,
                direction=MessageDirection.OUTBOUND,
                body="I found a few times that should work:\n1) Mon Mar 09 at 10:00 AM\n2) Mon Mar 09 at 12:00 PM\nReply with 1 or 2.",
                provider_message_sid="SM-OFFER-THU",
                raw_payload={
                    "booking_offer": {
                        "provider": "calendly",
                        "slots": [
                            {"index": 1, "start_time": "2026-03-09T15:00:00Z", "display_time": "Mon Mar 09 at 10:00 AM"},
                            {"index": 2, "start_time": "2026-03-09T17:00:00Z", "display_time": "Mon Mar 09 at 12:00 PM"},
                        ],
                    }
                },
            )
        )
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 666-7777",
            "Body": "Are you available next Thursday?",
            "MessageSid": "SM-IN-014",
        },
    )

    assert response.status_code == 200
    assert "thursday" in test_context.fake_sms.sent[-1]["body"].lower()
    assert "monday" not in test_context.fake_sms.sent[-1]["body"].lower()


def test_sms_inbound_requested_day_and_exact_time_are_respected(test_context):
    from app.main import app

    class DayTimeBookingProvider:
        def generate_json(self, system_prompt: str, user_prompt: str):
            _ = system_prompt
            _ = user_prompt
            return {
                "reply_text": "",
                "next_state": "BOOKING_SENT",
                "collected_fields": {},
                "next_question_key": None,
                "action": "none",
                "tool_call": {"name": "find_slots", "args": {}},
            }

    app.dependency_overrides[get_llm_agent] = lambda: LLMAgent(provider=DayTimeBookingProvider())

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-017",
            source=LeadSource.META,
            full_name="Wednesday Time Lead",
            phone="+15557770000",
            email="wednesday@example.com",
            city="Denver",
            form_answers={"interest": "consultation"},
            raw_payload={"source": "seed", "pending_step": "slot_selection_pending"},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 777-0000",
            "Body": "Can you do Wednesday 11 am?",
            "MessageSid": "SM-IN-016",
        },
    )

    assert response.status_code == 200
    assert "wednesday" in test_context.fake_sms.sent[-1]["body"].lower()
    assert "11:00 am" in test_context.fake_sms.sent[-1]["body"].lower()

    app.dependency_overrides[get_llm_agent] = lambda: test_context.fake_llm


def test_sms_inbound_requested_day_range_returns_same_day_options(test_context):
    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 777-8888",
            "Body": "What are your availabilities on Tuesday between 10 am and 3 pm?",
            "MessageSid": "SM-IN-016B",
        },
    )

    assert response.status_code == 200
    body = test_context.fake_sms.sent[-1]["body"].lower()
    assert "tuesday" in body
    assert "10:00 am" in body or "12:00 pm" in body or "2:00 pm" in body


def test_sms_inbound_non_booking_message_can_keep_qualifying_during_booking_sent(test_context):
    from app.main import app

    class QualifyingDuringBookingLLM:
        def __init__(self) -> None:
            self.calls = 0

        def run_turn(self, *, client: Client, lead, inbound_text: str, history, booking_service=None, db=None):
            _ = client
            _ = history
            _ = booking_service
            _ = db
            self.calls += 1
            assert lead.conversation_state == ConversationStateEnum.BOOKING_SENT
            assert inbound_text == "Do you also handle Revit?"
            return AgentResponse(
                reply_text="Yes, we do. Do you need CAD only, Revit/BIM, or both?",
                next_state=ConversationStateEnum.QUALIFYING,
                action="ask_next_question",
                next_question_key="urgency_driver",
            )

        def next_reply(self, client: Client, lead, inbound_text: str, history):
            return self.run_turn(client=client, lead=lead, inbound_text=inbound_text, history=history)

    qualifying_llm = QualifyingDuringBookingLLM()
    app.dependency_overrides[get_llm_agent] = lambda: qualifying_llm

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-006",
            source=LeadSource.META,
            full_name="Booking Question Lead",
            phone="+15552221111",
            email="offer@example.com",
            city="Denver",
            form_answers={"interest": "consultation"},
            raw_payload={"source": "seed", "pending_step": "slot_selection_pending"},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 222-1111",
            "Body": "Do you also handle Revit?",
            "MessageSid": "SM-IN-013",
        },
    )

    assert response.status_code == 200
    assert qualifying_llm.calls == 1
    assert "do you need cad only, revit/bim, or both" in test_context.fake_sms.sent[-1]["body"].lower()

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15552221111"))
        assert lead is not None
        assert lead.conversation_state.value == "QUALIFYING"

    app.dependency_overrides[get_llm_agent] = lambda: test_context.fake_llm


def test_sms_inbound_natural_slot_confirmation_still_books(test_context):
    from app.main import app

    class NaturalBookingProvider:
        def __init__(self) -> None:
            self.calls = 0

        def generate_json(self, system_prompt: str, user_prompt: str):
            _ = system_prompt
            self.calls += 1
            if self.calls == 1:
                return {
                    "reply_text": "",
                    "next_state": "BOOKING_SENT",
                    "collected_fields": {},
                    "next_question_key": None,
                    "action": "none",
                    "tool_call": {"name": "book_slot", "args": {}},
                }
            return {
                "reply_text": "Booked. You are set for Mon Mar 09 at 10:00 AM.",
                "next_state": "BOOKED",
                "collected_fields": {},
                "next_question_key": None,
                "action": "mark_booked",
                "tool_call": {"name": "none", "args": {}},
            }

    natural_provider = NaturalBookingProvider()
    natural_llm = LLMAgent(provider=natural_provider)
    app.dependency_overrides[get_llm_agent] = lambda: natural_llm

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-016",
            source=LeadSource.META,
            full_name="Natural Slot Lead",
            phone="+15559990000",
            email="natural@example.com",
            city="Denver",
            form_answers={"interest": "consultation"},
            raw_payload={"source": "seed", "pending_step": "slot_selection_pending"},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()
        db.add(
            Message(
                client_id=client.id,
                lead_id=lead.id,
                direction=MessageDirection.OUTBOUND,
                body="I found a few Monday times:\n1) Mon Mar 09 at 10:00 AM\n2) Mon Mar 09 at 12:00 PM",
                provider_message_sid="SM-OFFER-NATURAL",
                raw_payload={
                    "booking_offer": {
                        "provider": "calendly",
                        "slots": [
                            {
                                "index": 1,
                                "start_time": "2026-03-09T15:00:00Z",
                                "display_time": "Mon Mar 09 at 10:00 AM",
                                "display_hint": "Monday 10:00 AM",
                                "search_blob": "monday 10am | monday 10 am | mon 10am",
                            },
                            {
                                "index": 2,
                                "start_time": "2026-03-09T17:00:00Z",
                                "display_time": "Mon Mar 09 at 12:00 PM",
                                "display_hint": "Monday 12:00 PM",
                                "search_blob": "monday 12pm | monday 12 pm | mon 12pm",
                            },
                        ],
                    }
                },
            )
        )
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 999-0000",
            "Body": "Monday 10 am is good",
            "MessageSid": "SM-IN-015",
        },
    )

    assert response.status_code == 200
    assert "booked. you are set" in test_context.fake_sms.sent[-1]["body"].lower()

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15559990000"))
        assert lead is not None
        assert lead.conversation_state.value == "BOOKED"

    app.dependency_overrides[get_llm_agent] = lambda: test_context.fake_llm


def test_sms_inbound_llm_resolves_lock_it_in_against_visible_single_slot(test_context):
    from app.main import app

    class SlotResolutionLLM:
        def __init__(self) -> None:
            self.resolve_calls = 0
            self.run_calls = 0

        def resolve_booking_selection(self, *, client: Client, lead, inbound_text: str, history, active_offer):
            _ = client
            _ = lead
            self.resolve_calls += 1
            assert inbound_text == "Yes lock it in"
            assert any("Friday works" in str(message.body or "") for message in history)
            assert len(active_offer["slots"]) == 5
            return {
                "decision": "select_slot",
                "selected_slot_index": 2,
                "selected_slot_start_time": None,
                "reply_text": "",
                "reasoning_summary": "The visible outbound singled out the Friday slot and the lead affirmed it.",
            }

        def run_turn(self, *, client: Client, lead, inbound_text: str, history, booking_service=None, db=None):
            _ = client
            _ = lead
            _ = inbound_text
            _ = history
            _ = booking_service
            _ = db
            self.run_calls += 1
            raise AssertionError("Main LLM turn should not run after slot resolution selects a slot.")

        def next_reply(self, client: Client, lead, inbound_text: str, history):
            return self.run_turn(client=client, lead=lead, inbound_text=inbound_text, history=history)

    slot_resolution_llm = SlotResolutionLLM()
    app.dependency_overrides[get_llm_agent] = lambda: slot_resolution_llm

    slots = [
        {"index": 1, "start_time": "2026-06-18T15:00:00Z", "display_time": "Thu Jun 18 at 11:00 AM"},
        {"index": 2, "start_time": "2026-06-19T13:30:00Z", "display_time": "Fri Jun 19 at 9:30 AM"},
        {"index": 3, "start_time": "2026-06-22T13:30:00Z", "display_time": "Mon Jun 22 at 9:30 AM"},
        {"index": 4, "start_time": "2026-06-23T13:30:00Z", "display_time": "Tue Jun 23 at 9:30 AM"},
        {"index": 5, "start_time": "2026-06-24T13:30:00Z", "display_time": "Wed Jun 24 at 9:30 AM"},
    ]
    active_offer = {"provider": "calendly", "slots": slots}

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-021",
            source=LeadSource.META,
            full_name="Friday Lock Lead",
            phone="+15551231234",
            email="friday-lock@example.com",
            city="Denver",
            form_answers={"interest": "consultation"},
            raw_payload={
                "source": "seed",
                "pending_step": "slot_selection_pending",
                "booking_offer": active_offer,
                "active_booking_offer": active_offer,
            },
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()
        db.add(
            Message(
                client_id=client.id,
                lead_id=lead.id,
                direction=MessageDirection.OUTBOUND,
                body="Friday works — I have Fri Jun 19 at 9:30 AM EDT. If you want, I can lock that in now.",
                provider_message_sid="SM-OFFER-FRIDAY-SINGLE",
                raw_payload={"booking_offer": active_offer},
            )
        )
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 123-1234",
            "Body": "Yes lock it in",
            "MessageSid": "SM-IN-021",
        },
    )

    assert response.status_code == 200
    assert slot_resolution_llm.resolve_calls == 1
    assert slot_resolution_llm.run_calls == 0
    assert "fri jun 19 at 9:30 am" in test_context.fake_sms.sent[-1]["body"].lower()
    assert "did not catch" not in test_context.fake_sms.sent[-1]["body"].lower()

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15551231234"))
        assert lead is not None
        assert lead.conversation_state == ConversationStateEnum.BOOKED
        assert "active_booking_offer" not in (lead.raw_payload or {})

    app.dependency_overrides[get_llm_agent] = lambda: test_context.fake_llm


def test_sms_inbound_premature_mark_booked_without_booking_does_not_set_booked(test_context):
    from app.main import app

    class PrematureBookedLLM:
        def run_turn(self, *, client: Client, lead, inbound_text: str, history, booking_service=None, db=None):
            _ = client
            _ = lead
            _ = inbound_text
            _ = history
            _ = booking_service
            _ = db
            return AgentResponse(
                reply_text="3:00 PM works.",
                next_state=ConversationStateEnum.BOOKED,
                action="mark_booked",
            )

        def next_reply(self, client: Client, lead, inbound_text: str, history):
            return self.run_turn(client=client, lead=lead, inbound_text=inbound_text, history=history)

    premature_llm = PrematureBookedLLM()
    app.dependency_overrides[get_llm_agent] = lambda: premature_llm

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-019",
            source=LeadSource.META,
            full_name="Premature Booked Lead",
            phone="+15557776666",
            email="premature@example.com",
            city="Denver",
            form_answers={"interest": "consultation"},
            raw_payload={"source": "seed", "pending_step": "slot_selection_pending"},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()
        db.add(
            Message(
                client_id=client.id,
                lead_id=lead.id,
                direction=MessageDirection.OUTBOUND,
                body="I found a few times that should work:\n1) Mon Mar 09 at 10:00 AM\n2) Mon Mar 09 at 12:00 PM",
                provider_message_sid="SM-OFFER-PREMATURE",
                raw_payload={
                    "booking_offer": {
                        "provider": "calendly",
                        "slots": [
                            {
                                "index": 1,
                                "start_time": "2026-03-09T15:00:00Z",
                                "display_time": "Mon Mar 09 at 10:00 AM",
                                "display_hint": "Monday 10:00 AM",
                                "search_blob": "monday 10am",
                            },
                            {
                                "index": 2,
                                "start_time": "2026-03-09T17:00:00Z",
                                "display_time": "Mon Mar 09 at 12:00 PM",
                                "display_hint": "Monday 12:00 PM",
                                "search_blob": "monday 12pm",
                            },
                        ],
                    }
                },
            )
        )
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 777-6666",
            "Body": "Let's go with 3 PM",
            "MessageSid": "SM-IN-018",
        },
    )

    assert response.status_code == 200
    assert "pick one of the offered times" in test_context.fake_sms.sent[-1]["body"].lower()

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15557776666"))
        assert lead is not None
        assert lead.conversation_state.value == "BOOKING_SENT"
        assert lead.crm_stage != "Meeting Booked"

    app.dependency_overrides[get_llm_agent] = lambda: test_context.fake_llm


@pytest.mark.parametrize("confirmation", ["Oui", "Allez-y"])
def test_sms_exact_french_time_is_checked_then_one_word_confirmation_books(
    test_context,
    monkeypatch,
    confirmation,
):
    from app.main import app

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            fixed = cls(2026, 7, 23, 20, 36, tzinfo=timezone.utc)
            if tz is None:
                return fixed.replace(tzinfo=None)
            return fixed.astimezone(tz)

    monkeypatch.setattr("app.services.booking.datetime", FixedDateTime)
    booking_service = BookingService()
    app.dependency_overrides[get_booking_service] = lambda: booking_service

    old_offer = {
        "provider": "internal",
        "slots": [
            {
                "index": 1,
                "start_time": "2026-07-27T13:00:00Z",
                "end_time": "2026-07-27T13:30:00Z",
                "display_time": "lundi 27 juillet à 9 h 00",
                "display_hint": "lundi à 9 h 00",
                "search_blob": "monday 9am",
            },
            {
                "index": 2,
                "start_time": "2026-07-28T13:00:00Z",
                "end_time": "2026-07-28T13:30:00Z",
                "display_time": "mardi 28 juillet à 9 h 00",
                "display_hint": "mardi à 9 h 00",
                "search_blob": "tuesday 9am",
            },
            {
                "index": 3,
                "start_time": "2026-07-29T13:00:00Z",
                "end_time": "2026-07-29T13:30:00Z",
                "display_time": "mercredi 29 juillet à 9 h 00",
                "display_hint": "mercredi à 9 h 00",
                "search_blob": "wednesday 9am",
            },
        ],
    }

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.booking_mode = "internal"
        client.timezone = "America/Toronto"
        client.provider_config = {"language": "fr"}
        client.booking_config = {
            "internal_calendar": {
                "slot_minutes": 30,
                "notice_minutes": 0,
                "horizon_days": 14,
                "availability": [
                    {"day": 3, "enabled": True, "start": "15:00", "end": "16:00"},
                ],
            }
        }
        lead = Lead(
            client_id=client.id,
            external_lead_id=f"meta-exact-fr-{confirmation}",
            source=LeadSource.META,
            full_name="Exact French Lead",
            phone="+15551239991",
            email="exact-fr@example.com",
            city="Montreal",
            form_answers={"interest": "consultation"},
            raw_payload={
                "source": "seed",
                "lead_language": "fr",
                "pending_step": "slot_selection_pending",
                "booking_offer": old_offer,
                "active_booking_offer": old_offer,
            },
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()
        db.add(
            Message(
                client_id=client.id,
                lead_id=lead.id,
                direction=MessageDirection.OUTBOUND,
                body="Voici trois créneaux précédents.",
                provider_message_sid="SM-OLD-FR-OFFER",
                raw_payload={"booking_offer": old_offer},
            )
        )
        db.commit()

    availability_response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 123-9991",
            "Body": "Jeudi prochain 3 PM ça marche ?",
            "MessageSid": f"SM-EXACT-FR-LOOKUP-{confirmation}",
        },
    )

    assert availability_response.status_code == 200
    assert test_context.fake_llm.calls == 0
    availability_reply = test_context.fake_sms.sent[-1]["body"]
    assert "jeudi 30 juillet à 15 h 00" in availability_reply
    assert "est disponible" in availability_reply
    assert "Voulez-vous que je le réserve?" in availability_reply
    assert "préciser quel créneau" not in availability_reply

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15551239991"))
        assert lead is not None
        assert lead.conversation_state == ConversationStateEnum.BOOKING_SENT
        assert lead.raw_payload["pending_step"] == "slot_selection_pending"
        assert len(lead.raw_payload["active_booking_offer"]["slots"]) == 1
        assert not db.scalars(
            select(CalendarBooking).where(CalendarBooking.lead_id == lead.id)
        ).all()

    booking_response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 123-9991",
            "Body": confirmation,
            "MessageSid": f"SM-EXACT-FR-CONFIRM-{confirmation}",
        },
    )

    assert booking_response.status_code == 200
    assert test_context.fake_llm.calls == 0
    booking_reply = test_context.fake_sms.sent[-1]["body"]
    assert "Réservé" in booking_reply
    assert "Ajouté à notre calendrier" in booking_reply
    assert "rappel" not in booking_reply.lower()

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15551239991"))
        assert lead is not None
        assert lead.conversation_state == ConversationStateEnum.BOOKED
        assert "pending_step" not in lead.raw_payload
        assert "active_booking_offer" not in lead.raw_payload
        bookings = db.scalars(
            select(CalendarBooking).where(CalendarBooking.lead_id == lead.id)
        ).all()
        assert len(bookings) == 1
        assert bookings[0].status == "scheduled"


def test_sms_inbound_booked_lead_can_still_get_answers(test_context):
    from app.main import app

    class PostBookedQuestionLLM:
        def __init__(self) -> None:
            self.calls = 0

        def run_turn(self, *, client: Client, lead, inbound_text: str, history, booking_service=None, db=None):
            _ = client
            _ = history
            _ = booking_service
            _ = db
            self.calls += 1
            assert lead.conversation_state == ConversationStateEnum.BOOKED
            assert inbound_text == "How does pricing work?"
            return AgentResponse(
                reply_text="Pricing depends on the building size, deliverables, and site complexity. For a 12,000 sqft retail space needing CAD and Revit, we’d scope it after a quick review.",
                next_state=ConversationStateEnum.QUALIFYING,
                action="none",
            )

        def next_reply(self, client: Client, lead, inbound_text: str, history):
            return self.run_turn(client=client, lead=lead, inbound_text=inbound_text, history=history)

    booked_llm = PostBookedQuestionLLM()
    app.dependency_overrides[get_llm_agent] = lambda: booked_llm

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-018",
            source=LeadSource.META,
            full_name="Booked Support Lead",
            phone="+15558880000",
            email="booked@example.com",
            city="Denver",
            form_answers={"interest": "consultation"},
            raw_payload={"source": "seed"},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKED,
            crm_stage="Meeting Booked",
        )
        db.add(lead)
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 888-0000",
            "Body": "How does pricing work?",
            "MessageSid": "SM-IN-017",
        },
    )

    assert response.status_code == 200
    assert "pricing depends" in test_context.fake_sms.sent[-1]["body"].lower()

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15558880000"))
        assert lead is not None
        assert lead.conversation_state.value == "BOOKED"

    app.dependency_overrides[get_llm_agent] = lambda: test_context.fake_llm


def test_sms_inbound_post_llm_unsupported_commitment_is_handed_off(test_context):
    from app.main import app

    class RiskyCommitmentLLM:
        def __init__(self) -> None:
            self.calls = 0

        def run_turn(self, *, client: Client, lead, inbound_text: str, history, booking_service=None, db=None):
            _ = client
            _ = lead
            _ = inbound_text
            _ = history
            _ = booking_service
            _ = db
            self.calls += 1
            return AgentResponse(
                reply_text="We guarantee we will meet that deadline.",
                next_state=ConversationStateEnum.QUALIFYING,
                action="none",
            )

        def next_reply(self, client: Client, lead, inbound_text: str, history):
            return self.run_turn(client=client, lead=lead, inbound_text=inbound_text, history=history)

    risky_llm = RiskyCommitmentLLM()
    app.dependency_overrides[get_llm_agent] = lambda: risky_llm

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 000-4422",
            "Body": "Can you finish this by Friday?",
            "MessageSid": "SM-IN-HANDOFF-002",
        },
    )

    assert response.status_code == 200
    assert risky_llm.calls == 1
    assert "guarantee" not in test_context.fake_sms.sent[-1]["body"].lower()

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15550004422"))
        assert lead is not None
        assert lead.conversation_state == ConversationStateEnum.HANDOFF
        assert lead.raw_payload["handoff"]["reason"] == "unsupported_commitment"

    app.dependency_overrides[get_llm_agent] = lambda: test_context.fake_llm
