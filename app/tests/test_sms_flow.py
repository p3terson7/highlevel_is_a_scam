from datetime import datetime, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from app.api import routes_sms
from app.core.config import Settings
from app.core.deps import get_booking_service, get_llm_agent, get_sms_service
from app.db.models import (
    AuditLog,
    CalendarBooking,
    Client,
    ConversationStateEnum,
    InboundWebhookEvent,
    Lead,
    LeadSource,
    LeadTask,
    Message,
    MessageAttachment,
    MessageDirection,
    OutboundRequest,
)
from app.db.session import get_session_factory
from app.services.booking import BookingService, BookingSlot, SlotOffer
from app.services.llm_agent import AgentResponse, LLMAgent
from app.services.sms_delivery import with_initial_delivery_status
from app.services.sms_service import SMSDeliveryError
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


def _structured_booking_offer(
    *,
    request_fingerprint: str | None,
    offer_fingerprint: str | None,
    display_time: str = "Fri Jul 24 at 12:00 PM",
) -> tuple[dict, BookingSlot]:
    slot = BookingSlot(
        index=1,
        start_time="2026-07-24T16:00:00Z",
        end_time="2026-07-24T16:30:00Z",
        display_time=display_time,
        display_hint="Friday 12:00 PM",
        search_blob="friday 12pm",
    )
    offer = {
        "provider": "internal",
        "slots": [slot.__dict__],
        "request": {
            "scope": "specific_date",
            "requested_dates": ["2026-07-24"],
        },
        "outcome": "exact_match",
        "coverage": {
            "start": "2026-07-23",
            "end": "2026-08-06",
        },
    }
    if request_fingerprint is not None:
        offer["request_fingerprint"] = request_fingerprint
    if offer_fingerprint is not None:
        offer["offer_fingerprint"] = offer_fingerprint
    return offer, slot


class _NewTimesResolutionAgent:
    def __init__(self) -> None:
        self.resolve_calls = 0
        self.run_calls = 0

    def resolve_booking_selection(
        self,
        *,
        client,
        lead,
        inbound_text,
        history,
        active_offer,
    ):
        _ = client
        _ = lead
        _ = inbound_text
        _ = history
        _ = active_offer
        self.resolve_calls += 1
        return {
            "decision": "new_times",
            "selected_slot_index": None,
            "selected_slot_start_time": None,
            "reply_text": "",
            "reasoning_summary": "The lead requested different availability.",
        }

    def run_turn(self, **kwargs):
        _ = kwargs
        self.run_calls += 1
        raise AssertionError(
            "A new-times resolution must execute a fresh search before the main agent turn."
        )


class _FreshAvailabilityBookingService:
    def __init__(self, *, offer: dict, slot: BookingSlot, reply_text: str) -> None:
        self.offer = offer
        self.slot = slot
        self.reply_text = reply_text
        self.find_calls: list[dict] = []

    def find_slots(self, **kwargs):
        self.find_calls.append(dict(kwargs))
        return SlotOffer(
            reply_text=self.reply_text,
            slots=[self.slot],
            raw_payload={"booking_offer": self.offer},
        )


class _FailingReplySMSService:
    def __init__(self, failure: Exception) -> None:
        self.failure = failure
        self.send_calls = 0

    def send_message(self, to_number: str, body: str) -> str:
        _ = to_number, body
        self.send_calls += 1
        raise self.failure

    def with_delivery_status(
        self,
        raw_payload: dict | None,
        provider_sid: str,
    ) -> dict:
        return with_initial_delivery_status(
            raw_payload,
            provider_sid=provider_sid,
            provider="mock",
            callback_url="",
        )


class _FreshOfferAgent:
    def __init__(self, offer: dict, reply_text: str) -> None:
        self.offer = offer
        self.reply_text = reply_text
        self.run_calls = 0

    def run_turn(self, **kwargs):
        _ = kwargs
        self.run_calls += 1
        return AgentResponse(
            reply_text=self.reply_text,
            next_state=ConversationStateEnum.BOOKING_SENT,
            runtime_payload={
                "booking_offer": self.offer,
                "pending_step": "slot_selection_pending",
            },
            action="none",
        )


def test_sms_inbound_new_times_refreshes_offer_and_versions_active_request(
    test_context,
):
    from app.main import app

    old_offer, _ = _structured_booking_offer(
        request_fingerprint="request-old",
        offer_fingerprint="offer-old",
        display_time="Mon Jul 27 at 9:00 AM",
    )
    new_offer, new_slot = _structured_booking_offer(
        request_fingerprint="request-friday-noon",
        offer_fingerprint="offer-friday-noon",
    )
    resolution_agent = _NewTimesResolutionAgent()
    booking_service = _FreshAvailabilityBookingService(
        offer=new_offer,
        slot=new_slot,
        reply_text="Fresh Friday availability:\n1) Fri Jul 24 at 12:00 PM",
    )
    app.dependency_overrides[get_llm_agent] = lambda: resolution_agent
    app.dependency_overrides[get_booking_service] = lambda: booking_service

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(
            select(Client).where(Client.client_key == test_context.client_key)
        )
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-new-times-versioned",
            source=LeadSource.META,
            full_name="New Times Lead",
            phone="+15553334441",
            email="new-times@example.com",
            city="Toronto",
            form_answers={"interest": "consultation"},
            raw_payload={
                "pending_step": "slot_selection_pending",
                "booking_offer": old_offer,
                "active_booking_offer": old_offer,
                "active_booking_request": {
                    "request_fingerprint": "request-old",
                    "offer_fingerprint": "offer-old",
                    "revision": 3,
                },
            },
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 333-4441",
            "Body": "Could you check Friday at noon instead?",
            "MessageSid": "SM-IN-NEW-TIMES-VERSIONED",
        },
    )

    assert response.status_code == 200
    assert resolution_agent.resolve_calls == 1
    assert resolution_agent.run_calls == 0
    assert len(booking_service.find_calls) == 1
    assert (
        booking_service.find_calls[0]["request_text"]
        == "Could you check Friday at noon instead?"
    )
    assert "Fresh Friday availability" in test_context.fake_sms.sent[-1]["body"]

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15553334441"))
        assert lead is not None
        active_request = lead.raw_payload["active_booking_request"]
        assert active_request["request_fingerprint"] == "request-friday-noon"
        assert active_request["offer_fingerprint"] == "offer-friday-noon"
        assert active_request["revision"] == 4
        assert active_request["superseded_offer_fingerprint"] == "offer-old"
        assert (
            lead.raw_payload["active_booking_offer"]["offer_fingerprint"]
            == "offer-friday-noon"
        )

    app.dependency_overrides[get_llm_agent] = lambda: test_context.fake_llm
    app.dependency_overrides[get_booking_service] = lambda: test_context.fake_booking


def test_deterministic_offer_failure_keeps_prior_visible_menu_selectable(
    test_context,
):
    from app.main import app

    old_offer, _ = _structured_booking_offer(
        request_fingerprint="request-visible-old",
        offer_fingerprint="offer-visible-old",
        display_time="Mon Jul 27 at 9:00 AM",
    )
    new_offer, new_slot = _structured_booking_offer(
        request_fingerprint="request-undelivered-new",
        offer_fingerprint="offer-undelivered-new",
        display_time="Fri Jul 31 at 12:00 PM",
    )
    resolution_agent = _NewTimesResolutionAgent()
    booking_service = _FreshAvailabilityBookingService(
        offer=new_offer,
        slot=new_slot,
        reply_text="Fresh Friday availability:\n1) Fri Jul 31 at 12:00 PM",
    )
    failed_sms = _FailingReplySMSService(
        SMSDeliveryError(
            "Twilio rejected it",
            provider_status=400,
            provider_code="21610",
        )
    )
    app.dependency_overrides[get_llm_agent] = lambda: resolution_agent
    app.dependency_overrides[get_booking_service] = lambda: booking_service
    app.dependency_overrides[get_sms_service] = lambda: failed_sms

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(
            select(Client).where(Client.client_key == test_context.client_key)
        )
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-definitive-offer-delivery-failure",
            source=LeadSource.META,
            full_name="Definitive Delivery Lead",
            phone="+15553334451",
            email="definitive-delivery@example.com",
            city="Toronto",
            form_answers={"interest": "consultation"},
            raw_payload={
                "pending_step": "slot_selection_pending",
                "booking_offer": old_offer,
                "active_booking_offer": old_offer,
                "active_booking_request": {
                    "request_fingerprint": "request-visible-old",
                    "offer_fingerprint": "offer-visible-old",
                    "revision": 4,
                },
            },
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.commit()

    failed_response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 333-4451",
            "Body": "Could you check Friday at noon instead?",
            "MessageSid": "SM-IN-OFFER-DEFINITIVE-FAILURE",
        },
    )

    assert failed_response.status_code == 200
    assert failed_sms.send_calls == 1
    assert resolution_agent.resolve_calls == 1
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15553334451"))
        assert lead is not None
        assert lead.conversation_state == ConversationStateEnum.BOOKING_SENT
        assert lead.raw_payload["pending_step"] == "slot_selection_pending"
        assert (
            lead.raw_payload["active_booking_offer"]["offer_fingerprint"]
            == "offer-visible-old"
        )
        assert (
            lead.raw_payload["active_booking_request"]["request_fingerprint"]
            == "request-visible-old"
        )
        assert lead.raw_payload["active_booking_request"]["revision"] == 4
        request = db.scalar(
            select(OutboundRequest)
            .where(OutboundRequest.lead_id == lead.id)
            .order_by(OutboundRequest.id.desc())
        )
        assert request is not None
        assert request.status == "failed"

    app.dependency_overrides[get_llm_agent] = lambda: test_context.fake_llm
    app.dependency_overrides[get_booking_service] = lambda: test_context.fake_booking
    app.dependency_overrides[get_sms_service] = lambda: test_context.fake_sms

    selected_response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 333-4451",
            "Body": "1",
            "MessageSid": "SM-IN-SELECT-OLD-AFTER-DEFINITIVE-FAILURE",
        },
    )

    assert selected_response.status_code == 200
    assert "Mon Jul 27 at 9:00 AM" in test_context.fake_sms.sent[-1]["body"]
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15553334451"))
        assert lead is not None
        assert lead.conversation_state == ConversationStateEnum.BOOKED


def test_agent_offer_ambiguous_failure_freezes_selection_and_hands_off(
    test_context,
):
    from app.main import app

    old_offer, _ = _structured_booking_offer(
        request_fingerprint="request-visible-agent-old",
        offer_fingerprint="offer-visible-agent-old",
        display_time="Tue Jul 28 at 10:00 AM",
    )
    new_offer, _ = _structured_booking_offer(
        request_fingerprint="request-agent-undelivered-new",
        offer_fingerprint="offer-agent-undelivered-new",
        display_time="Fri Jul 31 at 1:00 PM",
    )
    new_offer_agent = _FreshOfferAgent(
        new_offer,
        "Here are different options:\n1) Fri Jul 31 at 1:00 PM",
    )
    failed_sms = _FailingReplySMSService(SMSDeliveryError("read timed out"))
    app.dependency_overrides[get_llm_agent] = lambda: new_offer_agent
    app.dependency_overrides[get_booking_service] = lambda: test_context.fake_booking
    app.dependency_overrides[get_sms_service] = lambda: failed_sms

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(
            select(Client).where(Client.client_key == test_context.client_key)
        )
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-ambiguous-offer-delivery-failure",
            source=LeadSource.META,
            full_name="Ambiguous Delivery Lead",
            phone="+15553334452",
            email="ambiguous-delivery@example.com",
            city="Toronto",
            form_answers={"interest": "consultation"},
            raw_payload={
                "pending_step": "slot_selection_pending",
                "booking_offer": old_offer,
                "active_booking_offer": old_offer,
                "active_booking_request": {
                    "request_fingerprint": "request-visible-agent-old",
                    "offer_fingerprint": "offer-visible-agent-old",
                    "revision": 6,
                },
            },
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.commit()

    failed_response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 333-4452",
            "Body": "Please show me a different set of options.",
            "MessageSid": "SM-IN-OFFER-AMBIGUOUS-FAILURE",
        },
    )

    assert failed_response.status_code == 200
    assert failed_sms.send_calls == 1
    assert new_offer_agent.run_calls == 1
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15553334452"))
        assert lead is not None
        assert lead.conversation_state == ConversationStateEnum.BOOKING_SENT
        assert "pending_step" not in lead.raw_payload
        assert (
            lead.raw_payload["active_booking_offer"]["status"]
            == "offer_delivery_unknown"
        )
        assert lead.raw_payload["active_booking_offer"]["slots"] == []
        assert isinstance(
            lead.raw_payload["booking_offer_delivery_unknown"],
            dict,
        )
        request = db.scalar(
            select(OutboundRequest)
            .where(OutboundRequest.lead_id == lead.id)
            .order_by(OutboundRequest.id.desc())
        )
        assert request is not None
        assert request.status == "ambiguous"

    app.dependency_overrides[get_llm_agent] = lambda: test_context.fake_llm
    app.dependency_overrides[get_booking_service] = lambda: test_context.fake_booking
    app.dependency_overrides[get_sms_service] = lambda: test_context.fake_sms

    selected_response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 333-4452",
            "Body": "1",
            "MessageSid": "SM-IN-SELECT-OLD-AFTER-AMBIGUOUS-FAILURE",
        },
    )

    assert selected_response.status_code == 200
    assert "don't book the wrong time" in test_context.fake_sms.sent[-1]["body"]
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15553334452"))
        assert lead is not None
        assert lead.conversation_state == ConversationStateEnum.HANDOFF
        assert not db.scalars(
            select(CalendarBooking).where(CalendarBooking.lead_id == lead.id)
        ).all()


def test_booking_offer_provider_success_then_worker_crash_keeps_selection_frozen(
    test_context,
    monkeypatch,
):
    from app.main import app
    from app.services import inbound_sms as inbound_sms_service

    old_offer, _ = _structured_booking_offer(
        request_fingerprint="request-visible-before-crash",
        offer_fingerprint="offer-visible-before-crash",
        display_time="Tue Jul 28 at 10:00 AM",
    )
    new_offer, _ = _structured_booking_offer(
        request_fingerprint="request-provider-accepted-before-crash",
        offer_fingerprint="offer-provider-accepted-before-crash",
        display_time="Fri Jul 31 at 1:00 PM",
    )
    new_offer_agent = _FreshOfferAgent(
        new_offer,
        "Here are the new options:\n1) Fri Jul 31 at 1:00 PM",
    )
    app.dependency_overrides[get_llm_agent] = lambda: new_offer_agent
    app.dependency_overrides[get_booking_service] = lambda: test_context.fake_booking

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(
            select(Client).where(Client.client_key == test_context.client_key)
        )
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-offer-activation-crash",
            source=LeadSource.META,
            full_name="Offer Activation Crash Lead",
            phone="+15553334453",
            email="offer-activation-crash@example.com",
            city="Toronto",
            form_answers={"interest": "consultation"},
            raw_payload={
                "pending_step": "slot_selection_pending",
                "booking_offer": old_offer,
                "active_booking_offer": old_offer,
                "active_booking_request": {
                    "request_fingerprint": "request-visible-before-crash",
                    "offer_fingerprint": "offer-visible-before-crash",
                    "revision": 2,
                },
            },
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.commit()

    activate_transition = (
        inbound_sms_service._activate_booking_offer_delivery_transition
    )

    def crash_after_provider_acceptance(**kwargs):
        _ = kwargs
        raise RuntimeError("simulated crash after provider acceptance")

    monkeypatch.setattr(
        inbound_sms_service,
        "_activate_booking_offer_delivery_transition",
        crash_after_provider_acceptance,
    )
    crashed_response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 333-4453",
            "Body": "Please show me a different set of options.",
            "MessageSid": "SM-IN-OFFER-ACTIVATION-CRASH",
        },
    )

    assert crashed_response.status_code == 200
    assert "Fri Jul 31 at 1:00 PM" in test_context.fake_sms.sent[-1]["body"]
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15553334453"))
        assert lead is not None
        marker = lead.raw_payload["booking_offer_delivery_transition"]
        assert marker["status"] == "offer_delivery_pending"
        assert lead.raw_payload["active_booking_offer"]["slots"] == []
        assert (
            lead.raw_payload["active_booking_offer"]["status"]
            == "offer_delivery_pending"
        )
        assert "pending_step" not in lead.raw_payload
        request = db.scalar(
            select(OutboundRequest)
            .where(OutboundRequest.lead_id == lead.id)
            .order_by(OutboundRequest.id.desc())
        )
        assert request is not None
        assert request.status == "pending"
        assert isinstance(
            request.response_json.get("booking_offer_transition"),
            dict,
        )

    monkeypatch.setattr(
        inbound_sms_service,
        "_activate_booking_offer_delivery_transition",
        activate_transition,
    )
    app.dependency_overrides[get_llm_agent] = lambda: test_context.fake_llm
    app.dependency_overrides[get_booking_service] = lambda: test_context.fake_booking

    selected_response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 333-4453",
            "Body": "1",
            "MessageSid": "SM-IN-SELECT-AFTER-OFFER-ACTIVATION-CRASH",
        },
    )

    assert selected_response.status_code == 200
    assert "don't book the wrong time" in test_context.fake_sms.sent[-1]["body"]
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15553334453"))
        assert lead is not None
        assert lead.conversation_state == ConversationStateEnum.HANDOFF
        assert not db.scalars(
            select(CalendarBooking).where(CalendarBooking.lead_id == lead.id)
        ).all()


def test_sms_inbound_unchanged_fingerprinted_offer_keeps_revision_and_skips_menu(
    test_context,
):
    from app.main import app

    unchanged_offer, unchanged_slot = _structured_booking_offer(
        request_fingerprint="request-friday-noon",
        offer_fingerprint="offer-friday-noon",
        display_time="vendredi 24 juillet à 12 h 00",
    )
    resolution_agent = _NewTimesResolutionAgent()
    booking_service = _FreshAvailabilityBookingService(
        offer=unchanged_offer,
        slot=unchanged_slot,
        reply_text=(
            "Voici les disponibilités:\n"
            "1) vendredi 24 juillet à 12 h 00\n"
            "Répondez 1 pour réserver."
        ),
    )
    app.dependency_overrides[get_llm_agent] = lambda: resolution_agent
    app.dependency_overrides[get_booking_service] = lambda: booking_service

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(
            select(Client).where(Client.client_key == test_context.client_key)
        )
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-new-times-unchanged",
            source=LeadSource.META,
            full_name="Unchanged Times Lead",
            phone="+15553334442",
            email="unchanged-times@example.com",
            city="Toronto",
            form_answers={"interest": "consultation"},
            raw_payload={
                "lead_language": "fr",
                "pending_step": "slot_selection_pending",
                "booking_offer": unchanged_offer,
                "active_booking_offer": unchanged_offer,
                "active_booking_request": {
                    "request_fingerprint": "request-friday-noon",
                    "offer_fingerprint": "offer-friday-noon",
                    "revision": 7,
                },
            },
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 333-4442",
            "Body": "Pouvez-vous vérifier vendredi midi encore une fois?",
            "MessageSid": "SM-IN-NEW-TIMES-UNCHANGED",
        },
    )

    assert response.status_code == 200
    body = test_context.fake_sms.sent[-1]["body"]
    assert "Les disponibilités n'ont pas changé." in body
    assert "vendredi 24 juillet à 12 h 00" not in body
    assert resolution_agent.run_calls == 0

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15553334442"))
        assert lead is not None
        active_request = lead.raw_payload["active_booking_request"]
        assert active_request["revision"] == 7
        assert "superseded_offer_fingerprint" not in active_request
        latest_outbound = db.scalar(
            select(Message)
            .where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.OUTBOUND,
            )
            .order_by(Message.created_at.desc())
        )
        assert latest_outbound is not None
        assert latest_outbound.raw_payload["booking_flow"]["offer_repeated"] is True

    app.dependency_overrides[get_llm_agent] = lambda: test_context.fake_llm
    app.dependency_overrides[get_booking_service] = lambda: test_context.fake_booking


def test_sms_inbound_new_times_preserves_legacy_unfingerprinted_offer_behavior(
    test_context,
):
    from app.main import app

    old_offer, _ = _structured_booking_offer(
        request_fingerprint=None,
        offer_fingerprint=None,
        display_time="Mon Jul 27 at 9:00 AM",
    )
    legacy_offer, legacy_slot = _structured_booking_offer(
        request_fingerprint=None,
        offer_fingerprint=None,
    )
    resolution_agent = _NewTimesResolutionAgent()
    booking_service = _FreshAvailabilityBookingService(
        offer=legacy_offer,
        slot=legacy_slot,
        reply_text="Legacy fresh menu:\n1) Fri Jul 24 at 12:00 PM",
    )
    app.dependency_overrides[get_llm_agent] = lambda: resolution_agent
    app.dependency_overrides[get_booking_service] = lambda: booking_service

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(
            select(Client).where(Client.client_key == test_context.client_key)
        )
        assert client is not None
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-new-times-legacy",
            source=LeadSource.META,
            full_name="Legacy New Times Lead",
            phone="+15553334443",
            email="legacy-new-times@example.com",
            city="Toronto",
            form_answers={"interest": "consultation"},
            raw_payload={
                "pending_step": "slot_selection_pending",
                "booking_offer": old_offer,
                "active_booking_offer": old_offer,
            },
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 333-4443",
            "Body": "Could you check Friday instead?",
            "MessageSid": "SM-IN-NEW-TIMES-LEGACY",
        },
    )

    assert response.status_code == 200
    assert test_context.fake_sms.sent[-1]["body"].startswith("Legacy fresh menu:")
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15553334443"))
        assert lead is not None
        assert "active_booking_request" not in lead.raw_payload

    app.dependency_overrides[get_llm_agent] = lambda: test_context.fake_llm
    app.dependency_overrides[get_booking_service] = lambda: test_context.fake_booking


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
        assert lead.raw_payload["active_booking_offer"]["status"] == "booked"
        assert lead.raw_payload["active_booking_offer"]["slots"] == []

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
        assert lead.raw_payload["active_booking_offer"]["status"] == "booked"
        assert lead.raw_payload["active_booking_offer"]["slots"] == []
        bookings = db.scalars(
            select(CalendarBooking).where(CalendarBooking.lead_id == lead.id)
        ).all()
        assert len(bookings) == 1
        assert bookings[0].status == "scheduled"


def test_duplicate_booking_selection_messagesid_creates_one_booking_and_confirmation(
    test_context,
    monkeypatch,
):
    from app.main import app

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            fixed = cls(2026, 7, 23, 16, 0, tzinfo=timezone.utc)
            if tz is None:
                return fixed.replace(tzinfo=None)
            return fixed.astimezone(tz)

    monkeypatch.setattr("app.services.booking.datetime", FixedDateTime)
    booking_service = BookingService()
    app.dependency_overrides[get_booking_service] = lambda: booking_service
    SessionLocal = get_session_factory()

    with SessionLocal() as db:
        client = db.scalar(
            select(Client).where(Client.client_key == test_context.client_key)
        )
        assert client is not None
        client.booking_mode = "internal"
        client.timezone = "America/Toronto"
        client.booking_config = {
            "internal_calendar": {
                "slot_minutes": 30,
                "notice_minutes": 0,
                "horizon_days": 14,
                "availability": [
                    {
                        "day": 3,
                        "enabled": True,
                        "start": "15:00",
                        "end": "16:00",
                    }
                ],
            }
        }
        old_offer = {
            "provider": "internal",
            "slots": [
                {
                    "index": 1,
                    "start_time": "2026-07-27T13:00:00Z",
                    "end_time": "2026-07-27T13:30:00Z",
                    "display_time": "Mon Jul 27 at 9:00 AM",
                    "display_hint": "Monday at 9:00 AM",
                    "search_blob": "monday 9am",
                }
            ],
        }
        lead = Lead(
            client_id=client.id,
            external_lead_id="duplicate-booking-selection",
            source=LeadSource.META,
            full_name="Duplicate Booking Lead",
            phone="+15551239992",
            email="duplicate-booking@example.test",
            city="Toronto",
            form_answers={"interest": "consultation"},
            raw_payload={
                "source": "seed",
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
                body="Here is the previous option: 1) Mon Jul 27 at 9:00 AM.",
                provider_message_sid="SM-DUPLICATE-BOOKING-OFFER",
                raw_payload={"booking_offer": old_offer},
            )
        )
        db.commit()

    availability = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 123-9992",
            "Body": "Jeudi prochain 3 PM ça marche ?",
            "MessageSid": "SM-IN-DUPLICATE-BOOKING-LOOKUP",
        },
    )

    assert availability.status_code == 200
    assert "jeudi 30 juillet à 15 h 00" in test_context.fake_sms.sent[-1]["body"]
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15551239992"))
        assert lead is not None
        assert len((lead.raw_payload or {})["active_booking_offer"]["slots"]) == 1

    sent_before = len(test_context.fake_sms.sent)
    payload = {
        "From": "+1 (555) 123-9992",
        "Body": "Oui",
        "MessageSid": "SM-IN-DUPLICATE-BOOKING-SELECTION",
    }
    first = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}", data=payload
    )
    second = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}", data=payload
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert len(test_context.fake_sms.sent) == sent_before + 1
    assert test_context.fake_llm.calls == 0

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15551239992"))
        assert lead is not None
        inbound_messages = db.scalars(
            select(Message).where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.INBOUND,
                Message.provider_message_sid
                == "SM-IN-DUPLICATE-BOOKING-SELECTION",
            )
        ).all()
        assert len(inbound_messages) == 1
        inbound_id = inbound_messages[0].id
        confirmations = [
            message
            for message in db.scalars(
                select(Message).where(
                    Message.lead_id == lead.id,
                    Message.direction == MessageDirection.OUTBOUND,
                )
            ).all()
            if (message.raw_payload or {}).get("inbound_message_id") == inbound_id
        ]
        assert len(confirmations) == 1
        confirmation = confirmations[0]
        assert "Réservé" in confirmation.body
        assert "calendrier" in confirmation.body.lower()
        assert lead.conversation_state == ConversationStateEnum.BOOKED
        assert lead.raw_payload["active_booking_offer"]["status"] == "booked"
        assert lead.raw_payload["active_booking_offer"]["slots"] == []
        bookings = db.scalars(
            select(CalendarBooking).where(
                CalendarBooking.lead_id == lead.id,
                CalendarBooking.status == "scheduled",
            )
        ).all()
        assert len(bookings) == 1, {
            "confirmation": confirmation.body,
            "lead_state": lead.conversation_state.value,
            "active_offer": (lead.raw_payload or {}).get("active_booking_offer"),
        }
        booking_audits = db.scalars(
            select(AuditLog).where(
                AuditLog.lead_id == lead.id,
                AuditLog.event_type == "calendar_booking_created",
            )
        ).all()
        assert len(booking_audits) == 1
        assert not db.scalars(
            select(LeadTask).where(LeadTask.lead_id == lead.id)
        ).all()


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
