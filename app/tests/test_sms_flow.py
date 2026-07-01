from sqlalchemy import select

from app.core.deps import get_booking_service, get_llm_agent
from app.db.models import AuditLog, CalendarBooking, Client, ConversationStateEnum, Lead, LeadSource, Message, MessageAttachment, MessageDirection
from app.db.session import get_session_factory
from app.services.booking import BookingService
from app.services.llm_agent import AgentResponse, LLMAgent


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
                raw_payload=test_context.fake_sms.with_delivery_status({"source": "test"}, "SM-DELIVERY-001"),
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

    with SessionLocal() as db:
        message = db.scalar(select(Message).where(Message.provider_message_sid == "SM-DELIVERY-001"))
        assert message is not None
        delivery = message.raw_payload["delivery"]
        assert delivery["status"] == "undelivered"
        assert delivery["severity"] == "warning"
        assert delivery["label_fr"] == "SMS non livré"
        assert "téléphone injoignable" in delivery["description_fr"]
        lead = db.get(Lead, lead_id)
        assert lead is not None
        assert lead.raw_payload["sms_contactability"]["status"] == "sms_failed"
        audit = db.scalar(select(AuditLog).where(AuditLog.lead_id == lead_id, AuditLog.event_type == "sms_delivery_failed"))
        assert audit is not None

    thread = test_context.client.get(
        f"/ui/api/conversations/{lead_id}/thread",
        headers={"X-Admin-Token": "test-admin-token"},
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
    async def fake_download_twilio_media(**kwargs):
        assert kwargs["media_url"] == "https://api.twilio.com/2010-04-01/Accounts/AC/Messages/MM/Media/ME"
        assert kwargs["content_type"] == "image/jpeg"
        return b"inbound-image"

    monkeypatch.setattr("app.api.routes_sms.download_twilio_media", fake_download_twilio_media)

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 000-4455",
            "Body": "",
            "MessageSid": "SM-IN-MEDIA-001",
            "NumMedia": "1",
            "MediaUrl0": "https://api.twilio.com/2010-04-01/Accounts/AC/Messages/MM/Media/ME",
            "MediaContentType0": "image/jpeg",
        },
    )

    assert response.status_code == 200
    assert test_context.fake_llm.calls == 0
    assert len(test_context.fake_sms.sent) == 1
    assert "attachment" in test_context.fake_sms.sent[-1]["body"].lower()

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15550004455"))
        assert lead is not None
        lead_id = lead.id
        assert lead.conversation_state == ConversationStateEnum.HANDOFF
        assert lead.raw_payload["handoff"]["reason"] == "unsupported_media"
        message = db.scalar(select(Message).where(Message.lead_id == lead.id, Message.direction == MessageDirection.INBOUND))
        assert message is not None
        assert message.raw_payload["attachments"][0]["media_kind"] == "image"
        attachment = db.scalar(select(MessageAttachment).where(MessageAttachment.message_id == message.id))
        assert attachment is not None
        assert attachment.content_type == "image/jpeg"

    thread = test_context.client.get(
        f"/ui/api/conversations/{lead_id}/thread",
        headers={"X-Admin-Token": "test-admin-token"},
    )
    assert thread.status_code == 200
    thread_payload = thread.json()
    assert thread_payload["messages"][0]["attachments"][0]["media_kind"] == "image"


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
