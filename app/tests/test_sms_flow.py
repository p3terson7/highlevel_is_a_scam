from sqlalchemy import select

from app.db.models import Client, ConversationStateEnum, Lead, LeadSource, Message
from app.db.session import get_session_factory


def test_sms_inbound_agent_reply_flow(test_context):
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
    assert response.headers["content-type"].startswith("application/xml")
    assert "<Response></Response>" in response.text
    assert test_context.fake_llm.calls == 1

    assert len(test_context.fake_sms.sent) >= 1
    assert "https://example.com/book" in test_context.fake_sms.sent[-1]["body"]

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15557778888"))
        assert lead is not None
        assert lead.conversation_state.value == "BOOKING_SENT"

        messages = db.scalars(select(Message).where(Message.lead_id == lead.id)).all()
        assert len(messages) >= 2


def test_sms_inbound_agent_reply_ignores_operating_hours(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.operating_hours = {"days": [], "start": "09:00", "end": "09:01"}

        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-003",
            source=LeadSource.META,
            full_name="After Hours Lead",
            phone="+15556667777",
            email="afterhours@example.com",
            city="Denver",
            form_answers={"interest": "roof repair"},
            raw_payload={"source": "seed"},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 666-7777",
            "Body": "Can you send the booking link?",
            "MessageSid": "SM-IN-002",
        },
    )

    assert response.status_code == 200
    assert test_context.fake_llm.calls == 1
    assert "https://example.com/book" in test_context.fake_sms.sent[-1]["body"]

    with SessionLocal() as db:
        messages = db.scalars(select(Message).join(Lead).where(Lead.phone == "+15556667777")).all()
        assert len(messages) >= 2


def test_sms_inbound_can_offer_and_book_calendar_slots(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.booking_mode = "calendly"
        client.booking_config = {
            "calendly_personal_access_token": "demo-token",
            "calendly_event_type_uri": "https://api.calendly.com/event_types/demo",
        }
        lead = Lead(
            client_id=client.id,
            external_lead_id="meta-lead-004",
            source=LeadSource.META,
            full_name="Calendar Lead",
            phone="+15554443333",
            email="calendar@example.com",
            city="Denver",
            form_answers={"interest": "consultation"},
            raw_payload={"source": "seed"},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.QUALIFYING,
        )
        db.add(lead)
        db.commit()

    offer = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 444-3333",
            "Body": "Can I book this week?",
            "MessageSid": "SM-IN-010",
        },
    )
    assert offer.status_code == 200
    assert test_context.fake_booking.offer_calls == 1
    assert "next available times" in test_context.fake_sms.sent[-1]["body"]

    confirm = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 444-3333",
            "Body": "1",
            "MessageSid": "SM-IN-011",
        },
    )
    assert confirm.status_code == 200
    assert test_context.fake_booking.selection_calls >= 1
    assert "Booked. You are set" in test_context.fake_sms.sent[-1]["body"]

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15554443333"))
        assert lead is not None
        assert lead.conversation_state.value == "BOOKED"
        messages = db.scalars(select(Message).where(Message.lead_id == lead.id)).all()
        assert any((message.raw_payload or {}).get("booking_offer") for message in messages)
        assert any((message.raw_payload or {}).get("calendar_booking") for message in messages)
