from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.db.models import CalendarBooking, Client, ConversationStateEnum, Lead, LeadSource, Message, MessageDirection
from app.db.session import get_session_factory
from app.services.booking import BookingService


def _internal_always_open_config() -> dict:
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


def test_internal_calendar_offer_and_confirm_selection_creates_booking(test_context):
    SessionLocal = get_session_factory()
    booking_service = BookingService()
    lead_id = 0

    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.booking_mode = "internal"
        client.booking_config = _internal_always_open_config()

        lead = Lead(
            client_id=client.id,
            source=LeadSource.META,
            full_name="Internal Calendar Lead",
            phone="+15551110000",
            email="internal@example.com",
            city="Austin",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()
        lead_id = lead.id

        offer = booking_service.offer_slots(client=client, lead=lead, db=db)
        assert offer.slots
        assert offer.raw_payload["booking_offer"]["provider"] == "internal"

        history = [
            Message(
                lead_id=lead.id,
                client_id=client.id,
                direction=MessageDirection.OUTBOUND,
                body=offer.reply_text,
                provider_message_sid="SM-OFFER-1",
                raw_payload=offer.raw_payload,
            )
        ]

        result = booking_service.handle_slot_selection(
            client=client,
            lead=lead,
            inbound_text="1",
            history=history,
            db=db,
        )
        assert result is not None
        assert result.handled is True
        assert result.next_state == ConversationStateEnum.BOOKED
        assert result.raw_payload["calendar_booking"]["provider"] == "internal"
        db.commit()

    with SessionLocal() as db:
        bookings = db.scalars(select(CalendarBooking).where(CalendarBooking.lead_id == lead_id)).all()
        assert len(bookings) == 1
        assert bookings[0].provider == "internal"
        assert bookings[0].status == "scheduled"


def test_internal_calendar_offer_excludes_already_booked_slots(test_context):
    SessionLocal = get_session_factory()
    booking_service = BookingService()

    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.booking_mode = "internal"
        client.booking_config = _internal_always_open_config()

        lead = Lead(
            client_id=client.id,
            source=LeadSource.META,
            full_name="Taken Slot Lead",
            phone="+15551110001",
            email="taken@example.com",
            city="Austin",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.flush()

        first_offer = booking_service.offer_slots(client=client, lead=lead, db=db)
        assert first_offer.slots
        first_slot = first_offer.slots[0]

        start_at = datetime.fromisoformat(first_slot.start_time.replace("Z", "+00:00")).astimezone(timezone.utc)
        end_at = datetime.fromisoformat(first_slot.end_time.replace("Z", "+00:00")).astimezone(timezone.utc)
        db.add(
            CalendarBooking(
                client_id=client.id,
                lead_id=lead.id,
                provider="internal",
                source="test",
                status="scheduled",
                start_at=start_at,
                end_at=end_at,
                timezone=client.timezone or "UTC",
                title="Existing booking",
                notes="",
                created_at=datetime.now(timezone.utc) - timedelta(minutes=1),
                updated_at=datetime.now(timezone.utc) - timedelta(minutes=1),
            )
        )
        db.flush()

        second_offer = booking_service.offer_slots(client=client, lead=lead, db=db)
        assert second_offer.slots
        assert all(slot.start_time != first_slot.start_time for slot in second_offer.slots)


def test_internal_calendar_slot_handler_ignores_plain_availability_question(test_context):
    SessionLocal = get_session_factory()
    booking_service = BookingService()

    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.booking_mode = "internal"
        client.booking_config = _internal_always_open_config()

        lead = Lead(
            client_id=client.id,
            source=LeadSource.META,
            full_name="Question Lead",
            phone="+15551110002",
            email="question@example.com",
            city="Austin",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()

        offer = booking_service.offer_slots(client=client, lead=lead, db=db)
        history = [
            Message(
                lead_id=lead.id,
                client_id=client.id,
                direction=MessageDirection.OUTBOUND,
                body=offer.reply_text,
                provider_message_sid="SM-OFFER-Q",
                raw_payload=offer.raw_payload,
            )
        ]

        result = booking_service.handle_slot_selection(
            client=client,
            lead=lead,
            inbound_text="Do you have availability on Wednesday?",
            history=history,
            db=db,
        )
        assert result is None
