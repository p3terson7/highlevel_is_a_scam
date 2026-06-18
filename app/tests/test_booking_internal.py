from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

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


def test_internal_calendar_french_time_reply_books_matching_slot(test_context):
    SessionLocal = get_session_factory()
    booking_service = BookingService()

    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.booking_mode = "internal"
        client.timezone = "America/Toronto"
        client.provider_config = {"language": "fr"}
        client.booking_config = _internal_always_open_config()

        lead = Lead(
            client_id=client.id,
            source=LeadSource.META,
            full_name="French Time Lead",
            phone="+15551110006",
            email="french-time@example.com",
            city="Montreal",
            form_answers={},
            raw_payload={"lead_language": "fr"},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()

        offer = booking_service.offer_slots(client=client, lead=lead, limit=1, db=db)
        assert len(offer.slots) == 1
        slot = offer.slots[0]
        local_dt = datetime.fromisoformat(slot.start_time.replace("Z", "+00:00")).astimezone(ZoneInfo(client.timezone))
        french_time = f"{local_dt.hour}h{local_dt.minute:02d}"
        history = [
            Message(
                lead_id=lead.id,
                client_id=client.id,
                direction=MessageDirection.OUTBOUND,
                body=offer.reply_text,
                provider_message_sid="SM-OFFER-FR-TIME",
                raw_payload=offer.raw_payload,
            )
        ]

        result = booking_service.handle_slot_selection(
            client=client,
            lead=lead,
            inbound_text=french_time,
            history=history,
            db=db,
        )

        assert result is not None
        assert result.next_state == ConversationStateEnum.BOOKED
        assert result.raw_payload["calendar_booking"]["provider"] == "internal"
        assert "Votre appel" in result.reply_text


def test_internal_calendar_repeats_stored_english_slots_in_french(test_context):
    SessionLocal = get_session_factory()
    booking_service = BookingService()

    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.booking_mode = "internal"
        client.timezone = "America/Toronto"
        client.provider_config = {"language": "fr"}
        client.booking_config = _internal_always_open_config()

        lead = Lead(
            client_id=client.id,
            source=LeadSource.META,
            full_name="French Repeat Lead",
            phone="+15551110016",
            email="french-repeat@example.com",
            city="Montreal",
            form_answers={},
            raw_payload={"lead_language": "fr"},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()

        offer_payload = {
            "booking_offer": {
                "provider": "internal",
                "slots": [
                    {
                        "index": 1,
                        "start_time": "2026-06-24T14:00:00Z",
                        "end_time": "2026-06-24T14:30:00Z",
                        "display_time": "Wed Jun 24 at 10:00 AM",
                    },
                    {
                        "index": 2,
                        "start_time": "2026-06-24T15:00:00Z",
                        "end_time": "2026-06-24T15:30:00Z",
                        "display_time": "Wed Jun 24 at 11:00 AM",
                    },
                ],
            }
        }
        history = [
            Message(
                lead_id=lead.id,
                client_id=client.id,
                direction=MessageDirection.OUTBOUND,
                body="Voici les options.",
                provider_message_sid="SM-OFFER-FR-REPEAT",
                raw_payload=offer_payload,
            )
        ]

        result = booking_service.handle_slot_selection(
            client=client,
            lead=lead,
            inbound_text="mercredi prochain",
            history=history,
            db=db,
        )

        assert result is not None
        assert result.next_state == ConversationStateEnum.BOOKING_SENT
        assert "Wed Jun" not in result.reply_text
        assert "10:00 AM" not in result.reply_text
        assert "mercredi 24 juin à 10 h 00" in result.reply_text
        assert "Répondez 1 ou 2" in result.reply_text


def test_internal_calendar_lock_it_in_books_single_offered_slot(test_context):
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
            full_name="Lock It Lead",
            phone="+15551110007",
            email="lockit@example.com",
            city="Austin",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()

        offer = booking_service.offer_slots(client=client, lead=lead, limit=1, db=db)
        history = [
            Message(
                lead_id=lead.id,
                client_id=client.id,
                direction=MessageDirection.OUTBOUND,
                body=f"{offer.reply_text}\nReply 1 and I'll lock it in.",
                provider_message_sid="SM-OFFER-LOCK",
                raw_payload=offer.raw_payload,
            )
        ]

        result = booking_service.handle_slot_selection(
            client=client,
            lead=lead,
            inbound_text="lock it in",
            history=history,
            db=db,
        )

        assert result is not None
        assert result.next_state == ConversationStateEnum.BOOKED
        assert result.raw_payload["calendar_booking"]["slot"]["index"] == 1


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


def test_internal_calendar_first_offer_spreads_across_days_with_coverage_summary(test_context):
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
            full_name="Coverage Lead",
            phone="+15551110003",
            email="coverage@example.com",
            city="Austin",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()

        offer = booking_service.offer_slots(client=client, lead=lead, limit=3, db=db)
        assert len(offer.slots) == 3
        offered_dates = {datetime.fromisoformat(slot.start_time.replace("Z", "+00:00")).date() for slot in offer.slots}
        assert len(offered_dates) >= 2
        assert "openings including" in offer.reply_text.lower()
        assert "if none of those work" in offer.reply_text.lower()


def test_internal_calendar_find_slots_next_monday_stays_on_that_date(test_context):
    SessionLocal = get_session_factory()
    booking_service = BookingService()

    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.booking_mode = "internal"
        client.timezone = "America/Toronto"
        client.booking_config = _internal_always_open_config()

        lead = Lead(
            client_id=client.id,
            source=LeadSource.META,
            full_name="Next Monday Lead",
            phone="+15551110005",
            email="next-monday@example.com",
            city="Austin",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()

        offer = booking_service.find_slots(
            client=client,
            lead=lead,
            request_text="I am available for a call next Monday all day",
            limit=3,
            db=db,
        )

        local_today = datetime.now(timezone.utc).astimezone(ZoneInfo(client.timezone)).date()
        days_until_monday = (0 - local_today.weekday()) % 7 or 7
        expected_date = local_today + timedelta(days=days_until_monday)
        offered_dates = {
            datetime.fromisoformat(slot.start_time.replace("Z", "+00:00")).astimezone(ZoneInfo(client.timezone)).date()
            for slot in offer.slots
        }

        assert len(offer.slots) == 3
        assert offered_dates == {expected_date}
        assert offer.raw_payload["booking_offer"]["request"]["scope"] == "specific_date"
        assert offer.raw_payload["booking_offer"]["planner"]["strategy"].startswith("specific_date")
        assert "consultation call" in offer.reply_text.lower()


def test_internal_calendar_reschedule_cancels_previous_booking(test_context):
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
            full_name="Reschedule Lead",
            phone="+15551110004",
            email="reschedule@example.com",
            city="Austin",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
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
        assert first_result["booking"]["status"] == "scheduled"

        second_offer = booking_service.offer_slots(client=client, lead=lead, db=db)
        second_result = booking_service.book_requested_slot(
            client=client,
            lead=lead,
            latest_offer=second_offer.raw_payload["booking_offer"],
            slot_index=1,
            db=db,
        )
        assert second_result["reply_text"].startswith("Updated.")
        db.commit()

        bookings = db.scalars(select(CalendarBooking).where(CalendarBooking.lead_id == lead.id)).all()
        scheduled = [booking for booking in bookings if booking.status == "scheduled"]
        cancelled = [booking for booking in bookings if booking.status == "cancelled"]
        assert len(scheduled) == 1
        assert len(cancelled) == 1
        assert scheduled[0].id != cancelled[0].id


def test_internal_calendar_conversation_reschedule_requires_confirmation(test_context):
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
            full_name="Confirm Reschedule Lead",
            phone="+15551110008",
            email="confirm-reschedule@example.com",
            city="Austin",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKED,
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
        history = [
            Message(
                lead_id=lead.id,
                client_id=client.id,
                direction=MessageDirection.OUTBOUND,
                body=second_offer.reply_text,
                provider_message_sid="SM-OFFER-RESCHEDULE",
                raw_payload=second_offer.raw_payload,
            )
        ]

        confirm_request = booking_service.handle_slot_selection(
            client=client,
            lead=lead,
            inbound_text="1",
            history=history,
            db=db,
        )

        assert confirm_request is not None
        assert confirm_request.next_state == ConversationStateEnum.BOOKED
        assert confirm_request.audit_event_type == "calendar_reschedule_confirmation_requested"
        assert "Should I cancel" in confirm_request.reply_text
        assert confirm_request.raw_payload["pending_step"] == "reschedule_confirmation_pending"
        assert "calendar_booking" not in confirm_request.raw_payload

        bookings_before_confirmation = db.scalars(select(CalendarBooking).where(CalendarBooking.lead_id == lead.id)).all()
        assert len([booking for booking in bookings_before_confirmation if booking.status == "scheduled"]) == 1

        lead.raw_payload = {
            **(lead.raw_payload or {}),
            "pending_reschedule_confirmation": confirm_request.raw_payload["pending_reschedule_confirmation"],
            "pending_step": confirm_request.raw_payload["pending_step"],
        }
        confirmed = booking_service.handle_reschedule_confirmation(
            client=client,
            lead=lead,
            inbound_text="yes",
            history=history,
            db=db,
        )

        assert confirmed is not None
        assert confirmed.next_state == ConversationStateEnum.BOOKED
        assert confirmed.raw_payload["calendar_booking"]["provider"] == "internal"
        db.commit()

        bookings = db.scalars(select(CalendarBooking).where(CalendarBooking.lead_id == lead.id)).all()
        scheduled = [booking for booking in bookings if booking.status == "scheduled"]
        cancelled = [booking for booking in bookings if booking.status == "cancelled"]
        assert len(scheduled) == 1
        assert len(cancelled) == 1
        assert cancelled[0].id == old_booking_id


def test_internal_calendar_reschedule_decline_keeps_existing_booking(test_context):
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
            full_name="Decline Reschedule Lead",
            phone="+15551110009",
            email="decline-reschedule@example.com",
            city="Austin",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKED,
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
        confirm_request = booking_service.handle_slot_selection(
            client=client,
            lead=lead,
            inbound_text="1",
            history=[
                Message(
                    lead_id=lead.id,
                    client_id=client.id,
                    direction=MessageDirection.OUTBOUND,
                    body=second_offer.reply_text,
                    provider_message_sid="SM-OFFER-DECLINE",
                    raw_payload=second_offer.raw_payload,
                )
            ],
            db=db,
        )
        assert confirm_request is not None
        lead.raw_payload = {
            "pending_reschedule_confirmation": confirm_request.raw_payload["pending_reschedule_confirmation"],
            "pending_step": confirm_request.raw_payload["pending_step"],
        }

        declined = booking_service.handle_reschedule_confirmation(
            client=client,
            lead=lead,
            inbound_text="no, keep the original",
            history=[],
            db=db,
        )

        assert declined is not None
        assert declined.audit_event_type == "calendar_reschedule_declined"
        assert declined.raw_payload["pending_reschedule_confirmation"] is None
        db.commit()

        bookings = db.scalars(select(CalendarBooking).where(CalendarBooking.lead_id == lead.id)).all()
        scheduled = [booking for booking in bookings if booking.status == "scheduled"]
        cancelled = [booking for booking in bookings if booking.status == "cancelled"]
        assert len(scheduled) == 1
        assert scheduled[0].id == old_booking_id
        assert cancelled == []
