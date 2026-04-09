from __future__ import annotations

from dataclasses import dataclass

import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.core.deps import clear_dependency_caches, get_booking_service, get_llm_agent, get_sms_service
from app.db.models import Base, Client, ConversationStateEnum
from app.db.session import get_engine, get_session_factory, reset_db_caches
from app.services.booking import BookingSelectionResult, BookingSlot, SlotOffer
from app.services.llm_agent import AgentResponse


class FakeSMSService:
    def __init__(self) -> None:
        self.sent: list[dict[str, str]] = []

    def render_template(self, client: Client, template_key: str, context: dict | None = None) -> str:
        templates = {
            "stop_confirmation": "You are unsubscribed.",
            "help_response": "Help message.",
            "after_hours": "We will reply tomorrow from {business_name}.",
            "initial_sms": "Hi from {business_name}.",
            "follow_up": "Follow up from {business_name}: {booking_url}",
        }
        template = templates.get(template_key, template_key)
        values = {
            "business_name": client.business_name,
            "booking_url": client.booking_url,
        }
        if context:
            values.update(context)
        return template.format(**values)

    def send_message(self, to_number: str, body: str) -> str:
        sid = f"SM{len(self.sent) + 1:06d}"
        self.sent.append({"to": to_number, "body": body, "sid": sid})
        return sid


class FakeLLMAgent:
    def __init__(self) -> None:
        self.calls = 0

    def run_turn(self, *, client: Client, lead, inbound_text: str, history, booking_service=None, db=None):
        self.calls += 1
        lower = inbound_text.strip().lower()
        if "what are you availabilities on tuesday" in lower or "what are your availabilities on tuesday" in lower:
            offer = booking_service.find_slots(client=client, lead=lead, preferred_day="tuesday", limit=3, db=db)
            return AgentResponse(
                reply_text=offer.reply_text,
                next_state=ConversationStateEnum.BOOKING_SENT,
                runtime_payload={
                    "booking_offer": offer.raw_payload.get("booking_offer", {}),
                    "pending_step": "slot_selection_pending",
                },
                action="none",
            )
        if "wednesday" in lower and booking_service:
            offer = booking_service.find_slots(
                client=client,
                lead=lead,
                preferred_day="wednesday",
                exact_time="11 am" if "11" in lower else None,
                range_start="10 am" if "between 10" in lower else None,
                range_end="3 pm" if "and 3" in lower else None,
                limit=3,
                db=db,
            )
            return AgentResponse(
                reply_text=offer.reply_text,
                next_state=ConversationStateEnum.BOOKING_SENT,
                runtime_payload={
                    "booking_offer": offer.raw_payload.get("booking_offer", {}),
                    "pending_step": "slot_selection_pending",
                },
                action="none",
            )
        if "thursday" in lower and booking_service:
            offer = booking_service.find_slots(
                client=client,
                lead=lead,
                preferred_day="thursday",
                limit=3,
                db=db,
            )
            return AgentResponse(
                reply_text=offer.reply_text,
                next_state=ConversationStateEnum.BOOKING_SENT,
                runtime_payload={
                    "booking_offer": offer.raw_payload.get("booking_offer", {}),
                    "pending_step": "slot_selection_pending",
                },
                action="none",
            )
        if booking_service and ("book this week" in lower or "schedule now" in lower or "send me times" in lower):
            offer = booking_service.find_slots(client=client, lead=lead, limit=3, db=db)
            return AgentResponse(
                reply_text=offer.reply_text,
                next_state=ConversationStateEnum.BOOKING_SENT,
                runtime_payload={
                    "booking_offer": offer.raw_payload.get("booking_offer", {}),
                    "pending_step": "slot_selection_pending",
                },
                action="none",
            )
        if booking_service and lower in {"1", "monday 10am", "monday 10 am"}:
            latest_offer = None
            for message in reversed(history):
                offer = (message.raw_payload or {}).get("booking_offer")
                if isinstance(offer, dict):
                    latest_offer = offer
                    break
            result = booking_service.book_requested_slot(
                client=client,
                lead=lead,
                latest_offer=latest_offer,
                slot_index=1,
                db=db,
            )
            return AgentResponse(
                reply_text=result["reply_text"],
                next_state=ConversationStateEnum.BOOKED,
                runtime_payload=result["runtime_payload"],
                action="mark_booked",
            )
        return AgentResponse(
            reply_text=f"Thanks for the message about '{inbound_text}'.",
            next_state=ConversationStateEnum.QUALIFYING,
            action="ask_next_question",
            next_question_key="decision_makers",
        )

    def next_reply(self, client: Client, lead, inbound_text: str, history):
        return self.run_turn(client=client, lead=lead, inbound_text=inbound_text, history=history, booking_service=None, db=None)


class FakeBookingService:
    def __init__(self) -> None:
        self.offer_calls = 0
        self.selection_calls = 0

    def preview_slots(self, client: Client, limit: int = 3, db=None) -> SlotOffer:
        _ = db
        return self.offer_slots(client=client, lead=None, limit=limit)

    def offer_slots(self, client: Client, lead, limit: int = 3, db=None) -> SlotOffer:
        _ = client
        _ = lead
        _ = db
        self.offer_calls += 1
        slots = [
            BookingSlot(
                index=1,
                start_time="2026-03-09T15:00:00Z",
                end_time="2026-03-09T15:30:00Z",
                display_time="Mon Mar 09 at 10:00 AM",
                display_hint="Monday 10:00 AM",
                search_blob="monday 10am | monday 10 00 am | mon 10am",
            ),
            BookingSlot(
                index=2,
                start_time="2026-03-09T17:00:00Z",
                end_time="2026-03-09T17:30:00Z",
                display_time="Mon Mar 09 at 12:00 PM",
                display_hint="Monday 12:00 PM",
                search_blob="monday 12pm | monday 12 00 pm | mon 12pm",
            ),
        ][:limit]
        return SlotOffer(
            reply_text="I can book this directly. Here are the next available times:\n1) Mon Mar 09 at 10:00 AM\n2) Mon Mar 09 at 12:00 PM\nReply with 1 or 2.",
            slots=slots,
            raw_payload={
                "booking_offer": {
                    "provider": "calendly",
                    "slots": [slot.__dict__ for slot in slots],
                }
            },
        )

    def find_slots(self, *, client: Client, lead, preferred_day: str | None = None, avoid_day: str | None = None, preferred_period: str | None = None, exact_time: str | None = None, range_start: str | None = None, range_end: str | None = None, limit: int = 3, db=None) -> SlotOffer:
        _ = avoid_day
        _ = preferred_period
        offer = self.offer_slots(client=client, lead=lead, limit=limit, db=db)
        if preferred_day and preferred_day.lower().startswith("tue"):
            slots = [
                BookingSlot(
                    index=1,
                    start_time="2026-03-10T14:00:00Z",
                    end_time="2026-03-10T14:30:00Z",
                    display_time="Tue Mar 10 at 10:00 AM",
                    display_hint="Tuesday 10:00 AM",
                    search_blob="tuesday 10am | tuesday 10 am | tue 10am",
                ),
                BookingSlot(
                    index=2,
                    start_time="2026-03-10T16:00:00Z",
                    end_time="2026-03-10T16:30:00Z",
                    display_time="Tue Mar 10 at 12:00 PM",
                    display_hint="Tuesday 12:00 PM",
                    search_blob="tuesday 12pm | tuesday 12 pm | tue 12pm",
                ),
                BookingSlot(
                    index=3,
                    start_time="2026-03-10T18:00:00Z",
                    end_time="2026-03-10T18:30:00Z",
                    display_time="Tue Mar 10 at 2:00 PM",
                    display_hint="Tuesday 2:00 PM",
                    search_blob="tuesday 2pm | tuesday 2 pm | tue 2pm",
                ),
            ]
            if range_start or range_end:
                slots = [slot for slot in slots if "10:00 AM" in slot.display_time or "12:00 PM" in slot.display_time or "2:00 PM" in slot.display_time]
            return SlotOffer(
                reply_text="I found a few Tuesday options:\n1) Tue Mar 10 at 10:00 AM\n2) Tue Mar 10 at 12:00 PM\n3) Tue Mar 10 at 2:00 PM\nReply with 1, 2, or 3.",
                slots=slots[:limit],
                raw_payload={"booking_offer": {"provider": "calendly", "slots": [slot.__dict__ for slot in slots[:limit]]}},
            )
        if preferred_day and preferred_day.lower().startswith("wed"):
            if exact_time and "11" in exact_time:
                slots = [
                    BookingSlot(
                        index=1,
                        start_time="2026-03-11T16:00:00Z",
                        end_time="2026-03-11T16:30:00Z",
                        display_time="Wed Mar 11 at 11:00 AM",
                        display_hint="Wednesday 11:00 AM",
                        search_blob="wednesday 11am | wednesday 11 am | wed 11am",
                    ),
                    BookingSlot(
                        index=2,
                        start_time="2026-03-11T16:30:00Z",
                        end_time="2026-03-11T17:00:00Z",
                        display_time="Wed Mar 11 at 11:30 AM",
                        display_hint="Wednesday 11:30 AM",
                        search_blob="wednesday 11 30am | wed 11 30am",
                    ),
                ][:limit]
                return SlotOffer(
                    reply_text="I found a couple Wednesday options around 11:\n1) Wed Mar 11 at 11:00 AM\n2) Wed Mar 11 at 11:30 AM\nReply with 1 or 2.",
                    slots=slots,
                    raw_payload={"booking_offer": {"provider": "calendly", "slots": [slot.__dict__ for slot in slots]}},
                )
            slots = [
                BookingSlot(
                    index=1,
                    start_time="2026-03-11T15:00:00Z",
                    end_time="2026-03-11T15:30:00Z",
                    display_time="Wed Mar 11 at 10:00 AM",
                    display_hint="Wednesday 10:00 AM",
                    search_blob="wednesday 10am | wed 10am",
                ),
                BookingSlot(
                    index=2,
                    start_time="2026-03-11T17:00:00Z",
                    end_time="2026-03-11T17:30:00Z",
                    display_time="Wed Mar 11 at 12:00 PM",
                    display_hint="Wednesday 12:00 PM",
                    search_blob="wednesday 12pm | wed 12pm",
                ),
            ][:limit]
            return SlotOffer(
                reply_text="I found a few Wednesday options:\n1) Wed Mar 11 at 10:00 AM\n2) Wed Mar 11 at 12:00 PM\nReply with 1 or 2.",
                slots=slots,
                raw_payload={"booking_offer": {"provider": "calendly", "slots": [slot.__dict__ for slot in slots]}},
            )
        if preferred_day and preferred_day.lower().startswith("thu"):
            slots = [
                BookingSlot(
                    index=1,
                    start_time="2026-03-12T15:00:00Z",
                    end_time="2026-03-12T15:30:00Z",
                    display_time="Thu Mar 12 at 10:00 AM",
                    display_hint="Thursday 10:00 AM",
                    search_blob="thursday 10am | thu 10am",
                ),
                BookingSlot(
                    index=2,
                    start_time="2026-03-12T17:00:00Z",
                    end_time="2026-03-12T17:30:00Z",
                    display_time="Thu Mar 12 at 12:00 PM",
                    display_hint="Thursday 12:00 PM",
                    search_blob="thursday 12pm | thu 12pm",
                ),
            ][:limit]
            return SlotOffer(
                reply_text="I found a few Thursday options:\n1) Thu Mar 12 at 10:00 AM\n2) Thu Mar 12 at 12:00 PM\nReply with 1 or 2.",
                slots=slots,
                raw_payload={"booking_offer": {"provider": "calendly", "slots": [slot.__dict__ for slot in slots]}},
            )
        return offer

    def book_requested_slot(self, *, client: Client, lead, latest_offer, slot_index=None, slot_start_time=None, slot_text=None, db=None):
        _ = client
        _ = slot_start_time
        _ = slot_text
        _ = db
        self.selection_calls += 1
        slots = (latest_offer or {}).get("slots", []) if isinstance(latest_offer, dict) else []
        selected = None
        for slot in slots:
            if slot_index and int(slot.get("index", 0)) == int(slot_index):
                selected = slot
                break
        if selected is None and slots:
            selected = slots[0]
        display_time = (selected or {}).get("display_time", "Mon Mar 09 at 10:00 AM")
        return {
            "reply_text": f"Booked. You are set for {display_time}. Confirmation will be sent to {lead.email}.",
            "booking": {"event_uri": "https://api.calendly.com/scheduled_events/1", "provider": "calendly"},
            "runtime_payload": {
                "calendar_booking": {
                    "provider": "calendly",
                    "slot": selected or {"index": 1, "display_time": display_time},
                    "booking": {"event_uri": "https://api.calendly.com/scheduled_events/1"},
                },
                "pending_step": None,
            },
        }

    def handle_slot_selection(self, *, client: Client, lead, inbound_text: str, history, db=None):
        _ = client
        _ = history
        _ = db
        self.selection_calls += 1
        if inbound_text.strip() not in {"1", "Monday 10am", "Monday 10 AM"}:
            return BookingSelectionResult(
                handled=True,
                reply_text="I did not catch which slot you want.\n1) Mon Mar 09 at 10:00 AM\n2) Mon Mar 09 at 12:00 PM\nReply with 1 or 2.",
                next_state=ConversationStateEnum.BOOKING_SENT,
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
                audit_event_type="calendar_booking_offer_repeated",
                audit_decision={"inbound": inbound_text},
                transition_reason="calendar_booking_offer_repeated",
            )

        return BookingSelectionResult(
            handled=True,
            reply_text=f"Booked. You are set for Mon Mar 09 at 10:00 AM. Confirmation will be sent to {lead.email}.",
            next_state=ConversationStateEnum.BOOKED,
            raw_payload={
                "calendar_booking": {
                    "provider": "calendly",
                    "slot": {"index": 1, "start_time": "2026-03-09T15:00:00Z", "display_time": "Mon Mar 09 at 10:00 AM"},
                    "booking": {"event_uri": "https://api.calendly.com/scheduled_events/1"},
                }
            },
            audit_event_type="calendar_booking_created",
            audit_decision={"inbound": inbound_text},
            transition_reason="calendar_booking_created",
        )


@dataclass
class TestContext:
    client: TestClient
    fake_sms: FakeSMSService
    fake_llm: FakeLLMAgent
    fake_booking: FakeBookingService
    client_key: str


@pytest.fixture
def test_context(tmp_path, monkeypatch) -> TestContext:
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+pysqlite:///{db_path.as_posix()}")
    monkeypatch.setenv("RQ_EAGER", "true")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "")
    monkeypatch.setenv("ADMIN_TOKEN", "test-admin-token")
    monkeypatch.setenv("AUTO_CREATE_TABLES", "false")

    get_settings.cache_clear()
    clear_dependency_caches()
    reset_db_caches()

    engine = get_engine()
    Base.metadata.create_all(engine)

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        seeded_client = Client(
            client_key="test-client-key",
            business_name="Acme Solar",
            tone="friendly",
            timezone="UTC",
            qualification_questions=[
                "What problem are you trying to solve?",
                "When would you like to start?",
            ],
            booking_url="https://example.com/book",
            booking_mode="link",
            booking_config={},
            fallback_handoff_number="+15550001111",
            consent_text="Reply STOP to opt out.",
            operating_hours={"days": [0, 1, 2, 3, 4, 5, 6], "start": "00:00", "end": "23:59"},
            faq_context="We install residential solar systems.",
            template_overrides={},
        )
        db.add(seeded_client)
        db.commit()

    from app.main import app

    fake_sms = FakeSMSService()
    fake_llm = FakeLLMAgent()
    fake_booking = FakeBookingService()
    app.dependency_overrides[get_sms_service] = lambda: fake_sms
    app.dependency_overrides[get_llm_agent] = lambda: fake_llm
    app.dependency_overrides[get_booking_service] = lambda: fake_booking

    with TestClient(app) as client:
        yield TestContext(
            client=client,
            fake_sms=fake_sms,
            fake_llm=fake_llm,
            fake_booking=fake_booking,
            client_key="test-client-key",
        )

    app.dependency_overrides.clear()
