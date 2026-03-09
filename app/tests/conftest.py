from __future__ import annotations

from dataclasses import dataclass

import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.core.deps import clear_dependency_caches, get_booking_service, get_llm_agent, get_sms_service
from app.db.models import Base, Client, ConversationStateEnum
from app.db.session import get_engine, get_session_factory, reset_db_caches
from app.services.booking import BookingSelectionResult, BookingSlot, SlotOffer
from app.services.llm_agent import AgentAction, AgentResponse


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

    def next_reply(self, client: Client, lead, inbound_text: str, history):
        self.calls += 1
        _ = lead
        _ = history
        action_type = "offer_calendar_slots" if getattr(client, "booking_mode", "link") == "calendly" else "send_booking_link"
        return AgentResponse(
            reply_text=f"Thanks for the message about '{inbound_text}'.",
            next_state=ConversationStateEnum.QUALIFYING,
            actions=[AgentAction(type=action_type, payload={"url": client.booking_url})],
        )


class FakeBookingService:
    def __init__(self) -> None:
        self.offer_calls = 0
        self.selection_calls = 0

    def preview_slots(self, client: Client, limit: int = 3) -> SlotOffer:
        return self.offer_slots(client=client, lead=None, limit=limit)

    def offer_slots(self, client: Client, lead, limit: int = 3) -> SlotOffer:
        _ = client
        _ = lead
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

    def handle_slot_selection(self, *, client: Client, lead, inbound_text: str, history):
        _ = client
        _ = history
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
