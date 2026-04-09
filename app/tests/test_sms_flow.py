from sqlalchemy import select

from app.core.deps import get_llm_agent
from app.db.models import Client, ConversationStateEnum, Lead, LeadSource, Message, MessageDirection
from app.db.session import get_session_factory
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
    assert test_context.fake_llm.calls >= 1
    assert test_context.fake_booking.selection_calls >= 1
    assert "Booked. You are set" in test_context.fake_sms.sent[-1]["body"]

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15554443333"))
        assert lead is not None
        assert lead.conversation_state.value == "BOOKED"
        assert lead.crm_stage == "Meeting Booked"


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
