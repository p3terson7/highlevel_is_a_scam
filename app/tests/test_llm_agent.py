import json

from app.db.models import Client, ConversationStateEnum, Lead, LeadSource, Message, MessageDirection
from app.services.llm_agent import LLMAgent, AgentResponse


class FailingProvider:
    name = "failing"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        _ = user_prompt
        raise RuntimeError("provider unavailable")


class PartialProjectProvider:
    name = "partial-project"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        payload = json.loads(user_prompt)
        memory = payload["qualification_memory"]
        assert "service_needed" in memory
        return {
            "reply_text": "Thanks. Are you the decision-maker, and should anyone else join the call?",
            "next_state": "QUALIFYING",
            "collected_fields": memory,
            "next_question_key": "decision_makers",
            "action": "ask_next_question",
            "tool_call": {"name": "none", "args": {}},
        }


class ServiceQuestionProvider:
    name = "service-question"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        payload = json.loads(user_prompt)
        assert "building documentation" in payload["faq_context"].lower()
        return {
            "reply_text": "Yes, we can handle single sites or multi-site rollouts. How many locations are you looking at?",
            "next_state": "QUALIFYING",
            "collected_fields": payload["qualification_memory"],
            "next_question_key": "decision_makers",
            "action": "ask_next_question",
            "tool_call": {"name": "none", "args": {}},
        }


class BookingToolProvider:
    name = "booking-tool"

    def __init__(self) -> None:
        self.calls = 0

    def generate_json(self, system_prompt: str, user_prompt: str):
        self.calls += 1
        payload = json.loads(user_prompt)
        if self.calls == 1:
            assert payload["booking_ready"] is True
            assert payload["latest_inbound_message"] == "Within 2 weeks. I'm the owner and email works best."
            return {
                "reply_text": "",
                "next_state": "BOOKING_SENT",
                "collected_fields": payload["qualification_memory"],
                "next_question_key": None,
                "action": "none",
                "tool_call": {"name": "find_slots", "args": {"limit": 3}},
            }
        tool_result = payload["tool_result"]
        assert tool_result["kind"] == "slots"
        return {
            "reply_text": "I can do Tuesday at 10:00 AM or 12:00 PM. Which works better?",
            "next_state": "BOOKING_SENT",
            "collected_fields": payload["conversation_context"]["qualification_memory"],
            "next_question_key": None,
            "action": "none",
            "tool_call": {"name": "none", "args": {}},
        }


class BookedProvider:
    name = "booked"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        payload = json.loads(user_prompt)
        return {
            "reply_text": "Perfect. You're booked.",
            "next_state": "BOOKED",
            "collected_fields": payload["qualification_memory"],
            "next_question_key": None,
            "action": "mark_booked",
            "tool_call": {"name": "mark_booked", "args": {}},
        }


class RepeatingProvider:
    name = "repeating"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        payload = json.loads(user_prompt)
        assert "decision_makers" in payload["asked_question_keys"]
        return {
            "reply_text": "Got it. Are you the decision-maker, and should anyone else join the call?",
            "next_state": "QUALIFYING",
            "collected_fields": payload["qualification_memory"],
            "next_question_key": "decision_makers",
            "action": "ask_next_question",
            "tool_call": {"name": "none", "args": {}},
        }


class FakeBookingService:
    def find_slots(
        self,
        *,
        client: Client,
        lead,
        preferred_day=None,
        avoid_day=None,
        preferred_period=None,
        exact_time=None,
        range_start=None,
        range_end=None,
        limit: int = 3,
        db=None,
    ):
        _ = client
        _ = lead
        _ = preferred_day
        _ = avoid_day
        _ = preferred_period
        _ = exact_time
        _ = range_start
        _ = range_end
        _ = db
        slots = [
            type("Slot", (), {"__dict__": {"index": 1, "display_time": "Tue Apr 07 at 10:00 AM", "start_time": "2026-04-07T14:00:00Z", "end_time": "2026-04-07T14:30:00Z"}})(),
            type("Slot", (), {"__dict__": {"index": 2, "display_time": "Tue Apr 07 at 12:00 PM", "start_time": "2026-04-07T16:00:00Z", "end_time": "2026-04-07T16:30:00Z"}})(),
        ][:limit]
        return type(
            "Offer",
            (),
            {
                "reply_text": "I found a few times that should work:\n1) Tue Apr 07 at 10:00 AM\n2) Tue Apr 07 at 12:00 PM\nReply with 1 or 2.",
                "slots": slots,
                "raw_payload": {"booking_offer": {"provider": "internal", "slots": [slot.__dict__ for slot in slots]}},
            },
        )()


def _client() -> Client:
    return Client(
        client_key="survey-north",
        business_name="Survey North",
        tone="clear, helpful, concise",
        timezone="UTC",
        qualification_questions=[],
        booking_url="https://survey.example/book",
        booking_mode="internal",
        booking_config={},
        fallback_handoff_number="+15550001111",
        consent_text="Reply STOP to opt out.",
        operating_hours={"days": [0, 1, 2, 3, 4, 5, 6], "start": "00:00", "end": "23:59"},
        faq_context="We provide building documentation, measured surveys, CAD as-builts, and Revit/BIM deliverables for commercial and multi-site projects.",
        ai_context="Speak like a practical project consultant. Answer clearly and move to booking when the project scope is clear.",
        template_overrides={},
        is_active=True,
    )


def _lead(state: ConversationStateEnum = ConversationStateEnum.QUALIFYING, raw_payload: dict | None = None) -> Lead:
    return Lead(
        client_id=1,
        source=LeadSource.MANUAL,
        full_name="Jordan Lee",
        phone="+15551234567",
        email="jordan@example.com",
        city="Toronto",
        form_answers={},
        raw_payload=raw_payload or {},
        consented=True,
        opted_out=False,
        conversation_state=state,
    )


def test_partial_project_info_is_extracted_and_next_best_question_is_asked():
    agent = LLMAgent(provider=PartialProjectProvider())

    response = agent.next_reply(
        client=_client(),
        lead=_lead(),
        inbound_text="We need Revit for one retail space around 12,000 sqft.",
        history=[],
    )

    assert response.action == "ask_next_question"
    assert response.next_state == ConversationStateEnum.QUALIFYING
    assert response.next_question_key == "decision_makers"
    assert response.collected_fields.service_needed is not None
    assert response.reply_text.endswith("?")


def test_service_question_is_answered_then_flow_continues():
    agent = LLMAgent(provider=ServiceQuestionProvider())

    response = agent.next_reply(
        client=_client(),
        lead=_lead(),
        inbound_text="Do you handle multiple buildings?",
        history=[],
    )

    text = response.reply_text.lower()
    assert "yes" in text
    assert "multi-site" in text or "multiple" in text
    assert response.action == "ask_next_question"
    assert response.next_question_key == "decision_makers"
    assert response.reply_text.count("?") == 1


def test_booking_ready_turn_uses_tool_and_returns_slots_reply():
    agent = LLMAgent(provider=BookingToolProvider())
    lead = _lead()
    history = [
        Message(direction=MessageDirection.INBOUND, body="We need CAD as-builts for one office."),
        Message(direction=MessageDirection.INBOUND, body="It is around 18,000 sqft."),
    ]

    response = agent.run_turn(
        client=_client(),
        lead=lead,
        inbound_text="Within 2 weeks. I'm the owner and email works best.",
        history=history,
        booking_service=FakeBookingService(),
        db=None,
    )

    assert response.next_state == ConversationStateEnum.BOOKING_SENT
    assert response.action == "none"
    assert response.runtime_payload["booking_offer"]["slots"]
    assert "10:00 AM" in response.reply_text


def test_booked_confirmation_marks_booked_and_stops_qualifying():
    agent = LLMAgent(provider=BookedProvider())

    response = agent.next_reply(
        client=_client(),
        lead=_lead(state=ConversationStateEnum.BOOKING_SENT),
        inbound_text="I booked already.",
        history=[],
    )

    assert response.next_state == ConversationStateEnum.BOOKED
    assert response.action == "mark_booked"
    assert response.next_question_key is None


def test_agent_does_not_repeat_same_question_twice():
    agent = LLMAgent(provider=RepeatingProvider())
    history = [
        Message(
            direction=MessageDirection.OUTBOUND,
            body="Are you the decision-maker, and should anyone else join the call?",
            raw_payload={"agent": {"next_question_key": "decision_makers", "action": "ask_next_question"}},
        )
    ]

    response = agent.next_reply(
        client=_client(),
        lead=_lead(),
        inbound_text="Still deciding.",
        history=history,
    )

    assert response.next_question_key != "decision_makers"
    assert response.reply_text.count("?") == 1


def test_fallback_still_returns_safe_next_step():
    agent = LLMAgent(provider=FailingProvider())

    response = agent.next_reply(
        client=_client(),
        lead=_lead(),
        inbound_text="Hello",
        history=[],
    )

    assert response.provider == "fallback"
    assert response.reply_text
