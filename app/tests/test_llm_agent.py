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


class CallInterestWithoutToolProvider:
    name = "call-interest-without-tool"

    def __init__(self) -> None:
        self.calls = 0

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        self.calls += 1
        payload = json.loads(user_prompt)
        if self.calls == 1:
            assert payload["scheduling_intent_detected"] is True
            return {
                "reply_text": "Absolutely - I can help with that. The next step is a strategy call with the owner at a convenient time.",
                "next_state": "QUALIFYING",
                "conversation_act": "answer_question",
                "lead_intent": "wants_call_now",
                "confidence": 0.93,
                "reasoning_summary": "The lead said they are interested in a call.",
                "uses_knowledge_context": False,
                "collected_fields": payload["qualification_memory"],
                "next_question_key": None,
                "action": "none",
                "tool_call": {"name": "none", "args": {}},
            }
        tool_result = payload["tool_result"]
        assert tool_result["kind"] == "slots"
        return {
            "reply_text": tool_result["fallback_reply"],
            "next_state": "BOOKING_SENT",
            "conversation_act": "offer_slots",
            "lead_intent": "wants_call_now",
            "confidence": 0.99,
            "reasoning_summary": "The backend found live times.",
            "uses_knowledge_context": False,
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


class RepeatingFactProvider:
    name = "repeating-fact"

    def generate_json(self, system_prompt: str, user_prompt: str):
        assert "acknowledged_form_fact_keys" in user_prompt
        _ = system_prompt
        payload = json.loads(user_prompt)
        assert "desired_outcome" in payload["answered_missing_field_keys"]
        assert payload["recommended_missing_field"]["key"] != "desired_outcome"
        assert "lead_city" in payload["acknowledged_form_fact_keys"]
        assert "decision_maker_role" in payload["acknowledged_form_fact_keys"]
        return {
            "reply_text": (
                "10 quality leads next month is a clear target. "
                "Since you're the decision-maker in Montreal, the only thing I'd confirm is timeline. "
                "When would you ideally like to get started or have this resolved?"
            ),
            "next_state": "QUALIFYING",
            "collected_fields": payload["qualification_memory"],
            "next_question_key": None,
            "action": "none",
            "tool_call": {"name": "none", "args": {}},
        }


class HighIntentConciergeProvider:
    name = "high-intent-concierge"

    def generate_json(self, system_prompt: str, user_prompt: str):
        assert "ai assistant" in system_prompt.lower()
        payload = json.loads(user_prompt)
        assert payload["intent_level"] == "HIGH_INTENT"
        assert payload["initial_outreach"] is True
        assert payload["lead_form_answers"]["deliverable_type"] == "CAD as-builts and Revit/BIM"
        assert payload["qualification_memory"]["timeline"] == "Within 2 weeks"
        assert payload["qualification_memory"]["decision_makers"] == "Owner"
        assert payload["recommended_missing_field"]["key"] == "desired_outcome"
        return {
            "reply_text": (
                "Hi Jordan, thanks for reaching out. I saw the retail space is about 12,000 sqft and you need CAD as-builts plus Revit/BIM within 2 weeks. "
                "What would a successful outcome look like for you?"
            ),
            "next_state": "QUALIFYING",
            "collected_fields": payload["qualification_memory"],
            "next_question_key": None,
            "action": "none",
            "tool_call": {"name": "none", "args": {}},
        }


class HighIntentOpeningWithCtaProvider:
    name = "high-intent-opening-with-cta"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        payload = json.loads(user_prompt)
        assert payload["initial_outreach"] is True
        return {
            "reply_text": (
                "Hi Strategy Call Contact, I'm Hermes, the assistant for StackLeads. "
                "I see you're the owner, looking to understand options and next steps within 2 weeks, and you want to avoid wasting time. "
                "Is there a deadline or key date driving this, or would you rather I help line up a strategy call with the owner at a convenient time?"
            ),
            "next_state": "QUALIFYING",
            "collected_fields": payload["qualification_memory"],
            "next_question_key": None,
            "action": "offer_booking",
            "tool_call": {"name": "none", "args": {}},
        }


class FactualAnswerWithCtaProvider:
    name = "factual-answer-with-cta"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        payload = json.loads(user_prompt)
        assert payload["lead_question_detected"] is True
        assert payload["pricing_question"] is False
        return {
            "reply_text": (
                "We usually work with a handful of active projects at a time so we can stay close to lead response, CRM cleanup, and follow-up quality. "
                "If you want, I can help line up a strategy call with the owner at a convenient time."
            ),
            "next_state": "QUALIFYING",
            "collected_fields": payload["qualification_memory"],
            "next_question_key": None,
            "action": "offer_booking",
            "tool_call": {"name": "none", "args": {}},
        }


class MediumIntentProvider:
    name = "medium-intent"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        payload = json.loads(user_prompt)
        assert payload["intent_level"] == "MEDIUM_INTENT"
        assert payload["cta_state"]["meeting_suggested_count"] == 0
        return {
            "reply_text": "That makes sense. What are you mainly trying to figure out before deciding?",
            "next_state": "QUALIFYING",
            "collected_fields": payload["qualification_memory"],
            "next_question_key": None,
            "action": "none",
            "tool_call": {"name": "none", "args": {}},
        }


class LowIntentPushyProvider:
    name = "low-intent-pushy"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        payload = json.loads(user_prompt)
        assert payload["intent_level"] == "LOW_INTENT"
        return {
            "reply_text": "No problem. I can help with the basics. Would you like to book a short scoping call?",
            "next_state": "QUALIFYING",
            "collected_fields": payload["qualification_memory"],
            "next_question_key": None,
            "action": "offer_booking",
            "tool_call": {"name": "none", "args": {}},
        }


class PricingQuestionProvider:
    name = "pricing-question"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        payload = json.loads(user_prompt)
        assert payload["pricing_question"] is True
        assert payload["intent_level"] == "HIGH_INTENT"
        return {
            "reply_text": "Pricing depends on scope, timing, requirements, and the level of support needed. With the details you shared, the team would need to confirm the exact fit before giving a reliable estimate.",
            "next_state": "QUALIFYING",
            "collected_fields": payload["qualification_memory"],
            "next_question_key": None,
            "action": "none",
            "tool_call": {"name": "none", "args": {}},
        }


class PushyPricingBookingProvider:
    name = "pushy-pricing-booking"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        payload = json.loads(user_prompt)
        assert payload["pricing_question"] is True
        assert payload["lead_question_detected"] is True
        assert payload["explicit_booking_intent"] is False
        assert payload["scheduling_intent_detected"] is False
        assert "scheduling_intent" not in payload["intent_reasons"]
        return {
            "reply_text": (
                "I can book a consultation call directly. "
                "I have openings for Thu Jun 18 at 10:00 AM, Fri Jun 19 at 9:30 AM, or Mon Jun 22 at 9:30 AM."
            ),
            "next_state": "BOOKING_SENT",
            "collected_fields": payload["qualification_memory"],
            "next_question_key": None,
            "action": "offer_booking",
            "tool_call": {"name": "find_slots", "args": {"limit": 3}},
        }


class RepetitivePricingCtaProvider:
    name = "repetitive-pricing-cta"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        payload = json.loads(user_prompt)
        assert payload["cta_state"]["suppress_meeting_cta"] is True
        return {
            "reply_text": (
                "I do not have confirmed package or pricing details here. "
                "It usually depends on scope and timeline. "
                "Would you like me to set up a consultation call?"
            ),
            "next_state": "QUALIFYING",
            "collected_fields": payload["qualification_memory"],
            "next_question_key": None,
            "action": "offer_booking",
            "tool_call": {"name": "none", "args": {}},
        }


class CallRefusalPushyProvider:
    name = "call-refusal-pushy"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        payload = json.loads(user_prompt)
        assert payload["call_refusal"] is True
        assert payload["cta_state"]["meeting_rejected"] is True
        return {
            "reply_text": "Sure, the process usually starts with confirming fit and scope. We can book a short call if you want.",
            "next_state": "QUALIFYING",
            "collected_fields": payload["qualification_memory"],
            "next_question_key": None,
            "action": "offer_booking",
            "tool_call": {"name": "none", "args": {}},
        }


class WrongBookSlotProvider:
    name = "wrong-book-slot"

    def __init__(self) -> None:
        self.calls = 0

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        self.calls += 1
        payload = json.loads(user_prompt)
        if self.calls == 1:
            assert payload["latest_inbound_booking_preferences"]["exact_time"] == "11 am"
            return {
                "reply_text": "I can lock that in.",
                "next_state": "BOOKING_SENT",
                "collected_fields": payload["qualification_memory"],
                "next_question_key": None,
                "action": "none",
                "tool_call": {"name": "book_slot", "args": {}},
            }
        tool_result = payload["tool_result"]
        assert tool_result["kind"] == "slots"
        assert tool_result["availability_query"]["exact_time"] == "11 am"
        return {
            "reply_text": tool_result["fallback_reply"],
            "next_state": "BOOKING_SENT",
            "collected_fields": payload["conversation_context"]["qualification_memory"],
            "next_question_key": None,
            "action": "none",
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
        request_text=None,
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
        _ = request_text
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


class ExactTimeFallbackBookingService(FakeBookingService):
    def __init__(self) -> None:
        super().__init__()
        self.find_args: list[dict] = []

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
        request_text=None,
        limit: int = 3,
        db=None,
    ):
        self.find_args.append(
            {
                "preferred_day": preferred_day,
                "avoid_day": avoid_day,
                "preferred_period": preferred_period,
                "exact_time": exact_time,
                "range_start": range_start,
                "range_end": range_end,
                "request_text": request_text,
                "limit": limit,
            }
        )
        if preferred_day == "monday" and exact_time == "11 am":
            slots = [
                type(
                    "Slot",
                    (),
                    {
                        "__dict__": {
                            "index": 1,
                            "display_time": "Mon May 25 at 11:00 AM",
                            "start_time": "2026-05-25T15:00:00Z",
                            "end_time": "2026-05-25T15:30:00Z",
                            "display_hint": "Monday 11:00 AM",
                            "search_blob": "monday 11am | monday 11 am",
                        }
                    },
                )(),
            ]
            return type(
                "Offer",
                (),
                {
                    "reply_text": "I found Monday 11:00 AM for the call. Reply with 1 to book the call.",
                    "slots": slots,
                    "raw_payload": {"booking_offer": {"provider": "internal", "slots": [slot.__dict__ for slot in slots]}},
                },
            )()
        return super().find_slots(
            client=client,
            lead=lead,
            preferred_day=preferred_day,
            avoid_day=avoid_day,
            preferred_period=preferred_period,
            exact_time=exact_time,
            range_start=range_start,
            range_end=range_end,
            limit=limit,
            db=db,
        )

    def book_requested_slot(self, *, client: Client, lead, latest_offer, slot_index=None, slot_start_time=None, slot_text=None, db=None):
        return {
            "reply_text": "I couldn’t match that to one of the current call options. I can check that time and send fresh call times.",
            "slots": (latest_offer or {}).get("slots", []) if isinstance(latest_offer, dict) else [],
            "runtime_payload": {
                "booking_offer": latest_offer or {},
                "pending_step": "slot_selection_pending",
            },
        }


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


def _lead(
    state: ConversationStateEnum = ConversationStateEnum.QUALIFYING,
    raw_payload: dict | None = None,
    form_answers: dict | None = None,
    source: LeadSource = LeadSource.MANUAL,
) -> Lead:
    return Lead(
        client_id=1,
        source=source,
        full_name="Jordan Lee",
        phone="+15551234567",
        email="jordan@example.com",
        city="Toronto",
        form_answers=form_answers or {},
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


def test_call_interest_without_tool_is_validated_into_live_slot_offer():
    provider = CallInterestWithoutToolProvider()
    agent = LLMAgent(provider=provider)

    response = agent.run_turn(
        client=_client(),
        lead=_lead(),
        inbound_text="alright, im interested in a call.",
        history=[],
        booking_service=FakeBookingService(),
        db=None,
    )

    assert provider.calls == 2
    assert response.next_state == ConversationStateEnum.BOOKING_SENT
    assert response.action == "none"
    assert response.tool_call.name == "none"
    assert response.runtime_payload["conversation_act"] == "offer_slots"
    assert response.runtime_payload["booking_offer"]["slots"]
    assert "10:00 AM" in response.reply_text
    assert "call" in response.reply_text.lower() or "times" in response.reply_text.lower()


def test_exact_time_request_after_offer_checks_calendar_instead_of_rejecting_current_options():
    agent = LLMAgent(provider=WrongBookSlotProvider())
    booking_service = ExactTimeFallbackBookingService()
    history = [
        Message(
            direction=MessageDirection.OUTBOUND,
            body=(
                "I found a few Monday call options: 1) Mon May 25 at 9:00 AM "
                "2) Mon May 25 at 9:30 AM 3) Mon May 25 at 10:00 AM"
            ),
            raw_payload={
                "booking_offer": {
                    "provider": "internal",
                    "slots": [
                        {
                            "index": 1,
                            "display_time": "Mon May 25 at 9:00 AM",
                            "start_time": "2026-05-25T13:00:00Z",
                            "end_time": "2026-05-25T13:30:00Z",
                            "display_hint": "Monday 9:00 AM",
                            "search_blob": "monday 9am | monday 9 am",
                        },
                        {
                            "index": 2,
                            "display_time": "Mon May 25 at 9:30 AM",
                            "start_time": "2026-05-25T13:30:00Z",
                            "end_time": "2026-05-25T14:00:00Z",
                            "display_hint": "Monday 9:30 AM",
                            "search_blob": "monday 9 30am",
                        },
                    ],
                }
            },
        )
    ]

    response = agent.run_turn(
        client=_client(),
        lead=_lead(state=ConversationStateEnum.BOOKING_SENT),
        inbound_text="Can you do Monday 11 AM?",
        history=history,
        booking_service=booking_service,
        db=None,
    )

    assert response.next_state == ConversationStateEnum.BOOKING_SENT
    assert booking_service.find_args[-1]["preferred_day"] == "monday"
    assert booking_service.find_args[-1]["exact_time"] == "11 am"
    assert "11:00 AM" in response.reply_text
    assert "call" in response.reply_text.lower()
    assert "couldn" not in response.reply_text.lower()


def test_booked_lead_reschedule_request_uses_calendar_tools():
    agent = LLMAgent(provider=WrongBookSlotProvider())
    booking_service = ExactTimeFallbackBookingService()
    history = [
        Message(
            direction=MessageDirection.OUTBOUND,
            body="Booked. Your call is set for Mon May 25 at 9:00 AM.",
            raw_payload={"calendar_booking": {"provider": "internal"}},
        )
    ]

    response = agent.run_turn(
        client=_client(),
        lead=_lead(state=ConversationStateEnum.BOOKED),
        inbound_text="Can we reschedule to Monday 11 AM?",
        history=history,
        booking_service=booking_service,
        db=None,
    )

    assert response.next_state == ConversationStateEnum.BOOKING_SENT
    assert booking_service.find_args[-1]["preferred_day"] == "monday"
    assert booking_service.find_args[-1]["exact_time"] == "11 am"
    assert "11:00 AM" in response.reply_text
    assert "if none of those work" in response.reply_text.lower()


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
    assert response.runtime_payload["lead_summary"]["meeting_status"] == "booked"
    assert response.runtime_payload["lead_summary"]["qualification_level"] == "qualified_booked"


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


def test_agent_tracks_answered_generic_missing_field_and_strips_repeated_fact_preamble():
    agent = LLMAgent(provider=RepeatingFactProvider())
    history = [
        Message(
            direction=MessageDirection.OUTBOUND,
            body=(
                "Perfect - thanks Johnny. Since you're the decision-maker and you're in Montreal, "
                "the next thing I'd want to confirm is the outcome you're aiming for. "
                "What would a successful result look like for you?"
            ),
        ),
        Message(direction=MessageDirection.INBOUND, body="10 quality leads in the next month"),
    ]

    lead = _lead(
        form_answers={"decision_maker_role": "Owner"},
        raw_payload={"qualification_memory": {"decision_makers": "Owner"}},
    )
    lead.city = "Montreal"

    response = agent.next_reply(
        client=_client(),
        lead=lead,
        inbound_text="10 quality leads in the next month",
        history=history,
    )

    text = response.reply_text.lower()
    assert "decision-maker" not in text
    assert "decision maker" not in text
    assert "montreal" not in text
    assert "budget" not in text
    assert "timeline" in text or "when would you" in text
    missing_keys = [field["key"] for field in response.runtime_payload["important_missing_fields"]]
    assert "desired_outcome" not in missing_keys


def test_identity_question_is_answered_as_hermes_without_calling_provider():
    agent = LLMAgent(provider=FailingProvider())
    client = _client()
    client.business_name = "StackLeads"
    client.faq_context = "StackLeads was founded by Peter Sarateanu to help businesses improve lead follow-up."

    response = agent.run_turn(
        client=client,
        lead=_lead(),
        inbound_text="You're the founder of the company?",
        history=[],
        booking_service=None,
        db=None,
    )

    text = response.reply_text.lower()
    assert response.provider_error is None
    assert "no" in text
    assert "hermes" in text
    assert "assistant" in text
    assert "peter sarateanu" in text
    assert "i'm the founder" not in text
    assert "i am the founder" not in text


def test_factual_answer_strips_generic_meeting_cta():
    agent = LLMAgent(provider=FactualAnswerWithCtaProvider())
    client = _client()
    client.business_name = "StackLeads"

    response = agent.next_reply(
        client=client,
        lead=_lead(),
        inbound_text="How many projects do you guys have?",
        history=[],
    )

    text = response.reply_text.lower()
    assert response.action == "none"
    assert response.tool_call.name == "none"
    assert response.runtime_payload["meeting_cta_stripped"] is True
    assert "handful of active projects" in text
    assert "strategy call" not in text
    assert "line up" not in text


def test_high_intent_form_answers_are_used_as_known_context():
    agent = LLMAgent(provider=HighIntentConciergeProvider())
    form_answers = {
        "project_scope": "One existing retail location",
        "locations_scope": "One building",
        "deliverable_type": "CAD as-builts and Revit/BIM",
        "building_type": "Retail space",
        "approximate_size_sqft": "12000",
        "timeline": "Within 2 weeks",
        "decision_maker_role": "Owner",
        "preferred_contact_method": "Email",
    }

    response = agent.next_reply(
        client=_client(),
        lead=_lead(form_answers=form_answers, source=LeadSource.META),
        inbound_text="New lead submitted from Meta Lead Ads. This is the first outbound SMS after the form submit.",
        history=[],
    )

    assert response.runtime_payload["intent_level"] == "HIGH_INTENT"
    assert response.runtime_payload["lead_summary"]["intent_level"] == "HIGH_INTENT"
    assert response.runtime_payload["lead_summary"]["meeting_status"] == "not_suggested"
    assert response.runtime_payload["important_missing_fields"][0]["key"] == "desired_outcome"
    assert "12,000" in response.reply_text
    assert "within 2 weeks" in response.reply_text.lower()
    assert "are you the decision-maker" not in response.reply_text.lower()


def test_initial_outreach_strips_generic_meeting_cta_but_keeps_question():
    agent = LLMAgent(provider=HighIntentOpeningWithCtaProvider())
    form_answers = {
        "decision_maker_role": "Owner",
        "main_goal": "Understand options and next steps",
        "growth_bottleneck": "Avoid wasting time",
        "timeline": "Within 2 weeks",
    }

    response = agent.next_reply(
        client=_client(),
        lead=_lead(form_answers=form_answers, source=LeadSource.META),
        inbound_text="New lead submitted from Meta Lead Ads. This is the first outbound SMS after the form submit.",
        history=[],
    )

    text = response.reply_text.lower()
    assert response.action == "none"
    assert response.tool_call.name == "none"
    assert response.runtime_payload["meeting_cta_stripped"] is True
    assert "deadline or key date" in text
    assert "strategy call" not in text
    assert "line up" not in text
    assert response.reply_text.count("?") == 1


def test_medium_intent_lead_clarifies_before_booking():
    agent = LLMAgent(provider=MediumIntentProvider())

    response = agent.next_reply(
        client=_client(),
        lead=_lead(
            form_answers={
                "service_interest": "CAD as-builts",
                "timeline": "No rush",
            }
        ),
        inbound_text="I am comparing options.",
        history=[],
    )

    assert response.runtime_payload["intent_level"] == "MEDIUM_INTENT"
    assert response.action == "none"
    assert "book" not in response.reply_text.lower()
    assert response.reply_text.count("?") == 1


def test_low_intent_lead_does_not_get_pushed_to_book():
    agent = LLMAgent(provider=LowIntentPushyProvider())

    response = agent.next_reply(
        client=_client(),
        lead=_lead(),
        inbound_text="I am just looking.",
        history=[],
    )

    text = response.reply_text.lower()
    assert response.runtime_payload["intent_level"] == "LOW_INTENT"
    assert response.action == "none"
    assert "book" not in text
    assert "call" not in text


def test_pricing_question_is_suppressed_without_ai_pricing_context():
    agent = LLMAgent(provider=PricingQuestionProvider())

    response = agent.next_reply(
        client=_client(),
        lead=_lead(
            form_answers={
                "project_scope": "One office that needs existing conditions captured",
                "deliverable_type": "CAD as-builts",
                "approximate_size_sqft": "18000",
            }
        ),
        inbound_text="How much would this cost?",
        history=[],
    )

    assert response.runtime_payload["intent_level"] == "HIGH_INTENT"
    text = response.reply_text.lower()
    assert "budget" not in text
    assert "pricing details" in text
    assert "depends on" in text
    assert "consultation call" in text
    assert "set that up" in text
    assert "$" not in response.reply_text
    assert response.tool_call.name == "none"


def test_pricing_question_blocks_pushy_booking_tool_on_second_message():
    agent = LLMAgent(provider=PushyPricingBookingProvider())
    history = [
        Message(
            direction=MessageDirection.OUTBOUND,
            body=(
                "Hi Kylian, I'm Hermes, the assistant for StackLeads. "
                "Is there a deadline or key date driving this?"
            ),
            raw_payload={"agent": {"next_question_key": "urgency_driver"}},
        )
    ]

    response = agent.next_reply(
        client=_client(),
        lead=_lead(
            form_answers={
                "decision_role": "Owner",
                "main_goal": "Understand options and next steps",
                "service_interest": "AI lead response system",
                "when_to_start": "Within 2 weeks",
            }
        ),
        inbound_text="yes, the deadline is end of June. How much do you guys charge?",
        history=history,
    )

    text = response.reply_text.lower()
    assert response.next_state == ConversationStateEnum.QUALIFYING
    assert response.action == "none"
    assert response.tool_call.name == "none"
    assert response.runtime_payload["booking_blocked_reason"] == "answer_first_question"
    assert "pricing details" in text
    assert "depends on" in text
    assert "consultation call" in text
    assert "set that up" in text
    assert response.runtime_payload["soft_cta_type"] == "consultation_call"
    assert "10:00 am" not in text


def test_soft_pricing_cta_is_not_repeated_when_lead_ignores_it():
    agent = LLMAgent(provider=RepetitivePricingCtaProvider())
    history = [
        Message(
            direction=MessageDirection.OUTBOUND,
            body=(
                "I do not have confirmed package or pricing details here. "
                "It usually depends on scope, timeline, service area, package level, and any special requirements. "
                "The team can review that on a consultation call; would you like me to help set that up?"
            ),
        )
    ]

    response = agent.next_reply(
        client=_client(),
        lead=_lead(
            form_answers={
                "decision_role": "Owner",
                "service_interest": "AI lead response system",
                "when_to_start": "Within 2 weeks",
            }
        ),
        inbound_text="okay?",
        history=history,
    )

    text = response.reply_text.lower()
    assert response.action == "none"
    assert response.tool_call.name == "none"
    assert "set that up" not in text
    assert "consultation call" not in text


def test_call_refusal_suppresses_repeated_meeting_cta():
    agent = LLMAgent(provider=CallRefusalPushyProvider())
    history = [
        Message(
            direction=MessageDirection.OUTBOUND,
            body="The team could probably give a clearer answer on a short call.",
            raw_payload={"agent": {"action": "offer_booking"}},
        )
    ]

    response = agent.next_reply(
        client=_client(),
        lead=_lead(),
        inbound_text="I don't want a call, can you just tell me the process?",
        history=history,
    )

    text = response.reply_text.lower()
    assert response.runtime_payload["cta_state"]["meeting_rejected"] is True
    assert response.action == "none"
    assert "book" not in text
    assert "call" not in text


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
