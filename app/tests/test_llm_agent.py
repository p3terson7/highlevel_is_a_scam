from app.core.config import Settings
from app.db.models import Client, ConversationStateEnum, Lead, LeadSource, Message, MessageDirection
from app.services.llm_agent import LLMAgent, build_llm_agent


class FailingProvider:
    name = "failing"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        _ = user_prompt
        raise RuntimeError("provider unavailable")


class RepeatingQuestionProvider:
    name = "repeat"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        _ = user_prompt
        return {
            "reply_text": "Makes sense. What result are you trying to achieve right now?",
            "next_state": "QUALIFYING",
            "actions": [{"type": "request_more_info", "payload": {}}],
        }


class BookingBlindProvider:
    name = "booking-blind"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        _ = user_prompt
        return {
            "reply_text": "Sounds good. Tell me a bit more.",
            "next_state": "QUALIFYING",
            "actions": [{"type": "request_more_info", "payload": {}}],
        }


class BookingSlotsProvider:
    name = "booking-slots"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        _ = user_prompt
        return {
            "reply_text": (
                "I can book this directly. Here are the next available times: "
                "1) Mon Mar 9 at 9:00 AM 2) Mon Mar 9 at 9:30 AM 3) Mon Mar 9 at 10:00 AM. "
                "Reply with 1, 2, or 3."
            ),
            "next_state": "BOOKING_SENT",
            "actions": [{"type": "offer_calendar_slots", "payload": {}}],
        }


class BookingLinkNoUrlProvider:
    name = "booking-link-no-url"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        _ = user_prompt
        return {
            "reply_text": "Perfect, let's lock in a time.",
            "next_state": "BOOKING_SENT",
            "actions": [{"type": "send_booking_link", "payload": {}}],
        }


class MixedOpeningProvider:
    name = "mixed-opening"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        _ = user_prompt
        return {
            "reply_text": (
                "Since you're already running Google Ads but want better lead quality and conversion, "
                "let's set a time to review your setup. What are you hoping to solve?"
            ),
            "next_state": "BOOKING_SENT",
            "actions": [{"type": "send_booking_link", "payload": {}}],
        }


class ExpertProvider:
    name = "expert"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        _ = user_prompt
        return {
            "reply_text": (
                "Absolutely. We run ads, automate follow-up with AI, and push qualified leads into your CRM. "
                "Who is your ideal customer right now?"
            ),
            "next_state": "QUALIFYING",
            "actions": [{"type": "request_more_info", "payload": {"focus": "ideal_customer"}}],
        }


class SpamAfterBookedProvider:
    name = "spam-after-booked"

    def generate_json(self, system_prompt: str, user_prompt: str):
        _ = system_prompt
        _ = user_prompt
        return {
            "reply_text": "Thanks for confirming your booking. Who is your ideal customer?",
            "next_state": "QUALIFYING",
            "actions": [{"type": "request_more_info", "payload": {}}],
        }


def _client() -> Client:
    return Client(
        client_key="prototype",
        business_name="PROTOTYPE",
        tone="concise, expert",
        timezone="UTC",
        qualification_questions=[
            "What result are you trying to achieve right now?",
            "Who is your ideal customer?",
            "Which city or area are you targeting?",
            "What timeline are you aiming for?",
            "What monthly budget are you comfortable with?",
        ],
        booking_url="https://prototype.example/book",
        fallback_handoff_number="+15550001111",
        consent_text="Reply STOP to opt out.",
        operating_hours={"days": [0, 1, 2, 3, 4, 5, 6], "start": "00:00", "end": "23:59"},
        faq_context="We run paid ads, automate lead conversations with AI, and sync qualified leads into the CRM.",
        ai_context="Position us as experts in ad optimization, lead qualification, and CRM pipeline quality.",
        template_overrides={},
        is_active=True,
    )


def _lead(state: ConversationStateEnum = ConversationStateEnum.QUALIFYING, form_answers: dict | None = None) -> Lead:
    return Lead(
        client_id=1,
        source=LeadSource.MANUAL,
        full_name="Peter Lead",
        phone="+15551234567",
        email="",
        city="Montreal",
        form_answers=form_answers or {},
        raw_payload={},
        consented=True,
        opted_out=False,
        conversation_state=state,
    )


def test_model_answer_is_preserved_without_template_overwrite():
    agent = LLMAgent(provider=ExpertProvider())
    response = agent.next_reply(
        client=_client(),
        lead=_lead(),
        inbound_text="How can you help with lead quality?",
        history=[],
    )

    text = response.reply_text.lower()
    assert "automate follow-up with ai" in text
    assert "qualified leads into your crm" in text or "qualified leads into the crm" in text
    assert "core services listed" not in text


def test_repeated_question_is_swapped_for_unasked_qualifier():
    agent = LLMAgent(provider=RepeatingQuestionProvider())
    history = [
        Message(direction=MessageDirection.OUTBOUND, body="What result are you trying to achieve right now?"),
        Message(direction=MessageDirection.INBOUND, body="More qualified leads"),
        Message(direction=MessageDirection.OUTBOUND, body="Makes sense. What result are you trying to achieve right now?"),
    ]

    response = agent.next_reply(
        client=_client(),
        lead=_lead(),
        inbound_text="more qualified leads (like I said)",
        history=history,
    )

    text = response.reply_text.lower()
    assert "what result are you trying to achieve" not in text
    assert any(token in text for token in ("ideal customer", "city", "area", "timeline", "budget"))
    assert response.reply_text.count("?") <= 1


def test_explicit_booking_intent_can_trigger_booking_even_if_model_forgets():
    agent = LLMAgent(provider=BookingBlindProvider())
    response = agent.next_reply(
        client=_client(),
        lead=_lead(),
        inbound_text="Can we schedule a call this week?",
        history=[],
    )

    assert response.next_state == ConversationStateEnum.BOOKING_SENT
    assert any(action.type in {"send_booking_link", "offer_calendar_slots"} for action in response.actions)


def test_booking_link_action_ensures_booking_url_is_present():
    agent = LLMAgent(provider=BookingLinkNoUrlProvider())
    response = agent.next_reply(
        client=_client(),
        lead=_lead(),
        inbound_text="Can we schedule a call this week?",
        history=[],
    )

    assert response.next_state == ConversationStateEnum.BOOKING_SENT
    assert any(action.type == "send_booking_link" for action in response.actions)
    assert "https://prototype.example/book" in response.reply_text


def test_booking_request_from_model_is_downgraded_when_gate_not_met():
    agent = LLMAgent(provider=BookingSlotsProvider())
    response = agent.next_reply(
        client=_client(),
        lead=_lead(),
        inbound_text="Are you guys experienced with google ads?",
        history=[],
    )

    text = response.reply_text.lower()
    assert response.next_state == ConversationStateEnum.QUALIFYING
    assert any(action.type == "request_more_info" for action in response.actions)
    assert all(action.type not in {"send_booking_link", "offer_calendar_slots"} for action in response.actions)
    assert "reply with 1" not in text


def test_booking_link_not_resent_if_recently_shared():
    agent = LLMAgent(provider=BookingLinkNoUrlProvider())
    history = [
        Message(direction=MessageDirection.OUTBOUND, body="Here is the booking link: https://prototype.example/book"),
        Message(direction=MessageDirection.INBOUND, body="Got it"),
        Message(direction=MessageDirection.OUTBOUND, body="Book here: https://prototype.example/book"),
    ]

    response = agent.next_reply(
        client=_client(),
        lead=_lead(ConversationStateEnum.BOOKING_SENT),
        inbound_text="Can we schedule a call?",
        history=history,
    )

    assert response.next_state == ConversationStateEnum.BOOKING_SENT
    assert all(action.type != "send_booking_link" for action in response.actions)
    assert "prototype.example/book" not in response.reply_text.lower()


def test_time_proposal_without_booking_context_does_not_force_booking():
    agent = LLMAgent(provider=BookingBlindProvider())
    history = [Message(direction=MessageDirection.OUTBOUND, body="How can we improve your lead quality this month?")]

    response = agent.next_reply(
        client=_client(),
        lead=_lead(ConversationStateEnum.QUALIFYING),
        inbound_text="Does tomorrow 10 am work for you?",
        history=history,
    )

    assert response.next_state == ConversationStateEnum.QUALIFYING
    assert all(action.type not in {"send_booking_link", "offer_calendar_slots"} for action in response.actions)


def test_time_proposal_after_booking_prompt_closes_to_booked():
    agent = LLMAgent(provider=BookingBlindProvider())
    history = [
        Message(direction=MessageDirection.OUTBOUND, body="Let's schedule a quick call. When's a good time for you?"),
    ]

    response = agent.next_reply(
        client=_client(),
        lead=_lead(ConversationStateEnum.QUALIFYING),
        inbound_text="Tuesday next week at 10 am works for me",
        history=history,
    )

    assert response.next_state == ConversationStateEnum.BOOKED
    assert response.actions == []
    assert "?" not in response.reply_text


def test_initial_seed_turn_does_not_mix_booking_with_qualification():
    agent = LLMAgent(provider=MixedOpeningProvider())
    seed = (
        "New lead submitted from Meta Lead Ads. This is the first outbound SMS after the form submit. "
        "Lead context: summary=Running Ads: Yes, Google Ads | Challenge: Getting more qualified leads."
    )
    response = agent.next_reply(
        client=_client(),
        lead=_lead(ConversationStateEnum.NEW),
        inbound_text=seed,
        history=[],
    )

    text = response.reply_text.lower()
    assert response.next_state == ConversationStateEnum.QUALIFYING
    assert any(action.type == "request_more_info" for action in response.actions)
    assert all(action.type not in {"send_booking_link", "offer_calendar_slots"} for action in response.actions)
    assert "book here" not in text
    assert "set a time" not in text
    assert response.reply_text.count("?") <= 1


def test_booked_then_thanks_does_not_restart_qualification():
    agent = LLMAgent(provider=RepeatingQuestionProvider())
    first = agent.next_reply(
        client=_client(),
        lead=_lead(ConversationStateEnum.QUALIFYING),
        inbound_text="I booked with the link",
        history=[],
    )
    assert first.next_state == ConversationStateEnum.BOOKED
    assert all(action.type not in {"send_booking_link", "offer_calendar_slots"} for action in first.actions)

    second = agent.next_reply(
        client=_client(),
        lead=_lead(ConversationStateEnum.BOOKED),
        inbound_text="Thanks",
        history=[Message(direction=MessageDirection.OUTBOUND, body=first.reply_text)],
    )
    assert second.next_state == ConversationStateEnum.BOOKED
    assert all(action.type not in {"send_booking_link", "offer_calendar_slots", "request_more_info"} for action in second.actions)
    assert "?" not in second.reply_text


def test_already_booked_does_not_send_duplicate_booking_confirmation_questions():
    agent = LLMAgent(provider=SpamAfterBookedProvider())
    response = agent.next_reply(
        client=_client(),
        lead=_lead(ConversationStateEnum.BOOKED),
        inbound_text="appointment booked",
        history=[],
    )
    assert response.next_state == ConversationStateEnum.BOOKED
    assert response.actions == []
    assert "?" not in response.reply_text
    assert "ideal customer" not in response.reply_text.lower()


def test_llm_failure_uses_tiny_safe_fallback_message():
    agent = LLMAgent(provider=FailingProvider())
    response = agent.next_reply(
        client=_client(),
        lead=_lead(),
        inbound_text="hello",
        history=[],
    )

    assert response.provider == "fallback"
    assert response.reply_text.startswith("Got it - quick question: what's the best time to chat?")
    assert "https://prototype.example/book" in response.reply_text
    assert response.next_state == ConversationStateEnum.BOOKING_SENT
    assert any(action.type == "send_booking_link" for action in response.actions)


def test_build_llm_agent_without_api_key_uses_safe_fallback():
    settings = Settings(openai_api_key="")
    agent = build_llm_agent(settings=settings)

    response = agent.next_reply(
        client=_client(),
        lead=_lead(),
        inbound_text="Need help",
        history=[],
    )

    assert response.provider == "fallback"
    assert response.next_state == ConversationStateEnum.BOOKING_SENT
