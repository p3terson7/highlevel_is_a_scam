from __future__ import annotations

import json
import re
from collections.abc import Sequence
from typing import Any, Literal, Protocol

from openai import OpenAI
from pydantic import BaseModel, Field

from app.core.config import Settings
from app.core.logging import get_logger
from app.db.models import Client, ConversationStateEnum, Lead, Message
from app.services.lead_summary import build_lead_summary_text, normalize_form_answers

logger = get_logger(__name__)

_ALLOWED_NEXT_STATES = {
    ConversationStateEnum.QUALIFYING,
    ConversationStateEnum.BOOKING_SENT,
    ConversationStateEnum.BOOKED,
    ConversationStateEnum.HANDOFF,
}

_BOOKING_ACTIONS = {"send_booking_link", "offer_calendar_slots"}
_ALLOWED_ACTIONS = _BOOKING_ACTIONS | {"request_more_info", "handoff_to_human"}

_START_INTENT_PHRASES = (
    "how do we start",
    "how do i start",
    "how can we start",
    "how can i start",
    "next step",
    "what's the next step",
    "how do we get started",
    "how can we get started",
)
_HANDOFF_KEYWORDS = (
    "human",
    "person",
    "real person",
    "representative",
    "agent",
    "manager",
    "someone",
)
_BOOKED_KEYWORDS = (
    "already booked",
    "i booked",
    "booked already",
    "already scheduled",
    "i scheduled",
)
_CLOSING_GRATITUDE_MARKERS = {
    "thanks",
    "thank you",
    "thankyou",
    "ok",
    "okay",
    "great",
    "cool",
    "perfect",
    "awesome",
    "sounds good",
}
_SHORT_REPLY_MARKERS = {"yes", "yep", "yeah", "ok", "okay", "sure", "maybe", "idk", "asap", "immediately"}


class AgentAction(BaseModel):
    type: Literal["send_booking_link", "offer_calendar_slots", "request_more_info", "handoff_to_human"]
    payload: dict[str, Any] = Field(default_factory=dict)


class AgentResponse(BaseModel):
    reply_text: str
    next_state: ConversationStateEnum
    actions: list[AgentAction] = Field(default_factory=list)
    provider: Literal["openai", "fallback"] = "openai"
    provider_error: str | None = None


class LLMProvider(Protocol):
    name: str

    def generate_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        ...


class OpenAIProvider:
    name = "openai"

    def __init__(self, *, api_key: str, model: str, timeout_seconds: int = 20) -> None:
        self._client = OpenAI(api_key=api_key, timeout=timeout_seconds, max_retries=0)
        self._model = model

    def _complete(self, system_prompt: str, user_prompt: str) -> str:
        response = self._client.chat.completions.create(
            model=self._model,
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        return response.choices[0].message.content or "{}"

    @staticmethod
    def _parse(content: str) -> dict[str, Any]:
        parsed = json.loads(content)
        if not isinstance(parsed, dict):
            raise ValueError("response is not a JSON object")
        return parsed

    def generate_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        content = self._complete(system_prompt=system_prompt, user_prompt=user_prompt)
        try:
            return self._parse(content)
        except Exception as first_error:
            # One repair pass before giving up.
            repair_system = (
                "Return valid JSON only and match this exact schema: "
                '{"reply_text":string,"next_state":"QUALIFYING|BOOKING_SENT|BOOKED|HANDOFF",'
                '"actions":[{"type":"send_booking_link|offer_calendar_slots|request_more_info|handoff_to_human","payload":{}}]}'
            )
            repair_user = (
                "Fix this invalid JSON into valid JSON only.\n"
                f"Invalid JSON:\n{content}\n"
                f"Parser error: {type(first_error).__name__}: {first_error}"
            )
            repaired = self._complete(system_prompt=repair_system, user_prompt=repair_user)
            return self._parse(repaired)


class UnavailableLLMProvider:
    name = "unavailable"

    def generate_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        _ = system_prompt
        _ = user_prompt
        raise RuntimeError("LLM provider unavailable")


class LLMAgentV2:
    def __init__(self, provider: LLMProvider) -> None:
        self._provider = provider

    def next_reply(
        self,
        client: Client,
        lead: Lead,
        inbound_text: str,
        history: Sequence[Message],
    ) -> AgentResponse:
        context = self._build_context_payload(client=client, lead=lead, inbound_text=inbound_text, history=history)
        system_prompt = self._build_system_prompt(client=client)
        user_prompt = json.dumps(context, ensure_ascii=False)

        try:
            raw = self._provider.generate_json(system_prompt=system_prompt, user_prompt=user_prompt)
            response = AgentResponse.model_validate(raw)
            if _is_parroting_reply(response.reply_text, inbound_text):
                regenerated = self._regenerate_once_for_originality(
                    system_prompt=system_prompt,
                    context=context,
                    inbound_text=inbound_text,
                    previous_response=response,
                )
                if regenerated is not None:
                    response = regenerated
            sanitized = self._sanitize_response(
                response=response,
                client=client,
                inbound_text=inbound_text,
                context=context,
            )
            sanitized.provider = "openai"
            return sanitized
        except Exception as exc:
            logger.exception("agent_v2_llm_failed", extra={"error": str(exc)})
            fallback = self._safe_fallback(client=client, context=context)
            fallback.provider = "fallback"
            fallback.provider_error = str(exc)
            return fallback

    def _regenerate_once_for_originality(
        self,
        *,
        system_prompt: str,
        context: dict[str, Any],
        inbound_text: str,
        previous_response: AgentResponse,
    ) -> AgentResponse | None:
        rewrite_context = dict(context)
        rewrite_context["rewrite_instruction"] = (
            "Rewrite the draft to be more original and human. "
            "Do not mirror/parrot the lead's full message. "
            "Use one insight sentence + one next-step sentence or one diagnostic question. "
            "Max one question mark."
        )
        rewrite_context["previous_reply_text"] = previous_response.reply_text
        rewrite_context["previous_next_state"] = previous_response.next_state.value
        rewrite_context["previous_actions"] = [{"type": action.type, "payload": action.payload} for action in previous_response.actions]
        try:
            raw = self._provider.generate_json(
                system_prompt=system_prompt,
                user_prompt=json.dumps(rewrite_context, ensure_ascii=False),
            )
            candidate = AgentResponse.model_validate(raw)
        except Exception:
            return None
        if _is_parroting_reply(candidate.reply_text, inbound_text):
            return None
        return candidate

    def _build_system_prompt(self, *, client: Client) -> str:
        faq_context = (client.faq_context or "").strip() or "none provided"
        ai_context = (getattr(client, "ai_context", "") or "").strip() or "none provided"
        return (
            "You are an expert SMS sales assistant writing as the business owner/team.\n"
            "Goal: answer the lead naturally, qualify efficiently, and move to booking.\n"
            "You write the final customer-facing SMS copy yourself. Avoid canned/template phrasing.\n"
            "Knowledge rules:\n"
            "- faq_context and ai_context are authoritative for services, process, positioning, and pricing rules.\n"
            "- If context does not contain an answer, do NOT invent details. Ask one clarifying question or guide to a quick call.\n"
            "Conversation rules:\n"
            "- Use latest_inbound_message + recent_messages + lead_form_answers + lead_summary.\n"
            "- Never ask a question already answered by lead_form_answers or prior inbound messages.\n"
            "- Never repeat a question listed in asked_questions.\n"
            "- Do not mirror/parrot. Never restate the lead's full message. Keep any acknowledgement to <=8 words.\n"
            "- Insight-first style: one expert sentence tailored to context, then one next-step sentence OR one diagnostic question.\n"
            "- Use teach-then-ask: explain the key lever briefly, then ask the single best next question.\n"
            "- Ask at most ONE question in a reply. Zero questions is allowed when booking/handoff/booked is the next step.\n"
            "- Handle short replies (yes/ok/asap/maybe/idk) using last_outbound_question context.\n"
            "- If the lead proposes a specific time, respond to that proposal directly; do not send a generic numbered slots list unless they asked for options.\n"
            "- If synthetic_seed_inbound=true (first outreach), send one clear opening message with one focused qualifier and no booking CTA.\n"
            "- Keep tone human and concise (1-2 short SMS max, <=320 chars each).\n"
            "State/action rules:\n"
            "- Booking CTA only when: explicit booking intent OR explicit start intent OR minimum context exists (offer/service + market/location + budget or timeline).\n"
            "- If inbound is an informational capability question, answer it directly first; do not jump to booking.\n"
            "- If booking link/CTA was sent in last 3 outbound messages, do not resend link; ask a gentle nudge question instead.\n"
            "- Already booked confirmation -> BOOKED with no booking action.\n"
            "- If conversation is already BOOKED: stop selling/qualifying; do not send booking links; keep replies short and closing.\n"
            "- Human handoff request -> HANDOFF with handoff action.\n"
            "- Otherwise stay QUALIFYING with request_more_info.\n"
            "Return STRICT JSON only with this exact schema:\n"
            '{"reply_text":"string","next_state":"QUALIFYING|BOOKING_SENT|BOOKED|HANDOFF","actions":[{"type":"send_booking_link|offer_calendar_slots|request_more_info|handoff_to_human","payload":{}}]}\n'
            f"Business name: {client.business_name}\n"
            f"Tone target: {client.tone or 'friendly'}\n"
            f"faq_context: {faq_context}\n"
            f"ai_context: {ai_context}"
        )

    def _build_context_payload(
        self,
        *,
        client: Client,
        lead: Lead,
        inbound_text: str,
        history: Sequence[Message],
    ) -> dict[str, Any]:
        normalized_answers = normalize_form_answers(lead.form_answers or {})
        recent_messages = [
            {
                "direction": msg.direction.value,
                "body": " ".join(str(msg.body or "").split()).strip(),
            }
            for msg in history[-10:]
        ]
        asked_questions = _extract_outbound_questions(recent_messages)
        asked_questions_norm = [_normalize_question(question) for question in asked_questions]
        last_outbound_question = _last_outbound_question(recent_messages)

        facts = _extract_facts(
            normalized_answers=normalized_answers,
            recent_messages=recent_messages,
            inbound_text=inbound_text,
        )

        qualification_questions = [
            str(question).strip()
            for question in (client.qualification_questions or [])
            if str(question).strip()
        ]
        missing_qualifiers = _derive_missing_qualifiers(
            qualification_questions=qualification_questions,
            facts=facts,
            normalized_answers=normalized_answers,
            recent_messages=recent_messages,
        )

        booking_mode = str(getattr(client, "booking_mode", "link") or "link").strip().lower()
        booking_config = getattr(client, "booking_config", {})
        automated_booking_enabled = bool(
            booking_mode == "calendly"
            and isinstance(booking_config, dict)
            and booking_config.get("calendly_event_type_uri")
        )

        enough_info_for_booking = bool(facts["goal"] and (facts["timeline"] or facts["budget"]))
        enough_min_context_for_booking = bool(
            facts["offer"] and (facts["audience"] or facts["geography"]) and (facts["timeline"] or facts["budget"])
        )
        booking_link_sent_recently = _booking_link_sent_recently(
            recent_messages=recent_messages,
            booking_url=client.booking_url,
            lookback=3,
        )

        return {
            "business_name": client.business_name,
            "tone": client.tone,
            "faq_context": client.faq_context or "",
            "ai_context": getattr(client, "ai_context", "") or "",
            "lead_name": lead.full_name,
            "lead_city": lead.city,
            "lead_summary": build_lead_summary_text(normalized_answers, limit=6),
            "lead_form_answers": normalized_answers,
            "latest_inbound_message": inbound_text,
            "recent_messages": recent_messages,
            "last_outbound_question": last_outbound_question,
            "asked_questions": asked_questions,
            "asked_questions_normalized": asked_questions_norm,
            "qualification_questions": qualification_questions,
            "missing_qualifiers": missing_qualifiers,
            "booking_url": client.booking_url,
            "booking_mode": booking_mode,
            "automated_booking_enabled": automated_booking_enabled,
            "enough_info_for_booking": enough_info_for_booking,
            "enough_min_context_for_booking": enough_min_context_for_booking,
            "booking_link_sent_recently": booking_link_sent_recently,
            "facts": facts,
            "is_short_reply": _is_short_reply(inbound_text),
            "synthetic_seed_inbound": _is_system_seed_inbound(inbound_text),
            "current_state": lead.conversation_state.value if lead.conversation_state else "",
        }

    def _sanitize_response(
        self,
        *,
        response: AgentResponse,
        client: Client,
        inbound_text: str,
        context: dict[str, Any],
    ) -> AgentResponse:
        inbound_norm = _normalize_text(inbound_text)
        booking_url = str(context.get("booking_url", "")).strip()
        link_recently_sent = bool(context.get("booking_link_sent_recently"))
        enough_min_context = bool(context.get("enough_min_context_for_booking"))
        automated_booking_enabled = bool(context.get("automated_booking_enabled"))
        seed_turn = bool(context.get("synthetic_seed_inbound"))
        recent_messages = context.get("recent_messages", [])
        recent_outbound_booking_cue = any(
            str(message.get("direction", "")).upper() == "OUTBOUND"
            and _contains_booking_cue(str(message.get("body", "")))
            for message in recent_messages[-3:]
            if isinstance(message, dict)
        )
        time_proposal = _looks_like_time_proposal(inbound_norm)

        response.reply_text = _trim_sms_text(response.reply_text)
        response.next_state = response.next_state if response.next_state in _ALLOWED_NEXT_STATES else ConversationStateEnum.QUALIFYING
        response.actions = [action for action in response.actions if action.type in _ALLOWED_ACTIONS]

        current_state = str(context.get("current_state", "")).upper()
        booking_intent = _shows_booking_intent(inbound_norm) or _shows_start_intent(inbound_norm)
        if time_proposal and recent_outbound_booking_cue:
            booking_intent = True
        booking_allowed = (booking_intent or enough_min_context) and not seed_turn
        handoff_intent = _shows_handoff_intent(inbound_norm)
        booked_intent = _shows_booked_confirmation(inbound_norm)
        already_booked = current_state == ConversationStateEnum.BOOKED.value

        if handoff_intent or response.next_state == ConversationStateEnum.HANDOFF:
            response.next_state = ConversationStateEnum.HANDOFF
            response.actions = [AgentAction(type="handoff_to_human", payload={})]
            if not response.reply_text:
                response.reply_text = "Understood. I'll connect you with a team member right now."
            response.reply_text = _trim_sms_text(_ensure_single_question(response.reply_text))
            return response

        if booked_intent:
            response.next_state = ConversationStateEnum.BOOKED
            response.actions = []
            response.reply_text = _strip_booking_links(response.reply_text, booking_url=booking_url, keep_link=False)
            if (
                not response.reply_text
                or _contains_booking_cue(response.reply_text)
                or _extract_primary_question(response.reply_text)
                or _is_gratitude_closing(inbound_norm)
            ):
                response.reply_text = "Perfect - you're booked. See you then!"
            response.reply_text = _remove_questions(response.reply_text)
            response.reply_text = _trim_sms_text(_ensure_single_question(response.reply_text))
            return response

        if already_booked:
            response.next_state = ConversationStateEnum.BOOKED
            response.actions = []
            if _is_gratitude_closing(inbound_norm):
                response.reply_text = "Perfect - see you then!"
                response.reply_text = _trim_sms_text(_remove_questions(response.reply_text))
                return response

            response.reply_text = _strip_booking_links(response.reply_text, booking_url=booking_url, keep_link=False)
            if not response.reply_text or _contains_booking_cue(response.reply_text):
                response.reply_text = "You're all set for the meeting."
            response.reply_text = _remove_questions(response.reply_text)
            response.reply_text = _trim_sms_text(_ensure_single_question(response.reply_text))
            return response

        if seed_turn:
            response.next_state = ConversationStateEnum.QUALIFYING
            response.actions = [AgentAction(type="request_more_info", payload={})]
            response.reply_text = _strip_booking_links(response.reply_text, booking_url=booking_url, keep_link=False)
            response.reply_text = _drop_booking_sentences(response.reply_text)
            if not _extract_primary_question(response.reply_text):
                next_question = _next_unasked_question(context=context) or "What would you like help with first?"
                response.reply_text = _append_question(response.reply_text, next_question)
            response.reply_text = _trim_sms_text(_ensure_single_question(response.reply_text))
            return response

        model_requests_booking = response.next_state == ConversationStateEnum.BOOKING_SENT or any(
            action.type in _BOOKING_ACTIONS for action in response.actions
        )

        if model_requests_booking and not booking_allowed:
            response.next_state = ConversationStateEnum.QUALIFYING
            response.actions = [AgentAction(type="request_more_info", payload={})]
            response.reply_text = _strip_booking_links(response.reply_text, booking_url=booking_url, keep_link=False)
            response.reply_text = _drop_booking_sentences(response.reply_text)
        elif model_requests_booking:
            response.next_state = ConversationStateEnum.BOOKING_SENT
            if not response.actions:
                response.actions = [_booking_action(context=context)]
            if link_recently_sent and any(action.type == "send_booking_link" for action in response.actions):
                response.actions = [AgentAction(type="request_more_info", payload={})]
                response.reply_text = _strip_booking_links(response.reply_text, booking_url=booking_url, keep_link=False)
                if not response.reply_text:
                    response.reply_text = _booking_nudge_reply()
        elif booking_intent and booking_allowed and booking_url and not link_recently_sent:
            response.next_state = ConversationStateEnum.BOOKING_SENT
            response.actions = [_booking_action(context=context)]
        else:
            if response.next_state == ConversationStateEnum.BOOKING_SENT:
                response.next_state = ConversationStateEnum.QUALIFYING
            if response.next_state == ConversationStateEnum.QUALIFYING and not response.actions:
                response.actions = [AgentAction(type="request_more_info", payload={})]

        if time_proposal and recent_outbound_booking_cue and not automated_booking_enabled:
            response.next_state = ConversationStateEnum.BOOKED
            response.actions = []
            response.reply_text = _strip_booking_links(response.reply_text, booking_url=booking_url, keep_link=False)
            if not response.reply_text or _contains_booking_cue(response.reply_text) or _extract_primary_question(response.reply_text):
                response.reply_text = "Perfect - that time works. You're all set."
            response.reply_text = _remove_questions(response.reply_text)
            response.reply_text = _trim_sms_text(_ensure_single_question(response.reply_text))
            return response

        asked_norm = {
            _normalize_question(question)
            for question in context.get("asked_questions_normalized", [])
            if _normalize_question(question)
        }
        current_question = _extract_primary_question(response.reply_text)
        current_question_norm = _normalize_question(current_question)
        if current_question_norm and current_question_norm in asked_norm:
            next_question = _next_unasked_question(context=context)
            if next_question:
                response.reply_text = _replace_primary_question(response.reply_text, next_question)

        if _is_parroting_reply(response.reply_text, inbound_text):
            response.reply_text = _deparrot_reply(response.reply_text)

        if any(action.type == "send_booking_link" for action in response.actions):
            response.reply_text = _ensure_booking_url_present(response.reply_text, booking_url)

        if not response.reply_text:
            response.reply_text = "Got it."

        response.reply_text = _trim_sms_text(_ensure_single_question(response.reply_text))
        return response

    def _safe_fallback(self, *, client: Client, context: dict[str, Any] | None = None) -> AgentResponse:
        text = "Got it - quick question: what's the best time to chat?"
        actions: list[AgentAction] = [AgentAction(type="request_more_info", payload={})]
        state = ConversationStateEnum.QUALIFYING

        allow_booking_link = bool((client.booking_url or "").strip()) and not bool((context or {}).get("synthetic_seed_inbound"))
        if allow_booking_link:
            text = f"{text} Book here: {client.booking_url.strip()}"
            actions = [AgentAction(type="send_booking_link", payload={})]
            state = ConversationStateEnum.BOOKING_SENT

        return AgentResponse(
            reply_text=_trim_sms_text(text),
            next_state=state,
            actions=actions,
        )


def _normalize_text(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "").strip().lower())
    cleaned = re.sub(r"[^a-z0-9$@+\-:/?.\s]", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _normalize_question(text: str) -> str:
    candidate = _normalize_text(text)
    if not candidate:
        return ""
    candidate = candidate.rstrip("?").strip()
    return candidate


def _is_short_reply(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    if normalized in _SHORT_REPLY_MARKERS:
        return True
    return len(normalized.split()) <= 3


def _extract_primary_question(text: str) -> str:
    raw = " ".join(str(text or "").split()).strip()
    raw = re.sub(r"https?://\S+", " ", raw)
    if not raw or "?" not in raw:
        return ""
    match = re.search(r"([^?]{4,}\?)", raw)
    if not match:
        return ""
    question = match.group(1).strip()
    if "." in question:
        tail = question.rsplit(".", 1)[-1].strip()
        if tail:
            question = tail if tail.endswith("?") else f"{tail}?"
    return question


def _is_system_seed_inbound(text: str) -> bool:
    normalized = _normalize_text(text)
    return normalized.startswith("new lead submitted from meta lead ads")


def _extract_outbound_questions(messages: Sequence[dict[str, Any]]) -> list[str]:
    output: list[str] = []
    for message in messages:
        if str(message.get("direction", "")).upper() != "OUTBOUND":
            continue
        body = str(message.get("body", "")).strip()
        body = re.sub(r"https?://\S+", " ", body)
        if not body or "?" not in body:
            continue
        for chunk in re.findall(r"([^?]{4,}\?)", body):
            question = chunk.strip()
            if "." in question:
                tail = question.rsplit(".", 1)[-1].strip()
                if tail:
                    question = tail if tail.endswith("?") else f"{tail}?"
            if question:
                output.append(question)
    return output


def _last_outbound_question(messages: Sequence[dict[str, Any]]) -> str:
    questions = _extract_outbound_questions(messages)
    return questions[-1] if questions else ""


def _extract_facts(
    *,
    normalized_answers: dict[str, Any],
    recent_messages: Sequence[dict[str, Any]],
    inbound_text: str,
) -> dict[str, str]:
    facts = {
        "goal": "",
        "timeline": "",
        "budget": "",
        "audience": "",
        "geography": "",
        "offer": "",
        "close_rate": "",
        "funnel": "",
        "ad_platform": "",
        "business_type": "",
    }

    for key, value in normalized_answers.items():
        key_norm = _normalize_text(key)
        value_text = " ".join(str(value).split()).strip()
        if not value_text:
            continue
        if any(token in key_norm for token in ("goal", "challenge", "problem", "result", "need")):
            facts["goal"] = facts["goal"] or value_text
        if any(token in key_norm for token in ("timeline", "when", "start", "availability")):
            facts["timeline"] = facts["timeline"] or value_text
        if any(token in key_norm for token in ("budget", "spend", "cost", "price")):
            facts["budget"] = facts["budget"] or value_text
        if any(token in key_norm for token in ("ideal", "audience", "customer", "avatar")):
            facts["audience"] = facts["audience"] or value_text
        if any(token in key_norm for token in ("city", "area", "geo", "location", "market")):
            facts["geography"] = facts["geography"] or value_text
        if any(token in key_norm for token in ("offer", "service", "package", "product", "industry", "business_type")):
            facts["offer"] = facts["offer"] or value_text
        if any(token in key_norm for token in ("industry", "business type", "niche", "vertical")):
            facts["business_type"] = facts["business_type"] or value_text
        if any(token in key_norm for token in ("running ads", "ad platform", "platform", "channel", "traffic source")):
            facts["ad_platform"] = facts["ad_platform"] or value_text
        if any(token in key_norm for token in ("close", "conversion", "win_rate")):
            facts["close_rate"] = facts["close_rate"] or value_text
        if any(token in key_norm for token in ("landing page", "lead form", "funnel", "tracking", "crm")):
            facts["funnel"] = facts["funnel"] or value_text

    inbound_bodies = [
        _normalize_text(message.get("body", ""))
        for message in recent_messages
        if str(message.get("direction", "")).upper() == "INBOUND"
    ]
    inbound_bodies.append(_normalize_text(inbound_text))

    budget_matcher = re.compile(r"\$\s?\d[\d,]*(?:\s?(?:/|per)?\s?(?:mo|month|monthly))?")
    geography_matcher = re.compile(
        r"\b("
        r"us|u\.s\.|usa|u\.s\.a\.|united states|canada|nationwide|statewide|worldwide|"
        r"north america|europe|uk|united kingdom|australia"
        r")\b"
    )
    offer_matcher = re.compile(
        r"\b(custom software|software development|web development|app development|saas|roofing|plumbing|hvac|marketing)\b"
    )

    for body in inbound_bodies:
        if not body:
            continue
        if not facts["budget"]:
            budget_hit = budget_matcher.search(body)
            if budget_hit:
                facts["budget"] = budget_hit.group(0)
            elif any(token in body for token in ("budget", "spending", "cost", "price")):
                facts["budget"] = body
        if not facts["timeline"] and (
            any(token in body for token in ("asap", "immediately", "soon", "this week", "next week"))
            or bool(re.search(r"\b\d{1,2}\s?(am|pm)\b", body))
            or any(day in body for day in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"))
        ):
            facts["timeline"] = body
        if not facts["goal"] and any(
            token in body
            for token in (
                "lead",
                "qualified",
                "unqualified",
                "problem",
                "issue",
                "trying to",
                "looking to",
                "want",
                "need",
            )
        ):
            facts["goal"] = body
        if not facts["audience"] and any(token in body for token in ("ideal customer", "target audience", "avatar")):
            facts["audience"] = body
        if not facts["geography"] and any(token in body for token in ("city", "area", "location", "market")):
            facts["geography"] = body
        if not facts["geography"] and geography_matcher.search(body):
            facts["geography"] = body
        if not facts["offer"] and any(token in body for token in ("offer", "service", "package", "program")):
            facts["offer"] = body
        if not facts["offer"]:
            offer_hit = offer_matcher.search(body)
            if offer_hit:
                facts["offer"] = offer_hit.group(1)
        if not facts["offer"]:
            business_match = re.search(r"\bmy\s+([a-z0-9\- ]{2,30})\s+business\b", body)
            if business_match:
                facts["offer"] = business_match.group(1).strip()
        if not facts["business_type"] and any(token in body for token in ("software", "technology", "saas", "roofing", "agency")):
            facts["business_type"] = body
        if not facts["ad_platform"] and any(
            token in body
            for token in ("google ads", "ppc", "search ads", "facebook ads", "meta ads", "instagram ads", "linkedin ads")
        ):
            facts["ad_platform"] = body
        if not facts["close_rate"] and any(token in body for token in ("close rate", "conversion", "closing")):
            facts["close_rate"] = body
        if not facts["funnel"] and any(token in body for token in ("landing page", "lead form", "funnel", "tracking", "crm")):
            facts["funnel"] = body

    return facts


def _domain_for_question(question: str) -> str:
    normalized = _normalize_text(question)
    if any(token in normalized for token in ("budget", "spend", "cost", "price")):
        return "budget"
    if any(token in normalized for token in ("timeline", "when", "start", "availability")):
        return "timeline"
    if any(token in normalized for token in ("ideal", "audience", "customer", "avatar")):
        return "audience"
    if any(token in normalized for token in ("city", "area", "location", "market", "geo")):
        return "geography"
    if any(token in normalized for token in ("offer", "service", "package", "product")):
        return "offer"
    if any(token in normalized for token in ("close", "conversion", "closing")):
        return "close_rate"
    if any(token in normalized for token in ("landing page", "lead form", "funnel", "tracking", "crm")):
        return "funnel"
    return "goal"


def _question_answered(
    *,
    question: str,
    facts: dict[str, str],
    normalized_answers: dict[str, Any],
    recent_messages: Sequence[dict[str, Any]],
) -> bool:
    domain = _domain_for_question(question)
    if facts.get(domain):
        return True

    if domain == "goal":
        inbound_bodies = [
            _normalize_text(message.get("body", ""))
            for message in recent_messages
            if str(message.get("direction", "")).upper() == "INBOUND"
        ]
        return any(len(body.split()) >= 3 for body in inbound_bodies)

    if normalized_answers:
        return False

    return False


def _derive_missing_qualifiers(
    *,
    qualification_questions: Sequence[str],
    facts: dict[str, str],
    normalized_answers: dict[str, Any],
    recent_messages: Sequence[dict[str, Any]],
) -> list[str]:
    missing: list[str] = []
    for question in qualification_questions:
        clean = str(question).strip()
        if not clean:
            continue
        if _question_answered(
            question=clean,
            facts=facts,
            normalized_answers=normalized_answers,
            recent_messages=recent_messages,
        ):
            continue
        missing.append(clean)
    return missing


def _shows_booking_intent(inbound: str) -> bool:
    if re.search(r"\bbook(?:ing)?\b", inbound):
        return True
    if any(phrase in inbound for phrase in ("schedule", "available times", "appointment", "meeting", "consult", "calendar")):
        return True
    call_phrases = (
        "schedule a call",
        "book a call",
        "set up a call",
        "hop on a call",
        "quick call",
        "available for a call",
        "call this week",
        "call tomorrow",
        "talk this week",
    )
    return any(phrase in inbound for phrase in call_phrases)


def _shows_start_intent(inbound: str) -> bool:
    return any(phrase in inbound for phrase in _START_INTENT_PHRASES)


def _looks_like_time_proposal(inbound: str) -> bool:
    if not inbound:
        return False
    if bool(re.search(r"\b\d{1,2}\s?(am|pm)\b", inbound)):
        return True
    if any(day in inbound for day in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")):
        return True
    if any(token in inbound for token in ("tomorrow", "tonight", "this week", "next week")):
        return True
    return False


def _is_gratitude_closing(inbound: str) -> bool:
    if not inbound:
        return False
    if inbound in _CLOSING_GRATITUDE_MARKERS:
        return True
    if inbound.startswith("thanks") or inbound.startswith("thank you"):
        return True
    return len(inbound.split()) <= 3 and inbound in _CLOSING_GRATITUDE_MARKERS


def _shows_handoff_intent(inbound: str) -> bool:
    return any(keyword in inbound for keyword in _HANDOFF_KEYWORDS)


def _shows_booked_confirmation(inbound: str) -> bool:
    return any(keyword in inbound for keyword in _BOOKED_KEYWORDS)


def _booking_action(*, context: dict[str, Any]) -> AgentAction:
    if bool(context.get("automated_booking_enabled")):
        return AgentAction(type="offer_calendar_slots", payload={})
    return AgentAction(type="send_booking_link", payload={})


def _contains_booking_cue(text: str) -> bool:
    normalized = _normalize_text(text)
    if re.search(r"\bbook(?:ing)?\b", normalized):
        return True
    if any(
        phrase in normalized
        for phrase in ("schedule", "available times", "appointment", "meeting", "consult", "calendar", "set a time", "good time")
    ):
        return True
    return any(
        phrase in normalized
        for phrase in (
            "schedule a call",
            "book a call",
            "set up a call",
            "set a time",
            "quick call",
            "available for a call",
            "call this week",
        )
    )


def _booking_link_sent_recently(*, recent_messages: Sequence[dict[str, Any]], booking_url: str, lookback: int = 3) -> bool:
    outbound = [
        str(message.get("body", "")).strip()
        for message in recent_messages
        if str(message.get("direction", "")).upper() == "OUTBOUND"
    ][-lookback:]
    if not outbound:
        return False
    booking_url_norm = str(booking_url or "").strip().lower()
    for body in outbound:
        body_norm = body.lower()
        if booking_url_norm and booking_url_norm in body_norm:
            return True
        if "book here" in body_norm and re.search(r"https?://\S+", body_norm):
            return True
    return False


def _strip_booking_links(text: str, *, booking_url: str, keep_link: bool = False) -> str:
    if keep_link:
        return " ".join(str(text or "").split()).strip()
    cleaned = str(text or "")
    if booking_url:
        cleaned = cleaned.replace(booking_url, " ")
        cleaned = cleaned.replace(booking_url.rstrip("/"), " ")
    cleaned = re.sub(r"https?://\S+", " ", cleaned)
    cleaned = re.sub(r"\b(book here|booking link|here's the booking link|pick a time here)\b[:]?", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(here(?:'s| is)\s+(?:my\s+)?calendar(?:\s+to\s+pick\s+a\s+time[^.?!]*)?)[:.]?", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(let'?s\s+book\s+(?:a\s+)?(?:quick\s+)?call)\b[:.]?", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+([,.:;!?])", r"\1", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.-")
    return cleaned


def _ensure_booking_url_present(text: str, booking_url: str) -> str:
    clean = " ".join(str(text or "").split()).strip()
    if not booking_url:
        return clean
    if booking_url in clean:
        return clean
    if clean.endswith(":"):
        return f"{clean} {booking_url}"
    if clean:
        return f"{clean} Book here: {booking_url}"
    return f"Book here: {booking_url}"


def _remove_questions(text: str) -> str:
    cleaned = str(text or "").replace("?", ".")
    cleaned = re.sub(r"\.\s*\.", ".", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip(" ")


def _booking_nudge_reply() -> str:
    return "I already sent the booking link. Want me to send a couple times that could work?"


def _token_overlap_ratio(reply: str, inbound: str) -> float:
    reply_tokens = [token for token in _normalize_text(reply).split() if len(token) > 2]
    inbound_tokens = {token for token in _normalize_text(inbound).split() if len(token) > 2}
    if not reply_tokens or not inbound_tokens:
        return 0.0
    overlap = sum(1 for token in reply_tokens if token in inbound_tokens)
    return overlap / len(reply_tokens)


def _is_parroting_reply(reply: str, inbound: str, threshold: float = 0.70) -> bool:
    if not reply.strip() or not inbound.strip():
        return False
    return _token_overlap_ratio(reply, inbound) >= threshold


def _deparrot_reply(reply_text: str) -> str:
    question = _extract_primary_question(reply_text)
    if question:
        return f"Got it. {question}"
    return "Got it."


def _drop_booking_sentences(text: str) -> str:
    chunks = [chunk.strip() for chunk in re.split(r"(?:\n+|(?<=[.!?])\s+)", str(text or "")) if chunk.strip()]
    kept: list[str] = []
    for chunk in chunks:
        normalized = _normalize_text(chunk)
        if _contains_booking_cue(normalized):
            continue
        if "reply with" in normalized:
            continue
        if re.search(r"https?://\S+", chunk):
            continue
        kept.append(chunk)
    return " ".join(kept).strip()


def _next_unasked_question(*, context: dict[str, Any]) -> str:
    asked_norm = {
        _normalize_question(question)
        for question in context.get("asked_questions_normalized", [])
        if _normalize_question(question)
    }

    for question in context.get("missing_qualifiers", []) or []:
        normalized = _normalize_question(str(question))
        if normalized and normalized not in asked_norm:
            return _ensure_question_mark(str(question).strip())

    for question in context.get("qualification_questions", []) or []:
        normalized = _normalize_question(str(question))
        if normalized and normalized not in asked_norm:
            return _ensure_question_mark(str(question).strip())

    return ""


def _replace_primary_question(text: str, replacement_question: str) -> str:
    clean = " ".join(str(text or "").split()).strip()
    replacement = _ensure_question_mark(replacement_question)
    if not clean:
        return replacement
    if "?" not in clean:
        return _append_question(clean, replacement)
    first_question = _extract_primary_question(clean)
    if not first_question:
        return _append_question(clean, replacement)
    return clean.replace(first_question, replacement, 1).strip()


def _append_question(text: str, question: str) -> str:
    clean_text = " ".join(str(text or "").split()).strip().rstrip(".")
    clean_question = _ensure_question_mark(question)
    if not clean_text:
        return clean_question
    if clean_text.endswith("?"):
        return clean_text
    return f"{clean_text}. {clean_question}".strip()


def _ensure_question_mark(question: str) -> str:
    clean = " ".join(str(question or "").split()).strip()
    if not clean:
        return ""
    return clean if clean.endswith("?") else f"{clean}?"


def _ensure_single_question(text: str) -> str:
    clean = " ".join(str(text or "").split()).strip()
    if clean.count("?") <= 1:
        return clean
    first = clean.find("?")
    head = clean[: first + 1]
    tail = clean[first + 1 :].replace("?", ".")
    return f"{head} {tail}".strip()


def _trim_sms_text(text: str) -> str:
    lines = [" ".join(line.split()).strip() for line in str(text or "").splitlines() if line.strip()]
    if not lines:
        return ""
    lines = lines[:2]
    lines = [line[:320].strip() for line in lines]
    return "\n".join(line for line in lines if line)


def build_llm_agent(settings: Settings, runtime_overrides: dict[str, str] | None = None) -> LLMAgentV2:
    effective = runtime_overrides or {}
    api_key = str(effective.get("openai_api_key", settings.openai_api_key) or "").strip()
    model = str(effective.get("openai_model", settings.openai_model) or settings.openai_model).strip()

    if api_key:
        provider: LLMProvider = OpenAIProvider(
            api_key=api_key,
            model=model,
            timeout_seconds=settings.request_timeout_seconds,
        )
    else:
        provider = UnavailableLLMProvider()

    return LLMAgentV2(provider=provider)


# Backward-compatible alias for existing imports.
LLMAgent = LLMAgentV2


__all__ = [
    "AgentAction",
    "AgentResponse",
    "LLMAgent",
    "LLMAgentV2",
    "LLMProvider",
    "OpenAIProvider",
    "build_llm_agent",
]
