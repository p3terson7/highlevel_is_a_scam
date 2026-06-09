from __future__ import annotations

import json
import re
from datetime import datetime
from collections.abc import Sequence
from functools import lru_cache
from typing import Any, Literal, Protocol

from openai import OpenAI
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.core.logging import get_logger
from app.db.models import Client, ConversationStateEnum, Lead, Message, MessageDirection
from app.services.booking import (
    BookingProviderError,
    BookingService,
    automated_booking_enabled,
    booking_mode_label,
)
from app.services.knowledge import build_knowledge_context
from app.services.lead_summary import build_lead_summary_text, normalize_form_answers

logger = get_logger(__name__)

QuestionKey = Literal[
    "decision_makers",
    "urgency_driver",
]
IntentLevel = Literal["HIGH_INTENT", "MEDIUM_INTENT", "LOW_INTENT"]
ActionType = Literal["none", "ask_next_question", "offer_booking", "mark_booked", "handoff_to_human"]
ToolName = Literal["none", "find_slots", "book_slot", "mark_booked", "handoff_to_human"]

_ALLOWED_STATES = {
    ConversationStateEnum.QUALIFYING,
    ConversationStateEnum.BOOKING_SENT,
    ConversationStateEnum.BOOKED,
    ConversationStateEnum.HANDOFF,
}
_BOOKED_CONFIRM_PATTERN = re.compile(
    r"\b(i booked|we booked|booked already|already booked|appointment booked|scheduled it|i scheduled|i'm booked|im booked)\b",
    re.IGNORECASE,
)
_HANDOFF_PATTERN = re.compile(r"\b(human|person|call me|someone from your team|manager|representative)\b", re.IGNORECASE)
_CLOSING_PATTERN = re.compile(r"^(thanks|thank you|ok|okay|cool|great|perfect|sounds good)[.! ]*$", re.IGNORECASE)
_TIMELINE_PATTERN = re.compile(
    r"\b(asap|immediately|this week|next week|within \d+\s+(?:day|days|week|weeks|month|months)|\d+\s+(?:day|days|week|weeks|month|months))\b",
    re.IGNORECASE,
)
_DECISION_MAKER_PATTERN = re.compile(
    r"\b(owner|founder|co-?founder|decision maker|final decision|approv|procurement|partner|stakeholder|team lead|director)\b",
    re.IGNORECASE,
)
_BOOKING_INTENT_PATTERN = re.compile(
    r"\b(yes|yeah|yep|sure|sounds good|works for me|let'?s do it|book it|go ahead|schedule|book|set it up|confirm)\b",
    re.IGNORECASE,
)
_PRICING_PATTERN = re.compile(r"\b(price|pricing|cost|quote|estimate|how much|rates?|budget)\b", re.IGNORECASE)
_LOW_INTENT_PATTERN = re.compile(
    r"\b(just looking|just browsing|browsing|researching|early stages?|curious|not ready|"
    r"not sure yet|general idea|learn more|info only|information only)\b",
    re.IGNORECASE,
)
_CALL_REFUSAL_PATTERN = re.compile(
    r"\b(no call|no meeting|don'?t want (?:a )?(?:call|meeting)|do not want (?:a )?(?:call|meeting)|"
    r"not ready to (?:book|schedule)|don'?t schedule|do not schedule|email only|text only|"
    r"prefer email|rather email|stop asking)\b",
    re.IGNORECASE,
)
_MEETING_CTA_PATTERN = re.compile(
    r"\b(scoping call|quick call|short call|short meeting|meeting|appointment|book(?:ing)?|schedule|calendar|"
    r"availability|available times|send (?:over )?times|share (?:live )?times|find (?:a )?time|"
    r"connect with (?:the )?team|coordinate (?:the )?next step|talk (?:with|to) (?:someone|the team))\b",
    re.IGNORECASE,
)
_BUYING_SIGNAL_PATTERN = re.compile(
    r"\b(ready to start|ready now|need this soon|asap|urgent|this week|next week|call me|"
    r"someone call|can someone call|can we talk|move forward|next step|send me times)\b",
    re.IGNORECASE,
)
_PROJECT_SIZE_PATTERN = re.compile(
    r"\b(\d[\d,]*(?:\.\d+)?\s?(?:sq\s?ft|sf|square feet|locations?|sites?|buildings?|units?|rooms?))\b",
    re.IGNORECASE,
)
_DAY_NAMES = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
_TOOL_JSON_SCHEMA = (
    '{"reply_text":"string","next_state":"QUALIFYING|BOOKING_SENT|BOOKED|HANDOFF",'
    '"collected_fields":{"service_needed":"string|null","timeline":"string|null","locations":"string|null","budget_range":"string|null",'
    '"decision_makers":"string|null","urgency_driver":"string|null","booking_intent_locked":"boolean"},'
    '"next_question_key":"decision_makers|urgency_driver|null",'
    '"action":"none|ask_next_question|offer_booking|mark_booked|handoff_to_human",'
    '"tool_call":{"name":"none|find_slots|book_slot|mark_booked|handoff_to_human","args":{}}}'
)


class QuestionSpec(BaseModel):
    key: QuestionKey
    label: str
    question: str
    description: str


_QUESTION_SPECS: tuple[QuestionSpec, ...] = (
    QuestionSpec(
        key="decision_makers",
        label="Decision-makers",
        question="Are you the decision-maker, and should anyone else join the call?",
        description="Identify who needs to attend or approve next steps.",
    ),
    QuestionSpec(
        key="urgency_driver",
        label="Urgency/driver",
        question="Is there a deadline or key date driving this (start date, event, or approval timeline)?",
        description="Identify urgency so scheduling and next steps match the timeline.",
    ),
)
_QUESTION_SPEC_BY_KEY = {spec.key: spec for spec in _QUESTION_SPECS}
_QUESTION_ORDER: tuple[QuestionKey, ...] = tuple(spec.key for spec in _QUESTION_SPECS)


class QualificationMemory(BaseModel):
    model_config = ConfigDict(extra="ignore")

    service_needed: str | None = None
    timeline: str | None = None
    locations: str | None = None
    budget_range: str | None = None
    decision_makers: str | None = None
    urgency_driver: str | None = None
    booking_intent_locked: bool = False

    def known_fields(self) -> dict[str, Any]:
        payload = self.model_dump(exclude_none=True)
        if payload.get("booking_intent_locked") is False:
            payload.pop("booking_intent_locked", None)
        return payload


class AgentAction(BaseModel):
    type: Literal["send_booking_link", "offer_calendar_slots", "request_more_info", "handoff_to_human"]
    payload: dict[str, Any] = Field(default_factory=dict)


class ToolCall(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: ToolName = "none"
    args: dict[str, Any] = Field(default_factory=dict)


class AgentResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    reply_text: str = ""
    next_state: ConversationStateEnum = ConversationStateEnum.QUALIFYING
    collected_fields: QualificationMemory = Field(default_factory=QualificationMemory)
    next_question_key: QuestionKey | None = None
    action: ActionType = "none"
    tool_call: ToolCall = Field(default_factory=ToolCall)
    runtime_payload: dict[str, Any] = Field(default_factory=dict)
    provider: Literal["openai", "fallback"] = "openai"
    provider_error: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _coerce_shape(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        raw = dict(data)
        raw.setdefault("collected_fields", {})
        raw.setdefault("tool_call", {"name": "none", "args": {}})
        tool = raw.get("tool_call")
        if isinstance(tool, dict):
            name = str(tool.get("name", "")).strip().lower()
            if name == "send_booking_link":
                tool["name"] = "find_slots"
        action = str(raw.get("action", "")).strip().lower()
        if action == "send_booking_link":
            raw["action"] = "offer_booking"
        return raw

    @property
    def actions(self) -> list[AgentAction]:
        return _action_to_legacy(self.action, self.next_question_key, self.runtime_payload)


class LLMProvider(Protocol):
    name: str

    def generate_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any]: ...


class OpenAIProvider:
    name = "openai"

    def __init__(self, *, api_key: str, model: str, timeout_seconds: int = 20) -> None:
        self._client = OpenAI(api_key=api_key, timeout=timeout_seconds, max_retries=0)
        self._model = model

    def _complete(self, system_prompt: str, user_prompt: str) -> str:
        response = self._client.chat.completions.create(
            model=self._model,
            temperature=0.35,
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
            repair_system = f"Return valid JSON only. Match this exact schema: {_TOOL_JSON_SCHEMA}"
            repair_user = (
                "Fix this invalid response into valid JSON only.\n"
                f"Invalid JSON:\n{content}\n"
                f"Parser error: {type(first_error).__name__}: {first_error}"
            )
            repaired = self._complete(system_prompt=repair_system, user_prompt=repair_user)
            return self._parse(repaired)


@lru_cache(maxsize=16)
def _cached_openai_provider(api_key: str, model: str, timeout_seconds: int) -> OpenAIProvider:
    return OpenAIProvider(api_key=api_key, model=model, timeout_seconds=timeout_seconds)


def clear_llm_provider_cache() -> None:
    _cached_openai_provider.cache_clear()


class UnavailableLLMProvider:
    name = "unavailable"

    def generate_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        _ = system_prompt
        _ = user_prompt
        raise RuntimeError("LLM provider unavailable")


class LLMAgentV3:
    def __init__(self, provider: LLMProvider) -> None:
        self._provider = provider

    def next_reply(
        self,
        client: Client,
        lead: Lead,
        inbound_text: str,
        history: Sequence[Message],
    ) -> AgentResponse:
        return self.run_turn(client=client, lead=lead, inbound_text=inbound_text, history=history, booking_service=None, db=None)

    def run_turn(
        self,
        *,
        client: Client,
        lead: Lead,
        inbound_text: str,
        history: Sequence[Message],
        booking_service: BookingService | None,
        db: Session | None,
    ) -> AgentResponse:
        knowledge_context = build_knowledge_context(db, client_id=client.id, query=inbound_text)
        context = self._build_context(
            client=client,
            lead=lead,
            inbound_text=inbound_text,
            history=history,
            knowledge_context=knowledge_context,
        )
        system_prompt = self._build_decision_prompt(client=client)
        user_prompt = json.dumps(context, ensure_ascii=False)

        try:
            raw = self._provider.generate_json(system_prompt=system_prompt, user_prompt=user_prompt)
            decision = AgentResponse.model_validate(raw)
            decision.provider = "openai"
            decision = self._sanitize_decision(decision=decision, context=context)

            tool_name = decision.tool_call.name
            if tool_name == "none" or booking_service is None:
                return decision

            if tool_name == "handoff_to_human":
                decision.action = "handoff_to_human"
                decision.next_state = ConversationStateEnum.HANDOFF
                decision.tool_call = ToolCall()
                return _finalize_response_with_context(decision, context)
            if tool_name == "mark_booked":
                if bool(context.get("booked_confirmation_intent")) or bool(context.get("already_booked")):
                    decision.action = "mark_booked"
                    decision.next_state = ConversationStateEnum.BOOKED
                    decision.tool_call = ToolCall()
                    return _finalize_response_with_context(decision, context)
                slot_choice = _extract_slot_choice(
                    inbound_text=str(context.get("latest_inbound_message") or ""),
                    latest_offer=context.get("latest_booking_offer"),
                )
                if slot_choice:
                    decision.tool_call = ToolCall(name="book_slot", args=slot_choice)
                    tool_name = "book_slot"
                else:
                    decision.action = "none"
                    if decision.next_state == ConversationStateEnum.BOOKED:
                        if str(context.get("current_state") or "").upper() == ConversationStateEnum.BOOKING_SENT.value:
                            decision.next_state = ConversationStateEnum.BOOKING_SENT
                        else:
                            decision.next_state = ConversationStateEnum.QUALIFYING
                    decision.tool_call = ToolCall()
                    if not decision.reply_text or "booked" in decision.reply_text.lower():
                        decision.reply_text = "I can lock that in once you pick one of the offered times."
                    return _finalize_response_with_context(decision, context)

            tool_result = self._execute_tool(
                tool_call=decision.tool_call,
                client=client,
                lead=lead,
                history=history,
                context=context,
                booking_service=booking_service,
                db=db,
            )
            final = self._compose_tool_response(decision=decision, tool_result=tool_result, context=context, client=client)
            final.provider = "openai"
            return final
        except Exception as exc:
            logger.exception("agent_v3_llm_failed", extra={"error": str(exc)})
            fallback = self._safe_fallback(client=client, context=context)
            fallback.provider = "fallback"
            fallback.provider_error = str(exc)
            return fallback

    def _build_decision_prompt(self, *, client: Client) -> str:
        faq_context = (client.faq_context or "").strip() or "none provided"
        ai_context = (getattr(client, "ai_context", "") or "").strip() or "none provided"
        return (
            "You are a calm, natural SMS lead concierge for a client business. "
            "You help leads understand services, answer questions, qualify through conversation, and guide qualified leads to the right next step.\n"
            "You are not a generic meeting booker. The backend can only do simple tools when you request them.\n"
            "Use faq_context as the source of truth for services, deliverables, process, and pricing rules.\n"
            "Use knowledge_context as source-backed website content for specific service, pricing, policy, and process facts.\n"
            "Use ai_context only for tone and do/don't-say guidance. It must not override the actual lead details or the business domain.\n"
            "If neither faq_context nor knowledge_context answers a factual question, say what you can confirm and avoid inventing details.\n"
            "Lead context policy:\n"
            "- lead_form_answers, known_form_facts, and qualification_memory are already-known information from the form or prior conversation.\n"
            "- Never ask the lead to repeat a known fact such as scope, service, size, timeline, role, contact preference, or location unless they contradicted it.\n"
            "- On the first outbound SMS, naturally reference 1-3 important known form facts, then ask one useful missing question or, only for very high intent, offer the next step softly.\n"
            "- Use important_missing_fields and recommended_missing_field to choose the next useful question, but ask only one question at a time.\n"
            "Intent strategy:\n"
            "- intent_level is HIGH_INTENT, MEDIUM_INTENT, or LOW_INTENT and may change each turn based on form answers plus the latest message.\n"
            "- HIGH_INTENT: acknowledge the known project, ask only the most important missing item, and frame booking as a logical option, not pressure.\n"
            "- MEDIUM_INTENT: clarify and educate first. Suggest a meeting only after the need is clearer or timing/next-step intent appears.\n"
            "- LOW_INTENT: nurture and educate. Do not push a call; ask what they are trying to understand.\n"
            "Conversation rules:\n"
            "- Be concise, human, and helpful. Usually 1-2 short SMS-sized sentences.\n"
            "- If the lead asks a question, answer it first. Only after answering should you guide to a next step, and only if useful.\n"
            "- Ask at most one follow-up question. Never dump multiple intake questions.\n"
            "- Do not repeat a question in asked_question_keys.\n"
            "- Avoid repeating meeting CTAs. Respect cta_state.meeting_rejected and cta_state.suppress_meeting_cta.\n"
            "- If a meeting was suggested and the lead ignored it, continue helping instead of repeating the same call-to-action.\n"
            "- If the lead refuses a call, stop suggesting calls and keep answering questions.\n"
            "- Vary next-step language. Avoid overusing phrases like 'short scoping call' or 'book a short call'.\n"
            "- Do not invent pricing, guarantees, deadlines, availability, or service details.\n"
            "- Do not pretend to be a human. If unsure or if the lead asks for a person, use handoff_to_human.\n"
            "- A booked lead can still ask questions. Keep helping without rebooking.\n"
            "Booking tool rules:\n"
            "- Only call find_slots when the lead explicitly asks for times, shares availability, chooses to schedule, or clearly says they want to book now.\n"
            "- If the lead asks about another day or another time, use the booking tools rather than repeating the same slots.\n"
            "- Never say a requested day is booked or unavailable unless the booking tool result actually shows no matching openings for that request.\n"
            "- If the lead already booked, use tool_call mark_booked.\n"
            "- If the lead asks for a human, use tool_call handoff_to_human.\n"
            "Tool rules:\n"
            "- tool_call none: no backend action needed, just send a normal reply.\n"
            "- tool_call find_slots: use when the lead wants availability, asks about a specific day/time, or you are ready to present live times. Args can include preferred_day, preferred_period, exact_time, range_start, range_end, and limit.\n"
            "- tool_call book_slot: use when the lead has clearly chosen one of the offered slots. Args can include slot_index or slot_start_time.\n"
            "- tool_call mark_booked: use when they tell you they already booked.\n"
            "Output strict JSON only with this exact schema:\n"
            f"{_TOOL_JSON_SCHEMA}\n"
            f"Business name: {client.business_name}\n"
            f"Tone target: {client.tone or 'clear, helpful, concise'}\n"
            f"faq_context: {faq_context}\n"
            f"ai_context: {ai_context}"
        )

    def _build_tool_followup_prompt(self, *, client: Client) -> str:
        faq_context = (client.faq_context or "").strip() or "none provided"
        ai_context = (getattr(client, "ai_context", "") or "").strip() or "none provided"
        return (
            "You are writing the final SMS after a backend tool returned structured booking data.\n"
            "Rules:\n"
            "- Use the tool_result as the source of truth.\n"
            "- Never invent availability or claim a slot is unavailable unless tool_result.match_mode says the request could not be matched exactly.\n"
            "- Mention only times that exist in tool_result.slots.\n"
            "- Keep the tone human and concise.\n"
            "- Make clear the availability is for a call/conference call, not a site visit.\n"
            "- If tool_result.kind is slots, present the call options naturally.\n"
            "- If tool_result.kind is booked, confirm the call booking clearly and briefly.\n"
            "- If tool_result.kind is no_slots, explain that honestly and offer the closest alternatives from tool_result.slots if any exist.\n"
            "- If the lead asked a non-booking question after booking, answer it without changing the booked status.\n"
            "- Do not request another tool call in this step; tool_call must be none.\n"
            "Return strict JSON only with the same schema, but tool_call.name must be none.\n"
            f"{_TOOL_JSON_SCHEMA}\n"
            f"Business name: {client.business_name}\n"
            f"faq_context: {faq_context}\n"
            f"ai_context: {ai_context}"
        )

    def _build_context(
        self,
        *,
        client: Client,
        lead: Lead,
        inbound_text: str,
        history: Sequence[Message],
        knowledge_context: str = "",
    ) -> dict[str, Any]:
        normalized_answers = normalize_form_answers(lead.form_answers or {})
        raw_payload = lead.raw_payload if isinstance(lead.raw_payload, dict) else {}
        prior_memory = QualificationMemory.model_validate(raw_payload.get("qualification_memory") or {})
        answer_memory = _extract_from_form_answers(normalized_answers)
        history_memory = _extract_from_messages(history)
        inbound_memory = _extract_from_text(inbound_text)
        memory = _merge_memory(prior_memory, answer_memory, history_memory, inbound_memory)
        recent_messages = [_serialize_message(message) for message in history[-20:]]
        asked_question_keys = _extract_asked_question_keys(history)
        latest_offer = _latest_booking_offer(history)
        explicit_booking_intent = _has_booking_intent(inbound_text)
        if explicit_booking_intent:
            memory.booking_intent_locked = True
        booking_ready, booking_gap_fields = _booking_threshold(memory=memory)
        flow_state = str(raw_payload.get("flow_state") or "NEW").strip().upper()
        inbound_preferences = _extract_booking_preferences(inbound_text)
        known_form_facts = _build_known_form_facts(normalized_answers, lead=lead)
        important_missing_fields = _important_missing_fields(
            answers=normalized_answers,
            memory=memory,
            lead=lead,
        )
        cta_state = _build_cta_state(
            history=history,
            raw_payload=raw_payload,
            inbound_text=inbound_text,
            explicit_booking_intent=explicit_booking_intent,
            inbound_preferences=inbound_preferences,
            latest_offer=latest_offer,
        )
        intent_profile = _classify_lead_intent(
            answers=normalized_answers,
            memory=memory,
            inbound_text=inbound_text,
            history=history,
            explicit_booking_intent=explicit_booking_intent,
            inbound_preferences=inbound_preferences,
        )
        internal_summary = _build_internal_lead_summary(
            lead=lead,
            normalized_answers=normalized_answers,
            memory=memory,
            intent_profile=intent_profile,
            important_missing_fields=important_missing_fields,
            cta_state=cta_state,
            meeting_status=_meeting_status(lead=lead, cta_state=cta_state, latest_offer=latest_offer),
        )

        return {
            "business_name": client.business_name,
            "tone": client.tone,
            "faq_context": client.faq_context or "",
            "ai_context": getattr(client, "ai_context", "") or "",
            "knowledge_context": knowledge_context,
            "lead_name": lead.full_name or "",
            "lead_phone": lead.phone or "",
            "lead_email": lead.email or "",
            "lead_city": lead.city or "",
            "lead_summary": build_lead_summary_text(normalized_answers, limit=8),
            "lead_form_answers": normalized_answers,
            "known_form_facts": known_form_facts,
            "known_form_field_keys": list(normalized_answers.keys()),
            "internal_lead_summary": internal_summary,
            "latest_inbound_message": inbound_text,
            "recent_messages": recent_messages,
            "current_state": lead.conversation_state.value if lead.conversation_state else ConversationStateEnum.NEW.value,
            "already_booked": lead.conversation_state == ConversationStateEnum.BOOKED,
            "crm_stage": getattr(lead, "crm_stage", None),
            "initial_outreach": len(history) == 0,
            "flow_state": flow_state,
            "qualification_memory": memory.model_dump(exclude_none=True),
            "asked_question_keys": asked_question_keys,
            "missing_fields": [key for key in _QUESTION_ORDER if not _memory_has_value(memory, key)],
            "important_missing_fields": important_missing_fields,
            "recommended_missing_field": important_missing_fields[0] if important_missing_fields else None,
            "recommended_next_question_key": _recommended_next_question_key(memory=memory, asked_question_keys=asked_question_keys),
            "booking_ready": booking_ready,
            "booking_gap_fields": booking_gap_fields,
            "booking_intent_locked": bool(memory.booking_intent_locked),
            "booking_mode": booking_mode_label(client),
            "automated_booking_enabled": automated_booking_enabled(client),
            "booking_url": client.booking_url or "",
            "latest_booking_offer": latest_offer,
            "latest_inbound_booking_preferences": inbound_preferences,
            "available_tools": ["find_slots", "book_slot", "mark_booked", "handoff_to_human"],
            "explicit_booking_intent": explicit_booking_intent,
            "booked_confirmation_intent": bool(_BOOKED_CONFIRM_PATTERN.search(inbound_text or "")),
            "handoff_intent": bool(_HANDOFF_PATTERN.search(inbound_text or "")),
            "closing_only": bool(_CLOSING_PATTERN.match((inbound_text or "").strip())),
            "pricing_question": bool(_PRICING_PATTERN.search(inbound_text or "")),
            "lead_question_detected": _lead_asked_question(inbound_text),
            "call_refusal": bool(_CALL_REFUSAL_PATTERN.search(inbound_text or "")),
            "low_intent_signal": bool(_LOW_INTENT_PATTERN.search(inbound_text or "")),
            "intent_level": intent_profile["level"],
            "intent_score": intent_profile["score"],
            "intent_reasons": intent_profile["reasons"],
            "cta_state": cta_state,
            "recommended_response_strategy": _recommended_response_strategy(
                intent_level=str(intent_profile["level"]),
                cta_state=cta_state,
                pricing_question=bool(_PRICING_PATTERN.search(inbound_text or "")),
                lead_question_detected=_lead_asked_question(inbound_text),
            ),
            "outbound_turn_count": len([message for message in history if message.direction == MessageDirection.OUTBOUND]),
            "question_specs": {
                spec.key: {
                    "label": spec.label,
                    "question": spec.question,
                    "description": spec.description,
                }
                for spec in _QUESTION_SPECS
            },
        }

    def _sanitize_decision(self, *, decision: AgentResponse, context: dict[str, Any]) -> AgentResponse:
        decision.collected_fields = _merge_memory(
            QualificationMemory.model_validate(context.get("qualification_memory") or {}),
            decision.collected_fields,
        )
        if bool(context.get("explicit_booking_intent")):
            decision.collected_fields.booking_intent_locked = True
        decision.reply_text = _trim_sms_text(decision.reply_text)
        if decision.next_state not in _ALLOWED_STATES:
            decision.next_state = ConversationStateEnum.QUALIFYING

        current_state = str(context.get("current_state") or "").upper()
        if current_state == ConversationStateEnum.BOOKED.value and bool(context.get("closing_only")):
            decision.reply_text = decision.reply_text or "Perfect. See you then."
            decision.next_state = ConversationStateEnum.BOOKED
            decision.action = "none"
            decision.next_question_key = None
            decision.tool_call = ToolCall()
            return _finalize_response_with_context(decision, context)

        if bool(context.get("booked_confirmation_intent")) and decision.tool_call.name == "none":
            decision.tool_call = ToolCall(name="mark_booked", args={})
            decision.action = "mark_booked"
            decision.next_state = ConversationStateEnum.BOOKED
            decision.next_question_key = None
            decision.reply_text = decision.reply_text or "Perfect. You're booked."
            return _finalize_response_with_context(decision, context)

        if bool(context.get("handoff_intent")) and decision.tool_call.name == "none":
            decision.tool_call = ToolCall(name="handoff_to_human", args={})
            decision.action = "handoff_to_human"
            decision.next_state = ConversationStateEnum.HANDOFF
            decision.next_question_key = None
            return _finalize_response_with_context(decision, context)

        slot_choice = _extract_slot_choice(
            inbound_text=str(context.get("latest_inbound_message") or ""),
            latest_offer=context.get("latest_booking_offer"),
        )
        if slot_choice and decision.tool_call.name == "none" and current_state != ConversationStateEnum.BOOKED.value:
            args: dict[str, Any] = {}
            if slot_choice.get("slot_index"):
                args["slot_index"] = slot_choice["slot_index"]
            if slot_choice.get("slot_start_time"):
                args["slot_start_time"] = slot_choice["slot_start_time"]
            decision.tool_call = ToolCall(name="book_slot", args=args)
            decision.action = "none"
            decision.next_state = ConversationStateEnum.BOOKING_SENT
            decision.next_question_key = None

        if decision.tool_call.name == "mark_booked" and not bool(context.get("booked_confirmation_intent")):
            if slot_choice and current_state != ConversationStateEnum.BOOKED.value:
                args: dict[str, Any] = {}
                if slot_choice.get("slot_index"):
                    args["slot_index"] = slot_choice["slot_index"]
                if slot_choice.get("slot_start_time"):
                    args["slot_start_time"] = slot_choice["slot_start_time"]
                decision.tool_call = ToolCall(name="book_slot", args=args)
                decision.action = "none"
                decision.next_state = ConversationStateEnum.BOOKING_SENT
            else:
                decision.tool_call = ToolCall()
                decision.action = "none"
                if current_state == ConversationStateEnum.BOOKING_SENT.value:
                    decision.next_state = ConversationStateEnum.BOOKING_SENT
                    decision.runtime_payload["flow_state"] = "WAITING_SLOT_CHOICE"
                else:
                    decision.next_state = ConversationStateEnum.QUALIFYING
                if not decision.reply_text or "booked" in decision.reply_text.lower():
                    decision.reply_text = "I can lock that in once you pick one of the offered times."

        inbound_text = str(context.get("latest_inbound_message") or "")
        inbound_preferences = context.get("latest_inbound_booking_preferences")
        should_offer_slots_now = bool(context.get("explicit_booking_intent")) or bool(inbound_preferences)
        if current_state == ConversationStateEnum.BOOKING_SENT.value and not should_offer_slots_now:
            normalized_inbound = _normalize_text(inbound_text)
            should_offer_slots_now = normalized_inbound in {
                "yes",
                "yeah",
                "yep",
                "sure",
                "ok",
                "okay",
                "sounds good",
                "works",
                "works for me",
                "go ahead",
                "book",
                "book it",
                "schedule",
            }

        cta_state = context.get("cta_state") if isinstance(context.get("cta_state"), dict) else {}
        suppress_meeting_cta = bool(
            cta_state.get("meeting_rejected")
            or cta_state.get("suppress_meeting_cta")
            or str(context.get("intent_level") or "") == "LOW_INTENT"
        )
        if suppress_meeting_cta and not should_offer_slots_now:
            if decision.tool_call.name in {"find_slots", "book_slot"}:
                decision.tool_call = ToolCall()
                decision.action = "none"
                if decision.next_state == ConversationStateEnum.BOOKING_SENT:
                    decision.next_state = ConversationStateEnum.QUALIFYING
            if decision.action == "offer_booking":
                decision.action = "none"
                if decision.next_state == ConversationStateEnum.BOOKING_SENT:
                    decision.next_state = ConversationStateEnum.QUALIFYING
            if _message_suggests_meeting(decision.reply_text):
                decision.reply_text = _strip_meeting_cta(
                    decision.reply_text,
                    fallback=_non_booking_bridge_reply(context),
                )

        refreshed_time_preferences = _booking_preferences_with_offer_context(
            dict(inbound_preferences or {}),
            latest_offer=context.get("latest_booking_offer"),
        )
        if (
            current_state == ConversationStateEnum.BOOKING_SENT.value
            and refreshed_time_preferences
            and not slot_choice
            and decision.tool_call.name == "none"
        ):
            decision.tool_call = ToolCall(name="find_slots", args=refreshed_time_preferences)
            decision.action = "none"
            decision.next_state = ConversationStateEnum.BOOKING_SENT
            decision.next_question_key = None

        if (
            bool(decision.collected_fields.booking_intent_locked)
            and decision.tool_call.name == "none"
            and current_state != ConversationStateEnum.BOOKED.value
            and should_offer_slots_now
        ):
            # Booking intent is clear for this turn; move directly to live times.
            decision.tool_call = ToolCall(name="find_slots", args={})
            decision.action = "none"
            decision.next_state = ConversationStateEnum.BOOKING_SENT
            decision.next_question_key = None
            if not decision.reply_text:
                decision.reply_text = "I can share a few times that work."

        if decision.tool_call.name != "none":
            decision.next_question_key = None
            if decision.tool_call.name == "send_booking_link":
                # Backward compatibility: old prompts or cached providers may emit this.
                # We no longer use link-based booking in AI flow, so route to live slots.
                decision.tool_call = ToolCall(name="find_slots", args=decision.tool_call.args or {})
            if decision.tool_call.name == "mark_booked":
                decision.action = "mark_booked"
                decision.next_state = ConversationStateEnum.BOOKED
            elif decision.tool_call.name == "handoff_to_human":
                decision.action = "handoff_to_human"
                decision.next_state = ConversationStateEnum.HANDOFF
            else:
                decision.action = "none"
                if decision.tool_call.name == "find_slots":
                    decision.next_state = ConversationStateEnum.BOOKING_SENT
                    decision.runtime_payload["flow_state"] = "WAITING_SLOT_CHOICE"
                elif decision.tool_call.name == "book_slot":
                    decision.runtime_payload["flow_state"] = "CONFIRMING"
            return _finalize_response_with_context(decision, context)

        if decision.action == "ask_next_question":
            original_key = decision.next_question_key
            if decision.next_question_key not in _QUESTION_SPEC_BY_KEY or decision.next_question_key in set(context.get("asked_question_keys", [])):
                decision.next_question_key = _recommended_next_question_key(
                    memory=decision.collected_fields,
                    asked_question_keys=context.get("asked_question_keys", []),
                )
            if decision.next_question_key and _memory_has_value(decision.collected_fields, decision.next_question_key):
                decision.next_question_key = _recommended_next_question_key(
                    memory=decision.collected_fields,
                    asked_question_keys=[*context.get("asked_question_keys", []), decision.next_question_key],
                )
            if decision.next_question_key and (decision.reply_text.count("?") == 0 or decision.next_question_key != original_key):
                question = _QUESTION_SPEC_BY_KEY[decision.next_question_key].question
                if decision.reply_text.count("?") > 0:
                    decision.reply_text = _replace_question(decision.reply_text, question)
                else:
                    decision.reply_text = _append_question(decision.reply_text, question)
            elif decision.next_question_key is None:
                decision.action = "none"

        if decision.action == "ask_next_question" and decision.next_question_key == "decision_makers":
            decision.runtime_payload["flow_state"] = "ASK_DECISION_MAKERS"
        elif decision.action == "ask_next_question" and decision.next_question_key == "urgency_driver":
            decision.runtime_payload["flow_state"] = "ASK_URGENCY"
        elif decision.next_state == ConversationStateEnum.QUALIFYING:
            decision.runtime_payload["flow_state"] = "ACKED"

        if current_state == ConversationStateEnum.BOOKED.value and decision.tool_call.name == "none":
            decision.next_state = ConversationStateEnum.BOOKED
            decision.runtime_payload["flow_state"] = "CONFIRMED"

        if decision.tool_call.name == "none":
            last_outbound_text = _latest_outbound_text(context.get("recent_messages"))
            if last_outbound_text and _normalize_text(last_outbound_text) == _normalize_text(decision.reply_text):
                if decision.action == "ask_next_question" and decision.next_question_key in _QUESTION_SPEC_BY_KEY:
                    decision.reply_text = _QUESTION_SPEC_BY_KEY[decision.next_question_key].question
                elif current_state == ConversationStateEnum.BOOKED.value:
                    decision.reply_text = "You're all set. Text me here anytime if something changes before the meeting."
                else:
                    decision.reply_text = _non_booking_bridge_reply(context)

        return _finalize_response_with_context(decision, context)

    def _compose_tool_response(
        self,
        *,
        decision: AgentResponse,
        tool_result: dict[str, Any],
        context: dict[str, Any],
        client: Client,
    ) -> AgentResponse:
        try:
            followup_prompt = self._build_tool_followup_prompt(client=client)
            followup_user = json.dumps(
                {
                    "conversation_context": context,
                    "first_pass": decision.model_dump(mode="json", exclude={"runtime_payload"}),
                    "tool_result": tool_result,
                },
                ensure_ascii=False,
            )
            followup_raw = self._provider.generate_json(system_prompt=followup_prompt, user_prompt=followup_user)
            final = AgentResponse.model_validate(followup_raw)
            final.provider = "openai"
            final.tool_call = ToolCall()
            final.collected_fields = _merge_memory(decision.collected_fields, final.collected_fields)
            final.runtime_payload = dict(tool_result.get("runtime_payload") or {})
            return self._sanitize_post_tool_response(final=final, decision=decision, tool_result=tool_result, context=context)
        except Exception as exc:
            logger.exception("agent_v3_tool_compose_failed", extra={"error": str(exc)})
            return self._compose_tool_response_fallback(decision=decision, tool_result=tool_result, context=context)

    def _compose_tool_response_fallback(
        self,
        *,
        decision: AgentResponse,
        tool_result: dict[str, Any],
        context: dict[str, Any],
    ) -> AgentResponse:
        kind = str(tool_result.get("kind") or "none")
        runtime_payload = dict(tool_result.get("runtime_payload") or {})
        current_state = str(context.get("current_state") or "").upper()
        reply_text = str(tool_result.get("reply_hint") or tool_result.get("fallback_reply") or decision.reply_text or "Understood.").strip()
        next_state = ConversationStateEnum.BOOKING_SENT if kind in {"slots", "no_slots"} else decision.next_state
        action: ActionType = "none"

        if kind == "booked":
            reply_text = str(tool_result.get("fallback_reply") or "Perfect. You're booked.").strip()
            next_state = ConversationStateEnum.BOOKED
            action = "mark_booked"
        elif kind == "handoff":
            reply_text = str(tool_result.get("fallback_reply") or "Understood. I’ll have someone reach out.").strip()
            next_state = ConversationStateEnum.HANDOFF
            action = "handoff_to_human"
        elif kind in {"slots", "no_slots"}:
            reply_text = str(tool_result.get("fallback_reply") or reply_text).strip()
            next_state = ConversationStateEnum.BOOKING_SENT
        elif current_state == ConversationStateEnum.BOOKED.value:
            next_state = ConversationStateEnum.BOOKED

        final = AgentResponse(
            reply_text=reply_text,
            next_state=next_state,
            collected_fields=decision.collected_fields,
            next_question_key=None,
            action=action,
            tool_call=ToolCall(),
            runtime_payload=runtime_payload,
        )
        return _finalize_response_with_context(final, context)

    def _sanitize_post_tool_response(
        self,
        *,
        final: AgentResponse,
        decision: AgentResponse,
        tool_result: dict[str, Any],
        context: dict[str, Any],
    ) -> AgentResponse:
        final.reply_text = _trim_sms_text(final.reply_text)
        final.next_question_key = None
        final.tool_call = ToolCall()
        if final.next_state not in _ALLOWED_STATES:
            final.next_state = ConversationStateEnum.BOOKING_SENT if tool_result.get("kind") in {"slots", "no_slots"} else ConversationStateEnum.QUALIFYING
        if tool_result.get("kind") == "booked":
            final.next_state = ConversationStateEnum.BOOKED
            final.action = "mark_booked"
            final.runtime_payload["flow_state"] = "CONFIRMED"
        elif tool_result.get("kind") == "handoff":
            final.next_state = ConversationStateEnum.HANDOFF
            final.action = "handoff_to_human"
            final.runtime_payload["flow_state"] = "CONFIRMED"
        else:
            final.action = "none"
            if tool_result.get("kind") in {"slots", "no_slots"}:
                final.runtime_payload["flow_state"] = "WAITING_SLOT_CHOICE"
        if str(context.get("current_state") or "").upper() == ConversationStateEnum.BOOKED.value and final.next_state == ConversationStateEnum.QUALIFYING:
            final.next_state = ConversationStateEnum.BOOKED
            final.runtime_payload["flow_state"] = "CONFIRMED"
        final.collected_fields = _merge_memory(decision.collected_fields, final.collected_fields)
        if not final.reply_text:
            final.reply_text = str(tool_result.get("fallback_reply") or decision.reply_text or "Understood.")
        return _finalize_response_with_context(final, context)

    def _execute_tool(
        self,
        *,
        tool_call: ToolCall,
        client: Client,
        lead: Lead,
        history: Sequence[Message],
        context: dict[str, Any],
        booking_service: BookingService,
        db: Session | None,
    ) -> dict[str, Any]:
        args = tool_call.args or {}
        latest_offer = _latest_booking_offer(history)
        inferred_preferences = _booking_preferences_with_offer_context(
            _extract_booking_preferences(str(context.get("latest_inbound_message") or "")),
            latest_offer=latest_offer,
        )
        if tool_call.name == "find_slots":
            preferred_day = _normalize_requested_day(_normalize_optional_string(args.get("preferred_day"))) or inferred_preferences.get("preferred_day")
            avoid_day = _normalize_requested_day(_normalize_optional_string(args.get("avoid_day"))) or inferred_preferences.get("avoid_day")
            preferred_period = _normalize_optional_string(args.get("preferred_period")) or inferred_preferences.get("preferred_period")
            exact_time = _normalize_optional_string(args.get("exact_time")) or inferred_preferences.get("exact_time")
            range_start = _normalize_optional_string(args.get("range_start")) or inferred_preferences.get("range_start")
            range_end = _normalize_optional_string(args.get("range_end")) or inferred_preferences.get("range_end")
            limit = _to_int(args.get("limit"), default=3)
            offer = booking_service.find_slots(
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
            return _slot_offer_tool_result(
                offer=offer,
                availability_query={
                    "preferred_day": preferred_day,
                    "avoid_day": avoid_day,
                    "preferred_period": preferred_period,
                    "exact_time": exact_time,
                    "range_start": range_start,
                    "range_end": range_end,
                },
            )
        if tool_call.name == "book_slot":
            slot_index = _to_int(args.get("slot_index"), default=0) or None
            slot_start_time = _normalize_optional_string(args.get("slot_start_time"))
            slot_text = str(context.get("latest_inbound_message") or "")
            try:
                result = booking_service.book_requested_slot(
                    client=client,
                    lead=lead,
                    latest_offer=latest_offer,
                    slot_index=slot_index,
                    slot_start_time=slot_start_time,
                    slot_text=slot_text,
                    db=db,
                )
            except BookingProviderError:
                try:
                    result = booking_service.book_requested_slot(
                        client=client,
                        lead=lead,
                        latest_offer=latest_offer,
                        slot_index=slot_index,
                        slot_start_time=slot_start_time,
                        slot_text=slot_text,
                        db=db,
                    )
                except BookingProviderError:
                    fallback_offer = booking_service.find_slots(
                        client=client,
                        lead=lead,
                        preferred_day=inferred_preferences.get("preferred_day"),
                        avoid_day=inferred_preferences.get("avoid_day"),
                        preferred_period=inferred_preferences.get("preferred_period"),
                        exact_time=inferred_preferences.get("exact_time"),
                        range_start=inferred_preferences.get("range_start"),
                        range_end=inferred_preferences.get("range_end"),
                        limit=3,
                        db=db,
                    )
                    return {
                        "kind": "no_slots",
                        "slots": [slot.__dict__ for slot in fallback_offer.slots],
                        "reply_hint": fallback_offer.reply_text,
                        "availability_query": {
                            "preferred_day": inferred_preferences.get("preferred_day"),
                            "avoid_day": inferred_preferences.get("avoid_day"),
                            "preferred_period": inferred_preferences.get("preferred_period"),
                            "exact_time": inferred_preferences.get("exact_time"),
                            "range_start": inferred_preferences.get("range_start"),
                            "range_end": inferred_preferences.get("range_end"),
                        },
                        "runtime_payload": {
                            "booking_offer": fallback_offer.raw_payload.get("booking_offer", {}),
                            "pending_step": "slot_selection_pending" if fallback_offer.slots else None,
                            "flow_state": "WAITING_SLOT_CHOICE",
                        },
                            "fallback_reply": fallback_offer.reply_text,
                        }
            if not result.get("booking") and _should_check_fresh_slots(inferred_preferences):
                refreshed = booking_service.find_slots(
                    client=client,
                    lead=lead,
                    preferred_day=inferred_preferences.get("preferred_day"),
                    avoid_day=inferred_preferences.get("avoid_day"),
                    preferred_period=inferred_preferences.get("preferred_period"),
                    exact_time=inferred_preferences.get("exact_time"),
                    range_start=inferred_preferences.get("range_start"),
                    range_end=inferred_preferences.get("range_end"),
                    limit=3,
                    db=db,
                )
                return _slot_offer_tool_result(
                    offer=refreshed,
                    availability_query={
                        "preferred_day": inferred_preferences.get("preferred_day"),
                        "avoid_day": inferred_preferences.get("avoid_day"),
                        "preferred_period": inferred_preferences.get("preferred_period"),
                        "exact_time": inferred_preferences.get("exact_time"),
                        "range_start": inferred_preferences.get("range_start"),
                        "range_end": inferred_preferences.get("range_end"),
                    },
                )
            return {
                "kind": "booked" if result.get("booking") else "slots",
                "booking": result.get("booking"),
                "slots": result.get("slots", []),
                "reply_hint": result.get("reply_text", ""),
                "runtime_payload": result.get("runtime_payload", {}),
                "fallback_reply": result.get("reply_text", ""),
            }
        if tool_call.name == "mark_booked":
            return {"kind": "booked", "runtime_payload": {"pending_step": None}, "fallback_reply": "Perfect. You're booked."}
        if tool_call.name == "handoff_to_human":
            return {"kind": "handoff", "runtime_payload": {"pending_step": None}, "fallback_reply": "Understood. I'll have someone reach out."}
        return {"kind": "none", "runtime_payload": {}, "fallback_reply": "Understood."}

    def _safe_fallback(self, *, client: Client, context: dict[str, Any]) -> AgentResponse:
        _ = client
        if (
            context.get("call_refusal")
            or context.get("pricing_question")
            or str(context.get("intent_level") or "") == "LOW_INTENT"
            or (context.get("cta_state") or {}).get("suppress_meeting_cta")
        ):
            return _finalize_response_with_context(
                AgentResponse(
                    reply_text=_non_booking_bridge_reply(context),
                    next_state=ConversationStateEnum.QUALIFYING,
                    collected_fields=QualificationMemory.model_validate(context.get("qualification_memory") or {}),
                    action="none",
                ),
                context,
            )
        next_key = _recommended_next_question_key(
            memory=QualificationMemory.model_validate(context.get("qualification_memory") or {}),
            asked_question_keys=context.get("asked_question_keys", []),
        )
        if next_key:
            reply = _QUESTION_SPEC_BY_KEY[next_key].question
            return _finalize_response_with_context(
                AgentResponse(
                    reply_text=reply,
                    next_state=ConversationStateEnum.QUALIFYING,
                    collected_fields=QualificationMemory.model_validate(context.get("qualification_memory") or {}),
                    next_question_key=next_key,
                    action="ask_next_question",
                ),
                context,
            )
        return _finalize_response_with_context(
            AgentResponse(
                reply_text=_non_booking_bridge_reply(context),
                next_state=ConversationStateEnum.QUALIFYING,
                collected_fields=QualificationMemory.model_validate(context.get("qualification_memory") or {}),
                action="none",
            ),
            context,
        )


def _serialize_message(message: Message) -> dict[str, Any]:
    raw_payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
    agent_payload = raw_payload.get("agent") if isinstance(raw_payload.get("agent"), dict) else {}
    booking_offer = raw_payload.get("booking_offer") if isinstance(raw_payload.get("booking_offer"), dict) else None
    calendar_booking = raw_payload.get("calendar_booking") if isinstance(raw_payload.get("calendar_booking"), dict) else None
    return {
        "direction": message.direction.value if isinstance(message.direction, MessageDirection) else str(message.direction),
        "body": " ".join(str(message.body or "").split()),
        "question_key": agent_payload.get("next_question_key"),
        "agent_action": agent_payload.get("action"),
        "booking_offer": booking_offer,
        "calendar_booking": calendar_booking,
    }


def _slot_offer_tool_result(*, offer: Any, availability_query: dict[str, Any]) -> dict[str, Any]:
    pending_step = "slot_selection_pending" if offer.slots else None
    return {
        "kind": "slots" if offer.slots else "no_slots",
        "slots": [slot.__dict__ for slot in offer.slots],
        "reply_hint": offer.reply_text,
        "availability_query": availability_query,
        "runtime_payload": {
            "booking_offer": offer.raw_payload.get("booking_offer", {}),
            "pending_step": pending_step,
        },
        "fallback_reply": offer.reply_text,
    }


def _should_check_fresh_slots(preferences: dict[str, str]) -> bool:
    return any(
        bool(preferences.get(key))
        for key in ("preferred_day", "preferred_period", "exact_time", "range_start", "range_end", "avoid_day")
    )


def _booking_preferences_with_offer_context(
    preferences: dict[str, str],
    *,
    latest_offer: dict[str, Any] | None,
) -> dict[str, str]:
    enriched = dict(preferences or {})
    if enriched.get("exact_time") and not enriched.get("preferred_day"):
        inferred_day = _single_day_from_offer(latest_offer)
        if inferred_day:
            enriched["preferred_day"] = inferred_day
    return enriched


def _single_day_from_offer(latest_offer: dict[str, Any] | None) -> str | None:
    if not isinstance(latest_offer, dict):
        return None
    slots = latest_offer.get("slots")
    if not isinstance(slots, list) or not slots:
        return None
    days: set[str] = set()
    for slot in slots:
        if not isinstance(slot, dict):
            continue
        haystack = _normalize_text(
            " ".join(
                [
                    str(slot.get("display_time", "")),
                    str(slot.get("display_hint", "")),
                    str(slot.get("search_blob", "")),
                ]
            )
        )
        matched_day = next((day for day in _DAY_NAMES if day in haystack), None)
        if matched_day:
            days.add(matched_day)
    if len(days) == 1:
        return next(iter(days))
    return None


def _latest_booking_offer(history: Sequence[Message]) -> dict[str, Any] | None:
    for message in reversed(history):
        raw_payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
        offer = raw_payload.get("booking_offer")
        if isinstance(offer, dict) and isinstance(offer.get("slots"), list) and offer.get("slots"):
            return offer
    return None


def _build_known_form_facts(answers: dict[str, Any], *, lead: Lead) -> list[dict[str, str]]:
    facts: list[dict[str, str]] = []
    if lead.full_name:
        facts.append({"key": "lead_name", "label": "Lead name", "value": lead.full_name})
    if lead.city:
        facts.append({"key": "lead_city", "label": "Lead city", "value": lead.city})
    for key, value in answers.items():
        text = _stringify_answer(value)
        if not text:
            continue
        facts.append({"key": key, "label": _humanize_key(key), "value": text})
    return facts[:14]


def _important_missing_fields(*, answers: dict[str, Any], memory: QualificationMemory, lead: Lead) -> list[dict[str, str]]:
    missing: list[dict[str, str]] = []
    if not _answers_have_signal(
        answers,
        key_tokens=("purpose", "goal", "reason", "driver", "use_case", "use case", "why"),
        value_tokens=("remodel", "permit", "design", "tenant improvement", "construction", "lease", "acquisition", "renovation"),
    ):
        missing.append(
            {
                "key": "project_purpose",
                "label": "Project purpose",
                "question": "What is the main goal for this project?",
                "why": "Helps route the lead to the right recommendation without re-asking scope.",
            }
        )
    if not _answers_have_signal(
        answers,
        key_tokens=("access", "occupied", "site_visit", "site visit", "availability", "entry"),
        value_tokens=("occupied", "vacant", "accessible", "access", "open"),
    ):
        missing.append(
            {
                "key": "site_access",
                "label": "Site access",
                "question": "Is the space currently accessible for a site visit?",
                "why": "Confirms whether the team can realistically execute on the requested timing.",
            }
        )
    if not memory.locations and not _answers_have_signal(
        answers,
        key_tokens=("location", "address", "market", "region", "city", "site_address", "project_site"),
    ):
        question = "Where is the project located?"
        if lead.city:
            question = f"Is the project also in {lead.city}, or somewhere else?"
        missing.append(
            {
                "key": "project_location",
                "label": "Project location",
                "question": question,
                "why": "Location affects availability, travel, and routing.",
            }
        )
    if not _answers_have_signal(
        answers,
        key_tokens=("existing_plan", "existing plans", "drawing", "drawings", "cad_file", "plan_file"),
        value_tokens=("plans", "drawings", "cad", "pdf", "blueprint"),
    ):
        missing.append(
            {
                "key": "existing_plans",
                "label": "Existing plans",
                "question": "Do you already have any existing plans or drawings?",
                "why": "This can change the review path, but it can often wait until a later step.",
            }
        )
    if memory.timeline and not _answers_have_signal(
        answers,
        key_tokens=("deadline_driver", "why_timeline", "timeline_reason", "milestone"),
        value_tokens=("permit", "construction", "lease", "closing", "opening", "design deadline", "bid"),
    ):
        missing.append(
            {
                "key": "deadline_driver",
                "label": "Deadline driver",
                "question": "Is that timing tied to design, permit, construction, lease, or something else?",
                "why": "Helps prioritize urgency without asking for the timeline again.",
            }
        )
    return missing[:5]


def _classify_lead_intent(
    *,
    answers: dict[str, Any],
    memory: QualificationMemory,
    inbound_text: str,
    history: Sequence[Message],
    explicit_booking_intent: bool,
    inbound_preferences: dict[str, str],
) -> dict[str, Any]:
    text = str(inbound_text or "")
    answer_blob = _answers_blob(answers)
    score = 0
    reasons: list[str] = []

    low_signal = bool(_LOW_INTENT_PATTERN.search(text))
    pricing_question = bool(_PRICING_PATTERN.search(text))
    buying_signal = bool(_BUYING_SIGNAL_PATTERN.search(text))

    if memory.service_needed:
        score += 2
        reasons.append("clear_service_or_project_need")
    if _answers_have_signal(answers, key_tokens=("deliverable", "service", "scope", "need", "project", "goal")):
        score += 2
        reasons.append("specific_form_scope")
    elif answers:
        score += 1
        reasons.append("some_form_context")

    if memory.timeline:
        score += 1
        reasons.append("timeline_provided")
        if _timeline_is_urgent(memory.timeline) or _timeline_is_urgent(answer_blob):
            score += 2
            reasons.append("urgent_timeline")

    if memory.decision_makers:
        score += 1
        reasons.append("decision_path_known")
        if _DECISION_MAKER_PATTERN.search(memory.decision_makers):
            score += 1
            reasons.append("decision_maker_signal")

    if memory.locations or _answers_have_signal(answers, key_tokens=("location", "address", "city", "market", "region")):
        score += 1
        reasons.append("location_context")

    if _PROJECT_SIZE_PATTERN.search(answer_blob) or _answers_have_signal(answers, key_tokens=("size", "sqft", "square_feet", "locations_scope", "building_count", "site_count")):
        score += 1
        reasons.append("meaningful_size_or_scope")

    if pricing_question:
        score += 2
        reasons.append("pricing_question")
    if explicit_booking_intent or inbound_preferences:
        score += 3
        reasons.append("scheduling_intent")
    if buying_signal:
        score += 2
        reasons.append("buying_signal")
    if low_signal:
        score -= 3
        reasons.append("low_intent_language")
    if not answers and not any(memory.known_fields().values()) and _lead_asked_question(text):
        score += 1
        reasons.append("generic_question")

    if low_signal and not explicit_booking_intent and not buying_signal:
        level: IntentLevel = "LOW_INTENT"
    elif score >= 6:
        level = "HIGH_INTENT"
    elif score >= 3:
        level = "MEDIUM_INTENT"
    else:
        level = "LOW_INTENT"

    # If prior conversation already sent slots, keep the lead at least medium unless they refused the call.
    if level == "LOW_INTENT" and not low_signal and any(_message_suggests_meeting(message.body or "") for message in history):
        level = "MEDIUM_INTENT"

    return {"level": level, "score": score, "reasons": reasons[:8]}


def _build_cta_state(
    *,
    history: Sequence[Message],
    raw_payload: dict[str, Any],
    inbound_text: str,
    explicit_booking_intent: bool,
    inbound_preferences: dict[str, str],
    latest_offer: dict[str, Any] | None,
) -> dict[str, Any]:
    previous = raw_payload.get("cta_state") if isinstance(raw_payload.get("cta_state"), dict) else {}
    previous_count = _to_int(previous.get("meeting_suggested_count"), default=0) if previous else 0
    history_count = _count_meeting_suggestions(history)
    meeting_suggested_count = max(previous_count, history_count)
    last_outbound = _latest_outbound_from_history(history)
    recent_meeting_cta = _message_suggests_meeting(last_outbound.get("body", "") if last_outbound else "")
    accepted = bool(explicit_booking_intent or inbound_preferences or _extract_slot_choice(inbound_text, latest_offer))
    rejected = bool(previous.get("meeting_rejected")) or bool(_CALL_REFUSAL_PATTERN.search(inbound_text or ""))
    ignored = bool(recent_meeting_cta and not accepted and not rejected and str(inbound_text or "").strip())
    renewed_buying_intent = bool(accepted or _BUYING_SIGNAL_PATTERN.search(inbound_text or ""))
    suppress = bool(rejected or (ignored and not renewed_buying_intent) or (meeting_suggested_count >= 2 and not renewed_buying_intent))

    return {
        "meeting_suggested_count": meeting_suggested_count,
        "meeting_accepted": bool(previous.get("meeting_accepted")) or accepted,
        "meeting_rejected": rejected,
        "meeting_ignored": ignored,
        "recent_meeting_cta": recent_meeting_cta,
        "suppress_meeting_cta": suppress,
        "last_cta": previous.get("last_cta") or (_cta_label_for_text(last_outbound.get("body", "")) if last_outbound else None),
        "renewed_buying_intent": renewed_buying_intent,
    }


def _build_internal_lead_summary(
    *,
    lead: Lead,
    normalized_answers: dict[str, Any],
    memory: QualificationMemory,
    intent_profile: dict[str, Any],
    important_missing_fields: list[dict[str, str]],
    cta_state: dict[str, Any],
    meeting_status: str,
) -> dict[str, Any]:
    unanswered = [field["label"] for field in important_missing_fields[:4]]
    intent_level = str(intent_profile.get("level") or "LOW_INTENT")
    if meeting_status == "booked":
        qualification_level = "qualified_booked"
    elif intent_level == "HIGH_INTENT" and len(unanswered) <= 2:
        qualification_level = "qualified"
    elif intent_level == "LOW_INTENT":
        qualification_level = "nurture"
    else:
        qualification_level = "qualifying"

    summary = {
        "lead_name": lead.full_name or "",
        "source_platform": lead.source.value if getattr(lead, "source", None) else "",
        "form_answers_summary": build_lead_summary_text(normalized_answers, limit=8),
        "service_interest": memory.service_needed or _first_answer_by_tokens(normalized_answers, ("service", "deliverable", "scope", "need", "project", "interest")),
        "pain_point": _first_answer_by_tokens(normalized_answers, ("pain", "problem", "challenge", "goal", "reason")),
        "project_purpose": _first_answer_by_tokens(normalized_answers, ("purpose", "goal", "reason", "use_case", "driver")),
        "timeline": memory.timeline or _first_answer_by_tokens(normalized_answers, ("timeline", "deadline", "start", "date")),
        "size_scope": _first_answer_by_tokens(normalized_answers, ("size", "sqft", "square_feet", "scope", "locations", "buildings", "sites")),
        "location": memory.locations or lead.city or _first_answer_by_tokens(normalized_answers, ("location", "address", "city", "region", "market")),
        "decision_maker_role": memory.decision_makers or _first_answer_by_tokens(normalized_answers, ("decision", "role", "approver", "stakeholder")),
        "intent_level": intent_level,
        "intent_reasons": intent_profile.get("reasons") or [],
        "qualification_level": qualification_level,
        "unanswered_questions": unanswered,
        "meeting_status": meeting_status,
        "meeting_suggested_count": cta_state.get("meeting_suggested_count", 0),
        "recommended_follow_up": _recommended_follow_up(
            intent_level=intent_level,
            qualification_level=qualification_level,
            meeting_status=meeting_status,
            missing_fields=important_missing_fields,
            cta_state=cta_state,
        ),
    }
    return {key: value for key, value in summary.items() if value not in (None, "", [])}


def _meeting_status(*, lead: Lead, cta_state: dict[str, Any], latest_offer: dict[str, Any] | None) -> str:
    if lead.conversation_state == ConversationStateEnum.BOOKED:
        return "booked"
    if cta_state.get("meeting_rejected"):
        return "call_refused"
    if latest_offer:
        return "slots_sent"
    if cta_state.get("meeting_accepted"):
        return "accepted_pending_slots"
    if int(cta_state.get("meeting_suggested_count") or 0) > 0:
        return "suggested"
    return "not_suggested"


def _recommended_follow_up(
    *,
    intent_level: str,
    qualification_level: str,
    meeting_status: str,
    missing_fields: list[dict[str, str]],
    cta_state: dict[str, Any],
) -> str:
    if meeting_status == "booked":
        return "Prepare a concise handoff with form answers, discovered project details, and unanswered questions."
    if cta_state.get("meeting_rejected"):
        return "Respect the no-call preference and continue helping by SMS/email unless the lead asks to schedule."
    if intent_level == "HIGH_INTENT":
        if missing_fields:
            return f"Confirm {missing_fields[0]['label'].lower()} and then offer the most useful next step."
        return "Offer to coordinate the next step with varied, low-pressure wording."
    if qualification_level == "nurture":
        return "Answer basic questions, clarify what they are trying to understand, and avoid pushing a meeting."
    return "Clarify the use case one question at a time before suggesting a meeting."


def _recommended_response_strategy(
    *,
    intent_level: str,
    cta_state: dict[str, Any],
    pricing_question: bool,
    lead_question_detected: bool,
) -> str:
    if cta_state.get("meeting_rejected"):
        return "Answer helpfully and do not suggest a call unless the lead reverses course."
    if pricing_question:
        return "Answer pricing carefully first; explain what price depends on and avoid exact numbers unless provided by context."
    if cta_state.get("suppress_meeting_cta"):
        return "Continue helping or ask one useful missing question; do not repeat the meeting CTA this turn."
    if intent_level == "HIGH_INTENT":
        return "Use known form details, ask one important missing question, and only offer booking as a soft next step if natural."
    if intent_level == "MEDIUM_INTENT":
        return "Clarify the need or answer the question before suggesting any meeting."
    if lead_question_detected:
        return "Answer the question and invite one next clarification, not a meeting."
    return "Educate and keep the lead warm with one simple question."


def _attach_behavior_runtime(response: AgentResponse, context: dict[str, Any]) -> None:
    cta_state = dict(context.get("cta_state") or {})
    reply_suggests_meeting = _message_suggests_meeting(response.reply_text)
    if response.tool_call.name == "find_slots" or response.runtime_payload.get("booking_offer"):
        reply_suggests_meeting = True
        cta_label = "live_availability"
    else:
        cta_label = _cta_label_for_text(response.reply_text) if reply_suggests_meeting else cta_state.get("last_cta")

    if reply_suggests_meeting:
        cta_state["meeting_suggested_count"] = int(cta_state.get("meeting_suggested_count") or 0) + 1
        cta_state["last_cta"] = cta_label
        cta_state["recent_meeting_cta"] = True

    if response.next_state == ConversationStateEnum.BOOKED or response.action == "mark_booked" or response.runtime_payload.get("calendar_booking"):
        cta_state["meeting_accepted"] = True
        cta_state["meeting_status"] = "booked"
    elif context.get("call_refusal"):
        cta_state["meeting_rejected"] = True
        cta_state["meeting_status"] = "call_refused"
    elif cta_state.get("meeting_accepted"):
        cta_state["meeting_status"] = "accepted_pending_slots"
    elif response.runtime_payload.get("booking_offer"):
        cta_state["meeting_status"] = "slots_sent"
    elif int(cta_state.get("meeting_suggested_count") or 0) > 0:
        cta_state["meeting_status"] = "suggested"
    else:
        cta_state["meeting_status"] = "not_suggested"

    response.runtime_payload["cta_state"] = cta_state
    response.runtime_payload["intent_level"] = context.get("intent_level", "LOW_INTENT")
    response.runtime_payload["intent_score"] = context.get("intent_score", 0)
    response.runtime_payload["intent_reasons"] = context.get("intent_reasons", [])
    response.runtime_payload["important_missing_fields"] = context.get("important_missing_fields", [])
    lead_summary = dict(context.get("internal_lead_summary") or {})
    if lead_summary:
        lead_summary["meeting_status"] = cta_state.get("meeting_status", lead_summary.get("meeting_status", "not_suggested"))
        lead_summary["meeting_suggested_count"] = cta_state.get(
            "meeting_suggested_count",
            lead_summary.get("meeting_suggested_count", 0),
        )
        if lead_summary["meeting_status"] == "booked":
            lead_summary["qualification_level"] = "qualified_booked"
        lead_summary["recommended_follow_up"] = _recommended_follow_up(
            intent_level=str(context.get("intent_level") or lead_summary.get("intent_level") or "LOW_INTENT"),
            qualification_level=str(lead_summary.get("qualification_level") or "qualifying"),
            meeting_status=str(lead_summary.get("meeting_status") or "not_suggested"),
            missing_fields=context.get("important_missing_fields", []),
            cta_state=cta_state,
        )
    response.runtime_payload["lead_summary"] = lead_summary
    recommended_follow_up = lead_summary.get("recommended_follow_up")
    if recommended_follow_up:
        response.runtime_payload["recommended_follow_up"] = recommended_follow_up


def _finalize_response_with_context(response: AgentResponse, context: dict[str, Any]) -> AgentResponse:
    _attach_behavior_runtime(response, context)
    return _finalize_response(response)


def _answers_blob(answers: dict[str, Any]) -> str:
    return " ".join(f"{key} {_stringify_answer(value)}" for key, value in answers.items()).strip()


def _answers_have_signal(
    answers: dict[str, Any],
    *,
    key_tokens: Sequence[str] = (),
    value_tokens: Sequence[str] = (),
) -> bool:
    for key, value in answers.items():
        key_norm = _normalize_text(key)
        value_norm = _normalize_text(_stringify_answer(value))
        if any(_normalize_text(token) in key_norm for token in key_tokens):
            return True
        if value_tokens and any(_normalize_text(token) in value_norm for token in value_tokens):
            return True
    return False


def _first_answer_by_tokens(answers: dict[str, Any], tokens: Sequence[str]) -> str | None:
    for key, value in answers.items():
        key_norm = _normalize_text(key)
        if any(_normalize_text(token) in key_norm for token in tokens):
            text = _stringify_answer(value)
            if text:
                return text
    return None


def _timeline_is_urgent(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    if any(phrase in normalized for phrase in ("no rush", "not urgent", "flexible", "no hurry", "whenever")):
        return False
    return bool(_TIMELINE_PATTERN.search(normalized)) or any(
        phrase in normalized
        for phrase in (
            "as soon as possible",
            "asap",
            "urgent",
            "right away",
            "soon",
            "rush",
        )
    )


def _lead_asked_question(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    if "?" in str(text or "") or _PRICING_PATTERN.search(normalized):
        return True
    return normalized.startswith(
        (
            "what ",
            "how ",
            "when ",
            "where ",
            "why ",
            "who ",
            "do ",
            "does ",
            "did ",
            "can ",
            "could ",
            "is ",
            "are ",
            "will ",
            "would ",
        )
    )


def _message_suggests_meeting(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    return bool(_MEETING_CTA_PATTERN.search(normalized))


def _count_meeting_suggestions(history: Sequence[Message]) -> int:
    count = 0
    for message in history:
        if message.direction == MessageDirection.OUTBOUND and _message_suggests_meeting(message.body or ""):
            count += 1
    return count


def _latest_outbound_from_history(history: Sequence[Message]) -> dict[str, Any] | None:
    for message in reversed(history):
        if message.direction != MessageDirection.OUTBOUND:
            continue
        return {
            "body": " ".join(str(message.body or "").split()).strip(),
            "raw_payload": message.raw_payload if isinstance(message.raw_payload, dict) else {},
        }
    return None


def _cta_label_for_text(text: str) -> str | None:
    normalized = _normalize_text(text)
    if not normalized:
        return None
    if "availability" in normalized or "times" in normalized or "calendar" in normalized:
        return "availability_offer"
    if "call" in normalized:
        return "call_suggestion"
    if "meeting" in normalized or "appointment" in normalized:
        return "meeting_suggestion"
    if "connect" in normalized or "coordinate" in normalized:
        return "connect_with_team"
    return "next_step_suggestion"


def _strip_meeting_cta(text: str, *, fallback: str) -> str:
    clean = " ".join(str(text or "").split()).strip()
    if not clean:
        return fallback
    parts = re.split(r"(?<=[.!?])\s+", clean)
    kept = [part.strip() for part in parts if part.strip() and not _message_suggests_meeting(part)]
    stripped = " ".join(kept).strip()
    return stripped or fallback


def _non_booking_bridge_reply(context: dict[str, Any]) -> str:
    if context.get("call_refusal") or (context.get("cta_state") or {}).get("meeting_rejected"):
        return "No problem. I can keep helping here instead. What would you like to understand next?"
    if context.get("pricing_question"):
        return "Pricing usually depends on scope, site details, deliverables, timing, and travel. I can help narrow the factors if you share what you are comparing against."
    if str(context.get("intent_level") or "") == "LOW_INTENT":
        return "No problem. I can help you get a general idea first. Are you mostly trying to understand pricing, process, timeline, or fit?"
    missing = context.get("recommended_missing_field")
    if isinstance(missing, dict) and missing.get("question"):
        return str(missing["question"])
    return "That makes sense. What would be most helpful to clarify first?"


def _stringify_answer(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value if str(item).strip())
    return " ".join(str(value or "").split()).strip()


def _humanize_key(key: str) -> str:
    return str(key or "").replace("_", " ").strip().title()


def _extract_from_form_answers(answers: dict[str, Any]) -> QualificationMemory:
    extracted: dict[str, Any] = {}
    for key, value in answers.items():
        key_norm = _normalize_text(key)
        text = " ".join(str(value or "").split()).strip()
        if not text:
            continue
        if any(token in key_norm for token in ("service", "problem", "scope", "need", "project", "goal")):
            extracted.setdefault("service_needed", text)
        if any(token in key_norm for token in ("timeline", "start", "deadline", "urgency", "date")):
            extracted.setdefault("timeline", text)
            extracted.setdefault("urgency_driver", text)
        if any(token in key_norm for token in ("location", "market", "region", "city", "site", "address")):
            extracted.setdefault("locations", text)
        if any(token in key_norm for token in ("budget", "spend", "price", "range")):
            extracted.setdefault("budget_range", text)
        if any(token in key_norm for token in ("decision", "approv", "stakeholder", "attendee", "join", "role")):
            extracted.setdefault("decision_makers", text)
        partial = _extract_from_text(text)
        extracted.update({k: v for k, v in partial.model_dump(exclude_none=True).items() if v not in (None, "")})
    return QualificationMemory.model_validate(extracted)


def _extract_from_messages(history: Sequence[Message]) -> QualificationMemory:
    extracted: dict[str, Any] = {}
    for message in history:
        if message.direction != MessageDirection.INBOUND:
            continue
        partial = _extract_from_text(message.body)
        extracted.update({k: v for k, v in partial.model_dump(exclude_none=True).items() if v is not None})
    return QualificationMemory.model_validate(extracted)


def _extract_from_text(text: str) -> QualificationMemory:
    raw = " ".join(str(text or "").split()).strip()
    normalized = _normalize_text(raw)
    if not normalized:
        return QualificationMemory()
    extracted: dict[str, Any] = {}
    timeline = _extract_timeline(raw)
    if timeline:
        extracted["timeline"] = timeline
        extracted["urgency_driver"] = timeline
    service_needed = _extract_service_needed(raw)
    if service_needed:
        extracted["service_needed"] = service_needed
    if _DECISION_MAKER_PATTERN.search(raw):
        extracted["decision_makers"] = raw
    if _has_booking_intent(raw):
        extracted["booking_intent_locked"] = True
    return QualificationMemory.model_validate(extracted)


def _extract_timeline(text: str) -> str | None:
    match = _TIMELINE_PATTERN.search(text)
    if not match:
        return None
    return " ".join(match.group(0).split())


def _extract_service_needed(text: str) -> str | None:
    normalized = _normalize_text(text)
    patterns = (
        r"\b(?:we need|i need|looking for|want help with|need help with)\s+(.+)$",
        r"\b(?:project is|scope is)\s+(.+)$",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized)
        if match:
            snippet = match.group(1).strip(" .,!;")
            if len(snippet) >= 4:
                return snippet[:160]
    return None


def _merge_memory(*memories: QualificationMemory) -> QualificationMemory:
    merged: dict[str, Any] = {}
    for memory in memories:
        for key, value in memory.model_dump(exclude_none=True).items():
            if value is None:
                continue
            merged[key] = value
    return QualificationMemory.model_validate(merged)


def _memory_has_value(memory: QualificationMemory, key: QuestionKey) -> bool:
    value = getattr(memory, key)
    return value is not None and value != ""


def _booking_threshold(*, memory: QualificationMemory) -> tuple[bool, list[str]]:
    qualifiers_ready = _memory_has_value(memory, "decision_makers") and _memory_has_value(memory, "urgency_driver")
    booking_ready = bool(memory.booking_intent_locked) or qualifiers_ready
    missing: list[str] = []
    if not _memory_has_value(memory, "decision_makers"):
        missing.append("decision_makers")
    if not _memory_has_value(memory, "urgency_driver"):
        missing.append("urgency_driver")
    return booking_ready, missing


def _extract_asked_question_keys(history: Sequence[Message]) -> list[QuestionKey]:
    keys: list[QuestionKey] = []
    for message in history:
        if message.direction != MessageDirection.OUTBOUND:
            continue
        raw_payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
        agent_payload = raw_payload.get("agent") if isinstance(raw_payload.get("agent"), dict) else {}
        stored_key = agent_payload.get("next_question_key")
        if stored_key in _QUESTION_SPEC_BY_KEY:
            keys.append(stored_key)
            continue
        guessed = _infer_question_key_from_text(message.body or "")
        if guessed:
            keys.append(guessed)
    return keys


def _infer_question_key_from_text(text: str) -> QuestionKey | None:
    normalized = _normalize_text(text)
    if "decision-maker" in normalized or "decision maker" in normalized or "anyone else join" in normalized:
        return "decision_makers"
    if "deadline" in normalized or "key date" in normalized or "approval timeline" in normalized or "driving this" in normalized:
        return "urgency_driver"
    return None


def _recommended_next_question_key(*, memory: QualificationMemory, asked_question_keys: Sequence[str]) -> QuestionKey | None:
    asked = {str(key) for key in asked_question_keys}
    for key in _QUESTION_ORDER:
        if key in asked:
            continue
        if _memory_has_value(memory, key):
            continue
        return key
    return None


def _normalize_text(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "").strip().lower())
    cleaned = re.sub(r"[^a-z0-9@+:/?.,\- ]", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _append_question(text: str, question: str) -> str:
    base = " ".join(str(text or "").split()).strip()
    question = question.strip()
    if not base:
        return question
    if base.endswith("?"):
        return base
    return f"{base} {question}".strip()


def _replace_question(text: str, question: str) -> str:
    base = " ".join(str(text or "").split()).strip()
    if "?" not in base:
        return _append_question(base, question)
    prefix = base.split("?", 1)[0]
    if "." in prefix:
        prefix = prefix.rsplit(".", 1)[0].strip()
    prefix = prefix.rstrip(" .")
    return f"{prefix}. {question}" if prefix else question


def _trim_sms_text(text: str) -> str:
    clean = " ".join(str(text or "").split()).strip()
    if len(clean) <= 320:
        return clean
    return clean[:317].rstrip() + "..."


def _ensure_single_question(text: str) -> str:
    clean = " ".join(str(text or "").split()).strip()
    if clean.count("?") <= 1:
        return clean
    first = clean.find("?")
    return f"{clean[: first + 1]} {clean[first + 1 :].replace('?', '.') }".strip()


def _finalize_response(response: AgentResponse) -> AgentResponse:
    response.reply_text = _trim_sms_text(_ensure_single_question(response.reply_text))
    if response.action != "ask_next_question":
        response.next_question_key = None
    return response


def _action_to_legacy(action: ActionType, next_question_key: QuestionKey | None, runtime_payload: dict[str, Any]) -> list[AgentAction]:
    if runtime_payload.get("booking_offer"):
        return [AgentAction(type="offer_calendar_slots", payload={})]
    if action == "ask_next_question":
        payload = {"question_key": next_question_key} if next_question_key else {}
        return [AgentAction(type="request_more_info", payload=payload)]
    if action == "handoff_to_human":
        return [AgentAction(type="handoff_to_human", payload={})]
    return []


def _has_booking_intent(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    if _BOOKING_INTENT_PATTERN.search(normalized):
        return True
    return any(
        phrase in normalized
        for phrase in (
            "available next",
            "what are your availabilities",
            "what availability",
            "can we schedule",
            "book a meeting",
            "book a call",
            "set a call",
            "send me times",
        )
    )


def _extract_slot_choice(inbound_text: str, latest_offer: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(latest_offer, dict):
        return None
    slots = latest_offer.get("slots")
    if not isinstance(slots, list) or not slots:
        return None
    normalized = _normalize_text(inbound_text)
    if not normalized:
        return None
    numeric_choice = re.search(r"\b([1-3])\b", normalized)
    if numeric_choice:
        return {"slot_index": int(numeric_choice.group(1))}
    for slot in slots:
        blob = _normalize_text(str(slot.get("search_blob", "")))
        start_time = str(slot.get("start_time", "")).strip()
        if blob and any(part.strip() and part.strip() in normalized for part in blob.split("|")):
            return {"slot_start_time": start_time} if start_time else {}
    time_match = re.search(r"\b(\d{1,2}(?::\d{2})?\s?(?:am|pm))\b", normalized)
    if time_match:
        for slot in slots:
            haystack = _normalize_text(
                " ".join(
                    [
                        str(slot.get("display_time", "")),
                        str(slot.get("display_hint", "")),
                        str(slot.get("search_blob", "")),
                    ]
                )
            )
            if _normalize_text(time_match.group(1)) in haystack:
                start_time = str(slot.get("start_time", "")).strip()
                return {"slot_start_time": start_time} if start_time else {}
    return None


def _normalize_optional_string(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _extract_booking_preferences(text: str) -> dict[str, str]:
    normalized = _normalize_text(text)
    if not normalized:
        return {}

    preferences: dict[str, str] = {}
    day_name = next((day for day in _DAY_NAMES if day in normalized), None)
    unavailable = any(
        phrase in normalized
        for phrase in (
            "not available",
            "dont work",
            "doesnt work",
            "can't do",
            "cant do",
            "won't work",
            "wont work",
            "not free",
        )
    )
    if day_name:
        if unavailable:
            preferences["avoid_day"] = day_name
        else:
            preferences["preferred_day"] = day_name

    date_match = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", normalized)
    if date_match and "preferred_day" not in preferences and "avoid_day" not in preferences:
        inferred_day = _normalize_requested_day(date_match.group(1))
        if inferred_day:
            preferences["preferred_day"] = inferred_day

    if "morning" in normalized:
        preferences["preferred_period"] = "morning"
    elif "afternoon" in normalized:
        preferences["preferred_period"] = "afternoon"
    elif "evening" in normalized:
        preferences["preferred_period"] = "evening"

    range_match = re.search(
        r"\b(?:between|from)\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\s+(?:and|to)\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\b",
        normalized,
    )
    if range_match:
        start_raw = range_match.group(1).strip()
        end_raw = range_match.group(2).strip()
        range_pair = _normalize_time_range(start_raw, end_raw)
        if range_pair:
            preferences["range_start"], preferences["range_end"] = range_pair

    time_match = re.search(r"\b(\d{1,2}(?::\d{2})?\s?(?:am|pm))\b", normalized)
    if time_match and "range_start" not in preferences:
        preferences["exact_time"] = time_match.group(1)

    return preferences


def _latest_outbound_text(recent_messages: Any) -> str | None:
    if not isinstance(recent_messages, list):
        return None
    for message in reversed(recent_messages):
        if not isinstance(message, dict):
            continue
        if str(message.get("direction", "")).strip().lower() != MessageDirection.OUTBOUND.value:
            continue
        text = " ".join(str(message.get("body", "")).split()).strip()
        if text:
            return text
    return None


def _normalize_requested_day(value: str | None) -> str | None:
    text = _normalize_text(value or "")
    if not text:
        return None
    for day in _DAY_NAMES:
        if day in text:
            return day
    if re.fullmatch(r"20\d{2}-\d{2}-\d{2}", text):
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d")
        except ValueError:
            return None
        return _DAY_NAMES[parsed.weekday()]
    return None


def _normalize_time_range(start_raw: str, end_raw: str) -> tuple[str, str] | None:
    start_match = re.search(r"^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$", start_raw)
    end_match = re.search(r"^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$", end_raw)
    if not start_match or not end_match:
        return None

    start_hour = int(start_match.group(1))
    start_minute = int(start_match.group(2) or "0")
    start_meridiem = (start_match.group(3) or "").strip()

    end_hour = int(end_match.group(1))
    end_minute = int(end_match.group(2) or "0")
    end_meridiem = (end_match.group(3) or "").strip()

    if not start_meridiem and end_meridiem:
        if end_meridiem == "pm" and start_hour > end_hour:
            start_meridiem = "am"
        else:
            start_meridiem = end_meridiem
    if not end_meridiem and start_meridiem:
        if start_meridiem == "am" and end_hour < start_hour:
            end_meridiem = "pm"
        else:
            end_meridiem = start_meridiem

    if not start_meridiem and not end_meridiem:
        if start_hour >= 8 and end_hour <= 6:
            start_meridiem = "am"
            end_meridiem = "pm"
        elif end_hour <= 11:
            start_meridiem = "am"
            end_meridiem = "am"
        else:
            start_meridiem = "pm"
            end_meridiem = "pm"

    start_time = f"{start_hour}:{start_minute:02d} {start_meridiem}"
    end_time = f"{end_hour}:{end_minute:02d} {end_meridiem}"
    return start_time, end_time


def _to_int(value: Any, *, default: int) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def build_llm_agent(settings: Settings, runtime_overrides: dict[str, str] | None = None) -> LLMAgentV3:
    effective = runtime_overrides or {}
    api_key = str(effective.get("openai_api_key", settings.openai_api_key) or "").strip()
    model = str(effective.get("openai_model", settings.openai_model) or settings.openai_model).strip()
    if api_key:
        provider: LLMProvider = _cached_openai_provider(
            api_key=api_key,
            model=model,
            timeout_seconds=settings.request_timeout_seconds,
        )
    else:
        provider = UnavailableLLMProvider()
    return LLMAgentV3(provider=provider)


LLMAgent = LLMAgentV3


__all__ = [
    "AgentAction",
    "AgentResponse",
    "LLMAgent",
    "LLMAgentV3",
    "LLMProvider",
    "OpenAIProvider",
    "QualificationMemory",
    "ToolCall",
    "build_llm_agent",
    "clear_llm_provider_cache",
]
