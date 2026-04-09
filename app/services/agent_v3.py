from __future__ import annotations

import json
import re
from datetime import datetime
from collections.abc import Sequence
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
from app.services.lead_summary import build_lead_summary_text, normalize_form_answers

logger = get_logger(__name__)

QuestionKey = Literal[
    "decision_makers",
    "urgency_driver",
]
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
        context = self._build_context(client=client, lead=lead, inbound_text=inbound_text, history=history)
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
                return _finalize_response(decision)
            if tool_name == "mark_booked":
                decision.action = "mark_booked"
                decision.next_state = ConversationStateEnum.BOOKED
                decision.tool_call = ToolCall()
                return _finalize_response(decision)

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
            "You are a human-sounding SMS assistant for a client business. "
            "You help leads understand services, answer questions, qualify quickly, and book meetings.\n"
            "You stay in control of the conversation. The backend can only do simple tools when you request them.\n"
            "Use faq_context as the source of truth for services, deliverables, process, and pricing rules.\n"
            "Use ai_context only for tone and do/don't-say guidance. It must not override the actual lead details or the business domain.\n"
            "Conversation rules:\n"
            "- Be concise, human, and helpful. Usually 1-2 short sentences.\n"
            "- Answer the lead's question first if they asked one.\n"
            "- Ask at most one follow-up question.\n"
            "- By the second outbound message, mention that we can schedule a short meeting/call to move the project forward.\n"
            "- Treat lead_form_answers and qualification_memory as known facts. Do not ask for them again unless a clarification is genuinely needed.\n"
            "- Do not repeat a question in asked_question_keys.\n"
            "- Ask no more than 2 qualifying questions before offering times: decision-makers and urgency/driver.\n"
            "- If booking intent is clear (yes/sure/go ahead/book it/works for me), do not ask if they are interested again.\n"
            "- A booked lead can still ask questions. Keep helping.\n"
            "- Only call find_slots when the lead explicitly asks for times, shares their availability, or clearly says they want to schedule now.\n"
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
            "- If tool_result.kind is slots, present the options naturally.\n"
            "- If tool_result.kind is booked, confirm the booking clearly and briefly.\n"
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
    ) -> dict[str, Any]:
        normalized_answers = normalize_form_answers(lead.form_answers or {})
        prior_memory = QualificationMemory.model_validate((lead.raw_payload or {}).get("qualification_memory") or {})
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
        flow_state = str((lead.raw_payload or {}).get("flow_state") or "NEW").strip().upper()

        return {
            "business_name": client.business_name,
            "tone": client.tone,
            "faq_context": client.faq_context or "",
            "ai_context": getattr(client, "ai_context", "") or "",
            "lead_name": lead.full_name or "",
            "lead_phone": lead.phone or "",
            "lead_email": lead.email or "",
            "lead_city": lead.city or "",
            "lead_summary": build_lead_summary_text(normalized_answers, limit=8),
            "lead_form_answers": normalized_answers,
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
            "recommended_next_question_key": _recommended_next_question_key(memory=memory, asked_question_keys=asked_question_keys),
            "booking_ready": booking_ready,
            "booking_gap_fields": booking_gap_fields,
            "booking_intent_locked": bool(memory.booking_intent_locked),
            "booking_mode": booking_mode_label(client),
            "automated_booking_enabled": automated_booking_enabled(client),
            "booking_url": client.booking_url or "",
            "latest_booking_offer": latest_offer,
            "latest_inbound_booking_preferences": _extract_booking_preferences(inbound_text),
            "available_tools": ["find_slots", "book_slot", "mark_booked", "handoff_to_human"],
            "explicit_booking_intent": explicit_booking_intent,
            "booked_confirmation_intent": bool(_BOOKED_CONFIRM_PATTERN.search(inbound_text or "")),
            "handoff_intent": bool(_HANDOFF_PATTERN.search(inbound_text or "")),
            "closing_only": bool(_CLOSING_PATTERN.match((inbound_text or "").strip())),
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
            return _finalize_response(decision)

        if bool(context.get("booked_confirmation_intent")) and decision.tool_call.name == "none":
            decision.tool_call = ToolCall(name="mark_booked", args={})
            decision.action = "mark_booked"
            decision.next_state = ConversationStateEnum.BOOKED
            decision.next_question_key = None
            decision.reply_text = decision.reply_text or "Perfect. You're booked."
            return _finalize_response(decision)

        if bool(context.get("handoff_intent")) and decision.tool_call.name == "none":
            decision.tool_call = ToolCall(name="handoff_to_human", args={})
            decision.action = "handoff_to_human"
            decision.next_state = ConversationStateEnum.HANDOFF
            decision.next_question_key = None
            return _finalize_response(decision)

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
            return _finalize_response(decision)

        if decision.action == "ask_next_question":
            original_key = decision.next_question_key
            if decision.next_question_key not in _QUESTION_SPEC_BY_KEY or decision.next_question_key in set(context.get("asked_question_keys", [])):
                decision.next_question_key = _recommended_next_question_key(
                    memory=decision.collected_fields,
                    asked_question_keys=context.get("asked_question_keys", []),
                )
            if decision.next_question_key and (context.get("initial_outreach") and _memory_has_value(decision.collected_fields, decision.next_question_key)):
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
                    decision.reply_text = "Thanks for confirming. I can share live times whenever you're ready."

        return _finalize_response(decision)

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
        return _finalize_response(final)

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
        return _finalize_response(final)

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
        inferred_preferences = _extract_booking_preferences(str(context.get("latest_inbound_message") or ""))
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
            pending_step = "slot_selection_pending" if offer.slots else None
            return {
                "kind": "slots" if offer.slots else "no_slots",
                "slots": [slot.__dict__ for slot in offer.slots],
                "reply_hint": offer.reply_text,
                "availability_query": {
                    "preferred_day": preferred_day,
                    "avoid_day": avoid_day,
                    "preferred_period": preferred_period,
                    "exact_time": exact_time,
                    "range_start": range_start,
                    "range_end": range_end,
                },
                "runtime_payload": {
                    "booking_offer": offer.raw_payload.get("booking_offer", {}),
                    "pending_step": pending_step,
                },
                "fallback_reply": offer.reply_text,
            }
        if tool_call.name == "book_slot":
            latest_offer = _latest_booking_offer(history)
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
        next_key = _recommended_next_question_key(
            memory=QualificationMemory.model_validate(context.get("qualification_memory") or {}),
            asked_question_keys=context.get("asked_question_keys", []),
        )
        if next_key:
            reply = _QUESTION_SPEC_BY_KEY[next_key].question
            return AgentResponse(
                reply_text=reply,
                next_state=ConversationStateEnum.QUALIFYING,
                collected_fields=QualificationMemory.model_validate(context.get("qualification_memory") or {}),
                next_question_key=next_key,
                action="ask_next_question",
            )
        fallback = "Got it. I can pull up live times whenever you're ready."
        return AgentResponse(
            reply_text=fallback,
            next_state=ConversationStateEnum.QUALIFYING,
            collected_fields=QualificationMemory.model_validate(context.get("qualification_memory") or {}),
            action="none",
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


def _latest_booking_offer(history: Sequence[Message]) -> dict[str, Any] | None:
    for message in reversed(history):
        raw_payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
        offer = raw_payload.get("booking_offer")
        if isinstance(offer, dict) and isinstance(offer.get("slots"), list) and offer.get("slots"):
            return offer
    return None


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
        provider: LLMProvider = OpenAIProvider(
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
]
