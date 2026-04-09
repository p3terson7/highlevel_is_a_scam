from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from openai import OpenAI
from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.core.config import Settings
from app.core.logging import get_logger
from app.db.models import Client, ConversationStateEnum, Lead, Message, MessageDirection
from app.services.lead_summary import build_lead_summary_text, normalize_form_answers

logger = get_logger(__name__)

QuestionKey = Literal[
    "locations_scope",
    "timeline",
    "deliverable_type",
    "building_type",
    "approximate_size_sqft",
    "decision_maker_role",
    "preferred_contact_method",
]
ActionType = Literal["none", "ask_next_question", "offer_booking", "send_booking_link", "mark_booked", "handoff_to_human"]

_ALLOWED_STATES = {
    ConversationStateEnum.QUALIFYING,
    ConversationStateEnum.BOOKING_SENT,
    ConversationStateEnum.BOOKED,
    ConversationStateEnum.HANDOFF,
}
_LEGACY_ACTION_TYPES = {"send_booking_link", "offer_calendar_slots", "request_more_info", "handoff_to_human"}
_BOOKING_INTENT_PATTERN = re.compile(
    r"\b(book|booking|schedule|scheduled|meeting|call|consult|availability|available times|next step)\b",
    re.IGNORECASE,
)
_BOOKED_CONFIRM_PATTERN = re.compile(
    r"\b(i booked|we booked|booked already|already booked|appointment booked|scheduled it|i scheduled)\b",
    re.IGNORECASE,
)
_HANDOFF_PATTERN = re.compile(r"\b(human|person|call me|someone from your team|manager|representative)\b", re.IGNORECASE)
_CLOSING_PATTERN = re.compile(r"^(thanks|thank you|ok|okay|cool|great|perfect|sounds good)[.! ]*$", re.IGNORECASE)
_SIZE_PATTERN = re.compile(r"\b(?P<size>\d{1,3}(?:,\d{3})+|\d{3,6})\s*(?:sq\.?\s*ft\.?|square feet|sf)\b", re.IGNORECASE)
_TIMELINE_PATTERN = re.compile(
    r"\b(asap|immediately|this week|next week|within \d+\s+(?:day|days|week|weeks|month|months)|\d+\s+(?:day|days|week|weeks|month|months))\b",
    re.IGNORECASE,
)
_ROLE_PATTERN = re.compile(
    r"\b(owner|architect|project manager|pm|facility manager|facilities manager|property manager|operations manager|director)\b",
    re.IGNORECASE,
)
_BUILDING_TYPES = (
    "retail space",
    "commercial office",
    "office",
    "retail",
    "warehouse",
    "industrial",
    "school",
    "hospital",
    "hotel",
    "mixed-use",
    "multifamily",
    "apartment",
    "condo",
    "residential",
    "commercial",
)
_BOOKING_REPLY_PATTERN = re.compile(
    r"\b(schedule|scheduled|booking link|book a quick call|book a time|book here|available times|calendar|send (?:you|over) (?:a|the) link|send over the next booking step|jump on a quick call|quick call)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class QuestionSpec:
    key: QuestionKey
    label: str
    question: str
    description: str


_QUESTION_SPECS: tuple[QuestionSpec, ...] = (
    QuestionSpec(
        key="locations_scope",
        label="Locations",
        question="Is this for one building or multiple locations?",
        description="Whether the project covers one site or multiple properties.",
    ),
    QuestionSpec(
        key="building_type",
        label="Building type",
        question="What kind of building is it?",
        description="The property or asset type for the documentation work.",
    ),
    QuestionSpec(
        key="approximate_size_sqft",
        label="Approximate size",
        question="Roughly how big is it in square feet?",
        description="Approximate size of the building or space.",
    ),
    QuestionSpec(
        key="deliverable_type",
        label="Deliverable",
        question="Do you need CAD as-builts, Revit/BIM, or both?",
        description="The deliverable the client needs from the survey or documentation work.",
    ),
    QuestionSpec(
        key="timeline",
        label="Timeline",
        question="What timeline are you working with?",
        description="The target date or urgency for receiving deliverables.",
    ),
    QuestionSpec(
        key="decision_maker_role",
        label="Decision-maker",
        question="Are you the main decision-maker for this project?",
        description="Whether the lead is the decision-maker and what role they hold.",
    ),
    QuestionSpec(
        key="preferred_contact_method",
        label="Contact method",
        question="If we need to coordinate details, do you prefer text, email, or a quick call?",
        description="The best follow-up channel if coordination is needed.",
    ),
)
_QUESTION_SPEC_BY_KEY = {spec.key: spec for spec in _QUESTION_SPECS}
_QUESTION_ORDER: tuple[QuestionKey, ...] = tuple(spec.key for spec in _QUESTION_SPECS)


class QualificationMemory(BaseModel):
    model_config = ConfigDict(extra="ignore")

    locations_scope: str | None = None
    timeline: str | None = None
    deliverable_type: str | None = None
    building_type: str | None = None
    approximate_size_sqft: int | None = None
    decision_maker_role: str | None = None
    preferred_contact_method: str | None = None

    def known_fields(self) -> dict[str, Any]:
        return self.model_dump(exclude_none=True)


class AgentAction(BaseModel):
    type: Literal["send_booking_link", "offer_calendar_slots", "request_more_info", "handoff_to_human"]
    payload: dict[str, Any] = Field(default_factory=dict)


class AgentResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    reply_text: str = ""
    next_state: ConversationStateEnum = ConversationStateEnum.QUALIFYING
    collected_fields: QualificationMemory = Field(default_factory=QualificationMemory)
    next_question_key: QuestionKey | None = None
    action: ActionType = "none"
    provider: Literal["openai", "fallback"] = "openai"
    provider_error: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_shape(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        raw = dict(data)
        raw.setdefault("collected_fields", {})
        if "action" not in raw:
            raw["action"] = _legacy_actions_to_action(raw.pop("actions", None))
        return raw

    @property
    def actions(self) -> list[AgentAction]:
        return _action_to_legacy(self.action, self.next_question_key)


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
            repair_system = (
                "Return valid JSON only. Match this exact schema: "
                '{"reply_text":"string","next_state":"QUALIFYING|BOOKING_SENT|BOOKED|HANDOFF",'
                '"collected_fields":{"locations_scope":"string|null","timeline":"string|null","deliverable_type":"string|null",'
                '"building_type":"string|null","approximate_size_sqft":"integer|null","decision_maker_role":"string|null",'
                '"preferred_contact_method":"string|null"},"next_question_key":"locations_scope|timeline|deliverable_type|building_type|approximate_size_sqft|decision_maker_role|preferred_contact_method|null",'
                '"action":"none|ask_next_question|offer_booking|send_booking_link|mark_booked|handoff_to_human"}'
            )
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
        context = self._build_context(client=client, lead=lead, inbound_text=inbound_text, history=history)
        system_prompt = self._build_system_prompt(client=client)
        user_prompt = json.dumps(context, ensure_ascii=False)

        try:
            raw = self._provider.generate_json(system_prompt=system_prompt, user_prompt=user_prompt)
            response = AgentResponse.model_validate(raw)
            response = self._sanitize_response(
                response=response,
                client=client,
                lead=lead,
                inbound_text=inbound_text,
                context=context,
            )
            response.provider = "openai"
            return response
        except Exception as exc:
            logger.exception("agent_v2_llm_failed", extra={"error": str(exc)})
            fallback = self._safe_fallback(client=client, context=context)
            fallback.provider = "fallback"
            fallback.provider_error = str(exc)
            return fallback

    def _build_system_prompt(self, *, client: Client) -> str:
        faq_context = (client.faq_context or "").strip() or "none provided"
        ai_context = (getattr(client, "ai_context", "") or "").strip() or "none provided"
        qualification_guide = "\n".join(
            f"- {spec.key}: {spec.description}. Ask with: {spec.question}" for spec in _QUESTION_SPECS
        )
        return (
            "You are a human-sounding SMS sales assistant writing on behalf of the business.\n"
            "The business provides building documentation, measured surveys, CAD as-builts, Revit/BIM models, and related deliverables.\n"
            "Your job is to understand the latest message, answer questions clearly, use the lead form and message history as trusted project context, and move the lead toward booking when it makes sense.\n"
            "Use faq_context as the source of truth for what the business does, what it delivers, and how it works.\n"
            "Use ai_context only as extra guidance for tone, positioning, and do/don't-say rules. It must not override the business domain, the lead's actual project details, or the qualification flow.\n"
            "If something is not in faq_context or clearly stated by the lead, do not invent it.\n"
            "Conversation rules:\n"
            "- Keep messages short and natural. Usually 1-2 short sentences.\n"
            "- Do not parrot the lead's wording back to them.\n"
            "- Do not use vague filler like 'What are you trying to solve?' if the lead already made that clear.\n"
            "- Answer direct service, process, pricing, or capability questions first, then continue the flow naturally.\n"
            "- Ask only one question at a time.\n"
            "- Treat lead_form_answers and qualification_memory as valid known information. Do not ask for those details again unless the lead contradicted them or one clarification is truly needed before the next step.\n"
            "- Never reuse a question key listed in asked_question_keys.\n"
            "- Do not include the actual booking URL in reply_text. If the next step is to ask whether they want to schedule, use action=offer_booking. If they are explicitly ready for the booking step now, use action=send_booking_link.\n"
            "- If the lead says they already booked, set action=mark_booked and stop qualifying.\n"
            "- If the current state is BOOKED and the latest message is only a short closing, reply with a short closing and no question.\n"
            "- If the lead gives a short affirmative reply right after you invited them to schedule, treat that as booking intent and move forward instead of asking again.\n"
            "- On the first outbound message, do not ask to confirm a field that is already present in qualification_memory or lead_form_answers.\n"
            "- Qualification questions are optional supporting tools, not a checklist. Ask one only when it genuinely helps you book the meeting or answer accurately.\n"
            "- During or after the booking flow, if the lead asks a question, answer it clearly first and then continue the next booking step without restarting the whole qualification flow.\n"
            "Helpful project details if still missing:\n"
            f"{qualification_guide}\n"
            "Booking rules:\n"
            "- Explicit booking intent is the clearest reason to switch to booking.\n"
            "- booking_threshold_met means the business already has enough context from the form, prior messages, or both to move to scheduling.\n"
            "- If booking_threshold_met is true, it is okay to move toward booking even if not every qualification field is filled.\n"
            "- On the very first outbound message, do not jump straight to scheduling unless the lead explicitly asked for it. Start with either one useful question or a concise helpful reply.\n"
            "- If booking sounds appropriate but the lead has not said yes yet, use action=offer_booking with a short scheduling question.\n"
            "- If pending_step is booking_approved_pending_qualification, the lead already said yes to booking. Do not ask for booking again. Either ask one final necessary clarification or use action=send_booking_link.\n"
            "- If booking is not appropriate yet, ask the single best missing detail that would help scope or coordinate the project.\n"
            "Output rules:\n"
            "- reply_text is the exact SMS to send.\n"
            "- next_state must be QUALIFYING, BOOKING_SENT, BOOKED, or HANDOFF.\n"
            "- collected_fields must contain all known structured qualification values you can confidently infer.\n"
            "- next_question_key should be null unless you are asking the next question.\n"
            "- action must be one of: none, ask_next_question, offer_booking, send_booking_link, mark_booked, handoff_to_human.\n"
            "Return strict JSON only with this exact schema:\n"
            '{"reply_text":"string","next_state":"QUALIFYING|BOOKING_SENT|BOOKED|HANDOFF","collected_fields":{"locations_scope":"string|null","timeline":"string|null","deliverable_type":"string|null","building_type":"string|null","approximate_size_sqft":"integer|null","decision_maker_role":"string|null","preferred_contact_method":"string|null"},"next_question_key":"locations_scope|timeline|deliverable_type|building_type|approximate_size_sqft|decision_maker_role|preferred_contact_method|null","action":"none|ask_next_question|offer_booking|send_booking_link|mark_booked|handoff_to_human"}\n'
            f"Business name: {client.business_name}\n"
            f"Tone target: {client.tone or 'clear, helpful, concise'}\n"
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

        recent_messages = [_serialize_message(message) for message in history[-10:]]
        initial_outreach = len(history) == 0
        last_outbound = _last_outbound_message(history)
        booking_invite_pending = _booking_invite_pending(last_outbound)
        affirmative_reply = is_affirmative_reply(inbound_text)
        asked_question_keys = _extract_asked_question_keys(history)
        missing_fields = [key for key in _QUESTION_ORDER if not _memory_has_value(memory, key)]
        conversation_memory = _merge_memory(history_memory, inbound_memory)
        recommended_next_question_key = _recommended_next_question_key(
            memory=memory,
            asked_question_keys=asked_question_keys,
        )
        conversation_threshold_met, booking_gap_fields = _booking_threshold(memory=memory)

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
            "last_outbound_message": last_outbound,
            "initial_outreach": initial_outreach,
            "current_state": lead.conversation_state.value if lead.conversation_state else ConversationStateEnum.NEW.value,
            "qualification_memory": memory.model_dump(exclude_none=True),
            "conversation_confirmed_fields": conversation_memory.model_dump(exclude_none=True),
            "conversation_confirmation_count": len(conversation_memory.model_dump(exclude_none=True)),
            "asked_question_keys": asked_question_keys,
            "asked_question_count": len(asked_question_keys),
            "missing_fields": missing_fields,
            "recommended_next_question_key": recommended_next_question_key,
            "booking_threshold_met": conversation_threshold_met,
            "booking_gap_fields": booking_gap_fields,
            "pending_step": str((lead.raw_payload or {}).get("pending_step") or "").strip() or None,
            "booking_invite_pending": booking_invite_pending,
            "affirmative_reply": affirmative_reply,
            "explicit_booking_intent": bool(_BOOKING_INTENT_PATTERN.search(inbound_text or "")),
            "booked_confirmation_intent": bool(_BOOKED_CONFIRM_PATTERN.search(inbound_text or "")),
            "handoff_intent": bool(_HANDOFF_PATTERN.search(inbound_text or "")),
            "closing_only": bool(_CLOSING_PATTERN.match((inbound_text or "").strip())),
            "question_specs": {
                spec.key: {
                    "label": spec.label,
                    "question": spec.question,
                    "description": spec.description,
                }
                for spec in _QUESTION_SPECS
            },
        }

    def _sanitize_response(
        self,
        *,
        response: AgentResponse,
        client: Client,
        lead: Lead,
        inbound_text: str,
        context: dict[str, Any],
    ) -> AgentResponse:
        current_state = str(context.get("current_state") or "").upper()
        explicit_booking_intent = bool(context.get("explicit_booking_intent"))
        pending_step = str(context.get("pending_step") or "").strip()
        booking_already_approved = pending_step == "booking_approved_pending_qualification"
        booking_threshold_met = bool(context.get("booking_threshold_met"))
        initial_outreach = bool(context.get("initial_outreach"))
        asked_question_keys = {str(key) for key in context.get("asked_question_keys", [])}
        coerced_back_to_qualification = False
        merged_memory = _merge_memory(
            QualificationMemory.model_validate(context.get("qualification_memory") or {}),
            response.collected_fields,
        )

        response.collected_fields = merged_memory
        response.reply_text = _trim_sms_text(_strip_urls(response.reply_text))
        if response.next_state not in _ALLOWED_STATES:
            response.next_state = ConversationStateEnum.QUALIFYING

        if current_state == ConversationStateEnum.BOOKED.value and bool(context.get("closing_only")):
            response.action = "none"
            response.next_state = ConversationStateEnum.BOOKED
            response.next_question_key = None
            response.reply_text = response.reply_text or "Perfect. See you then."
            return _finalize_response(response)

        if response.action == "handoff_to_human":
            response.next_state = ConversationStateEnum.HANDOFF
            response.next_question_key = None
            response.reply_text = response.reply_text or "Understood. I'll have someone from the team reach out."
            return _finalize_response(response)

        if response.action == "mark_booked":
            response.next_state = ConversationStateEnum.BOOKED
            response.next_question_key = None
            response.reply_text = response.reply_text or "Perfect. You're booked."
            return _finalize_response(response)

        if response.action == "offer_booking":
            if booking_already_approved:
                if booking_threshold_met:
                    response.action = "send_booking_link"
                    response.next_state = ConversationStateEnum.BOOKING_SENT
                    response.next_question_key = None
                    if not response.reply_text or "?" in response.reply_text:
                        response.reply_text = "Perfect. I'll send over the next booking step."
                else:
                    response.action = "ask_next_question"
                    response.next_state = ConversationStateEnum.QUALIFYING
                    response.reply_text = ""
                    coerced_back_to_qualification = True
            else:
                response.next_state = ConversationStateEnum.QUALIFYING
                response.next_question_key = None
                response.reply_text = response.reply_text or "If you'd like, I can send over the next booking step."
                return _finalize_response(response)

        if response.action == "send_booking_link":
            if explicit_booking_intent or booking_threshold_met or booking_already_approved:
                response.next_state = ConversationStateEnum.BOOKING_SENT
                response.next_question_key = None
                response.reply_text = response.reply_text or "That gives me enough to move this forward."
            else:
                response.action = "ask_next_question"
                response.next_state = ConversationStateEnum.QUALIFYING
                response.reply_text = ""
                coerced_back_to_qualification = True

        if booking_already_approved and booking_threshold_met and response.action in {"ask_next_question", "none"}:
            response.action = "send_booking_link"
            response.next_state = ConversationStateEnum.BOOKING_SENT
            response.next_question_key = None
            response.reply_text = "Perfect. I'll send over the next booking step."

        if response.action == "ask_next_question":
            original_key = response.next_question_key
            next_key = response.next_question_key
            if (
                initial_outreach
                and next_key in _QUESTION_SPEC_BY_KEY
                and _memory_has_value(merged_memory, next_key)
            ):
                next_key = None
            if next_key not in _QUESTION_SPEC_BY_KEY or next_key in asked_question_keys:
                next_key = _recommended_next_question_key(
                    memory=merged_memory,
                    asked_question_keys=list(asked_question_keys),
                )
            response.next_question_key = next_key
            response.next_state = ConversationStateEnum.QUALIFYING
            if coerced_back_to_qualification or (booking_already_approved and _reply_mentions_booking(response.reply_text)):
                response.reply_text = ""
            if response.next_question_key and (response.reply_text.count("?") == 0 or response.next_question_key != original_key):
                question = _QUESTION_SPEC_BY_KEY[response.next_question_key].question
                if response.reply_text.count("?") > 0:
                    response.reply_text = _replace_question(response.reply_text, question)
                else:
                    response.reply_text = _append_question(response.reply_text, question)
            elif not response.reply_text and response.next_question_key:
                response.reply_text = _QUESTION_SPEC_BY_KEY[response.next_question_key].question
            elif response.next_question_key is None:
                response.action = "none"
        else:
            response.next_question_key = None

        if current_state == ConversationStateEnum.BOOKED.value:
            response.next_state = ConversationStateEnum.BOOKED
            response.action = "none"
            response.next_question_key = None
            response.reply_text = response.reply_text or "You're all set."
            if "?" in response.reply_text:
                response.reply_text = "You're all set."

        if not response.reply_text:
            if response.action == "ask_next_question" and response.next_question_key:
                response.reply_text = _QUESTION_SPEC_BY_KEY[response.next_question_key].question
            elif response.action == "send_booking_link":
                response.reply_text = "I can send over the booking link."
            else:
                response.reply_text = "Understood."

        return _finalize_response(response)

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
        fallback = "Got it. I can send over the next step when you're ready."
        if client.booking_url:
            fallback = "Got it. I can send over the booking link when you're ready."
        return AgentResponse(
            reply_text=fallback,
            next_state=ConversationStateEnum.QUALIFYING,
            collected_fields=QualificationMemory.model_validate(context.get("qualification_memory") or {}),
            action="none",
        )


def _legacy_actions_to_action(actions: Any) -> ActionType:
    if not isinstance(actions, list) or not actions:
        return "none"
    first = actions[0]
    if not isinstance(first, dict):
        return "none"
    action_type = str(first.get("type") or "").strip()
    if action_type in {"send_booking_link", "offer_calendar_slots"}:
        return "send_booking_link"
    if action_type == "request_more_info":
        return "ask_next_question"
    if action_type == "handoff_to_human":
        return "handoff_to_human"
    return "none"


def _action_to_legacy(action: ActionType, next_question_key: QuestionKey | None) -> list[AgentAction]:
    if action == "send_booking_link":
        return [AgentAction(type="send_booking_link", payload={})]
    if action == "ask_next_question":
        payload = {"question_key": next_question_key} if next_question_key else {}
        return [AgentAction(type="request_more_info", payload=payload)]
    if action == "offer_booking":
        return []
    if action == "handoff_to_human":
        return [AgentAction(type="handoff_to_human", payload={})]
    return []


def _serialize_message(message: Message) -> dict[str, Any]:
    raw_payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
    agent_payload = raw_payload.get("agent") if isinstance(raw_payload.get("agent"), dict) else {}
    return {
        "direction": message.direction.value if isinstance(message.direction, MessageDirection) else str(message.direction),
        "body": " ".join(str(message.body or "").split()),
        "question_key": agent_payload.get("next_question_key"),
        "agent_action": agent_payload.get("action"),
    }


def _last_outbound_message(history: Sequence[Message]) -> dict[str, Any] | None:
    for message in reversed(history):
        if message.direction == MessageDirection.OUTBOUND:
            return _serialize_message(message)
    return None


def _booking_invite_pending(last_outbound: dict[str, Any] | None) -> bool:
    if not isinstance(last_outbound, dict):
        return False
    if str(last_outbound.get("agent_action") or "").strip() == "send_booking_link":
        return True
    body = _normalize_text(last_outbound.get("body") or "")
    if not body:
        return False
    booking_signals = (
        "interested",
        "want to schedule",
        "want me to send",
        "available times",
        "book a quick call",
        "jump on a quick call",
        "schedule that",
        "set something up",
        "next step",
    )
    return any(signal in body for signal in booking_signals)


def _extract_from_form_answers(answers: dict[str, Any]) -> QualificationMemory:
    extracted: dict[str, Any] = {}
    for key, value in answers.items():
        key_norm = _normalize_text(key)
        text = " ".join(str(value or "").split()).strip()
        if not text:
            continue
        partial = _extract_from_text(text)
        extracted.update({k: v for k, v in partial.model_dump(exclude_none=True).items() if v is not None})
        if any(token in key_norm for token in ("deliverable", "cad", "revit", "bim", "as built")):
            deliverable = _extract_deliverable_type(text)
            if deliverable:
                extracted["deliverable_type"] = deliverable
        if any(token in key_norm for token in ("location", "building count", "site", "scope")) and "locations_scope" not in extracted:
            scope = _extract_locations_scope(text)
            if scope:
                extracted["locations_scope"] = scope
        if any(token in key_norm for token in ("building type", "property type", "asset type")) and "building_type" not in extracted:
            building_type = _extract_building_type(text)
            if building_type:
                extracted["building_type"] = building_type
        if any(token in key_norm for token in ("size", "sqft", "square feet")) and "approximate_size_sqft" not in extracted:
            size = _extract_size_sqft(text)
            if size:
                extracted["approximate_size_sqft"] = size
        if any(token in key_norm for token in ("decision", "role")) and "decision_maker_role" not in extracted:
            role = _extract_role(text)
            if role:
                extracted["decision_maker_role"] = role
        if any(token in key_norm for token in ("contact", "email", "phone", "text")) and "preferred_contact_method" not in extracted:
            method = _extract_contact_method(text)
            if method:
                extracted["preferred_contact_method"] = method
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
    if not normalized or "?" in raw:
        return QualificationMemory()

    extracted: dict[str, Any] = {}

    scope = _extract_locations_scope(raw)
    if scope:
        extracted["locations_scope"] = scope

    timeline = _extract_timeline(raw)
    if timeline:
        extracted["timeline"] = timeline

    deliverable = _extract_deliverable_type(raw)
    if deliverable:
        extracted["deliverable_type"] = deliverable

    building_type = _extract_building_type(raw)
    if building_type:
        extracted["building_type"] = building_type

    size = _extract_size_sqft(raw)
    if size:
        extracted["approximate_size_sqft"] = size

    role = _extract_role(raw)
    if role:
        extracted["decision_maker_role"] = role

    contact_method = _extract_contact_method(raw)
    if contact_method:
        extracted["preferred_contact_method"] = contact_method

    return QualificationMemory.model_validate(extracted)


def _extract_locations_scope(text: str) -> str | None:
    normalized = _normalize_text(text)
    if re.search(r"\b(one|single)\s+(?:[a-z]+\s+){0,2}(building|site|location|property|space|office|warehouse|store)\b", normalized):
        return "one building"
    if re.search(r"\b(multiple|multi|several|many|\d+\s+)\s+(buildings|sites|locations|properties)\b", normalized):
        return "multiple locations"
    if "multiple sites" in normalized or "multiple locations" in normalized:
        return "multiple locations"
    return None


def _extract_timeline(text: str) -> str | None:
    match = _TIMELINE_PATTERN.search(text)
    if not match:
        return None
    return " ".join(match.group(0).split())


def _extract_deliverable_type(text: str) -> str | None:
    normalized = _normalize_text(text)
    cad = bool(re.search(r"\b(cad|as builts?|as-builts?)\b", normalized))
    revit = bool(re.search(r"\b(revit|bim)\b", normalized))
    if cad and revit:
        return "CAD as-builts and Revit/BIM"
    if cad:
        return "CAD as-builts"
    if revit:
        return "Revit/BIM"
    return None


def _extract_building_type(text: str) -> str | None:
    normalized = _normalize_text(text)
    for candidate in _BUILDING_TYPES:
        if candidate in normalized:
            return candidate
    match = re.search(r"\b(?:for|it's|it is|its)\s+(?:a|an)?\s*([a-z][a-z\- ]{3,30})\b", normalized)
    if match:
        snippet = match.group(1).strip()
        if any(token in snippet for token in ("building", "office", "retail", "warehouse", "school", "site", "space")):
            return snippet
    return None


def _extract_size_sqft(text: str) -> int | None:
    match = _SIZE_PATTERN.search(text)
    if not match:
        return None
    value = match.group("size").replace(",", "")
    try:
        return int(value)
    except ValueError:
        return None


def _extract_role(text: str) -> str | None:
    match = _ROLE_PATTERN.search(text)
    if not match:
        return None
    return match.group(1).strip()


def _extract_contact_method(text: str) -> str | None:
    normalized = _normalize_text(text)
    if "email" in normalized:
        return "email"
    if "call" in normalized or "phone" in normalized:
        return "call"
    if "text" in normalized or "sms" in normalized:
        return "text"
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


def _booking_threshold(
    *,
    memory: QualificationMemory,
) -> tuple[bool, list[str]]:
    scope_ready = any(
        _memory_has_value(memory, key)
        for key in ("locations_scope", "deliverable_type", "building_type", "approximate_size_sqft")
    )
    coordination_ready = any(
        _memory_has_value(memory, key)
        for key in ("timeline", "decision_maker_role", "preferred_contact_method")
    )
    known_fields = memory.known_fields()
    booking_ready = scope_ready and coordination_ready and len(known_fields) >= 3

    missing: list[str] = []
    if not scope_ready:
        missing.append("project_scope")
    if not coordination_ready:
        missing.append("coordination_signal")
    if len(known_fields) < 3:
        missing.append("project_detail")
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
    if "one building" in normalized or "multiple locations" in normalized:
        return "locations_scope"
    if "timeline" in normalized or "working with" in normalized or "how soon" in normalized:
        return "timeline"
    if "cad as-builts" in normalized or "revit" in normalized or "bim" in normalized:
        return "deliverable_type"
    if "kind of building" in normalized or "building type" in normalized:
        return "building_type"
    if "square feet" in normalized or "sqft" in normalized or "sq ft" in normalized:
        return "approximate_size_sqft"
    if "decision-maker" in normalized or "decision maker" in normalized:
        return "decision_maker_role"
    if "text, email, or a quick call" in normalized or "prefer text" in normalized:
        return "preferred_contact_method"
    return None


def _recommended_next_question_key(
    *,
    memory: QualificationMemory,
    asked_question_keys: Sequence[str],
) -> QuestionKey | None:
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


def _reply_mentions_booking(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    return bool(_BOOKING_REPLY_PATTERN.search(normalized))


def is_affirmative_reply(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    exact_phrases = {
        "yes",
        "yeah",
        "yea",
        "yep",
        "sure",
        "absolutely",
        "definitely",
        "sounds good",
        "that works",
        "works for me",
        "ok",
        "okay",
        "go ahead",
        "lets do it",
        "let s do it",
        "please do",
        "yea sure",
        "yeah sure",
        "yes sure",
    }
    if normalized in exact_phrases:
        return True
    tokens = [token for token in normalized.split(" ") if token]
    if 0 < len(tokens) <= 3 and all(token in {"yes", "yeah", "yea", "yep", "sure", "ok", "okay"} for token in tokens):
        return True
    return False


def _strip_urls(text: str) -> str:
    cleaned = re.sub(r"https?://\S+", " ", str(text or ""))
    cleaned = re.sub(r"\s+([,.:;!?])", r"\1", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip(" ")


def _append_question(text: str, question: str) -> str:
    base = " ".join(str(text or "").split()).strip()
    question = question.strip()
    if not base:
        return question
    if base.endswith("?"):
        return base
    if base.endswith("."):
        return f"{base} {question}"
    return f"{base} {question}"


def _replace_question(text: str, question: str) -> str:
    base = " ".join(str(text or "").split()).strip()
    if "?" not in base:
        return _append_question(base, question)
    prefix = base.split("?", 1)[0]
    if "." in prefix:
        prefix = prefix.rsplit(".", 1)[0].strip()
    prefix = prefix.rstrip(" .")
    if prefix:
        return f"{prefix}. {question}"
    return question


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
    return f"{clean[: first + 1]} {clean[first + 1 :].replace('?', '.')}".strip()


def _finalize_response(response: AgentResponse) -> AgentResponse:
    response.reply_text = _trim_sms_text(_ensure_single_question(response.reply_text))
    if response.action != "ask_next_question":
        response.next_question_key = None
    return response


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


LLMAgent = LLMAgentV2


__all__ = [
    "AgentAction",
    "AgentResponse",
    "LLMAgent",
    "LLMAgentV2",
    "LLMProvider",
    "OpenAIProvider",
    "QualificationMemory",
    "build_llm_agent",
    "is_affirmative_reply",
]
