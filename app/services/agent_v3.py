from __future__ import annotations

import json
import time
from collections.abc import Sequence
from functools import lru_cache
from typing import Any

from openai import OpenAI
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.core.logging import get_logger
from app.db.models import Client, ConversationStateEnum, Lead, Message, MessageDirection
from app.services.agent_v3_helpers import *
from app.services.agent_v3_types import *
from app.services.booking import BookingProviderError, BookingService, automated_booking_enabled, booking_mode_label
from app.services.knowledge import build_knowledge_context
from app.services.lead_summary import normalize_form_answers

logger = get_logger(__name__)


class OpenAIProvider:
    name = "openai"
    _retry_delays = (0.5, 1.5)

    def __init__(self, *, api_key: str, model: str, timeout_seconds: int = 20) -> None:
        self._client = OpenAI(api_key=api_key, timeout=timeout_seconds, max_retries=0)
        self._model = model

    def _complete(self, system_prompt: str, user_prompt: str) -> str:
        last_error: Exception | None = None
        for attempt in range(len(self._retry_delays) + 1):
            try:
                response = self._client.chat.completions.create(
                    model=self._model,
                    temperature=0.35,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                )
                break
            except Exception as exc:
                last_error = exc
                if attempt >= len(self._retry_delays) or not self._is_retryable_error(exc):
                    raise
                time.sleep(self._retry_delays[attempt])
        else:
            raise last_error or RuntimeError("OpenAI request failed")
        return response.choices[0].message.content or "{}"

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        status_code = getattr(exc, "status_code", None)
        if status_code in {408, 409, 429, 500, 502, 503, 504}:
            return True
        return type(exc).__name__ in {
            "APIConnectionError",
            "APITimeoutError",
            "RateLimitError",
            "InternalServerError",
        }

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
        if context.get("identity_question"):
            return _identity_agent_response(context)
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
            f"You are {_ASSISTANT_NAME}, an AI assistant for a client business. "
            "You help leads understand services, answer questions, qualify through conversation, and guide qualified leads to the right next step.\n"
            "You are not a generic meeting booker. The backend can only do simple tools when you request them.\n"
            "Identity policy:\n"
            f"- Your name is {_ASSISTANT_NAME}. You are an assistant, not the business owner, founder, employee, or human salesperson.\n"
            "- If asked who you are, say you are the assistant for the business and can help answer questions or book a meeting.\n"
            "- If asked who owns, founded, runs, or is behind the business, answer only from faq_context or knowledge_context; if it is not present, say you do not have confirmed founder/owner details in context.\n"
            "Use faq_context as the source of truth for services, offerings, and process rules.\n"
            "Use knowledge_context as source-backed website content for specific service, policy, and process facts.\n"
            "Use ai_context for business-provided positioning, tone, do/don't-say guidance, and pricing only when pricing_context_available is true.\n"
            "If neither faq_context nor knowledge_context answers a factual question, say what you can confirm and avoid inventing details.\n"
            "Lead context policy:\n"
            "- lead_form_answers, known_form_facts, and qualification_memory are already-known information from the form or prior conversation.\n"
            "- Never ask the lead to repeat a known fact such as scope, service, size, timeline, role, contact preference, or location unless they contradicted it.\n"
            "- acknowledged_form_fact_keys were already mentioned in an outbound reply; do not restate those facts again unless the lead asks or corrects them.\n"
            "- answered_missing_field_keys were already asked and answered; do not ask those same missing-field questions again.\n"
            f"- On the first outbound SMS, start with: \"Hi {{first_name}}, I'm {_ASSISTANT_NAME}, the assistant for {{business_name}}.\" Then acknowledge the inquiry and help toward an answer or booking.\n"
            "- On the first outbound SMS, naturally reference 1-3 important known form facts, then ask one useful missing question or, only for very high intent, offer the next step softly.\n"
            "- Use important_missing_fields and recommended_missing_field to choose the next useful question, but ask only one question at a time.\n"
            "Intent strategy:\n"
            "- intent_level is HIGH_INTENT, MEDIUM_INTENT, or LOW_INTENT and may change each turn based on form answers plus the latest message.\n"
            "- HIGH_INTENT: acknowledge the known request, ask only the most important missing item, and frame booking as a logical option, not pressure.\n"
            "- MEDIUM_INTENT: clarify and educate first. Suggest a meeting only after the need is clearer or timing/next-step intent appears.\n"
            "- LOW_INTENT: nurture and educate. Do not push a call; ask what they are trying to understand.\n"
            "Conversation rules:\n"
            "- Be concise, clear, natural, and helpful. Usually 1-2 short SMS-sized sentences.\n"
            "- If the lead asks a question, answer it first. Only after answering should you guide to a next step, and only if useful.\n"
            "- Ask at most one follow-up question. Never dump multiple intake questions.\n"
            "- Do not repeat a question in asked_question_keys.\n"
            "- Avoid repeating meeting CTAs. Respect cta_state.meeting_rejected and cta_state.suppress_meeting_cta.\n"
            "- If a meeting was suggested and the lead ignored it, continue helping instead of repeating the same call-to-action.\n"
            "- If the lead refuses a call, stop suggesting calls and keep answering questions.\n"
            "- Vary next-step language. Avoid overusing phrases like 'short scoping call' or 'book a short call'.\n"
            "- Never ask about budget, target budget, spend, or investment range.\n"
            "- Do not mention pricing, rates, cost, quotes, estimates, or budget unless pricing_context_available is true and the answer comes from ai_context.\n"
            "- Do not invent guarantees, deadlines, availability, or service details.\n"
            "- Do not impersonate a human, founder, owner, or employee. If the lead asks for a person, use handoff_to_human.\n"
            "- A booked lead can still ask questions. Keep helping without rebooking.\n"
            "Booking tool rules:\n"
            "- Only call find_slots when the lead explicitly asks for times, shares availability, chooses to schedule, or clearly says they want to book now.\n"
            "- If the lead asks about another day or another time, use the booking tools rather than repeating the same slots.\n"
            "- If the lead already has a booked meeting and asks to reschedule, use booking tools to find or confirm a replacement time.\n"
            "- Once the lead chooses a slot or exact available time, do not ask again whether they want to book; call book_slot.\n"
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
            f"- You are {_ASSISTANT_NAME}, the assistant for the business. Never write as the founder, owner, or a human employee.\n"
            "- Use the tool_result as the source of truth.\n"
            "- Never invent availability or claim a slot is unavailable unless tool_result.match_mode says the request could not be matched exactly.\n"
            "- Mention only times that exist in tool_result.slots.\n"
            "- Keep the tone human and concise.\n"
            "- Do not mention pricing, rates, cost, quotes, estimates, or budget unless conversation_context.pricing_context_available is true.\n"
            "- Make clear the availability is for a consultation or meeting unless the business context says otherwise.\n"
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
        ai_context = getattr(client, "ai_context", "") or ""
        pricing_context_available = _has_explicit_pricing_context(ai_context)
        prior_memory = QualificationMemory.model_validate(raw_payload.get("qualification_memory") or {})
        answer_memory = _extract_from_form_answers(normalized_answers)
        history_memory = _extract_from_messages(history)
        inbound_memory = _extract_from_text(inbound_text)
        memory = _merge_memory(prior_memory, answer_memory, history_memory, inbound_memory)
        recent_messages = [_serialize_message(message) for message in history[-20:]]
        asked_question_keys = _extract_asked_question_keys(history)
        latest_offer = _latest_booking_offer(history)
        answered_missing_field_keys = _extract_answered_missing_field_keys(history)
        explicit_booking_intent = _has_booking_intent(inbound_text)
        if explicit_booking_intent:
            memory.booking_intent_locked = True
        booking_ready, booking_gap_fields = _booking_threshold(memory=memory)
        flow_state = str(raw_payload.get("flow_state") or "NEW").strip().upper()
        inbound_preferences = _extract_booking_preferences(inbound_text)
        known_form_facts = _build_known_form_facts(normalized_answers, lead=lead)
        acknowledged_form_fact_keys = _extract_acknowledged_form_fact_keys(known_form_facts=known_form_facts, history=history)
        important_missing_fields = _important_missing_fields(
            answers=normalized_answers,
            memory=memory,
            lead=lead,
            answered_field_keys=answered_missing_field_keys,
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
            "ai_context": ai_context,
            "pricing_context_available": pricing_context_available,
            "knowledge_context": knowledge_context,
            "agent_identity": _agent_identity_context(client),
            "identity_question": _is_identity_question(inbound_text),
            "lead_name": lead.full_name or "",
            "lead_phone": lead.phone or "",
            "lead_email": lead.email or "",
            "lead_city": lead.city or "",
            "lead_summary": build_lead_summary_text(normalized_answers, limit=8),
            "lead_form_answers": normalized_answers,
            "known_form_facts": known_form_facts,
            "acknowledged_form_fact_keys": acknowledged_form_fact_keys,
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
            "answered_missing_field_keys": answered_missing_field_keys,
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
                pricing_context_available=pricing_context_available,
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
        inbound_text = str(context.get("latest_inbound_message") or "")
        inbound_preferences = context.get("latest_inbound_booking_preferences")
        slot_choice = _extract_slot_choice(
            inbound_text=inbound_text,
            latest_offer=context.get("latest_booking_offer"),
        )
        refreshed_time_preferences = _booking_preferences_with_offer_context(
            dict(inbound_preferences or {}),
            latest_offer=context.get("latest_booking_offer"),
        )
        reschedule_requested = bool(
            current_state == ConversationStateEnum.BOOKED.value
            and (
                slot_choice
                or inbound_preferences
                or context.get("explicit_booking_intent")
                or _RESCHEDULE_PATTERN.search(inbound_text or "")
            )
        )
        if current_state == ConversationStateEnum.BOOKED.value and bool(context.get("closing_only")):
            decision.reply_text = decision.reply_text or "Perfect. See you then."
            decision.next_state = ConversationStateEnum.BOOKED
            decision.action = "none"
            decision.next_question_key = None
            decision.tool_call = ToolCall()
            return _finalize_response_with_context(decision, context)

        if reschedule_requested and decision.tool_call.name in {"none", "mark_booked"}:
            if slot_choice:
                args: dict[str, Any] = {}
                if slot_choice.get("slot_index"):
                    args["slot_index"] = slot_choice["slot_index"]
                if slot_choice.get("slot_start_time"):
                    args["slot_start_time"] = slot_choice["slot_start_time"]
                decision.tool_call = ToolCall(name="book_slot", args=args)
            else:
                decision.tool_call = ToolCall(name="find_slots", args=refreshed_time_preferences)
            decision.action = "none"
            decision.next_state = ConversationStateEnum.BOOKING_SENT
            decision.next_question_key = None
            decision.runtime_payload["flow_state"] = "RESCHEDULING"

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

        if slot_choice and decision.tool_call.name == "none":
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
            if slot_choice:
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

        should_offer_slots_now = bool(context.get("explicit_booking_intent")) or bool(inbound_preferences)
        if reschedule_requested:
            should_offer_slots_now = True
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
        elif tool_result.get("kind") in {"slots", "no_slots"}:
            final.next_state = ConversationStateEnum.BOOKING_SENT
            final.action = "none"
            final.runtime_payload["flow_state"] = "WAITING_SLOT_CHOICE"
        else:
            final.action = "none"
        if str(context.get("current_state") or "").upper() == ConversationStateEnum.BOOKED.value and final.next_state == ConversationStateEnum.QUALIFYING:
            final.next_state = ConversationStateEnum.BOOKED
            final.runtime_payload["flow_state"] = "CONFIRMED"
        final.collected_fields = _merge_memory(decision.collected_fields, final.collected_fields)
        if not final.reply_text:
            final.reply_text = str(tool_result.get("fallback_reply") or decision.reply_text or "Understood.")
        if tool_result.get("kind") == "slots":
            final.reply_text = _ensure_slot_fallback_line(final.reply_text)
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
