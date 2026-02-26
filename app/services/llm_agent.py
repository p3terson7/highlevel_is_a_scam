from __future__ import annotations

import json
from typing import Any, Literal, Protocol, Sequence

from openai import OpenAI
from pydantic import BaseModel, Field

from app.core.config import Settings
from app.core.logging import get_logger
from app.db.models import Client, ConversationStateEnum, Lead, Message

logger = get_logger(__name__)


class AgentAction(BaseModel):
    type: Literal["send_booking_link", "request_more_info", "handoff_to_human"]
    payload: dict[str, Any] = Field(default_factory=dict)


class AgentResponse(BaseModel):
    reply_text: str
    next_state: ConversationStateEnum
    actions: list[AgentAction] = Field(default_factory=list)


class LLMProvider(Protocol):
    def generate_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        ...


class OpenAIProvider:
    def __init__(self, api_key: str, model: str) -> None:
        self._client = OpenAI(api_key=api_key)
        self._model = model

    def generate_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        response = self._client.chat.completions.create(
            model=self._model,
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        content = response.choices[0].message.content or "{}"
        parsed = json.loads(content)
        if not isinstance(parsed, dict):
            raise ValueError("LLM response is not a JSON object")
        return parsed


class HeuristicProvider:
    def generate_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        _ = system_prompt
        context = json.loads(user_prompt)
        inbound = str(context.get("latest_inbound_message", "")).lower()
        booking_url = str(context.get("booking_url", "")).strip()
        questions = context.get("qualification_questions", [])
        state = str(context.get("state", "NEW"))

        if any(token in inbound for token in ["book", "schedule", "call", "meeting", "time"]):
            return {
                "reply_text": "Great. Please pick a time that works for you.",
                "next_state": "BOOKING_SENT",
                "actions": [{"type": "send_booking_link", "payload": {"booking_url": booking_url}}],
            }

        if state in {"NEW", "GREETED"} and questions:
            return {
                "reply_text": str(questions[0]),
                "next_state": "QUALIFYING",
                "actions": [{"type": "request_more_info", "payload": {}}],
            }

        return {
            "reply_text": "Thanks for sharing. Want me to send a booking link so you can choose a time?",
            "next_state": "QUALIFYING",
            "actions": [{"type": "request_more_info", "payload": {}}],
        }


class LLMAgent:
    def __init__(self, provider: LLMProvider) -> None:
        self._provider = provider

    def next_reply(
        self,
        client: Client,
        lead: Lead,
        inbound_text: str,
        history: Sequence[Message],
    ) -> AgentResponse:
        system_prompt = self._build_system_prompt(client)
        user_prompt = self._build_user_prompt(client, lead, inbound_text, history)

        try:
            payload = self._provider.generate_json(system_prompt=system_prompt, user_prompt=user_prompt)
            response = AgentResponse.model_validate(payload)
            if not response.reply_text.strip():
                raise ValueError("Empty reply from LLM")
            return response
        except Exception as exc:
            logger.exception("llm_agent_fallback", extra={"error": str(exc)})
            return self._fallback_reply(client, lead)

    def _build_system_prompt(self, client: Client) -> str:
        qualification = "\n".join(f"- {q}" for q in (client.qualification_questions or []))
        return (
            "You are an SMS sales assistant for inbound leads. "
            "Respond with strict JSON using this schema: "
            "{reply_text:string,next_state:string,actions:[{type:string,payload:object}]}.\n"
            "Rules:\n"
            "1) Be concise, friendly, and professional.\n"
            "2) Ask at most one question per message.\n"
            "3) Never fabricate pricing or policy details. If unknown, push to booking.\n"
            "4) Always attempt to get a meeting booked.\n"
            "5) Never request sensitive data (SSN, card, passwords).\n"
            "6) STOP or unsubscribe intent must not be handled here; compliance handles that earlier.\n"
            "7) actions may only be send_booking_link, request_more_info, handoff_to_human.\n"
            f"Client tone override: {client.tone}.\n"
            f"Client FAQs/context: {client.faq_context or 'none provided'}.\n"
            f"Qualification question bank:\n{qualification or '- none'}"
        )

    def _build_user_prompt(
        self,
        client: Client,
        lead: Lead,
        inbound_text: str,
        history: Sequence[Message],
    ) -> str:
        transcript = [
            {"direction": msg.direction.value, "body": msg.body}
            for msg in history[-8:]
        ]
        payload = {
            "business_name": client.business_name,
            "booking_url": client.booking_url,
            "lead_name": lead.full_name,
            "lead_city": lead.city,
            "state": lead.conversation_state.value,
            "qualification_questions": client.qualification_questions,
            "latest_inbound_message": inbound_text,
            "recent_messages": transcript,
        }
        return json.dumps(payload)

    def _fallback_reply(self, client: Client, lead: Lead) -> AgentResponse:
        if lead.conversation_state in {ConversationStateEnum.NEW, ConversationStateEnum.GREETED}:
            question = (client.qualification_questions or ["What are you looking for help with?"])[0]
            return AgentResponse(
                reply_text=question,
                next_state=ConversationStateEnum.QUALIFYING,
                actions=[AgentAction(type="request_more_info", payload={})],
            )

        return AgentResponse(
            reply_text="I can help you get set up quickly. Want me to send the booking link?",
            next_state=ConversationStateEnum.QUALIFYING,
            actions=[AgentAction(type="request_more_info", payload={})],
        )


def build_llm_agent(settings: Settings, runtime_overrides: dict[str, str] | None = None) -> LLMAgent:
    api_key = (runtime_overrides or {}).get("openai_api_key", settings.openai_api_key)
    model = (runtime_overrides or {}).get("openai_model", settings.openai_model)

    provider: LLMProvider
    if api_key:
        provider = OpenAIProvider(api_key=api_key, model=model)
    else:
        provider = HeuristicProvider()
    return LLMAgent(provider=provider)
