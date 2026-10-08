"""One source-independent opening for live intake and Test Lab."""

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from app.db.models import Client, ConversationStateEnum, Lead
from app.services.agent_v3_types import ASSISTANT_NAME
from app.services.i18n import client_language
from app.services.inbound_sms import safe_agent_diagnostics
from app.services.lead_summary import (
    build_lead_summary_text,
    filter_question_form_answers,
)


def initial_seed_text(lead: Lead) -> str:
    # Put project facts first so bounded retrieval queries retain them.
    answers = filter_question_form_answers(lead.form_answers or {})
    details: list[str] = []
    summary = build_lead_summary_text(answers, limit=6)
    if summary and summary != "No qualification details captured yet.":
        details.append(f"summary={summary}")
    if lead.full_name:
        details.append(f"name={lead.full_name}")
    if lead.city:
        details.append(f"city={lead.city}")
    if lead.email:
        details.append(f"email={lead.email}")
    context_blob = " | ".join(details) if details else "no extra lead details"
    return (
        f"Lead context: {context_blob}. "
        "New lead submitted an inquiry. "
        "This is the first outbound message after the submission. "
    )


@dataclass
class InitialOutreach:
    body: str
    next_state: ConversationStateEnum
    memory: dict[str, Any]
    payload: dict[str, Any]


def generate_initial_outreach(
    *,
    db: Session,
    client: Client,
    lead: Lead,
    llm_agent: Any,
) -> InitialOutreach:
    seed = initial_seed_text(lead)
    run_turn = getattr(llm_agent, "run_turn", None)
    if callable(run_turn):
        response = run_turn(
            client=client,
            lead=lead,
            inbound_text=seed,
            history=[],
            booking_service=None,
            db=db,
        )
    else:
        # Compatibility with existing next_reply-only adapters/test doubles.
        response = llm_agent.next_reply(
            client=client,
            lead=lead,
            inbound_text=seed,
            history=[],
        )
    body = response.reply_text.strip()
    if not body:
        first_name = lead.full_name.split(" ")[0] if lead.full_name else ""
        if client_language(client, lead=lead) == "fr":
            body = f"Bonjour {first_name}, ici {ASSISTANT_NAME}, l'assistante de {client.business_name}. Merci de nous avoir contactés."
        else:
            body = f"Hi {first_name or 'there'}, I'm {ASSISTANT_NAME}, the assistant for {client.business_name}. Thanks for reaching out."
    runtime = dict(response.runtime_payload or {})
    memory = {
        "qualification_memory": response.collected_fields.model_dump(exclude_none=True),
        "last_question_key": response.next_question_key,
        "pending_step": runtime.get("pending_step"),
    }
    for key in (
        "cta_state",
        "intent_level",
        "intent_score",
        "intent_reasons",
        "important_missing_fields",
        "lead_summary",
        "recommended_follow_up",
        "calendar_booking",
    ):
        if key in runtime:
            memory[key] = runtime[key]
    return InitialOutreach(
        body=body,
        next_state=(
            response.next_state
            if response.next_state != ConversationStateEnum.NEW
            else ConversationStateEnum.QUALIFYING
        ),
        memory=memory,
        payload={
            "provider": response.provider,
            "provider_error": response.provider_error,
            "agent": {
                "action": response.action,
                "next_question_key": response.next_question_key,
                "collected_fields": memory["qualification_memory"],
                "provider": response.provider,
                "provider_error": response.provider_error,
                "intent_level": runtime.get("intent_level"),
                "intent_score": runtime.get("intent_score"),
                "cta_state": runtime.get("cta_state"),
                "lead_summary": runtime.get("lead_summary"),
                **safe_agent_diagnostics(runtime),
            },
            "actions": [action.model_dump() for action in response.actions],
            "seed_context": seed,
        },
    )


def apply_initial_memory(lead: Lead, memory: dict[str, Any]) -> None:
    """Apply the memory belonging to the delivered opening, preserving attribution."""
    payload = dict(lead.raw_payload or {})
    payload.update(memory)
    for key in ("last_question_key", "pending_step"):
        if not payload.get(key):
            payload.pop(key, None)
    lead.raw_payload = payload
