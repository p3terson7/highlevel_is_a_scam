from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.metrics import incr
from app.db.models import (
    AuditLog,
    Client,
    ConversationState,
    ConversationStateEnum,
    Lead,
    Message,
    MessageDirection,
)
from app.services.booking import BookingService, extract_email, handoff_suffix
from app.services.crm import (
    CRM_STAGE_CONTACTED,
    CRM_STAGE_MEETING_BOOKED,
    progress_crm_stage,
)
from app.services.llm_agent import LLMAgent
from app.services.sms_service import SMSService

_PENDING_STEP_KEY = "pending_step"
_DEFAULT_HISTORY_LIMIT = 40


def _store_outbound_message(
    db: Session,
    lead: Lead,
    body: str,
    provider_sid: str,
    raw_payload: dict[str, Any] | None = None,
) -> None:
    db.add(
        Message(
            lead_id=lead.id,
            client_id=lead.client_id,
            direction=MessageDirection.OUTBOUND,
            body=body,
            provider_message_sid=provider_sid,
            raw_payload=raw_payload or {},
        )
    )


def _auto_update_crm_stage(
    *,
    db: Session,
    lead: Lead,
    client_id: int,
    target_stage: str,
    reason: str,
    inbound_text: str | None = None,
) -> None:
    previous_stage = lead.crm_stage
    next_stage = progress_crm_stage(previous_stage, target_stage)
    if next_stage == previous_stage:
        return
    lead.crm_stage = next_stage
    decision: dict[str, Any] = {
        "previous_stage": previous_stage,
        "new_stage": next_stage,
        "reason": reason,
    }
    if inbound_text:
        decision["inbound"] = inbound_text
    db.add(
        AuditLog(
            client_id=client_id,
            lead_id=lead.id,
            event_type="crm_stage_auto_updated",
            decision=decision,
        )
    )


def _current_pending_step(lead: Lead) -> str:
    raw_payload = lead.raw_payload if isinstance(lead.raw_payload, dict) else {}
    return str(raw_payload.get(_PENDING_STEP_KEY) or "").strip()


def _store_agent_memory(
    *,
    lead: Lead,
    agent_response,
    pending_step: str | None,
) -> None:
    payload = dict(lead.raw_payload or {})
    payload["qualification_memory"] = agent_response.collected_fields.model_dump(exclude_none=True)
    if agent_response.next_question_key:
        payload["last_question_key"] = agent_response.next_question_key
    else:
        payload.pop("last_question_key", None)
    if pending_step:
        payload[_PENDING_STEP_KEY] = pending_step
    else:
        payload.pop(_PENDING_STEP_KEY, None)
    lead.raw_payload = payload


def already_processed_inbound_message(*, db: Session, lead_id: int, inbound_message_id: int) -> bool:
    recent_outbound = db.scalars(
        select(Message)
        .where(Message.lead_id == lead_id, Message.direction == MessageDirection.OUTBOUND)
        .order_by(Message.created_at.desc())
        .limit(50)
    ).all()
    for message in recent_outbound:
        raw_payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
        if int(raw_payload.get("inbound_message_id") or 0) == int(inbound_message_id):
            return True
    return False


def process_inbound_turn(
    *,
    db: Session,
    client: Client,
    lead: Lead,
    inbound_text: str,
    now: datetime | None,
    sms_service: SMSService,
    booking_service: BookingService,
    llm_agent: LLMAgent,
    inbound_message_id: int | None = None,
    history_limit: int = _DEFAULT_HISTORY_LIMIT,
) -> None:
    turn_time = now or datetime.now(timezone.utc)

    recent_desc = db.scalars(
        select(Message)
        .where(Message.lead_id == lead.id)
        .order_by(Message.created_at.desc())
        .limit(max(10, int(history_limit)))
    ).all()
    history = list(reversed(recent_desc))

    detected_email = extract_email(inbound_text)
    if detected_email and not lead.email:
        lead.email = detected_email

    pending_step_before = _current_pending_step(lead)

    run_turn = getattr(llm_agent, "run_turn", None)
    if callable(run_turn):
        agent_response = run_turn(
            client=client,
            lead=lead,
            inbound_text=inbound_text,
            history=history,
            booking_service=booking_service,
            db=db,
        )
    else:
        agent_response = llm_agent.next_reply(
            client=client,
            lead=lead,
            inbound_text=inbound_text,
            history=history,
        )

    reply_text = agent_response.reply_text.strip()
    next_state = agent_response.next_state
    action = agent_response.action
    runtime_payload = dict(agent_response.runtime_payload or {})
    next_pending_step: str | None = runtime_payload.get("pending_step", pending_step_before or None)

    if not reply_text:
        reply_text = "I’m still with you. Let me send a fresh set of times."
        runtime_payload.setdefault("pending_step", next_pending_step)
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="agent_empty_reply_fallback",
                decision={"inbound": inbound_text, "provider": agent_response.provider, "provider_error": agent_response.provider_error},
            )
        )

    if agent_response.provider_error:
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="agent_provider_fallback",
                decision={"inbound": inbound_text, "provider": agent_response.provider, "provider_error": agent_response.provider_error},
            )
        )

    if action == "handoff_to_human":
        reply_text = f"{reply_text}{handoff_suffix(client)}".strip()
        next_state = ConversationStateEnum.HANDOFF
        next_pending_step = None
    elif action == "mark_booked":
        next_state = ConversationStateEnum.BOOKED
        next_pending_step = None
    elif lead.conversation_state == ConversationStateEnum.BOOKED and next_state == ConversationStateEnum.QUALIFYING:
        next_state = ConversationStateEnum.BOOKED

    _store_agent_memory(lead=lead, agent_response=agent_response, pending_step=next_pending_step)

    outbound_raw_payload = {
        "agent": {
            "action": agent_response.action,
            "next_question_key": agent_response.next_question_key,
            "collected_fields": agent_response.collected_fields.model_dump(exclude_none=True),
            "provider": agent_response.provider,
            "provider_error": agent_response.provider_error,
        },
        "actions": [action_item.model_dump() for action_item in agent_response.actions],
        "pending_step_before": pending_step_before or None,
        "pending_step_after": next_pending_step,
    }
    if inbound_message_id is not None:
        outbound_raw_payload["inbound_message_id"] = int(inbound_message_id)
    if runtime_payload.get("booking_offer"):
        outbound_raw_payload["booking_offer"] = runtime_payload["booking_offer"]
    if runtime_payload.get("calendar_booking"):
        outbound_raw_payload["calendar_booking"] = runtime_payload["calendar_booking"]

    sid = sms_service.send_message(to_number=lead.phone, body=reply_text)
    _store_outbound_message(
        db=db,
        lead=lead,
        body=reply_text,
        provider_sid=sid,
        raw_payload=outbound_raw_payload,
    )

    previous_state = lead.conversation_state
    lead.conversation_state = next_state
    lead.last_outbound_at = turn_time
    _auto_update_crm_stage(
        db=db,
        lead=lead,
        client_id=client.id,
        target_stage=CRM_STAGE_CONTACTED,
        reason="outbound_sms_sent",
        inbound_text=inbound_text,
    )
    if next_state == ConversationStateEnum.BOOKED:
        _auto_update_crm_stage(
            db=db,
            lead=lead,
            client_id=client.id,
            target_stage=CRM_STAGE_MEETING_BOOKED,
            reason="booking_confirmed",
            inbound_text=inbound_text,
        )

    if previous_state != lead.conversation_state:
        db.add(
            ConversationState(
                lead_id=lead.id,
                previous_state=previous_state,
                new_state=lead.conversation_state,
                reason="agent_transition" if not runtime_payload.get("calendar_booking") else "calendar_booking_created",
                metadata_json={
                    **outbound_raw_payload,
                    "provider": agent_response.provider,
                },
            )
        )

    if runtime_payload.get("booking_offer"):
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="calendar_booking_offer_sent",
                decision={"inbound": inbound_text, "booking_offer": runtime_payload["booking_offer"]},
            )
        )
    if runtime_payload.get("calendar_booking"):
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="calendar_booking_created",
                decision={"inbound": inbound_text, "calendar_booking": runtime_payload["calendar_booking"]},
            )
        )

    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=lead.id,
            event_type="agent_decision",
            decision={
                "inbound": inbound_text,
                "outbound": reply_text,
                "next_state": lead.conversation_state.value,
                "provider": agent_response.provider,
                "provider_error": agent_response.provider_error,
                **outbound_raw_payload,
            },
        )
    )
    db.commit()
    incr("sms_outbound_total")
