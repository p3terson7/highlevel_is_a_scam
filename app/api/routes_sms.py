from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.core.deps import get_app_settings, get_llm_agent, get_sms_service
from app.core.metrics import incr
from app.core.security import verify_twilio_signature
from app.db.models import (
    AuditLog,
    Client,
    ConversationState,
    ConversationStateEnum,
    Lead,
    LeadSource,
    Message,
    MessageDirection,
)
from app.db.session import get_db
from app.services.booking import ensure_booking_link, handoff_suffix
from app.services.compliance import evaluate_text, is_rate_limited, within_operating_hours
from app.services.lead_intake import normalize_phone
from app.services.llm_agent import LLMAgent
from app.services.runtime_config import get_effective_runtime_value, load_runtime_overrides
from app.services.sms_service import SMSService
from app.workers.tasks import enqueue_followup_sms, get_redis_connection

router = APIRouter(prefix="/sms", tags=["sms"])


def _load_client(db: Session, client_key: str) -> Client:
    client = db.scalar(select(Client).where(Client.client_key == client_key, Client.is_active.is_(True)))
    if client is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    return client


def _load_or_create_lead(db: Session, client: Client, phone: str, raw_payload: dict[str, Any]) -> Lead:
    lead = db.scalar(
        select(Lead)
        .where(Lead.client_id == client.id, Lead.phone == phone)
        .order_by(Lead.created_at.desc())
        .limit(1)
    )
    if lead is not None:
        return lead

    lead = Lead(
        client_id=client.id,
        source=LeadSource.SMS,
        full_name="",
        phone=phone,
        email="",
        city="",
        form_answers={},
        raw_payload={"first_inbound_payload": raw_payload},
        consented=True,
        opted_out=False,
        conversation_state=ConversationStateEnum.NEW,
    )
    db.add(lead)
    db.flush()
    return lead


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


@router.post("/inbound/{client_key}")
async def inbound_sms(
    client_key: str,
    request: Request,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    sms_service: SMSService = Depends(get_sms_service),
    llm_agent: LLMAgent = Depends(get_llm_agent),
) -> dict[str, Any]:
    client = _load_client(db, client_key)

    form = await request.form()
    payload = {str(key): str(value) for key, value in form.items()}

    runtime_overrides = load_runtime_overrides(db)
    effective_twilio_auth_token = get_effective_runtime_value(
        settings=settings,
        overrides=runtime_overrides,
        key="twilio_auth_token",
    )

    if not verify_twilio_signature(request=request, form_data=payload, auth_token=effective_twilio_auth_token):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Twilio signature")

    from_phone = normalize_phone(payload.get("From"))
    body = str(payload.get("Body", "")).strip()
    inbound_sid = str(payload.get("MessageSid", "")).strip()

    if not from_phone:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing sender phone")

    lead = _load_or_create_lead(db=db, client=client, phone=from_phone, raw_payload=payload)

    now = datetime.now(timezone.utc)
    db.add(
        Message(
            lead_id=lead.id,
            client_id=lead.client_id,
            direction=MessageDirection.INBOUND,
            body=body,
            provider_message_sid=inbound_sid,
            raw_payload=payload,
        )
    )
    lead.last_inbound_at = now
    incr("sms_inbound_total")

    decision = evaluate_text(body)
    if decision.is_stop:
        previous_state = lead.conversation_state
        lead.opted_out = True
        lead.conversation_state = ConversationStateEnum.OPTED_OUT

        reply_text = sms_service.render_template(client, "stop_confirmation", context={})
        sid = sms_service.send_message(to_number=lead.phone, body=reply_text)
        _store_outbound_message(db, lead, reply_text, sid, raw_payload={"reason": "stop"})

        if previous_state != lead.conversation_state:
            db.add(
                ConversationState(
                    lead_id=lead.id,
                    previous_state=previous_state,
                    new_state=lead.conversation_state,
                    reason="STOP keyword",
                    metadata_json={},
                )
            )

        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="compliance_stop",
                decision={"inbound": body, "outbound": reply_text},
            )
        )
        db.commit()
        incr("sms_outbound_total")
        return {"status": "ok", "state": lead.conversation_state.value}

    if lead.opted_out:
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="opted_out_message_ignored",
                decision={"inbound": body},
            )
        )
        db.commit()
        return {"status": "ignored", "reason": "lead_opted_out"}

    if decision.is_help:
        reply_text = sms_service.render_template(client, "help_response", context={})
        sid = sms_service.send_message(to_number=lead.phone, body=reply_text)
        _store_outbound_message(db, lead, reply_text, sid, raw_payload={"reason": "help"})
        lead.last_outbound_at = now
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="compliance_help",
                decision={"inbound": body, "outbound": reply_text},
            )
        )
        db.commit()
        incr("sms_outbound_total")
        return {"status": "ok", "state": lead.conversation_state.value}

    if not within_operating_hours(client):
        reply_text = sms_service.render_template(client, "after_hours", context={"business_name": client.business_name})
        sid = sms_service.send_message(to_number=lead.phone, body=reply_text)
        _store_outbound_message(db, lead, reply_text, sid, raw_payload={"reason": "outside_hours"})
        lead.last_outbound_at = now

        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="outside_operating_hours",
                decision={"inbound": body, "outbound": reply_text},
            )
        )
        db.commit()
        incr("sms_outbound_total")

        enqueue_followup_sms(lead.id, reason="outside_hours_inbound")
        return {"status": "queued_followup", "state": lead.conversation_state.value}

    redis_conn = get_redis_connection()
    if is_rate_limited(
        redis_client=redis_conn,
        lead_id=lead.id,
        max_messages=settings.rate_limit_count,
        window_minutes=settings.rate_limit_window_minutes,
    ):
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="rate_limited",
                decision={"inbound": body},
            )
        )
        db.commit()
        return {"status": "rate_limited"}

    recent_desc = db.scalars(
        select(Message)
        .where(Message.lead_id == lead.id)
        .order_by(Message.created_at.desc())
        .limit(20)
    ).all()
    history = list(reversed(recent_desc))

    agent_response = llm_agent.next_reply(
        client=client,
        lead=lead,
        inbound_text=body,
        history=history,
    )

    reply_text = agent_response.reply_text.strip()
    next_state = agent_response.next_state

    for action in agent_response.actions:
        if action.type == "send_booking_link":
            reply_text = ensure_booking_link(reply_text=reply_text, client=client)
            if next_state in {
                ConversationStateEnum.NEW,
                ConversationStateEnum.GREETED,
                ConversationStateEnum.QUALIFYING,
            }:
                next_state = ConversationStateEnum.BOOKING_SENT
        elif action.type == "handoff_to_human":
            reply_text = f"{reply_text}{handoff_suffix(client)}".strip()
            next_state = ConversationStateEnum.HANDOFF

    sid = sms_service.send_message(to_number=lead.phone, body=reply_text)
    _store_outbound_message(
        db=db,
        lead=lead,
        body=reply_text,
        provider_sid=sid,
        raw_payload={"actions": [action.model_dump() for action in agent_response.actions]},
    )

    previous_state = lead.conversation_state
    lead.conversation_state = next_state
    lead.last_outbound_at = now

    if previous_state != lead.conversation_state:
        db.add(
            ConversationState(
                lead_id=lead.id,
                previous_state=previous_state,
                new_state=lead.conversation_state,
                reason="agent_transition",
                metadata_json={"actions": [a.model_dump() for a in agent_response.actions]},
            )
        )

    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=lead.id,
            event_type="agent_decision",
            decision={
                "inbound": body,
                "outbound": reply_text,
                "next_state": lead.conversation_state.value,
                "actions": [a.model_dump() for a in agent_response.actions],
            },
        )
    )
    db.commit()
    incr("sms_outbound_total")

    return {"status": "ok", "state": lead.conversation_state.value}
