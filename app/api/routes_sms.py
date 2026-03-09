from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.core.deps import get_app_settings, get_booking_service, get_llm_agent, get_sms_service
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
from app.services.booking import (
    BookingProviderError,
    BookingService,
    automated_booking_enabled,
    ensure_booking_link,
    extract_email,
    handoff_suffix,
)
from app.services.compliance import evaluate_text, is_rate_limited
from app.services.lead_intake import normalize_phone
from app.services.llm_agent import LLMAgent, build_llm_agent
from app.services.runtime_config import (
    client_runtime_overrides,
    get_effective_runtime_map_for_client,
    load_runtime_overrides,
)
from app.services.sms_service import SMSService, build_sms_service
from app.workers.tasks import get_redis_connection

router = APIRouter(prefix="/sms", tags=["sms"])


def _empty_twiml_response() -> Response:
    return Response(content="<?xml version='1.0' encoding='UTF-8'?><Response></Response>", media_type="application/xml")


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
    booking_service: BookingService = Depends(get_booking_service),
    llm_agent: LLMAgent = Depends(get_llm_agent),
) -> Response:
    client = _load_client(db, client_key)

    form = await request.form()
    payload = {str(key): str(value) for key, value in form.items()}

    runtime_overrides = load_runtime_overrides(db)
    effective_runtime = get_effective_runtime_map_for_client(
        settings=settings,
        overrides=runtime_overrides,
        client=client,
    )
    if client_runtime_overrides(client):
        sms_service = build_sms_service(settings, runtime_overrides=effective_runtime)
        llm_agent = build_llm_agent(settings=settings, runtime_overrides=effective_runtime)

    if not verify_twilio_signature(request=request, form_data=payload, auth_token=effective_runtime["twilio_auth_token"]):
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
        return _empty_twiml_response()

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
        return _empty_twiml_response()

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
        return _empty_twiml_response()

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
        return _empty_twiml_response()

    recent_desc = db.scalars(
        select(Message)
        .where(Message.lead_id == lead.id)
        .order_by(Message.created_at.desc())
        .limit(20)
    ).all()
    history = list(reversed(recent_desc))

    detected_email = extract_email(body)
    if detected_email and not lead.email:
        lead.email = detected_email

    if automated_booking_enabled(client) and lead.conversation_state == ConversationStateEnum.BOOKING_SENT:
        try:
            selection = booking_service.handle_slot_selection(
                client=client,
                lead=lead,
                inbound_text=body,
                history=history,
            )
        except BookingProviderError as exc:
            fallback_reply = (
                ensure_booking_link("I could not reach live scheduling right now, but you can still book here.", client)
                if client.booking_url
                else "I could not reach live scheduling right now. A team member will follow up shortly."
            )
            sid = sms_service.send_message(to_number=lead.phone, body=fallback_reply)
            _store_outbound_message(
                db=db,
                lead=lead,
                body=fallback_reply,
                provider_sid=sid,
                raw_payload={"booking_provider_error": str(exc)},
            )
            lead.last_outbound_at = now
            db.add(
                AuditLog(
                    client_id=client.id,
                    lead_id=lead.id,
                    event_type="calendar_booking_error",
                    decision={"inbound": body, "error": str(exc)},
                )
            )
            db.commit()
            incr("sms_outbound_total")
            return _empty_twiml_response()

        if selection and selection.handled:
            sid = sms_service.send_message(to_number=lead.phone, body=selection.reply_text)
            _store_outbound_message(
                db=db,
                lead=lead,
                body=selection.reply_text,
                provider_sid=sid,
                raw_payload=selection.raw_payload,
            )
            previous_state = lead.conversation_state
            lead.conversation_state = selection.next_state
            lead.last_outbound_at = now
            if previous_state != lead.conversation_state:
                db.add(
                    ConversationState(
                        lead_id=lead.id,
                        previous_state=previous_state,
                        new_state=lead.conversation_state,
                        reason=selection.transition_reason,
                        metadata_json=selection.raw_payload,
                    )
                )
            db.add(
                AuditLog(
                    client_id=client.id,
                    lead_id=lead.id,
                    event_type=selection.audit_event_type,
                    decision=selection.audit_decision,
                )
            )
            db.commit()
            incr("sms_outbound_total")
            return _empty_twiml_response()

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
        elif action.type == "offer_calendar_slots":
            if not lead.email:
                reply_text = "I can book this directly. What email should I use for the calendar confirmation?"
                next_state = ConversationStateEnum.QUALIFYING
                continue
            try:
                offer = booking_service.offer_slots(client=client, lead=lead)
                reply_text = offer.reply_text
                if offer.slots:
                    next_state = ConversationStateEnum.BOOKING_SENT
                else:
                    next_state = ConversationStateEnum.BOOKING_SENT if client.booking_url else ConversationStateEnum.QUALIFYING
                action.payload = offer.raw_payload.get("booking_offer", {})
            except BookingProviderError as exc:
                reply_text = (
                    ensure_booking_link("I could not pull live availability right now, but you can still book here.", client)
                    if client.booking_url
                    else "I could not pull live availability right now. A team member will follow up shortly."
                )
                action.payload = {"error": str(exc)}
        elif action.type == "handoff_to_human":
            reply_text = f"{reply_text}{handoff_suffix(client)}".strip()
            next_state = ConversationStateEnum.HANDOFF

    outbound_raw_payload = {"actions": [action.model_dump() for action in agent_response.actions]}
    booking_offer_payload = next(
        (
            action.payload
            for action in agent_response.actions
            if action.type == "offer_calendar_slots" and isinstance(action.payload, dict) and action.payload.get("slots")
        ),
        None,
    )
    if booking_offer_payload:
        outbound_raw_payload["booking_offer"] = booking_offer_payload

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
    lead.last_outbound_at = now

    if previous_state != lead.conversation_state:
        db.add(
                ConversationState(
                    lead_id=lead.id,
                    previous_state=previous_state,
                    new_state=lead.conversation_state,
                    reason="agent_transition",
                    metadata_json={
                        **outbound_raw_payload,
                        "provider": agent_response.provider,
                    },
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
                "provider": agent_response.provider,
                "provider_error": agent_response.provider_error,
                **outbound_raw_payload,
            },
        )
    )
    db.commit()
    incr("sms_outbound_total")

    return _empty_twiml_response()
