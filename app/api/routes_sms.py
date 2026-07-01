from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
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
    MessageAttachment,
    MessageDirection,
)
from app.db.session import get_db
from app.services.booking import BookingService
from app.services.compliance import evaluate_text, is_rate_limited
from app.services.crm import (
    CRM_STAGE_CONTACTED,
    CRM_STAGE_LOST,
    CRM_STAGE_MEETING_BOOKED,
    CRM_STAGE_QUALIFIED,
    is_meaningful_inbound,
    progress_crm_stage,
)
from app.services.lead_intake import normalize_phone
from app.services.inbound_sms import process_inbound_turn
from app.services.llm_agent import LLMAgent
from app.services.message_media import (
    MessageMediaError,
    create_message_attachment,
    download_twilio_media,
    filename_from_url,
    store_message_media,
)
from app.services.runtime_config import (
    client_runtime_overrides,
    get_effective_runtime_map_for_client,
    load_runtime_overrides,
)
from app.services.sms_service import SMSService, build_sms_service
from app.services.sms_delivery import apply_twilio_delivery_callback
from app.workers.tasks import enqueue_process_inbound_sms, get_redis_connection

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
    sms_service: SMSService | None = None,
) -> None:
    payload = raw_payload or {}
    if sms_service is not None:
        payload = sms_service.with_delivery_status(payload, provider_sid)
    db.add(
        Message(
            lead_id=lead.id,
            client_id=lead.client_id,
            direction=MessageDirection.OUTBOUND,
            body=body,
            provider_message_sid=provider_sid,
            raw_payload=payload,
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


def _inbound_sid_already_seen(*, db: Session, client_id: int, inbound_sid: str) -> bool:
    if not inbound_sid:
        return False
    existing_id = db.scalar(
        select(Message.id)
        .where(
            Message.client_id == client_id,
            Message.direction == MessageDirection.INBOUND,
            Message.provider_message_sid == inbound_sid,
        )
        .limit(1)
    )
    return existing_id is not None


def _twilio_media_items(payload: dict[str, str]) -> list[dict[str, str]]:
    try:
        count = min(max(int(payload.get("NumMedia") or 0), 0), 10)
    except ValueError:
        count = 0
    items: list[dict[str, str]] = []
    for index in range(count):
        media_url = str(payload.get(f"MediaUrl{index}") or "").strip()
        content_type = str(payload.get(f"MediaContentType{index}") or "").strip()
        if media_url:
            items.append({"index": str(index), "url": media_url, "content_type": content_type})
    return items


@router.post("/status-callback")
async def sms_status_callback(
    request: Request,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
) -> Response:
    form = await request.form()
    payload = {str(key): str(value) for key, value in form.items()}
    provider_sid = str(payload.get("MessageSid") or payload.get("SmsSid") or "").strip()
    if not provider_sid:
        return _empty_twiml_response()

    message = db.scalar(
        select(Message)
        .where(
            Message.direction == MessageDirection.OUTBOUND,
            Message.provider_message_sid == provider_sid,
        )
        .order_by(Message.created_at.desc(), Message.id.desc())
        .limit(1)
    )
    if message is None:
        db.add(
            AuditLog(
                client_id=None,
                lead_id=None,
                event_type="sms_delivery_callback_unmatched",
                decision={"provider_sid": provider_sid, "payload": payload},
            )
        )
        db.commit()
        return _empty_twiml_response()

    client = db.get(Client, message.client_id)
    if client is not None:
        effective_runtime = get_effective_runtime_map_for_client(
            settings=settings,
            overrides=load_runtime_overrides(db),
            client=client,
        )
        if not verify_twilio_signature(request=request, form_data=payload, auth_token=effective_runtime["twilio_auth_token"]):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Twilio signature")

    now = datetime.now(timezone.utc)
    delivery = apply_twilio_delivery_callback(message, payload=payload, now=now)
    lead = db.get(Lead, message.lead_id)
    if lead is not None:
        lead.updated_at = now
        if delivery.get("severity") == "warning":
            raw_payload = dict(lead.raw_payload or {})
            raw_payload["sms_contactability"] = {
                "status": "sms_failed",
                "reason": delivery.get("status") or "",
                "label": delivery.get("label") or "SMS not delivered",
                "description": delivery.get("description") or "",
                "provider_message_sid": provider_sid,
                "updated_at": now.isoformat(),
            }
            lead.raw_payload = raw_payload
    db.add(
        AuditLog(
            client_id=message.client_id,
            lead_id=message.lead_id,
            event_type="sms_delivery_failed" if delivery.get("severity") == "warning" else "sms_delivery_updated",
            decision={"provider_sid": provider_sid, "delivery": delivery},
            created_at=now,
        )
    )
    db.commit()
    return _empty_twiml_response()


async def _store_inbound_media_attachments(
    *,
    db: Session,
    settings: Settings,
    client: Client,
    lead: Lead,
    message: Message,
    media_items: list[dict[str, str]],
    effective_runtime: dict[str, str],
) -> list[dict[str, Any]]:
    attachments: list[dict[str, Any]] = []
    for item in media_items:
        index = int(item.get("index") or 0)
        media_url = str(item.get("url") or "")
        content_type = str(item.get("content_type") or "")
        try:
            content = await download_twilio_media(
                media_url=media_url,
                content_type=content_type,
                account_sid=effective_runtime.get("twilio_account_sid", ""),
                auth_token=effective_runtime.get("twilio_auth_token", ""),
                timeout_seconds=settings.request_timeout_seconds,
            )
            stored = store_message_media(
                settings=settings,
                client_id=client.id,
                message_id=message.id,
                filename=filename_from_url(media_url, content_type, index=index),
                content_type=content_type,
                content=content,
                provider_media_url=media_url,
                raw_payload={"source": "twilio_mms", "media_index": index, "provider_media_url": media_url},
            )
            attachment = create_message_attachment(message=message, lead=lead, stored=stored)
            db.add(attachment)
            db.flush()
            attachments.append(
                {
                    "id": attachment.id,
                    "filename": attachment.filename,
                    "content_type": attachment.content_type,
                    "media_kind": attachment.media_kind,
                    "size_bytes": attachment.size_bytes,
                    "url": f"/media/public/{attachment.public_token}",
                }
            )
        except (MessageMediaError, Exception) as exc:
            db.add(
                AuditLog(
                    client_id=client.id,
                    lead_id=lead.id,
                    event_type="inbound_media_download_failed",
                    decision={
                        "media_index": index,
                        "media_url": media_url,
                        "content_type": content_type,
                        "error": str(exc),
                    },
                )
            )
    return attachments


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
    provider_overrides = client_runtime_overrides(client)
    if all(provider_overrides.get(key) for key in ("twilio_account_sid", "twilio_auth_token", "twilio_from_number")):
        sms_service = build_sms_service(settings, runtime_overrides=effective_runtime)

    if not verify_twilio_signature(request=request, form_data=payload, auth_token=effective_runtime["twilio_auth_token"]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Twilio signature")

    from_phone = normalize_phone(payload.get("From"))
    body = str(payload.get("Body", "")).strip()
    inbound_sid = str(payload.get("MessageSid", "")).strip()
    media_items = _twilio_media_items(payload)

    if not from_phone:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing sender phone")
    if _inbound_sid_already_seen(db=db, client_id=client.id, inbound_sid=inbound_sid):
        return _empty_twiml_response()

    lead = _load_or_create_lead(db=db, client=client, phone=from_phone, raw_payload=payload)

    now = datetime.now(timezone.utc)
    inbound_message = Message(
        lead_id=lead.id,
        client_id=lead.client_id,
        direction=MessageDirection.INBOUND,
        body=body,
        provider_message_sid=inbound_sid,
        raw_payload=payload,
    )
    db.add(inbound_message)
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        return _empty_twiml_response()
    media_attachments = await _store_inbound_media_attachments(
        db=db,
        settings=settings,
        client=client,
        lead=lead,
        message=inbound_message,
        media_items=media_items,
        effective_runtime=effective_runtime,
    )
    if media_attachments:
        inbound_message.raw_payload = {
            **(inbound_message.raw_payload or {}),
            "attachments": media_attachments,
            "num_media_saved": len(media_attachments),
        }
    lead.last_inbound_at = now
    incr("sms_inbound_total")

    decision = evaluate_text(body)
    if decision.is_stop:
        previous_state = lead.conversation_state
        lead.opted_out = True
        lead.conversation_state = ConversationStateEnum.OPTED_OUT
        previous_crm_stage = lead.crm_stage
        lead.crm_stage = CRM_STAGE_LOST

        reply_text = sms_service.render_template(client, "stop_confirmation", context={})
        sid = sms_service.send_message(to_number=lead.phone, body=reply_text)
        _store_outbound_message(db, lead, reply_text, sid, raw_payload={"reason": "stop"}, sms_service=sms_service)

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
        if previous_crm_stage != lead.crm_stage:
            db.add(
                AuditLog(
                    client_id=client.id,
                    lead_id=lead.id,
                    event_type="crm_stage_auto_updated",
                    decision={
                        "previous_stage": previous_crm_stage,
                        "new_stage": lead.crm_stage,
                        "reason": "opt_out_stop_keyword",
                        "inbound": body,
                    },
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

    if media_attachments and not body:
        process_inbound_turn(
            db=db,
            client=client,
            lead=lead,
            inbound_text="",
            now=now,
            sms_service=sms_service,
            booking_service=booking_service,
            llm_agent=llm_agent,
            inbound_message_id=inbound_message.id,
            media_attachments=media_attachments,
        )
        return _empty_twiml_response()

    if decision.is_help:
        reply_text = sms_service.render_template(client, "help_response", context={})
        sid = sms_service.send_message(to_number=lead.phone, body=reply_text)
        _store_outbound_message(db, lead, reply_text, sid, raw_payload={"reason": "help"}, sms_service=sms_service)
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

    if is_meaningful_inbound(body):
        _auto_update_crm_stage(
            db=db,
            lead=lead,
            client_id=client.id,
            target_stage=CRM_STAGE_QUALIFIED,
            reason="meaningful_inbound",
            inbound_text=body,
        )

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

    if settings.rq_eager:
        process_inbound_turn(
            db=db,
            client=client,
            lead=lead,
            inbound_text=body,
            now=now,
            sms_service=sms_service,
            booking_service=booking_service,
            llm_agent=llm_agent,
            inbound_message_id=inbound_message.id,
            media_attachments=media_attachments,
        )
    else:
        db.commit()
        enqueue_process_inbound_sms(lead_id=lead.id, inbound_message_id=inbound_message.id)

    return _empty_twiml_response()
