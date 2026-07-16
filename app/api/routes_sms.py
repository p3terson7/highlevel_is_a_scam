from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.core.deps import (
    get_app_settings,
    get_booking_service,
    get_llm_agent,
    get_sms_service,
)
from app.core.logging import get_logger
from app.core.metrics import incr
from app.core.security import verify_twilio_signature, verify_twilio_tenant_binding
from app.db.models import (
    AuditLog,
    Client,
    ConversationState,
    ConversationStateEnum,
    InboundWebhookEvent,
    Lead,
    LeadSource,
    Message,
    MessageDirection,
)
from app.db.session import get_db
from app.services.booking import BookingService
from app.services.compliance import evaluate_text, is_rate_limited
from app.services.crm import (
    CRM_STAGE_CONTACTED,
    CRM_STAGE_LOST,
    CRM_STAGE_QUALIFIED,
    CRM_STAGE_SET,
    is_meaningful_inbound,
    progress_crm_stage,
)
from app.services.inbound_sms import process_inbound_turn
from app.services.inbound_work import (
    INBOUND_WORK_COMPLETED,
    INBOUND_WORK_QUEUED,
    INBOUND_WORK_RECEIVED,
    INBOUND_WORK_SUPPRESSED,
    INBOUND_WORK_WAITING_MEDIA,
    claim_inbound_work,
    fail_inbound_work_safely,
    finish_inbound_work,
    is_inbound_work_recoverable,
    set_inbound_work_state,
)
from app.services.lead_intake import normalize_phone
from app.services.llm_agent import LLMAgent
from app.services.runtime_config import (
    client_runtime_overrides,
    get_effective_runtime_map_for_client,
    load_runtime_overrides,
)
from app.services.sms_delivery import apply_twilio_delivery_callback
from app.services.sms_service import SMSDeliveryError, SMSService, build_sms_service
from app.services.twilio_inbound_admission import admit_twilio_inbound
from app.workers.tasks import (
    INBOUND_MEDIA_EVENT_ENDPOINT,
    enqueue_process_inbound_media_event,
    enqueue_process_inbound_sms,
    get_redis_connection,
)

router = APIRouter(prefix="/sms", tags=["sms"])
logger = get_logger(__name__)

_MAX_TWILIO_FORM_BYTES = 64 * 1024
_MAX_TWILIO_FORM_FIELDS = 128


async def _bounded_twilio_form(request: Request) -> dict[str, str]:
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/x-www-form-urlencoded":
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="Twilio webhooks require URL-encoded form data",
        )

    content_length = request.headers.get("content-length", "").strip()
    if content_length:
        try:
            declared_length = int(content_length)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Content-Length") from exc
        if declared_length < 0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Content-Length")
        if declared_length > _MAX_TWILIO_FORM_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail="Twilio webhook payload is too large",
            )

    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > _MAX_TWILIO_FORM_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail="Twilio webhook payload is too large",
            )
        chunks.append(chunk)
    try:
        encoded = b"".join(chunks).decode("utf-8")
        pairs = parse_qsl(
            encoded,
            keep_blank_values=True,
            strict_parsing=False,
            max_num_fields=_MAX_TWILIO_FORM_FIELDS,
        )
    except (UnicodeDecodeError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Twilio form payload") from exc

    payload: dict[str, str] = {}
    for key, value in pairs:
        if key in payload:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Duplicate Twilio form field")
        payload[key] = value
    return payload


def _twilio_request_is_valid(
    *,
    request: Request,
    payload: dict[str, str],
    auth_token: str,
    settings: Settings,
    public_base_url: str = "",
    expected_account_sid: str = "",
    expected_number: str = "",
    number_field: str = "To",
) -> bool:
    if (
        not auth_token
        and settings.allow_unsigned_twilio_webhooks
        and settings.env.strip().lower() in {"dev", "development", "local", "test"}
    ):
        return verify_twilio_tenant_binding(
            payload,
            expected_account_sid=expected_account_sid,
            expected_number=expected_number,
            number_field=number_field,
        )
    if not verify_twilio_signature(
        request=request,
        form_data=payload,
        auth_token=auth_token,
        public_base_url=public_base_url,
    ):
        return False
    return verify_twilio_tenant_binding(
        payload,
        expected_account_sid=expected_account_sid,
        expected_number=expected_number,
        number_field=number_field,
        require_account=True,
    )


def _empty_twiml_response() -> Response:
    return Response(content="<?xml version='1.0' encoding='UTF-8'?><Response></Response>", media_type="application/xml")


def _load_client(db: Session, client_key: str) -> Client:
    client = db.scalar(select(Client).where(Client.client_key == client_key, Client.is_active.is_(True)))
    if client is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    return client


def _load_lead_by_phone(*, db: Session, client_id: int, phone: str) -> Lead | None:
    return db.scalar(
        select(Lead)
        .where(Lead.client_id == client_id, Lead.phone == phone)
        .order_by(Lead.created_at.desc())
        .limit(1)
    )


def _load_or_create_lead(db: Session, client: Client, phone: str, raw_payload: dict[str, Any]) -> Lead:
    lead = _load_lead_by_phone(db=db, client_id=client.id, phone=phone)
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
        raw_payload={
            "first_inbound_payload": raw_payload,
            "consent_evidence": {
                "granted": True,
                "status": "granted",
                "method": "consumer_initiated_sms",
                "source_fields": ["From", "MessageSid"],
            },
        },
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


def _load_inbound_message(*, db: Session, client_id: int, inbound_sid: str) -> Message | None:
    if not inbound_sid:
        return None
    return db.scalar(
        select(Message)
        .where(
            Message.client_id == client_id,
            Message.direction == MessageDirection.INBOUND,
            Message.provider_message_sid == inbound_sid,
        )
        .limit(1)
    )


def _record_inbound_queue_handoff_failure(
    *,
    db: Session,
    client_id: int,
    lead_id: int,
    message_id: int,
    reason: str,
) -> None:
    try:
        db.add(
            AuditLog(
                client_id=client_id,
                lead_id=lead_id,
                event_type="inbound_sms_queue_handoff_failed",
                decision={
                    "message_id": int(message_id),
                    "reason": str(reason or "queue_unavailable")[:64],
                    "recovery": "durable_inbound_work",
                },
            )
        )
        db.commit()
    except Exception as exc:
        # The inbound message was already committed. An observability write
        # must not turn a recoverable queue outage into a Twilio retry storm.
        db.rollback()
        logger.warning(
            "inbound_sms_queue_handoff_audit_failed",
            extra={"message_id": message_id, "error_type": type(exc).__name__},
        )


def _enqueue_persisted_inbound_work(
    *,
    db: Session,
    client_id: int,
    message: Message,
) -> bool:
    try:
        enqueued = enqueue_process_inbound_sms(
            lead_id=message.lead_id,
            inbound_message_id=message.id,
        )
    except Exception as exc:
        logger.warning(
            "inbound_sms_queue_handoff_failed",
            extra={"message_id": message.id, "error_type": type(exc).__name__},
        )
        _record_inbound_queue_handoff_failure(
            db=db,
            client_id=client_id,
            lead_id=message.lead_id,
            message_id=message.id,
            reason=type(exc).__name__,
        )
        return False
    if enqueued is False:
        logger.warning(
            "inbound_sms_queue_unavailable",
            extra={"message_id": message.id},
        )
        _record_inbound_queue_handoff_failure(
            db=db,
            client_id=client_id,
            lead_id=message.lead_id,
            message_id=message.id,
            reason="queue_unavailable",
        )
        return False
    return True


def _inbound_media_event_key(*, client_id: int, inbound_sid: str) -> str:
    fingerprint = hashlib.sha256(f"{client_id}:{inbound_sid}".encode()).hexdigest()
    return f"twilio-mms:{fingerprint}"


def _enqueue_inbound_media_or_raise(*, event_id: int, settings: Settings) -> None:
    try:
        enqueued = enqueue_process_inbound_media_event(event_id)
    except Exception as exc:
        logger.warning(
            "inbound_media_enqueue_failed",
            extra={"event_id": event_id, "error_type": type(exc).__name__},
        )
        if not settings.rq_eager:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="MMS processing is temporarily unavailable",
            ) from exc
        return
    if enqueued is False and not settings.rq_eager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="MMS processing is temporarily unavailable",
        )


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


def _record_compliance_reply_once(
    *,
    db: Session,
    client: Client,
    lead: Lead,
    inbound_sid: str,
    inbound_text: str,
    event_type: str,
    reason: str,
    reply_text: str,
    sms_service: SMSService,
    now: datetime,
    should_send: bool,
    suppression_reason: str = "",
) -> bool:
    decision: dict[str, Any] = {
        "inbound": inbound_text,
        "outbound": reply_text,
        "inbound_provider_sid": inbound_sid,
        "reply_status": "reserved" if should_send else "suppressed",
    }
    if suppression_reason:
        decision["suppression_reason"] = suppression_reason
    audit = AuditLog(
        client_id=client.id,
        lead_id=lead.id,
        event_type=event_type,
        decision=decision,
    )
    db.add(audit)
    # Commit the inbound SID and the reply reservation before the provider call.
    # A retried Twilio callback will then be deduplicated instead of sending a
    # second compliance response after an ambiguous provider/DB failure.
    db.commit()
    if not should_send:
        return False

    try:
        provider_sid = sms_service.send_message(to_number=lead.phone, body=reply_text)
    except SMSDeliveryError as exc:
        audit.decision = {
            **decision,
            "reply_status": "failed",
            "error": str(exc)[:500],
        }
        db.commit()
        return False

    _store_outbound_message(
        db,
        lead,
        reply_text,
        provider_sid,
        raw_payload={"reason": reason},
        sms_service=sms_service,
    )
    lead.last_outbound_at = now
    audit.decision = {
        **decision,
        "reply_status": "sent",
        "provider_sid": provider_sid,
    }
    db.commit()
    incr("sms_outbound_total")
    return True


def _lock_delivery_message_for_callback(
    *,
    db: Session,
    message_id: int,
    client_id: int,
    provider_sid: str,
) -> Message | None:
    """Serialize delivery transitions and refresh any identity-map snapshot."""

    return db.scalar(
        select(Message)
        .where(
            Message.id == message_id,
            Message.client_id == client_id,
            Message.direction == MessageDirection.OUTBOUND,
            Message.provider_message_sid == provider_sid,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )


@router.post("/status-callback")
async def sms_status_callback(
    request: Request,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
) -> Response:
    payload = await _bounded_twilio_form(request)
    provider_sid = str(payload.get("MessageSid") or payload.get("SmsSid") or "").strip()
    if not provider_sid:
        return _empty_twiml_response()

    candidate_message = db.scalar(
        select(Message)
        .where(
            Message.direction == MessageDirection.OUTBOUND,
            Message.provider_message_sid == provider_sid,
        )
        .order_by(Message.created_at.desc(), Message.id.desc())
        .limit(1)
    )
    if candidate_message is None:
        # There is no tenant credential with which to authenticate an unknown
        # SID. Ignore it without retaining attacker-controlled payloads.
        return _empty_twiml_response()

    client = db.get(Client, candidate_message.client_id)
    if client is None:
        return _empty_twiml_response()
    effective_runtime = get_effective_runtime_map_for_client(
        settings=settings,
        overrides=load_runtime_overrides(db),
        client=client,
    )
    if not _twilio_request_is_valid(
        request=request,
        payload=payload,
        auth_token=effective_runtime["twilio_auth_token"],
        settings=settings,
        public_base_url=effective_runtime.get("public_base_url", ""),
        expected_account_sid=effective_runtime.get("twilio_account_sid", ""),
        expected_number=effective_runtime.get("twilio_from_number", ""),
        number_field="From",
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Twilio signature")

    # Authentication intentionally happens before taking the row lock so an
    # invalid callback cannot hold delivery rows. PostgreSQL READ COMMITTED plus
    # populate_existing ensures a waiter applies its transition to the state
    # committed by the callback that held this lock first, not to the candidate
    # object already present in this Session's identity map.
    message = _lock_delivery_message_for_callback(
        db=db,
        message_id=candidate_message.id,
        client_id=client.id,
        provider_sid=provider_sid,
    )
    if message is None:
        db.rollback()
        return _empty_twiml_response()

    now = datetime.now(timezone.utc)
    update = apply_twilio_delivery_callback(message, payload=payload, now=now)
    if not update.applied:
        db.rollback()
        return _empty_twiml_response()
    delivery = update.delivery
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

    payload = await _bounded_twilio_form(request)

    runtime_overrides = load_runtime_overrides(db)
    effective_runtime = get_effective_runtime_map_for_client(
        settings=settings,
        overrides=runtime_overrides,
        client=client,
    )
    provider_overrides = client_runtime_overrides(client)
    twilio_keys = ("twilio_account_sid", "twilio_auth_token", "twilio_from_number")
    has_twilio_override = any(provider_overrides.get(key) for key in twilio_keys)
    has_callback_override = bool(provider_overrides.get("public_base_url")) and (
        getattr(sms_service, "provider_kind", "") == "twilio"
    )
    if (has_twilio_override or has_callback_override) and all(
        effective_runtime.get(key) for key in twilio_keys
    ):
        sms_service = build_sms_service(settings, runtime_overrides=effective_runtime)

    if not _twilio_request_is_valid(
        request=request,
        payload=payload,
        auth_token=effective_runtime["twilio_auth_token"],
        settings=settings,
        public_base_url=effective_runtime.get("public_base_url", ""),
        expected_account_sid=effective_runtime.get("twilio_account_sid", ""),
        expected_number=effective_runtime.get("twilio_from_number", ""),
        number_field="To",
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Twilio signature")

    from_phone = normalize_phone(payload.get("From"))
    body = str(payload.get("Body", "")).strip()
    inbound_sid = str(payload.get("MessageSid", "")).strip()
    media_items = _twilio_media_items(payload)

    if not from_phone:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing sender phone")
    if not inbound_sid:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing MessageSid")

    decision = evaluate_text(body)
    existing_message = _load_inbound_message(
        db=db,
        client_id=client.id,
        inbound_sid=inbound_sid,
    )
    if existing_message is not None:
        media_processing_pending = False
        if media_items:
            media_event_key = _inbound_media_event_key(
                client_id=client.id,
                inbound_sid=inbound_sid,
            )
            existing_event = db.scalar(
                select(InboundWebhookEvent).where(
                    InboundWebhookEvent.client_id == client.id,
                    InboundWebhookEvent.event_key == media_event_key,
                )
            )
            if existing_event is not None and existing_event.status != "completed":
                _enqueue_inbound_media_or_raise(event_id=existing_event.id, settings=settings)
                media_processing_pending = True
        if not media_processing_pending and is_inbound_work_recoverable(existing_message):
            _enqueue_persisted_inbound_work(
                db=db,
                client_id=client.id,
                message=existing_message,
            )
        return _empty_twiml_response()

    redis_conn = get_redis_connection()
    existing_lead = None
    if decision.is_stop or decision.is_start:
        existing_lead = _load_lead_by_phone(
            db=db,
            client_id=client.id,
            phone=from_phone,
        )
    tenant_admission = admit_twilio_inbound(
        redis_client=redis_conn,
        settings=settings,
        client_id=client.id,
        account_sid=effective_runtime.get("twilio_account_sid", ""),
        message_sid=inbound_sid,
    )
    if not tenant_admission.admitted:
        # This audit deliberately excludes sender, body, MessageSid, account
        # SID, and raw provider errors. Twilio receives 2xx TwiML so a quota
        # rejection does not amplify into provider retries.
        db.add(
            AuditLog(
                client_id=client.id,
                event_type="twilio_inbound_admission_rejected",
                decision={
                    "reason": tenant_admission.reason,
                    "backend": tenant_admission.backend,
                    "limiting_scope": tenant_admission.limiting_scope,
                    "limit": (
                        settings.twilio_inbound_account_limit
                        if tenant_admission.limiting_scope == "account"
                        else settings.twilio_inbound_tenant_limit
                    ),
                    "window_seconds": settings.twilio_inbound_window_seconds,
                    "retry_after_seconds": tenant_admission.retry_after_seconds,
                    "sid_fingerprint": tenant_admission.sid_fingerprint,
                },
            )
        )
        incr("twilio_inbound_admission_rejected_total")
        logger.warning(
            "twilio_inbound_admission_rejected",
            extra={
                "client_id": client.id,
                "reason": tenant_admission.reason,
                "backend": tenant_admission.backend,
                "limiting_scope": tenant_admission.limiting_scope,
            },
        )
        # Known-lead STOP/START consent transitions must survive a Redis outage
        # or quota spike. They create no new lead and their replies are suppressed.
        if not ((decision.is_stop or decision.is_start) and existing_lead is not None):
            try:
                db.commit()
            except Exception as exc:
                db.rollback()
                logger.warning(
                    "twilio_inbound_admission_audit_failed",
                    extra={"client_id": client.id, "error_type": type(exc).__name__},
                )
            return _empty_twiml_response()

    lead = existing_lead or _load_or_create_lead(
        db=db,
        client=client,
        phone=from_phone,
        raw_payload=payload,
    )

    now = datetime.now(timezone.utc)
    inbound_message = Message(
        lead_id=lead.id,
        client_id=lead.client_id,
        direction=MessageDirection.INBOUND,
        body=body,
        provider_message_sid=inbound_sid,
        raw_payload=payload,
        inbound_work_status=INBOUND_WORK_RECEIVED,
        inbound_work_updated_at=now,
    )
    db.add(inbound_message)
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        concurrent_message = _load_inbound_message(
            db=db,
            client_id=client.id,
            inbound_sid=inbound_sid,
        )
        if concurrent_message is not None and is_inbound_work_recoverable(concurrent_message):
            _enqueue_persisted_inbound_work(
                db=db,
                client_id=client.id,
                message=concurrent_message,
            )
        return _empty_twiml_response()
    lead.last_inbound_at = now
    incr("sms_inbound_total")

    admission_limited = (not tenant_admission.admitted) or is_rate_limited(
        redis_client=redis_conn,
        lead_id=lead.id,
        max_messages=settings.rate_limit_count,
        window_minutes=settings.rate_limit_window_minutes,
    )

    if decision.is_stop or decision.is_start:
        # Serialize consent changes with automated delivery. Senders hold the
        # same lead row lock from their final consent read through provider
        # acceptance and local persistence.
        db.flush()
        locked_lead = db.scalar(
            select(Lead)
            .where(Lead.id == lead.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if locked_lead is None:
            db.rollback()
            return _empty_twiml_response()
        lead = locked_lead

    if decision.is_stop:
        was_opted_out = lead.opted_out
        previous_state = lead.conversation_state
        previous_crm_stage = lead.crm_stage
        lead.opted_out = True
        lead.consented = False
        lead.conversation_state = ConversationStateEnum.OPTED_OUT
        lead.crm_stage = CRM_STAGE_LOST
        lead_payload = dict(lead.raw_payload or {})
        if not was_opted_out:
            lead_payload["opt_out"] = {
                "at": now.isoformat(),
                "method": "sms_stop_keyword",
                "message_sid": inbound_sid,
                "previous_state": previous_state.value,
                "previous_crm_stage": previous_crm_stage,
            }
            lead_payload["consent_evidence"] = {
                "granted": False,
                "status": "withdrawn",
                "method": "sms_stop_keyword",
                "captured_at": now.isoformat(),
                "source_fields": ["Body", "MessageSid"],
            }
        lead.raw_payload = lead_payload

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

        reply_text = sms_service.render_template(client, "stop_confirmation", context={})
        suppression_reason = ""
        if was_opted_out:
            suppression_reason = "already_opted_out"
        elif admission_limited:
            suppression_reason = "rate_limited"
        set_inbound_work_state(inbound_message, INBOUND_WORK_COMPLETED, now=now)
        _record_compliance_reply_once(
            db=db,
            client=client,
            lead=lead,
            inbound_sid=inbound_sid,
            inbound_text=body,
            event_type="compliance_stop",
            reason="stop",
            reply_text=reply_text,
            sms_service=sms_service,
            now=now,
            should_send=not suppression_reason,
            suppression_reason=suppression_reason,
        )
        return _empty_twiml_response()

    if decision.is_start:
        was_opted_out = lead.opted_out
        previous_state = lead.conversation_state
        previous_crm_stage = lead.crm_stage
        if was_opted_out:
            lead_payload = dict(lead.raw_payload or {})
            opt_out_snapshot = (
                lead_payload.get("opt_out")
                if isinstance(lead_payload.get("opt_out"), dict)
                else {}
            )
            try:
                restored_state = ConversationStateEnum(
                    str(opt_out_snapshot.get("previous_state") or "")
                )
            except ValueError:
                restored_state = ConversationStateEnum.QUALIFYING
            if restored_state == ConversationStateEnum.OPTED_OUT:
                restored_state = ConversationStateEnum.QUALIFYING
            saved_stage = str(opt_out_snapshot.get("previous_crm_stage") or "")
            restored_crm_stage = saved_stage if saved_stage in CRM_STAGE_SET else CRM_STAGE_CONTACTED

            lead.opted_out = False
            lead.consented = True
            lead.conversation_state = restored_state
            lead.crm_stage = restored_crm_stage
            lead_payload.pop("opt_out", None)
            lead_payload["last_resubscribe"] = {
                "at": now.isoformat(),
                "method": "sms_start_keyword",
                "message_sid": inbound_sid,
                "restored_state": restored_state.value,
                "restored_crm_stage": restored_crm_stage,
            }
            lead_payload["consent_evidence"] = {
                "granted": True,
                "status": "granted",
                "method": "sms_start_keyword",
                "captured_at": now.isoformat(),
                "source_fields": ["Body", "MessageSid"],
            }
            lead.raw_payload = lead_payload
            if previous_state != lead.conversation_state:
                db.add(
                    ConversationState(
                        lead_id=lead.id,
                        previous_state=previous_state,
                        new_state=lead.conversation_state,
                        reason="START keyword",
                        metadata_json={"consent_restored": True},
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
                            "reason": "resubscribe_start_keyword",
                            "inbound": body,
                        },
                    )
                )

        reply_text = sms_service.render_template(client, "start_confirmation", context={})
        suppression_reason = ""
        if not was_opted_out:
            suppression_reason = "already_subscribed"
        elif admission_limited:
            suppression_reason = "rate_limited"
        set_inbound_work_state(inbound_message, INBOUND_WORK_COMPLETED, now=now)
        _record_compliance_reply_once(
            db=db,
            client=client,
            lead=lead,
            inbound_sid=inbound_sid,
            inbound_text=body,
            event_type="compliance_start",
            reason="start",
            reply_text=reply_text,
            sms_service=sms_service,
            now=now,
            should_send=not suppression_reason,
            suppression_reason=suppression_reason,
        )
        return _empty_twiml_response()

    if admission_limited:
        set_inbound_work_state(inbound_message, INBOUND_WORK_SUPPRESSED, now=now)
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="rate_limited",
                decision={"inbound": body, "admission_stage": "before_media_and_reply"},
            )
        )
        db.commit()
        return _empty_twiml_response()

    if decision.is_help:
        reply_text = sms_service.render_template(client, "help_response", context={})
        help_reply_limited = is_rate_limited(
            redis_client=redis_conn,
            lead_id=lead.id,
            max_messages=1,
            window_minutes=settings.rate_limit_window_minutes,
            scope="compliance-help",
        )
        set_inbound_work_state(inbound_message, INBOUND_WORK_COMPLETED, now=now)
        _record_compliance_reply_once(
            db=db,
            client=client,
            lead=lead,
            inbound_sid=inbound_sid,
            inbound_text=body,
            event_type="compliance_help",
            reason="help",
            reply_text=reply_text,
            sms_service=sms_service,
            now=now,
            should_send=not help_reply_limited,
            suppression_reason="rate_limited" if help_reply_limited else "",
        )
        return _empty_twiml_response()

    if lead.opted_out:
        set_inbound_work_state(inbound_message, INBOUND_WORK_SUPPRESSED, now=now)
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

    if is_meaningful_inbound(body):
        _auto_update_crm_stage(
            db=db,
            lead=lead,
            client_id=client.id,
            target_stage=CRM_STAGE_QUALIFIED,
            reason="meaningful_inbound",
            inbound_text=body,
        )

    if media_items:
        set_inbound_work_state(inbound_message, INBOUND_WORK_WAITING_MEDIA, now=now)
        media_event_payload: dict[str, Any] = {
            "lead_id": lead.id,
            "message_id": inbound_message.id,
            "message_sid": inbound_sid,
            "media_items": media_items,
        }
        serialized_event = json.dumps(
            media_event_payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        event = InboundWebhookEvent(
            client_id=client.id,
            endpoint=INBOUND_MEDIA_EVENT_ENDPOINT,
            source="twilio",
            event_key=_inbound_media_event_key(
                client_id=client.id,
                inbound_sid=inbound_sid,
            ),
            payload_sha256=hashlib.sha256(serialized_event).hexdigest(),
            payload_json=media_event_payload,
            status="pending",
            attempt_count=0,
        )
        db.add(event)
        try:
            db.flush()
            inbound_message.raw_payload = {
                **(inbound_message.raw_payload or {}),
                "media_ingestion": {
                    "status": "pending",
                    "event_id": event.id,
                    "expected": len(media_items),
                },
            }
            db.commit()
        except IntegrityError:
            db.rollback()
            return _empty_twiml_response()

        _enqueue_inbound_media_or_raise(event_id=event.id, settings=settings)
        return _empty_twiml_response()

    # Persist the inbound SID and CRM transition before an eager worker can
    # reserve or send a response in its own transaction.
    set_inbound_work_state(inbound_message, INBOUND_WORK_QUEUED, now=now)
    db.commit()
    if settings.rq_eager:
        if claim_inbound_work(db=db, message_id=inbound_message.id):
            try:
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
                )
            except Exception as exc:
                fail_inbound_work_safely(
                    db=db,
                    message_id=inbound_message.id,
                    error_type=type(exc).__name__,
                )
                logger.warning(
                    "eager_inbound_sms_processing_failed",
                    extra={"message_id": inbound_message.id, "error_type": type(exc).__name__},
                )
            else:
                finish_inbound_work(db=db, message_id=inbound_message.id)
    else:
        _enqueue_persisted_inbound_work(
            db=db,
            client_id=client.id,
            message=inbound_message,
        )

    return _empty_twiml_response()
