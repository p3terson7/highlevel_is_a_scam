from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any

from redis import Redis
from rq import Queue, Retry
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.logging import get_logger
from app.core.metrics import incr
from app.db.models import AuditLog, Client, ConversationState, ConversationStateEnum, Lead, Message, MessageDirection
from app.db.session import get_session_factory
from app.services.compliance import within_operating_hours
from app.services.lead_intake import normalize_webhook_payload, upsert_lead
from app.services.runtime_config import load_runtime_overrides
from app.services.sms_service import build_sms_service

logger = get_logger(__name__)


@lru_cache
def get_redis_connection() -> Redis | None:
    settings = get_settings()
    try:
        return Redis.from_url(settings.redis_url)
    except Exception as exc:
        logger.exception("redis_connection_error", extra={"error": str(exc)})
        return None


@lru_cache
def get_queue() -> Queue | None:
    redis_conn = get_redis_connection()
    if redis_conn is None:
        return None
    return Queue("default", connection=redis_conn)


def _enqueue(task_func, *args, **kwargs):
    settings = get_settings()
    if settings.rq_eager:
        return task_func(*args, **kwargs)

    queue = get_queue()
    if queue is None:
        logger.warning("queue_unavailable_running_inline", extra={"task": task_func.__name__})
        return task_func(*args, **kwargs)

    return queue.enqueue(
        task_func,
        *args,
        retry=Retry(max=3, interval=[30, 120, 300]),
        **kwargs,
    )


def enqueue_process_webhook(client_id: int, source: str, payload: dict[str, Any]):
    return _enqueue(process_webhook_payload_task, client_id, source, payload)


def enqueue_send_initial_sms(lead_id: int):
    return _enqueue(send_initial_sms_task, lead_id)


def enqueue_followup_sms(lead_id: int, reason: str = "after_hours"):
    settings = get_settings()
    if settings.rq_eager:
        return send_followup_sms_task(lead_id=lead_id, reason=reason)

    queue = get_queue()
    if queue is None:
        return send_followup_sms_task(lead_id=lead_id, reason=reason)

    return queue.enqueue_in(
        timedelta(minutes=settings.after_hours_followup_minutes),
        send_followup_sms_task,
        lead_id,
        reason,
        retry=Retry(max=3, interval=[60, 240, 600]),
    )


def process_webhook_payload_task(client_id: int, source: str, payload: dict[str, Any]) -> dict[str, Any]:
    SessionLocal = get_session_factory()
    lead_ids_for_initial_sms: list[int] = []

    with SessionLocal() as db:
        client = db.get(Client, client_id)
        if client is None or not client.is_active:
            return {"status": "skipped", "reason": "client_not_found_or_inactive"}

        normalized = normalize_webhook_payload(source=source, payload=payload, client=client)
        for candidate in normalized:
            lead, created, should_send = upsert_lead(
                db=db,
                client=client,
                source=source,
                normalized=candidate,
            )
            db.add(
                AuditLog(
                    client_id=client.id,
                    lead_id=lead.id,
                    event_type="lead_normalized",
                    decision={
                        "source": source,
                        "created": created,
                        "should_send_initial_sms": should_send,
                        "external_lead_id": lead.external_lead_id,
                    },
                )
            )
            incr("leads_normalized_total")
            if should_send:
                lead_ids_for_initial_sms.append(lead.id)

        db.commit()

    for lead_id in lead_ids_for_initial_sms:
        enqueue_send_initial_sms(lead_id)

    return {"status": "ok", "processed": len(lead_ids_for_initial_sms), "total": len(normalized)}


def _record_outbound(
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


def send_initial_sms_task(lead_id: int) -> dict[str, Any]:
    SessionLocal = get_session_factory()
    settings = get_settings()

    enqueue_followup = False
    with SessionLocal() as db:
        runtime_overrides = load_runtime_overrides(db)
        sms_service = build_sms_service(settings, runtime_overrides=runtime_overrides)
        lead = db.get(Lead, lead_id)
        if lead is None:
            return {"status": "skipped", "reason": "lead_not_found"}
        client = db.get(Client, lead.client_id)
        if client is None:
            return {"status": "skipped", "reason": "client_not_found"}

        if lead.opted_out or not lead.phone:
            db.add(
                AuditLog(
                    client_id=client.id,
                    lead_id=lead.id,
                    event_type="initial_sms_skipped",
                    decision={"reason": "opted_out_or_missing_phone"},
                )
            )
            db.commit()
            return {"status": "skipped", "reason": "opted_out_or_missing_phone"}

        if lead.initial_sms_sent_at is not None:
            return {"status": "skipped", "reason": "already_sent"}

        first_name = lead.full_name.split(" ")[0] if lead.full_name else "there"
        context = {
            "first_name": first_name,
            "business_name": client.business_name,
            "booking_url": client.booking_url,
            "consent_text": client.consent_text,
        }

        if within_operating_hours(client):
            body = sms_service.render_template(client, "initial_sms", context=context)
            reason = "initial_sms_sent"
        else:
            body = sms_service.render_template(client, "after_hours", context=context)
            reason = "after_hours_initial_sms_sent"
            enqueue_followup = True

        provider_sid = sms_service.send_message(to_number=lead.phone, body=body)
        _record_outbound(db, lead=lead, body=body, provider_sid=provider_sid, raw_payload={"template": reason})

        now = datetime.now(timezone.utc)
        previous_state = lead.conversation_state
        lead.conversation_state = ConversationStateEnum.GREETED
        lead.initial_sms_sent_at = lead.initial_sms_sent_at or now
        lead.last_outbound_at = now

        if previous_state != lead.conversation_state:
            db.add(
                ConversationState(
                    lead_id=lead.id,
                    previous_state=previous_state,
                    new_state=lead.conversation_state,
                    reason=reason,
                    metadata_json={},
                )
            )

        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type=reason,
                decision={"body": body, "provider_sid": provider_sid},
            )
        )
        db.commit()

    if enqueue_followup:
        enqueue_followup_sms(lead_id=lead_id, reason="after_hours_followup")

    incr("sms_outbound_total")
    return {"status": "ok", "lead_id": lead_id}


def send_followup_sms_task(lead_id: int, reason: str = "after_hours_followup") -> dict[str, Any]:
    SessionLocal = get_session_factory()
    settings = get_settings()

    with SessionLocal() as db:
        runtime_overrides = load_runtime_overrides(db)
        sms_service = build_sms_service(settings, runtime_overrides=runtime_overrides)
        lead = db.get(Lead, lead_id)
        if lead is None or lead.opted_out or not lead.phone:
            return {"status": "skipped"}

        client = db.get(Client, lead.client_id)
        if client is None:
            return {"status": "skipped", "reason": "client_not_found"}

        body = sms_service.render_template(
            client,
            "follow_up",
            context={"booking_url": client.booking_url, "business_name": client.business_name},
        )
        provider_sid = sms_service.send_message(to_number=lead.phone, body=body)
        _record_outbound(db, lead=lead, body=body, provider_sid=provider_sid, raw_payload={"reason": reason})
        lead.last_outbound_at = datetime.now(timezone.utc)

        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="follow_up_sms_sent",
                decision={"reason": reason, "provider_sid": provider_sid},
            )
        )
        db.commit()

    incr("sms_outbound_total")
    return {"status": "ok", "lead_id": lead_id, "reason": reason}
