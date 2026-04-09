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
from app.db.session import get_session_factory
from app.services.booking import build_booking_service
from app.services.compliance import within_operating_hours
from app.services.crm import CRM_STAGE_CONTACTED, progress_crm_stage
from app.services.inbound_sms import already_processed_inbound_message, process_inbound_turn
from app.services.lead_intake import normalize_webhook_payload, upsert_lead
from app.services.lead_summary import build_lead_summary_text, normalize_form_answers
from app.services.llm_agent import build_llm_agent
from app.services.runtime_config import get_effective_runtime_map_for_client, load_runtime_overrides
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


def enqueue_process_inbound_sms(lead_id: int, inbound_message_id: int):
    settings = get_settings()
    if settings.rq_eager:
        return process_inbound_sms_task(lead_id=lead_id, inbound_message_id=inbound_message_id)

    queue = get_queue()
    if queue is None:
        logger.warning(
            "queue_unavailable_running_inline",
            extra={"task": "process_inbound_sms_task", "lead_id": lead_id, "inbound_message_id": inbound_message_id},
        )
        return process_inbound_sms_task(lead_id=lead_id, inbound_message_id=inbound_message_id)
    # Avoid automatic retries for inbound reply jobs to reduce duplicate sends.
    return queue.enqueue(process_inbound_sms_task, lead_id=lead_id, inbound_message_id=inbound_message_id)


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
    settings = get_settings()
    lead_ids_for_initial_sms: list[int] = []

    with SessionLocal() as db:
        client = db.get(Client, client_id)
        if client is None or not client.is_active:
            return {"status": "skipped", "reason": "client_not_found_or_inactive"}

        runtime_overrides = load_runtime_overrides(db)
        effective_runtime = get_effective_runtime_map_for_client(
            settings=settings,
            overrides=runtime_overrides,
            client=client,
        )
        normalized = normalize_webhook_payload(
            source=source,
            payload=payload,
            client=client,
            meta_access_token=effective_runtime["meta_access_token"],
            meta_api_version=effective_runtime["meta_graph_api_version"],
            request_timeout_seconds=settings.request_timeout_seconds,
        )
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


def process_inbound_sms_task(lead_id: int, inbound_message_id: int) -> dict[str, Any]:
    SessionLocal = get_session_factory()
    settings = get_settings()

    with SessionLocal() as db:
        runtime_overrides = load_runtime_overrides(db)
        lead = db.get(Lead, lead_id)
        if lead is None:
            return {"status": "skipped", "reason": "lead_not_found"}
        client = db.get(Client, lead.client_id)
        if client is None:
            return {"status": "skipped", "reason": "client_not_found"}
        inbound_message = db.get(Message, inbound_message_id)
        if inbound_message is None:
            return {"status": "skipped", "reason": "inbound_message_not_found"}
        if inbound_message.lead_id != lead.id or inbound_message.direction != MessageDirection.INBOUND:
            return {"status": "skipped", "reason": "inbound_message_mismatch"}
        if already_processed_inbound_message(db=db, lead_id=lead.id, inbound_message_id=inbound_message.id):
            return {"status": "skipped", "reason": "already_processed"}
        if lead.opted_out or not lead.phone:
            return {"status": "skipped", "reason": "opted_out_or_missing_phone"}

        effective_runtime = get_effective_runtime_map_for_client(
            settings=settings,
            overrides=runtime_overrides,
            client=client,
        )
        sms_service = build_sms_service(settings, runtime_overrides=effective_runtime)
        llm_agent = build_llm_agent(settings=settings, runtime_overrides=effective_runtime)
        booking_service = build_booking_service(timeout_seconds=settings.request_timeout_seconds)

        process_inbound_turn(
            db=db,
            client=client,
            lead=lead,
            inbound_text=str(inbound_message.body or ""),
            now=datetime.now(timezone.utc),
            sms_service=sms_service,
            booking_service=booking_service,
            llm_agent=llm_agent,
            inbound_message_id=inbound_message.id,
        )
        return {"status": "ok", "lead_id": lead.id, "inbound_message_id": inbound_message.id}


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


def _meta_initial_seed_text(lead: Lead) -> str:
    normalized_answers = normalize_form_answers(lead.form_answers or {})
    details: list[str] = []
    if lead.full_name:
        details.append(f"name={lead.full_name}")
    if lead.city:
        details.append(f"city={lead.city}")
    if lead.email:
        details.append(f"email={lead.email}")

    summary = build_lead_summary_text(normalized_answers, limit=6)
    if summary and summary != "No qualification details captured yet.":
        details.append(f"summary={summary}")

    context_blob = " | ".join(details) if details else "no extra lead details"
    return (
        "New lead submitted from Meta Lead Ads. "
        "This is the first outbound SMS after the form submit. "
        f"Lead context: {context_blob}."
    )


def send_initial_sms_task(lead_id: int) -> dict[str, Any]:
    SessionLocal = get_session_factory()
    settings = get_settings()

    enqueue_followup = False
    with SessionLocal() as db:
        runtime_overrides = load_runtime_overrides(db)
        lead = db.get(Lead, lead_id)
        if lead is None:
            return {"status": "skipped", "reason": "lead_not_found"}
        client = db.get(Client, lead.client_id)
        if client is None:
            return {"status": "skipped", "reason": "client_not_found"}
        effective_runtime = get_effective_runtime_map_for_client(
            settings=settings,
            overrides=runtime_overrides,
            client=client,
        )
        sms_service = build_sms_service(settings, runtime_overrides=effective_runtime)

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

        outbound_payload: dict[str, Any]
        next_state = ConversationStateEnum.GREETED
        if lead.source == LeadSource.META:
            llm_agent = build_llm_agent(settings=settings, runtime_overrides=effective_runtime)
            ai_seed = _meta_initial_seed_text(lead)
            ai_response = llm_agent.next_reply(
                client=client,
                lead=lead,
                inbound_text=ai_seed,
                history=[],
            )
            body = ai_response.reply_text.strip() or sms_service.render_template(client, "initial_sms", context=context)
            next_state = (
                ai_response.next_state
                if ai_response.next_state != ConversationStateEnum.NEW
                else ConversationStateEnum.QUALIFYING
            )
            reason = "initial_ai_sms_sent"
            qualification_memory = dict(lead.raw_payload or {})
            qualification_memory["qualification_memory"] = ai_response.collected_fields.model_dump(exclude_none=True)
            if ai_response.next_question_key:
                qualification_memory["last_question_key"] = ai_response.next_question_key
            else:
                qualification_memory.pop("last_question_key", None)
            pending_step = (ai_response.runtime_payload or {}).get("pending_step")
            if pending_step:
                qualification_memory["pending_step"] = pending_step
            else:
                qualification_memory.pop("pending_step", None)
            lead.raw_payload = qualification_memory
            outbound_payload = {
                "reason": reason,
                "provider": ai_response.provider,
                "provider_error": ai_response.provider_error,
                "agent": {
                    "action": ai_response.action,
                    "next_question_key": ai_response.next_question_key,
                    "collected_fields": ai_response.collected_fields.model_dump(exclude_none=True),
                    "provider": ai_response.provider,
                    "provider_error": ai_response.provider_error,
                },
                "actions": [action.model_dump() for action in ai_response.actions],
                "seed_context": ai_seed,
            }
        elif within_operating_hours(client):
            body = sms_service.render_template(client, "initial_sms", context=context)
            reason = "initial_sms_sent"
            outbound_payload = {"template": reason}
        else:
            body = sms_service.render_template(client, "after_hours", context=context)
            reason = "after_hours_initial_sms_sent"
            outbound_payload = {"template": reason}
            enqueue_followup = True

        provider_sid = sms_service.send_message(to_number=lead.phone, body=body)
        _record_outbound(db, lead=lead, body=body, provider_sid=provider_sid, raw_payload=outbound_payload)

        now = datetime.now(timezone.utc)
        previous_state = lead.conversation_state
        previous_crm_stage = lead.crm_stage
        lead.conversation_state = next_state
        lead.crm_stage = progress_crm_stage(lead.crm_stage, CRM_STAGE_CONTACTED)
        lead.initial_sms_sent_at = lead.initial_sms_sent_at or now
        lead.last_outbound_at = now

        if previous_state != lead.conversation_state:
            db.add(
                ConversationState(
                    lead_id=lead.id,
                    previous_state=previous_state,
                    new_state=lead.conversation_state,
                    reason=reason,
                    metadata_json=outbound_payload,
                )
            )
        if lead.crm_stage != previous_crm_stage:
            db.add(
                AuditLog(
                    client_id=client.id,
                    lead_id=lead.id,
                    event_type="crm_stage_auto_updated",
                    decision={
                        "previous_stage": previous_crm_stage,
                        "new_stage": lead.crm_stage,
                        "reason": "initial_outbound_sms",
                    },
                )
            )

        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type=reason,
                decision={
                    "body": body,
                    "provider_sid": provider_sid,
                    **outbound_payload,
                },
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
        lead = db.get(Lead, lead_id)
        if lead is None or lead.opted_out or not lead.phone:
            return {"status": "skipped"}

        client = db.get(Client, lead.client_id)
        if client is None:
            return {"status": "skipped", "reason": "client_not_found"}
        effective_runtime = get_effective_runtime_map_for_client(
            settings=settings,
            overrides=runtime_overrides,
            client=client,
        )
        sms_service = build_sms_service(settings, runtime_overrides=effective_runtime)

        body = sms_service.render_template(
            client,
            "follow_up",
            context={"booking_url": client.booking_url, "business_name": client.business_name},
        )
        provider_sid = sms_service.send_message(to_number=lead.phone, body=body)
        _record_outbound(db, lead=lead, body=body, provider_sid=provider_sid, raw_payload={"reason": reason})
        lead.last_outbound_at = datetime.now(timezone.utc)
        previous_crm_stage = lead.crm_stage
        lead.crm_stage = progress_crm_stage(lead.crm_stage, CRM_STAGE_CONTACTED)
        if lead.crm_stage != previous_crm_stage:
            db.add(
                AuditLog(
                    client_id=client.id,
                    lead_id=lead.id,
                    event_type="crm_stage_auto_updated",
                    decision={
                        "previous_stage": previous_crm_stage,
                        "new_stage": lead.crm_stage,
                        "reason": "follow_up_sms_sent",
                    },
                )
            )

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
