from __future__ import annotations

import asyncio
import hashlib
import math
import time
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

from redis import Redis
from redis.exceptions import RedisError
from rq import Queue, Retry
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.logging import get_logger
from app.core.metrics import incr
from app.db.models import (
    AuditLog,
    Client,
    ConversationState,
    ConversationStateEnum,
    InboundWebhookEvent,
    Lead,
    LeadSource,
    Message,
    MessageAttachment,
    MessageDirection,
)
from app.db.session import get_session_factory
from app.services.booking import build_booking_service
from app.services.compliance import within_operating_hours
from app.services.crm import CRM_STAGE_CONTACTED, progress_crm_stage
from app.services.inbound_sms import already_processed_inbound_message, process_inbound_turn
from app.services.inbound_work import (
    INBOUND_WORK_COMPLETED,
    INBOUND_WORK_DEAD_LETTER,
    INBOUND_WORK_SUPPRESSED,
    claim_inbound_work,
    fail_inbound_work_safely,
    finish_inbound_work,
    queue_inbound_work_after_media,
    release_inbound_work_enqueue,
    recoverable_inbound_work_plans,
    reserve_inbound_work_enqueue,
)
from app.services.lead_intake import normalize_webhook_payload, upsert_lead
from app.services.lead_summary import build_lead_summary_text, normalize_form_answers
from app.services.llm_agent import build_llm_agent
from app.services.message_media import (
    MessageMediaError,
    create_message_attachment,
    download_twilio_media,
    filename_from_url,
    media_storage_root,
    store_message_media,
)
from app.services.outbound_requests import (
    cancel_outbound_request,
    complete_outbound_request,
    fail_outbound_request,
    lock_lead_for_outbound_delivery,
    reserve_outbound_request,
)
from app.services.outbound_recovery import (
    OutboundRetryDirective,
    clear_outbound_recovery_enqueue_marker,
    reconcile_stale_outbound_requests,
)
from app.services.runtime_config import get_effective_runtime_map_for_client, load_runtime_overrides
from app.services.sms_service import SMSService, build_sms_service, classify_sms_delivery_failure

logger = get_logger(__name__)

_LEAD_WORKFLOW_LOCK_SECONDS = 180
_LEAD_WORKFLOW_BLOCK_SECONDS = 45
INBOUND_MEDIA_EVENT_ENDPOINT = "sms_inbound_media"
_MAX_INBOUND_MEDIA_ITEMS = 10
_DEFAULT_INBOUND_MEDIA_ITEM_BYTES = 25 * 1024 * 1024
_MAX_INBOUND_MEDIA_AGGREGATE_BYTES = 50 * 1024 * 1024
_MAX_INBOUND_MEDIA_TASK_SECONDS = 45.0
_INBOUND_MEDIA_PROCESSING_STALE_AFTER = timedelta(minutes=5)
_OUTBOUND_RECOVERY_INTERVAL = timedelta(minutes=5)
_OUTBOUND_RECOVERY_SCHEDULE_KEY = "outbound-delivery-recovery:scheduled"
_INBOUND_SMS_RECOVERY_INTERVAL = timedelta(minutes=5)
_INBOUND_SMS_RECOVERY_SCHEDULE_KEY = "inbound-sms-recovery:scheduled"


def _automated_sms_delay_seconds(settings) -> int:
    """Return the configured per-lead pacing interval.

    Startup validation bounds the real setting. The fallback keeps rolling
    workers and focused tests compatible while a deployment is being upgraded.
    """

    try:
        return max(0, int(getattr(settings, "automated_sms_delay_seconds", 20)))
    except (TypeError, ValueError):
        return 20


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _remaining_automated_sms_delay(
    settings,
    *anchors: datetime | None,
    now: datetime | None = None,
) -> int:
    """Seconds until both trigger and prior-outbound pacing windows expire."""

    delay_seconds = _automated_sms_delay_seconds(settings)
    if delay_seconds <= 0 or bool(getattr(settings, "rq_eager", False)):
        return 0
    normalized = [value for value in (_as_utc(anchor) for anchor in anchors) if value is not None]
    if not normalized:
        return 0
    current = _as_utc(now) or datetime.now(timezone.utc)
    remaining = (max(normalized) + timedelta(seconds=delay_seconds) - current).total_seconds()
    return max(0, math.ceil(remaining))


def _requeue_automated_task_for_pacing(
    task_func,
    *args,
    delay_seconds: int,
    retry: Retry | None = None,
) -> bool:
    """Requeue an automated task without sleeping or holding a worker."""

    if delay_seconds <= 0:
        return False
    queue = get_queue()
    if queue is None:
        raise RuntimeError("Automated SMS pacing requires the default RQ queue")
    enqueue_options: dict[str, Any] = {}
    if retry is not None:
        enqueue_options["retry"] = retry
    queue.enqueue_in(
        timedelta(seconds=delay_seconds),
        task_func,
        *args,
        **enqueue_options,
    )
    return True


@lru_cache
def get_redis_connection() -> Redis | None:
    settings = get_settings()
    try:
        return Redis.from_url(
            settings.redis_url,
            socket_connect_timeout=0.25,
            socket_timeout=0.25,
            retry_on_timeout=False,
            health_check_interval=30,
        )
    except Exception as exc:
        logger.exception("redis_connection_error", extra={"error": str(exc)})
        return None


@lru_cache
def get_queue() -> Queue | None:
    redis_conn = get_redis_connection()
    if redis_conn is None:
        return None
    return Queue("default", connection=redis_conn)


def _acquire_lead_workflow_lock(*, lead_id: int, purpose: str):
    redis_conn = get_redis_connection()
    if redis_conn is None:
        return None
    try:
        lock = redis_conn.lock(
            f"lead-workflow:{lead_id}",
            timeout=_LEAD_WORKFLOW_LOCK_SECONDS,
            blocking_timeout=_LEAD_WORKFLOW_BLOCK_SECONDS,
        )
        if lock.acquire(blocking=True):
            return lock
    except RedisError as exc:
        logger.warning(
            "lead_workflow_lock_unavailable",
            extra={"lead_id": lead_id, "purpose": purpose, "error": str(exc)},
        )
        return None
    logger.warning("lead_workflow_lock_contention", extra={"lead_id": lead_id, "purpose": purpose})
    return False


def _release_lead_workflow_lock(lock, *, lead_id: int, purpose: str) -> None:
    if not lock:
        return
    try:
        lock.release()
    except RedisError as exc:
        logger.warning(
            "lead_workflow_lock_release_failed",
            extra={"lead_id": lead_id, "purpose": purpose, "error": str(exc)},
        )


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


def enqueue_process_webhook_event(event_id: int):
    return _enqueue(process_webhook_event_task, event_id)


def enqueue_send_initial_sms(lead_id: int):
    settings = get_settings()
    if settings.rq_eager:
        return send_initial_sms_task(lead_id)

    queue = get_queue()
    if queue is None:
        # A missing scheduler must not turn a deliberately paced first message
        # into an immediate inline send. The durable webhook dispatch remains
        # recoverable when this exception propagates.
        raise RuntimeError("Initial SMS scheduling requires the default RQ queue")

    delay_seconds = _automated_sms_delay_seconds(settings)
    enqueue_options = {"retry": Retry(max=3, interval=[30, 120, 300])}
    if delay_seconds > 0:
        return queue.enqueue_in(
            timedelta(seconds=delay_seconds),
            send_initial_sms_task,
            lead_id,
            **enqueue_options,
        )
    return queue.enqueue(send_initial_sms_task, lead_id, **enqueue_options)


def enqueue_process_inbound_sms(lead_id: int, inbound_message_id: int):
    settings = get_settings()
    if settings.rq_eager:
        return process_inbound_sms_task(lead_id=lead_id, inbound_message_id=inbound_message_id)

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        reserved = reserve_inbound_work_enqueue(
            db=db,
            message_id=inbound_message_id,
        )
    if not reserved:
        # Another callback/recovery pass already handed this row to the queue,
        # or a worker has claimed/finished it. Treat that as a successful handoff.
        return True

    queue = get_queue()
    if queue is None:
        logger.warning(
            "inbound_sms_queue_unavailable",
            extra={"task": "process_inbound_sms_task", "lead_id": lead_id, "inbound_message_id": inbound_message_id},
        )
        with SessionLocal() as db:
            release_inbound_work_enqueue(db=db, message_id=inbound_message_id)
        return False
    # Avoid automatic retries for inbound reply jobs to reduce duplicate sends.
    try:
        delay_seconds = _automated_sms_delay_seconds(settings)
        if delay_seconds > 0:
            return queue.enqueue_in(
                timedelta(seconds=delay_seconds),
                process_inbound_sms_task,
                lead_id=lead_id,
                inbound_message_id=inbound_message_id,
            )
        return queue.enqueue(
            process_inbound_sms_task,
            lead_id=lead_id,
            inbound_message_id=inbound_message_id,
        )
    except Exception:
        with SessionLocal() as db:
            release_inbound_work_enqueue(db=db, message_id=inbound_message_id)
        raise


def enqueue_process_inbound_media_event(event_id: int):
    """Queue remote MMS work without ever running it inside the callback."""

    settings = get_settings()
    if settings.rq_eager:
        logger.info(
            "inbound_media_job_left_pending_in_eager_mode",
            extra={"event_id": event_id},
        )
        return False
    queue = get_queue()
    if queue is None:
        logger.warning(
            "inbound_media_queue_unavailable",
            extra={"event_id": event_id},
        )
        return False
    return queue.enqueue(
        process_inbound_media_event_task,
        event_id,
        retry=Retry(max=3, interval=[30, 120, 300]),
        job_timeout=max(60, int(_MAX_INBOUND_MEDIA_TASK_SECONDS) + 15),
    )


def enqueue_followup_sms(lead_id: int, reason: str = "after_hours"):
    settings = get_settings()
    if settings.rq_eager:
        return send_followup_sms_task(lead_id=lead_id, reason=reason)

    queue = get_queue()
    if queue is None:
        logger.warning(
            "followup_sms_queue_unavailable",
            extra={"lead_id": lead_id, "reason": reason},
        )
        return False

    return queue.enqueue_in(
        timedelta(minutes=settings.after_hours_followup_minutes),
        send_followup_sms_task,
        lead_id,
        reason,
        retry=Retry(max=3, interval=[60, 240, 600]),
    )


def enqueue_zapier_booking_retry(request_id: int) -> bool:
    """Schedule a retry without blocking the API request that created a booking."""

    settings = get_settings()
    if settings.rq_eager:
        return False
    queue = get_queue()
    if queue is None:
        logger.warning(
            "zapier_booking_retry_queue_unavailable",
            extra={"request_id": request_id},
        )
        return False
    queue.enqueue_in(
        timedelta(seconds=60),
        retry_zapier_booking_webhook_task,
        request_id,
        retry=Retry(max=3, interval=[120, 300, 900]),
    )
    return True


def retry_zapier_booking_webhook_task(request_id: int) -> dict[str, Any]:
    from app.services.zapier_booking import retry_zapier_booking_webhook_delivery

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        return retry_zapier_booking_webhook_delivery(db=db, request_id=request_id)


def recover_stale_outbound_requests(*, limit: int = 100) -> dict[str, int]:
    """Reconcile abandoned outbox rows and queue only proven-safe retries."""

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        result = reconcile_stale_outbound_requests(db=db, limit=limit)
        db.commit()

    retries_queued = 0
    for directive in result.retry_directives:
        try:
            _enqueue_outbound_recovery_retry(directive)
            retries_queued += 1
        except Exception as exc:
            logger.warning(
                "outbound_delivery_recovery_enqueue_failed",
                extra={
                    "request_id": directive.request_id,
                    "request_kind": directive.request_kind,
                    "error_type": type(exc).__name__,
                },
            )
            with SessionLocal() as db:
                clear_outbound_recovery_enqueue_marker(
                    db=db,
                    request_id=directive.request_id,
                    detail=str(exc),
                )
                db.commit()

    return {
        "pending_marked_ambiguous": result.pending_marked_ambiguous,
        "dead_lettered": result.dead_lettered,
        "retries_queued": retries_queued,
    }


def outbound_delivery_recovery_task(
    schedule_token: str = "",
    *,
    limit: int = 100,
) -> dict[str, int]:
    """Periodic recovery entrypoint scheduled by the application at startup."""

    redis_conn = get_redis_connection()
    if redis_conn is not None and schedule_token:
        _clear_outbound_recovery_schedule_marker(redis_conn, schedule_token)
    try:
        return recover_stale_outbound_requests(limit=limit)
    finally:
        ensure_outbound_delivery_recovery_scheduled()


def ensure_outbound_delivery_recovery_scheduled() -> bool:
    """Ensure one delayed recovery job exists across all application replicas."""

    settings = get_settings()
    if settings.rq_eager:
        return False
    redis_conn = get_redis_connection()
    queue = get_queue()
    if redis_conn is None or queue is None:
        return False

    marker_ttl = max(int(_OUTBOUND_RECOVERY_INTERVAL.total_seconds() * 2), 60)
    schedule_token = uuid4().hex
    try:
        marker_created = bool(
            redis_conn.set(
                _OUTBOUND_RECOVERY_SCHEDULE_KEY,
                schedule_token,
                nx=True,
                ex=marker_ttl,
            )
        )
    except RedisError as exc:
        logger.warning(
            "outbound_recovery_schedule_marker_failed",
            extra={"error_type": type(exc).__name__},
        )
        return False
    if not marker_created:
        return False

    try:
        queue.enqueue_in(
            _OUTBOUND_RECOVERY_INTERVAL,
            outbound_delivery_recovery_task,
            schedule_token,
        )
    except Exception:
        _clear_outbound_recovery_schedule_marker(redis_conn, schedule_token)
        raise
    return True


def _clear_outbound_recovery_schedule_marker(
    redis_conn: Redis,
    schedule_token: str,
) -> None:
    try:
        redis_conn.eval(
            """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            end
            return 0
            """,
            1,
            _OUTBOUND_RECOVERY_SCHEDULE_KEY,
            schedule_token,
        )
    except RedisError as exc:
        logger.warning(
            "outbound_recovery_schedule_marker_clear_failed",
            extra={"error_type": type(exc).__name__},
        )


def recover_inbound_sms_work(*, limit: int = 100) -> int:
    """Re-enqueue a bounded set of queued, retryable, or abandoned turns."""

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        plans = recoverable_inbound_work_plans(db=db, limit=limit)

    queued = 0
    for message_id, lead_id in plans:
        try:
            enqueued = enqueue_process_inbound_sms(
                lead_id=lead_id,
                inbound_message_id=message_id,
            )
            if enqueued is not False:
                queued += 1
        except Exception as exc:
            logger.warning(
                "inbound_sms_recovery_enqueue_failed",
                extra={"message_id": message_id, "error_type": type(exc).__name__},
            )
    return queued


def inbound_sms_recovery_task(
    schedule_token: str = "",
    *,
    limit: int = 100,
) -> int:
    redis_conn = get_redis_connection()
    if redis_conn is not None and schedule_token:
        _clear_inbound_sms_recovery_schedule_marker(redis_conn, schedule_token)
    try:
        return recover_inbound_sms_work(limit=limit)
    finally:
        ensure_inbound_sms_recovery_scheduled()


def ensure_inbound_sms_recovery_scheduled() -> bool:
    """Ensure one periodic durable-inbound recovery job across replicas."""

    settings = get_settings()
    if settings.rq_eager:
        return False
    redis_conn = get_redis_connection()
    queue = get_queue()
    if redis_conn is None or queue is None:
        return False

    marker_ttl = max(int(_INBOUND_SMS_RECOVERY_INTERVAL.total_seconds() * 2), 60)
    schedule_token = uuid4().hex
    try:
        marker_created = bool(
            redis_conn.set(
                _INBOUND_SMS_RECOVERY_SCHEDULE_KEY,
                schedule_token,
                nx=True,
                ex=marker_ttl,
            )
        )
    except RedisError as exc:
        logger.warning(
            "inbound_sms_recovery_schedule_marker_failed",
            extra={"error_type": type(exc).__name__},
        )
        return False
    if not marker_created:
        return False

    try:
        queue.enqueue_in(
            _INBOUND_SMS_RECOVERY_INTERVAL,
            inbound_sms_recovery_task,
            schedule_token,
        )
    except Exception:
        _clear_inbound_sms_recovery_schedule_marker(redis_conn, schedule_token)
        raise
    return True


def _clear_inbound_sms_recovery_schedule_marker(
    redis_conn: Redis,
    schedule_token: str,
) -> None:
    try:
        redis_conn.eval(
            """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            end
            return 0
            """,
            1,
            _INBOUND_SMS_RECOVERY_SCHEDULE_KEY,
            schedule_token,
        )
    except RedisError as exc:
        logger.warning(
            "inbound_sms_recovery_schedule_marker_clear_failed",
            extra={"error_type": type(exc).__name__},
        )


def _enqueue_outbound_recovery_retry(directive: OutboundRetryDirective) -> None:
    if directive.request_kind == "zapier_booking_webhook":
        if not enqueue_zapier_booking_retry(directive.request_id):
            raise RuntimeError("Zapier retry queue is unavailable")
        return

    queue = get_queue()
    if queue is None:
        raise RuntimeError("Outbound retry queue is unavailable")
    if directive.request_kind == "automated_initial_sms":
        queue.enqueue(send_initial_sms_task, directive.lead_id, True)
        return
    if directive.request_kind == "automated_followup_sms":
        queue.enqueue(
            send_followup_sms_task,
            directive.lead_id,
            directive.reason or "after_hours_followup",
            True,
        )
        return
    raise RuntimeError(f"Unsupported outbound retry kind: {directive.request_kind}")


def _process_webhook_payload_in_session(
    *,
    db: Session,
    client_id: int,
    source: str,
    payload: dict[str, Any],
) -> tuple[dict[str, Any], list[int]]:
    lead_ids_for_initial_sms: list[int] = []
    client = db.get(Client, client_id)
    if client is None or not client.is_active:
        return {"status": "skipped", "reason": "client_not_found_or_inactive"}, []

    normalized = normalize_webhook_payload(source=source, payload=payload)
    for candidate in normalized:
        try:
            with db.begin_nested():
                lead, created, should_send = upsert_lead(
                    db=db,
                    client=client,
                    source=source,
                    normalized=candidate,
                )
        except IntegrityError:
            lead = None
            if candidate.external_lead_id:
                lead = db.scalar(
                    select(Lead).where(
                        Lead.client_id == client.id,
                        Lead.external_lead_id == candidate.external_lead_id,
                    )
                )
            if lead is None and candidate.phone:
                lead = db.scalar(
                    select(Lead)
                    .where(Lead.client_id == client.id, Lead.phone == candidate.phone)
                    .order_by(Lead.created_at.desc())
                    .limit(1)
                )
            if lead is None and candidate.email:
                lead = db.scalar(
                    select(Lead)
                    .where(Lead.client_id == client.id, Lead.email == candidate.email)
                    .order_by(Lead.created_at.desc())
                    .limit(1)
                )
            if lead is None:
                raise
            created = False
            should_send = bool(
                lead.phone
                and lead.consented
                and not lead.opted_out
                and lead.initial_sms_sent_at is None
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

    return (
        {
            "status": "ok",
            "processed": len(lead_ids_for_initial_sms),
            "total": len(normalized),
        },
        lead_ids_for_initial_sms,
    )


def process_webhook_event_task(event_id: int) -> dict[str, Any]:
    """Process a durable inbox row and erase its transient payload on success."""

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        event = db.scalar(
            select(InboundWebhookEvent)
            .where(InboundWebhookEvent.id == event_id)
            .with_for_update()
        )
        if event is None:
            return {"status": "skipped", "reason": "webhook_event_not_found"}
        if event.status == "completed":
            return {"status": "skipped", "reason": "already_processed", "event_id": event.id}
        dispatch_payload = dict(event.payload_json or {})
        if event.status == "dispatching":
            lead_ids_for_initial_sms = [
                int(lead_id)
                for lead_id in dispatch_payload.get("initial_sms_lead_ids", [])
                if str(lead_id).isdigit()
            ]
            stored_result = dispatch_payload.get("result")
            result = dict(stored_result) if isinstance(stored_result, dict) else {"status": "ok"}
            db.commit()
        elif not event.payload_json:
            event.status = "failed"
            event.error_detail = "Webhook inbox payload is missing"
            event.updated_at = datetime.now(timezone.utc)
            db.commit()
            return {"status": "failed", "reason": "payload_missing", "event_id": event.id}
        else:
            event.status = "processing"
            event.attempt_count += 1
            event.error_detail = ""
            event.updated_at = datetime.now(timezone.utc)

            try:
                result, lead_ids_for_initial_sms = _process_webhook_payload_in_session(
                    db=db,
                    client_id=event.client_id,
                    source=event.source,
                    payload=dict(event.payload_json),
                )
            except Exception as exc:
                db.rollback()
                event = db.get(InboundWebhookEvent, event_id)
                if event is None:
                    raise
                event.status = "failed"
                event.error_detail = str(exc or "Webhook processing failed")[:500]
                event.updated_at = datetime.now(timezone.utc)
                db.commit()
                raise
            else:
                # Commit normalized leads and a non-PII dispatch plan together.
                # A crash during queue handoff can then resume without retaining
                # or reprocessing the original webhook body.
                event.status = "dispatching"
                event.payload_json = {
                    "initial_sms_lead_ids": lead_ids_for_initial_sms,
                    "result": result,
                }
                event.error_detail = ""
                event.updated_at = datetime.now(timezone.utc)
                db.commit()

    for lead_id in lead_ids_for_initial_sms:
        enqueue_send_initial_sms(lead_id)

    with SessionLocal() as db:
        event = db.scalar(
            select(InboundWebhookEvent)
            .where(InboundWebhookEvent.id == event_id)
            .with_for_update()
        )
        if event is not None and event.status != "completed":
            event.status = "completed"
            event.payload_json = {}
            event.error_detail = ""
            event.processed_at = datetime.now(timezone.utc)
            event.updated_at = event.processed_at
            db.commit()
    return {**result, "event_id": event_id}


def recover_webhook_inbox_events(*, limit: int = 100) -> int:
    """Re-enqueue durable inbox rows left behind by a crash or Redis loss."""

    SessionLocal = get_session_factory()
    stale_processing_before = datetime.now(timezone.utc) - timedelta(minutes=5)
    with SessionLocal() as db:
        events = db.scalars(
            select(InboundWebhookEvent)
            .where(
                InboundWebhookEvent.status.in_(
                    ("pending", "queued", "failed", "processing", "dispatching")
                ),
            )
            .order_by(InboundWebhookEvent.created_at.asc(), InboundWebhookEvent.id.asc())
            .limit(max(1, min(int(limit), 1_000)))
        ).all()

        event_plans: list[tuple[int, str]] = []
        for event in events:
            if event.status == "processing":
                updated_at = event.updated_at
                if updated_at.tzinfo is None:
                    updated_at = updated_at.replace(tzinfo=timezone.utc)
                if updated_at > stale_processing_before:
                    continue
            if event.status != "dispatching":
                event.status = "queued"
            event.error_detail = ""
            event.updated_at = datetime.now(timezone.utc)
            event_plans.append((event.id, event.endpoint))
        db.commit()

    queued = 0
    for event_id, endpoint in event_plans:
        try:
            if endpoint == INBOUND_MEDIA_EVENT_ENDPOINT:
                enqueued = enqueue_process_inbound_media_event(event_id)
            else:
                enqueued = enqueue_process_webhook_event(event_id)
            if enqueued is not False:
                queued += 1
        except Exception as exc:
            logger.warning(
                "webhook_inbox_recovery_enqueue_failed",
                extra={"event_id": event_id, "error_type": type(exc).__name__},
            )
    try:
        recovered_inbound = recover_inbound_sms_work(limit=limit)
        if recovered_inbound:
            logger.info(
                "inbound_sms_work_recovered",
                extra={"message_count": recovered_inbound},
            )
    except Exception as exc:
        logger.exception(
            "inbound_sms_work_recovery_failed",
            extra={"error_type": type(exc).__name__},
        )
    try:
        recovery_result = recover_stale_outbound_requests(limit=limit)
        if any(recovery_result.values()):
            logger.info("outbound_delivery_recovered", extra=recovery_result)
    except Exception as exc:
        logger.exception(
            "outbound_delivery_recovery_failed",
            extra={"error_type": type(exc).__name__},
        )
    try:
        ensure_outbound_delivery_recovery_scheduled()
    except Exception as exc:
        logger.warning(
            "outbound_delivery_recovery_schedule_failed",
            extra={"error_type": type(exc).__name__},
        )
    try:
        ensure_inbound_sms_recovery_scheduled()
    except Exception as exc:
        logger.warning(
            "inbound_sms_recovery_schedule_failed",
            extra={"error_type": type(exc).__name__},
        )
    return queued


def process_webhook_payload_task(client_id: int, source: str, payload: dict[str, Any]) -> dict[str, Any]:
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        result, lead_ids_for_initial_sms = _process_webhook_payload_in_session(
            db=db,
            client_id=client_id,
            source=source,
            payload=payload,
        )
        db.commit()

    for lead_id in lead_ids_for_initial_sms:
        enqueue_send_initial_sms(lead_id)

    return result


def _normalized_inbound_media_items(raw_items: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_items, list):
        return []
    items: list[dict[str, Any]] = []
    seen_indices: set[int] = set()
    for raw_item in raw_items[:_MAX_INBOUND_MEDIA_ITEMS]:
        if not isinstance(raw_item, dict):
            continue
        try:
            index = int(raw_item.get("index") or 0)
        except (TypeError, ValueError):
            continue
        media_url = str(raw_item.get("url") or "").strip()
        content_type = str(raw_item.get("content_type") or "").strip()
        if index < 0 or index >= _MAX_INBOUND_MEDIA_ITEMS or index in seen_indices:
            continue
        if not media_url or len(media_url) > 4096 or len(content_type) > 128:
            continue
        seen_indices.add(index)
        items.append({"index": index, "url": media_url, "content_type": content_type})
    return items


def _inbound_attachment_payload(attachment: MessageAttachment) -> dict[str, Any]:
    return {
        "id": attachment.id,
        "filename": attachment.filename,
        "content_type": attachment.content_type,
        "media_kind": attachment.media_kind,
        "size_bytes": attachment.size_bytes,
        "url": f"/media/public/{attachment.public_token}",
    }


def _safe_media_worker_error(exc: Exception) -> str:
    if isinstance(exc, MessageMediaError):
        return str(exc)[:500]
    if isinstance(exc, TimeoutError):
        return "MMS download exceeded the aggregate processing time limit."
    return f"Media download failed ({type(exc).__name__})."


async def _download_inbound_media_with_timeout(*, aggregate_timeout_seconds: float, **kwargs) -> bytes:
    try:
        return await asyncio.wait_for(
            download_twilio_media(**kwargs),
            timeout=max(0.1, aggregate_timeout_seconds),
        )
    except asyncio.TimeoutError as exc:
        # Python 3.10 exposes asyncio.TimeoutError as a separate exception,
        # while newer Python versions alias it to the built-in TimeoutError.
        # Normalize the worker boundary so retry/error classification is stable
        # across every supported runtime.
        raise TimeoutError("MMS download exceeded the aggregate processing time limit") from exc


def _record_inbound_media_failure(
    *,
    db: Session,
    client_id: int,
    lead_id: int,
    item: dict[str, Any],
    exc: Exception,
) -> None:
    media_host = (urlparse(str(item.get("url") or "")).hostname or "").lower()
    db.add(
        AuditLog(
            client_id=client_id,
            lead_id=lead_id,
            event_type="inbound_media_download_failed",
            decision={
                "media_index": int(item.get("index") or 0),
                "media_host": media_host,
                "content_type": str(item.get("content_type") or "")[:128],
                "error": _safe_media_worker_error(exc),
            },
        )
    )


def _complete_inbound_media_dispatch(
    *, event_id: int, lead_id: int, message_id: int, result: dict[str, Any]
) -> dict[str, Any]:
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        work_status = queue_inbound_work_after_media(
            db=db,
            message_id=message_id,
        )
    if work_status is None:
        raise RuntimeError("Inbound media message is missing")
    enqueued = enqueue_process_inbound_sms(
        lead_id=lead_id,
        inbound_message_id=message_id,
    )
    if enqueued is False:
        raise RuntimeError("Inbound SMS queue is unavailable")
    with SessionLocal() as db:
        event = db.scalar(
            select(InboundWebhookEvent)
            .where(InboundWebhookEvent.id == event_id)
            .with_for_update()
        )
        if event is not None and event.status != "completed":
            event.status = "completed"
            event.payload_json = {}
            event.error_detail = ""
            event.processed_at = datetime.now(timezone.utc)
            event.updated_at = event.processed_at
            db.commit()
    return {**result, "event_id": event_id}


def process_inbound_media_event_task(event_id: int) -> dict[str, Any]:
    """Download a persisted Twilio MMS plan outside the HTTP callback."""

    SessionLocal = get_session_factory()
    settings = get_settings()
    dispatch_plan: tuple[int, int, dict[str, Any]] | None = None
    with SessionLocal() as db:
        event = db.get(InboundWebhookEvent, event_id)
        if event is None:
            return {"status": "skipped", "reason": "media_event_not_found"}
        if event.endpoint != INBOUND_MEDIA_EVENT_ENDPOINT:
            return {"status": "skipped", "reason": "not_media_event", "event_id": event_id}
        if event.status == "completed":
            return {"status": "skipped", "reason": "already_processed", "event_id": event_id}
        event_payload = dict(event.payload_json or {})
        lead_id = int(event_payload.get("lead_id") or 0)
        message_id = int(event_payload.get("message_id") or 0)
        if event.status == "dispatching":
            stored_result = event_payload.get("result")
            result = dict(stored_result) if isinstance(stored_result, dict) else {"status": "ok"}
            dispatch_plan = (lead_id, message_id, result)

    if dispatch_plan is not None:
        lead_id, message_id, result = dispatch_plan
        return _complete_inbound_media_dispatch(
            event_id=event_id,
            lead_id=lead_id,
            message_id=message_id,
            result=result,
        )

    lock = _acquire_lead_workflow_lock(lead_id=lead_id, purpose="process_inbound_media")
    if lock is False:
        queue = get_queue()
        if queue is not None and not settings.rq_eager:
            queue.enqueue_in(timedelta(seconds=15), process_inbound_media_event_task, event_id)
        return {"status": "requeued", "reason": "lead_locked", "event_id": event_id}

    try:
        with SessionLocal() as db:
            event = db.scalar(
                select(InboundWebhookEvent)
                .where(InboundWebhookEvent.id == event_id)
                .with_for_update()
            )
            if event is None:
                return {"status": "skipped", "reason": "media_event_not_found"}
            if event.status == "completed":
                return {"status": "skipped", "reason": "already_processed", "event_id": event_id}
            event_payload = dict(event.payload_json or {})
            lead_id = int(event_payload.get("lead_id") or 0)
            message_id = int(event_payload.get("message_id") or 0)
            if event.status == "dispatching":
                stored_result = event_payload.get("result")
                result = dict(stored_result) if isinstance(stored_result, dict) else {"status": "ok"}
                db.commit()
                _release_lead_workflow_lock(
                    lock,
                    lead_id=lead_id,
                    purpose="process_inbound_media",
                )
                lock = None
                return _complete_inbound_media_dispatch(
                    event_id=event_id,
                    lead_id=lead_id,
                    message_id=message_id,
                    result=result,
                )
            if event.status == "processing":
                updated_at = event.updated_at
                if updated_at.tzinfo is None:
                    updated_at = updated_at.replace(tzinfo=timezone.utc)
                if updated_at > datetime.now(timezone.utc) - _INBOUND_MEDIA_PROCESSING_STALE_AFTER:
                    return {"status": "skipped", "reason": "already_processing", "event_id": event_id}

            claim_time = datetime.now(timezone.utc)
            stale_before = claim_time - _INBOUND_MEDIA_PROCESSING_STALE_AFTER
            claim = db.execute(
                update(InboundWebhookEvent)
                .where(
                    InboundWebhookEvent.id == event_id,
                    InboundWebhookEvent.status.in_(("pending", "queued", "failed"))
                    | (
                        (InboundWebhookEvent.status == "processing")
                        & (InboundWebhookEvent.updated_at <= stale_before)
                    ),
                )
                .values(
                    status="processing",
                    attempt_count=InboundWebhookEvent.attempt_count + 1,
                    error_detail="",
                    updated_at=claim_time,
                )
            )
            if claim.rowcount != 1:
                db.rollback()
                return {
                    "status": "skipped",
                    "reason": "media_event_already_claimed",
                    "event_id": event_id,
                }
            db.commit()

            message = db.get(Message, message_id)
            lead = db.get(Lead, lead_id)
            client = db.get(Client, event.client_id)
            if (
                message is None
                or lead is None
                or client is None
                or message.client_id != event.client_id
                or message.lead_id != lead.id
                or message.direction != MessageDirection.INBOUND
            ):
                event.status = "failed"
                event.error_detail = "MMS event references are invalid"
                event.updated_at = datetime.now(timezone.utc)
                db.commit()
                return {"status": "failed", "reason": "invalid_media_event_references", "event_id": event_id}

            media_items = _normalized_inbound_media_items(event_payload.get("media_items"))
            runtime_overrides = load_runtime_overrides(db)
            effective_runtime = get_effective_runtime_map_for_client(
                settings=settings,
                overrides=runtime_overrides,
                client=client,
            )
            attachments = db.scalars(
                select(MessageAttachment)
                .where(MessageAttachment.message_id == message_id)
                .order_by(MessageAttachment.id.asc())
            ).all()
            completed_indices = {
                int((attachment.raw_payload or {}).get("media_index"))
                for attachment in attachments
                if str((attachment.raw_payload or {}).get("media_index", "")).isdigit()
            }
            total_bytes = sum(max(0, int(attachment.size_bytes or 0)) for attachment in attachments)
            configured_item_limit = int(settings.message_media_max_bytes or 0)
            per_item_limit = (
                configured_item_limit
                if configured_item_limit > 0
                else _DEFAULT_INBOUND_MEDIA_ITEM_BYTES
            )
            aggregate_limit = min(
                _MAX_INBOUND_MEDIA_AGGREGATE_BYTES,
                per_item_limit * max(1, len(media_items)),
            )
            deadline = time.monotonic() + max(0.1, _MAX_INBOUND_MEDIA_TASK_SECONDS)

            for item in media_items:
                index = int(item["index"])
                if index in completed_indices:
                    continue
                remaining_bytes = aggregate_limit - total_bytes
                remaining_seconds = deadline - time.monotonic()
                if remaining_bytes <= 0:
                    _record_inbound_media_failure(
                        db=db,
                        client_id=client.id,
                        lead_id=lead.id,
                        item=item,
                        exc=MessageMediaError("MMS attachments exceed the aggregate byte limit."),
                    )
                    db.commit()
                    continue
                if remaining_seconds <= 0:
                    _record_inbound_media_failure(
                        db=db,
                        client_id=client.id,
                        lead_id=lead.id,
                        item=item,
                        exc=TimeoutError(),
                    )
                    db.commit()
                    continue

                stored = None
                try:
                    content = asyncio.run(
                        _download_inbound_media_with_timeout(
                            aggregate_timeout_seconds=remaining_seconds,
                            media_url=str(item["url"]),
                            content_type=str(item["content_type"]),
                            account_sid=effective_runtime.get("twilio_account_sid", ""),
                            auth_token=effective_runtime.get("twilio_auth_token", ""),
                            max_bytes=min(per_item_limit, remaining_bytes),
                            timeout_seconds=min(
                                max(1, int(settings.request_timeout_seconds)),
                                max(1, int(remaining_seconds)),
                            ),
                        )
                    )
                    if len(content) > remaining_bytes:
                        raise MessageMediaError("MMS attachments exceed the aggregate byte limit.")
                    stored = store_message_media(
                        settings=settings,
                        client_id=client.id,
                        message_id=message.id,
                        filename=filename_from_url(
                            str(item["url"]), str(item["content_type"]), index=index
                        ),
                        content_type=str(item["content_type"]),
                        content=content,
                        provider_media_url=str(item["url"]),
                        raw_payload={
                            "source": "twilio_mms",
                            "media_index": index,
                            "provider_media_url": str(item["url"]),
                        },
                    )
                    attachment = create_message_attachment(message=message, lead=lead, stored=stored)
                    db.add(attachment)
                    db.commit()
                    completed_indices.add(index)
                    total_bytes += len(content)
                except Exception as exc:
                    db.rollback()
                    if stored is not None:
                        (media_storage_root(settings) / stored.storage_path).unlink(missing_ok=True)
                    _record_inbound_media_failure(
                        db=db,
                        client_id=client.id,
                        lead_id=lead.id,
                        item=item,
                        exc=exc,
                    )
                    db.commit()

            message = db.get(Message, message_id)
            attachments = db.scalars(
                select(MessageAttachment)
                .where(MessageAttachment.message_id == message_id)
                .order_by(MessageAttachment.id.asc())
            ).all()
            attachment_payloads = [_inbound_attachment_payload(attachment) for attachment in attachments]
            expected_indices = {int(item["index"]) for item in media_items}
            saved_indices = {
                int((attachment.raw_payload or {}).get("media_index"))
                for attachment in attachments
                if str((attachment.raw_payload or {}).get("media_index", "")).isdigit()
            }
            failed_count = len(expected_indices - saved_indices)
            result = {
                "status": "ok" if failed_count == 0 else ("partial" if attachments else "failed"),
                "saved": len(attachments),
                "failed": failed_count,
                "message_id": message_id,
            }
            message_payload = dict(message.raw_payload or {})
            message_payload["attachments"] = attachment_payloads
            message_payload["num_media_saved"] = len(attachments)
            message_payload["media_ingestion"] = {
                "status": result["status"],
                "saved": len(attachments),
                "failed": failed_count,
                "event_id": event_id,
            }
            message.raw_payload = message_payload
            event = db.get(InboundWebhookEvent, event_id)
            event.status = "dispatching"
            event.payload_json = {"lead_id": lead_id, "message_id": message_id, "result": result}
            event.error_detail = ""
            event.updated_at = datetime.now(timezone.utc)
            db.commit()

        _release_lead_workflow_lock(
            lock,
            lead_id=lead_id,
            purpose="process_inbound_media",
        )
        lock = None
        return _complete_inbound_media_dispatch(
            event_id=event_id,
            lead_id=lead_id,
            message_id=message_id,
            result=result,
        )
    except Exception as exc:
        with SessionLocal() as db:
            event = db.get(InboundWebhookEvent, event_id)
            if event is not None and event.status not in {"completed", "dispatching"}:
                event.status = "failed"
                event.error_detail = _safe_media_worker_error(exc)
                event.updated_at = datetime.now(timezone.utc)
                db.commit()
        raise
    finally:
        _release_lead_workflow_lock(lock, lead_id=lead_id, purpose="process_inbound_media")


def process_inbound_sms_task(
    lead_id: int,
    inbound_message_id: int,
    retry_definitive_failure: bool = False,
) -> dict[str, Any]:
    SessionLocal = get_session_factory()
    settings = get_settings()

    lock = _acquire_lead_workflow_lock(lead_id=lead_id, purpose="process_inbound_sms")
    if lock is False:
        queue = get_queue()
        if queue is not None and not settings.rq_eager:
            queue.enqueue_in(
                timedelta(seconds=15),
                process_inbound_sms_task,
                lead_id,
                inbound_message_id,
                retry_definitive_failure,
            )
            return {"status": "requeued", "reason": "lead_locked", "lead_id": lead_id}
        return {"status": "skipped", "reason": "lead_locked", "lead_id": lead_id}

    try:
        with SessionLocal() as db:
            inbound_message = db.get(Message, inbound_message_id)
            if inbound_message is None:
                return {"status": "skipped", "reason": "inbound_message_not_found"}
            if inbound_message.lead_id != lead_id or inbound_message.direction != MessageDirection.INBOUND:
                return {"status": "skipped", "reason": "inbound_message_mismatch"}
            lead = db.get(Lead, lead_id)
            if lead is None:
                return {"status": "skipped", "reason": "lead_not_found"}
            client = db.get(Client, lead.client_id)
            if client is None:
                return {"status": "skipped", "reason": "client_not_found"}
            if inbound_message.inbound_work_status in {
                INBOUND_WORK_COMPLETED,
                INBOUND_WORK_SUPPRESSED,
                INBOUND_WORK_DEAD_LETTER,
            }:
                return {"status": "skipped", "reason": "work_not_recoverable_or_claimed"}
            pacing_delay = _remaining_automated_sms_delay(
                settings,
                inbound_message.created_at,
                lead.last_outbound_at,
            )
            if _requeue_automated_task_for_pacing(
                process_inbound_sms_task,
                lead_id,
                inbound_message_id,
                retry_definitive_failure,
                delay_seconds=pacing_delay,
            ):
                return {
                    "status": "requeued",
                    "reason": "automated_sms_pacing",
                    "lead_id": lead_id,
                    "delay_seconds": pacing_delay,
                }
            if not claim_inbound_work(
                db=db,
                message_id=inbound_message.id,
                allow_untracked_job=True,
            ):
                return {"status": "skipped", "reason": "work_not_recoverable_or_claimed"}

            try:
                if already_processed_inbound_message(
                    db=db,
                    lead_id=lead.id,
                    inbound_message_id=inbound_message.id,
                ):
                    finish_inbound_work(db=db, message_id=inbound_message.id)
                    return {"status": "skipped", "reason": "already_processed"}
                if lead.opted_out or not lead.consented or not lead.phone:
                    finish_inbound_work(
                        db=db,
                        message_id=inbound_message.id,
                        status=INBOUND_WORK_SUPPRESSED,
                    )
                    return {"status": "skipped", "reason": "missing_sms_permission_or_phone"}

                runtime_overrides = load_runtime_overrides(db)
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
                    retry_definitive_failure=retry_definitive_failure,
                )
                finish_inbound_work(db=db, message_id=inbound_message.id)
                return {
                    "status": "ok",
                    "lead_id": lead.id,
                    "inbound_message_id": inbound_message.id,
                }
            except Exception as exc:
                fail_inbound_work_safely(
                    db=db,
                    message_id=inbound_message.id,
                    error_type=type(exc).__name__,
                )
                raise
    finally:
        _release_lead_workflow_lock(lock, lead_id=lead_id, purpose="process_inbound_sms")


def _record_outbound(
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


def send_initial_sms_task(
    lead_id: int,
    retry_definitive_failure: bool = False,
) -> dict[str, Any]:
    SessionLocal = get_session_factory()
    settings = get_settings()

    enqueue_followup = False
    lock = _acquire_lead_workflow_lock(lead_id=lead_id, purpose="send_initial_sms")
    if lock is False:
        queue = get_queue()
        if queue is not None and not settings.rq_eager:
            queue.enqueue_in(
                timedelta(seconds=15),
                send_initial_sms_task,
                lead_id,
                retry_definitive_failure,
            )
            return {"status": "requeued", "reason": "lead_locked", "lead_id": lead_id}
        return {"status": "skipped", "reason": "lead_locked", "lead_id": lead_id}

    try:
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

            if lead.opted_out or not lead.consented or not lead.phone:
                db.add(
                    AuditLog(
                        client_id=client.id,
                        lead_id=lead.id,
                        event_type="initial_sms_skipped",
                        decision={"reason": "missing_sms_permission_or_phone"},
                    )
                )
                db.commit()
                return {"status": "skipped", "reason": "missing_sms_permission_or_phone"}

            if lead.initial_sms_sent_at is not None:
                return {"status": "skipped", "reason": "already_sent"}

            if lead.last_inbound_at is not None or lead.last_outbound_at is not None:
                db.add(
                    AuditLog(
                        client_id=client.id,
                        lead_id=lead.id,
                        event_type="initial_sms_skipped",
                        decision={"reason": "conversation_already_started"},
                    )
                )
                db.commit()
                return {"status": "skipped", "reason": "conversation_already_started"}

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
                for key in (
                    "cta_state",
                    "intent_level",
                    "intent_score",
                    "intent_reasons",
                    "important_missing_fields",
                    "lead_summary",
                    "recommended_follow_up",
                ):
                    if key in (ai_response.runtime_payload or {}):
                        qualification_memory[key] = ai_response.runtime_payload[key]
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
                        "intent_level": (ai_response.runtime_payload or {}).get("intent_level"),
                        "intent_score": (ai_response.runtime_payload or {}).get("intent_score"),
                        "cta_state": (ai_response.runtime_payload or {}).get("cta_state"),
                        "lead_summary": (ai_response.runtime_payload or {}).get("lead_summary"),
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

            reservation = reserve_outbound_request(
                db=db,
                lead=lead,
                idempotency_key=f"automated-initial-sms:{lead.id}",
                request_kind="automated_initial_sms",
                fingerprint_data={"lead_id": lead.id, "reason": reason},
                pending_response={
                    "reason": reason,
                    "body": body,
                    "outbound_payload": outbound_payload,
                    "next_state": next_state.value,
                    "attempt_count": 1,
                    "max_attempts": 3,
                },
                retry_failed=retry_definitive_failure,
                require_safe_retry=retry_definitive_failure,
            )
            if not reservation.should_send:
                db.add(
                    AuditLog(
                        client_id=client.id,
                        lead_id=lead.id,
                        event_type="initial_sms_skipped",
                        decision={"reason": f"delivery_{reservation.status}"},
                    )
                )
                db.commit()
                return {
                    "status": "skipped",
                    "reason": f"delivery_{reservation.status}",
                    "lead_id": lead.id,
                }

            body = str(reservation.response.get("body") or body)
            stored_outbound_payload = reservation.response.get("outbound_payload")
            if isinstance(stored_outbound_payload, dict):
                outbound_payload = stored_outbound_payload
            stored_next_state = str(reservation.response.get("next_state") or "")
            if stored_next_state:
                try:
                    next_state = ConversationStateEnum(stored_next_state)
                except ValueError:
                    pass

            delivery_state = lock_lead_for_outbound_delivery(db=db, lead_id=lead.id)
            if delivery_state is None:
                cancel_outbound_request(
                    db=db,
                    request_id=reservation.request_id,
                    reason="consent_withdrawn_before_send",
                    response={"reason": reason},
                )
                db.add(
                    AuditLog(
                        client_id=client.id,
                        lead_id=lead.id,
                        event_type="initial_sms_skipped",
                        decision={"reason": "consent_withdrawn_before_send"},
                    )
                )
                db.commit()
                return {
                    "status": "skipped",
                    "reason": "consent_withdrawn_before_send",
                    "lead_id": lead.id,
                }

            try:
                provider_sid = sms_service.send_message(to_number=delivery_state.phone, body=body)
            except Exception as exc:
                failure = classify_sms_delivery_failure(exc)
                fail_outbound_request(
                    db=db,
                    request_id=reservation.request_id,
                    detail=exc,
                    ambiguous=failure.ambiguous,
                    response={
                        "reason": reason,
                        "failure_reason": failure.reason,
                        "safe_to_retry": failure.safe_to_retry,
                        "provider_status": failure.provider_status,
                        "provider_code": failure.provider_code,
                        "last_failed_at": datetime.now(timezone.utc).isoformat(),
                    },
                    merge_response=True,
                )
                db.add(
                    AuditLog(
                        client_id=client.id,
                        lead_id=lead.id,
                        event_type="initial_sms_failed",
                        decision={
                            "reason": reason,
                            "failure_reason": failure.reason,
                            "delivery_result_unknown": failure.ambiguous,
                            "safe_to_retry": failure.safe_to_retry,
                            "provider_status": failure.provider_status,
                            "provider_code": failure.provider_code,
                            "error": str(exc)[:500],
                        },
                    )
                )
                db.commit()
                return {
                    "status": "failed",
                    "reason": failure.reason,
                    "lead_id": lead.id,
                }
            _record_outbound(
                db,
                lead=lead,
                body=body,
                provider_sid=provider_sid,
                raw_payload=outbound_payload,
                sms_service=sms_service,
            )
            complete_outbound_request(
                db=db,
                request_id=reservation.request_id,
                provider_reference=provider_sid,
                response={
                    "reason": reason,
                    "provider_sid": provider_sid,
                    "attempt_count": reservation.response.get("attempt_count", 1),
                },
            )

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
    finally:
        _release_lead_workflow_lock(lock, lead_id=lead_id, purpose="send_initial_sms")

    if enqueue_followup:
        enqueue_followup_sms(lead_id=lead_id, reason="after_hours_followup")

    incr("sms_outbound_total")
    return {"status": "ok", "lead_id": lead_id}


def send_followup_sms_task(
    lead_id: int,
    reason: str = "after_hours_followup",
    retry_definitive_failure: bool = False,
) -> dict[str, Any]:
    SessionLocal = get_session_factory()
    settings = get_settings()

    lock = _acquire_lead_workflow_lock(lead_id=lead_id, purpose="send_followup_sms")
    if lock is False:
        queue = get_queue()
        if queue is not None and not settings.rq_eager:
            queue.enqueue_in(
                timedelta(seconds=15),
                send_followup_sms_task,
                lead_id,
                reason,
                retry_definitive_failure,
            )
            return {"status": "requeued", "reason": "lead_locked", "lead_id": lead_id}
        return {"status": "skipped", "reason": "lead_locked", "lead_id": lead_id}

    try:
        with SessionLocal() as db:
            runtime_overrides = load_runtime_overrides(db)
            lead = db.get(Lead, lead_id)
            if lead is None or lead.opted_out or not lead.consented or not lead.phone:
                return {"status": "skipped"}

            client = db.get(Client, lead.client_id)
            if client is None:
                return {"status": "skipped", "reason": "client_not_found"}
            pacing_delay = _remaining_automated_sms_delay(
                settings,
                lead.last_outbound_at,
            )
            if _requeue_automated_task_for_pacing(
                send_followup_sms_task,
                lead_id,
                reason,
                retry_definitive_failure,
                delay_seconds=pacing_delay,
                retry=Retry(max=3, interval=[60, 240, 600]),
            ):
                return {
                    "status": "requeued",
                    "reason": "automated_sms_pacing",
                    "lead_id": lead_id,
                    "delay_seconds": pacing_delay,
                }
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
            reason_key = str(reason or "followup")
            reason_digest = hashlib.sha256(reason_key.encode("utf-8")).hexdigest()[:16]
            reservation = reserve_outbound_request(
                db=db,
                lead=lead,
                idempotency_key=f"automated-followup-sms:{lead.id}:{reason_digest}",
                request_kind="automated_followup_sms",
                fingerprint_data={"lead_id": lead.id, "reason": reason_key},
                pending_response={
                    "reason": reason_key,
                    "body": body,
                    "attempt_count": 1,
                    "max_attempts": 3,
                },
                retry_failed=retry_definitive_failure,
                require_safe_retry=retry_definitive_failure,
            )
            if not reservation.should_send:
                return {
                    "status": "skipped",
                    "reason": f"delivery_{reservation.status}",
                    "lead_id": lead.id,
                }
            body = str(reservation.response.get("body") or body)
            delivery_state = lock_lead_for_outbound_delivery(db=db, lead_id=lead.id)
            if delivery_state is None:
                cancel_outbound_request(
                    db=db,
                    request_id=reservation.request_id,
                    reason="consent_withdrawn_before_send",
                    response={"reason": reason_key},
                )
                db.add(
                    AuditLog(
                        client_id=client.id,
                        lead_id=lead.id,
                        event_type="follow_up_sms_skipped",
                        decision={
                            "reason": reason_key,
                            "suppression_reason": "consent_withdrawn_before_send",
                        },
                    )
                )
                db.commit()
                return {
                    "status": "skipped",
                    "reason": "consent_withdrawn_before_send",
                    "lead_id": lead.id,
                }
            try:
                provider_sid = sms_service.send_message(to_number=delivery_state.phone, body=body)
            except Exception as exc:
                failure = classify_sms_delivery_failure(exc)
                fail_outbound_request(
                    db=db,
                    request_id=reservation.request_id,
                    detail=exc,
                    ambiguous=failure.ambiguous,
                    response={
                        "reason": reason_key,
                        "failure_reason": failure.reason,
                        "safe_to_retry": failure.safe_to_retry,
                        "provider_status": failure.provider_status,
                        "provider_code": failure.provider_code,
                        "last_failed_at": datetime.now(timezone.utc).isoformat(),
                    },
                    merge_response=True,
                )
                db.add(
                    AuditLog(
                        client_id=client.id,
                        lead_id=lead.id,
                        event_type="follow_up_sms_failed",
                        decision={
                            "reason": reason_key,
                            "failure_reason": failure.reason,
                            "delivery_result_unknown": failure.ambiguous,
                            "safe_to_retry": failure.safe_to_retry,
                            "provider_status": failure.provider_status,
                            "provider_code": failure.provider_code,
                            "error": str(exc)[:500],
                        },
                    )
                )
                db.commit()
                return {
                    "status": "failed",
                    "reason": failure.reason,
                    "lead_id": lead.id,
                }
            _record_outbound(
                db,
                lead=lead,
                body=body,
                provider_sid=provider_sid,
                raw_payload={"reason": reason},
                sms_service=sms_service,
            )
            complete_outbound_request(
                db=db,
                request_id=reservation.request_id,
                provider_reference=provider_sid,
                response={
                    "reason": reason_key,
                    "provider_sid": provider_sid,
                    "attempt_count": reservation.response.get("attempt_count", 1),
                },
            )
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
    finally:
        _release_lead_workflow_lock(lock, lead_id=lead_id, purpose="send_followup_sms")

    incr("sms_outbound_total")
    return {"status": "ok", "lead_id": lead_id, "reason": reason}
