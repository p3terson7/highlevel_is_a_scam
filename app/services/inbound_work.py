from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, or_, select, update
from sqlalchemy.orm import Session

from app.db.models import Message, MessageDirection


INBOUND_WORK_RECEIVED = "received"
INBOUND_WORK_WAITING_MEDIA = "waiting_media"
INBOUND_WORK_QUEUED = "queued"
INBOUND_WORK_ENQUEUED = "enqueued"
INBOUND_WORK_PROCESSING = "processing"
INBOUND_WORK_COMPLETED = "completed"
INBOUND_WORK_SUPPRESSED = "suppressed"
INBOUND_WORK_RETRYABLE_FAILED = "retryable_failed"
INBOUND_WORK_DEAD_LETTER = "dead_letter"

MAX_INBOUND_WORK_ATTEMPTS = 3
INBOUND_WORK_ENQUEUE_STALE_AFTER = timedelta(minutes=5)
INBOUND_WORK_STALE_AFTER = timedelta(minutes=15)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def set_inbound_work_state(
    message: Message,
    status: str,
    *,
    error: str = "",
    now: datetime | None = None,
) -> None:
    """Set work state on a message that is already part of the caller's transaction."""

    message.inbound_work_status = status
    message.inbound_work_error = str(error or "")[:128]
    message.inbound_work_updated_at = now or utc_now()


def claim_inbound_work(
    *,
    db: Session,
    message_id: int,
    now: datetime | None = None,
    max_attempts: int = MAX_INBOUND_WORK_ATTEMPTS,
    stale_after: timedelta = INBOUND_WORK_STALE_AFTER,
    allow_untracked_job: bool = False,
) -> bool:
    """Atomically claim one recoverable inbound turn.

    The claim is committed before any LLM or provider work starts. Concurrent
    jobs therefore cannot both run the same turn, even when Redis locking is
    unavailable.
    """

    claimed_at = now or utc_now()
    stale_before = claimed_at - stale_after
    eligible_statuses = [
        Message.inbound_work_status == INBOUND_WORK_QUEUED,
        Message.inbound_work_status == INBOUND_WORK_ENQUEUED,
        Message.inbound_work_status == INBOUND_WORK_RETRYABLE_FAILED,
        and_(
            Message.inbound_work_status == INBOUND_WORK_PROCESSING,
            or_(
                Message.inbound_work_updated_at.is_(None),
                Message.inbound_work_updated_at <= stale_before,
            ),
        ),
    ]
    if allow_untracked_job:
        # Rolling-deploy compatibility: an old API may have enqueued the job
        # after the columns were added but before it knew to set their state.
        # Blank rows are never found by periodic recovery on their own.
        eligible_statuses.append(Message.inbound_work_status == "")

    result = db.execute(
        update(Message)
        .where(
            Message.id == int(message_id),
            Message.direction == MessageDirection.INBOUND,
            Message.inbound_work_attempt_count < max(1, int(max_attempts)),
            or_(*eligible_statuses),
        )
        .values(
            inbound_work_status=INBOUND_WORK_PROCESSING,
            inbound_work_attempt_count=Message.inbound_work_attempt_count + 1,
            inbound_work_error="",
            inbound_work_updated_at=claimed_at,
        )
        .execution_options(synchronize_session=False)
    )
    db.commit()
    return result.rowcount == 1


def reserve_inbound_work_enqueue(
    *,
    db: Session,
    message_id: int,
    now: datetime | None = None,
    enqueue_stale_after: timedelta = INBOUND_WORK_ENQUEUE_STALE_AFTER,
) -> bool:
    """Atomically reserve one queue handoff for a recoverable message."""

    reserved_at = now or utc_now()
    stale_before = reserved_at - enqueue_stale_after
    result = db.execute(
        update(Message)
        .where(
            Message.id == int(message_id),
            Message.direction == MessageDirection.INBOUND,
            Message.inbound_work_attempt_count < MAX_INBOUND_WORK_ATTEMPTS,
            or_(
                Message.inbound_work_status == INBOUND_WORK_QUEUED,
                Message.inbound_work_status == INBOUND_WORK_RETRYABLE_FAILED,
                and_(
                    Message.inbound_work_status == INBOUND_WORK_ENQUEUED,
                    or_(
                        Message.inbound_work_updated_at.is_(None),
                        Message.inbound_work_updated_at <= stale_before,
                    ),
                ),
            ),
        )
        .values(
            inbound_work_status=INBOUND_WORK_ENQUEUED,
            inbound_work_error="",
            inbound_work_updated_at=reserved_at,
        )
        .execution_options(synchronize_session=False)
    )
    db.commit()
    return result.rowcount == 1


def release_inbound_work_enqueue(
    *,
    db: Session,
    message_id: int,
    now: datetime | None = None,
) -> None:
    """Make a known-failed queue handoff immediately recoverable again."""

    db.execute(
        update(Message)
        .where(
            Message.id == int(message_id),
            Message.direction == MessageDirection.INBOUND,
            Message.inbound_work_status == INBOUND_WORK_ENQUEUED,
        )
        .values(
            inbound_work_status=INBOUND_WORK_QUEUED,
            inbound_work_updated_at=now or utc_now(),
        )
        .execution_options(synchronize_session=False)
    )
    db.commit()


def finish_inbound_work(
    *,
    db: Session,
    message_id: int,
    status: str = INBOUND_WORK_COMPLETED,
    now: datetime | None = None,
) -> None:
    if status not in {INBOUND_WORK_COMPLETED, INBOUND_WORK_SUPPRESSED}:
        raise ValueError("Inbound work can only finish as completed or suppressed")
    db.execute(
        update(Message)
        .where(
            Message.id == int(message_id),
            Message.direction == MessageDirection.INBOUND,
        )
        .values(
            inbound_work_status=status,
            inbound_work_error="",
            inbound_work_updated_at=now or utc_now(),
        )
        .execution_options(synchronize_session=False)
    )
    db.commit()


def fail_inbound_work_safely(
    *,
    db: Session,
    message_id: int,
    error_type: str,
    now: datetime | None = None,
) -> None:
    """Persist a retryable failure without storing exception text or payload PII."""

    db.rollback()
    attempt_count = db.scalar(
        select(Message.inbound_work_attempt_count).where(Message.id == int(message_id))
    )
    next_status = (
        INBOUND_WORK_DEAD_LETTER
        if int(attempt_count or 0) >= MAX_INBOUND_WORK_ATTEMPTS
        else INBOUND_WORK_RETRYABLE_FAILED
    )
    db.execute(
        update(Message)
        .where(
            Message.id == int(message_id),
            Message.direction == MessageDirection.INBOUND,
            Message.inbound_work_status == INBOUND_WORK_PROCESSING,
        )
        .values(
            inbound_work_status=next_status,
            inbound_work_error=str(error_type or "processing_error")[:128],
            inbound_work_updated_at=now or utc_now(),
        )
        .execution_options(synchronize_session=False)
    )
    db.commit()


def queue_inbound_work_after_media(
    *,
    db: Session,
    message_id: int,
    now: datetime | None = None,
) -> str | None:
    """Move a persisted MMS turn to queued once its attachments are durable."""

    message = db.get(Message, int(message_id))
    if message is None or message.direction != MessageDirection.INBOUND:
        return None
    if message.inbound_work_status == INBOUND_WORK_WAITING_MEDIA:
        set_inbound_work_state(message, INBOUND_WORK_QUEUED, now=now)
        db.commit()
    return message.inbound_work_status


def is_inbound_work_recoverable(
    message: Message,
    *,
    now: datetime | None = None,
    max_attempts: int = MAX_INBOUND_WORK_ATTEMPTS,
    stale_after: timedelta = INBOUND_WORK_STALE_AFTER,
    enqueue_stale_after: timedelta = INBOUND_WORK_ENQUEUE_STALE_AFTER,
) -> bool:
    if message.direction != MessageDirection.INBOUND:
        return False
    if int(message.inbound_work_attempt_count or 0) >= max(1, int(max_attempts)):
        return False
    if message.inbound_work_status in {
        INBOUND_WORK_QUEUED,
        INBOUND_WORK_RETRYABLE_FAILED,
    }:
        return True
    if message.inbound_work_status not in {
        INBOUND_WORK_ENQUEUED,
        INBOUND_WORK_PROCESSING,
    }:
        return False
    updated_at = message.inbound_work_updated_at
    if updated_at is None:
        return True
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    current_time = now or utc_now()
    threshold = (
        enqueue_stale_after
        if message.inbound_work_status == INBOUND_WORK_ENQUEUED
        else stale_after
    )
    return updated_at <= current_time - threshold


def recoverable_inbound_work_plans(
    *,
    db: Session,
    limit: int = 100,
    now: datetime | None = None,
    max_attempts: int = MAX_INBOUND_WORK_ATTEMPTS,
    stale_after: timedelta = INBOUND_WORK_STALE_AFTER,
    enqueue_stale_after: timedelta = INBOUND_WORK_ENQUEUE_STALE_AFTER,
) -> list[tuple[int, int]]:
    """Return a bounded oldest-first set of work plans for queue recovery."""

    current_time = now or utc_now()
    stale_before = current_time - stale_after
    stale_enqueue_before = current_time - enqueue_stale_after
    rows = db.execute(
        select(Message.id, Message.lead_id)
        .where(
            Message.direction == MessageDirection.INBOUND,
            Message.inbound_work_attempt_count < max(1, int(max_attempts)),
            or_(
                Message.inbound_work_status == INBOUND_WORK_QUEUED,
                Message.inbound_work_status == INBOUND_WORK_RETRYABLE_FAILED,
                and_(
                    Message.inbound_work_status == INBOUND_WORK_ENQUEUED,
                    or_(
                        Message.inbound_work_updated_at.is_(None),
                        Message.inbound_work_updated_at <= stale_enqueue_before,
                    ),
                ),
                and_(
                    Message.inbound_work_status == INBOUND_WORK_PROCESSING,
                    or_(
                        Message.inbound_work_updated_at.is_(None),
                        Message.inbound_work_updated_at <= stale_before,
                    ),
                ),
            ),
        )
        .order_by(Message.inbound_work_updated_at.asc(), Message.id.asc())
        .limit(max(1, min(int(limit), 1_000)))
    ).all()
    return [(int(row.id), int(row.lead_id)) for row in rows]
