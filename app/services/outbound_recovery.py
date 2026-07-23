from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import AuditLog, OutboundRequest

_STALE_PENDING_AFTER = timedelta(minutes=5)
_DEAD_LETTER_AFTER = timedelta(hours=24)
_REENQUEUE_AFTER = timedelta(minutes=10)
_DEFAULT_MAX_ATTEMPTS = 3
_RECOVERABLE_KINDS = {
    "automated_initial_sms",
    "automated_followup_sms",
    "zapier_booking_webhook",
}


@dataclass(frozen=True)
class OutboundRetryDirective:
    request_id: int
    request_kind: str
    lead_id: int
    reason: str = ""


@dataclass(frozen=True)
class OutboundRecoveryResult:
    pending_marked_ambiguous: int
    dead_lettered: int
    retry_directives: tuple[OutboundRetryDirective, ...]


def reconcile_stale_outbound_requests(
    *,
    db: Session,
    now: datetime | None = None,
    limit: int = 100,
) -> OutboundRecoveryResult:
    """Conservatively reconcile delivery rows left behind by process failure.

    Unknown provider outcomes are never resent. Only rows explicitly known to
    be safe to retry are handed back to the worker, and their durable attempt
    metadata limits the total number of provider calls.
    """

    observed_at = _as_utc(now or datetime.now(timezone.utc))
    rows = db.scalars(
        select(OutboundRequest)
        .where(OutboundRequest.status.in_(("pending", "ambiguous", "failed")))
        .order_by(OutboundRequest.updated_at.asc(), OutboundRequest.id.asc())
        .limit(max(1, min(int(limit), 1_000)))
        .with_for_update(skip_locked=True)
    ).all()

    pending_marked_ambiguous = 0
    dead_lettered = 0
    directives: list[OutboundRetryDirective] = []

    for record in rows:
        response = dict(record.response_json or {})
        updated_at = _as_utc(record.updated_at)

        if record.status == "pending":
            if observed_at - updated_at < _STALE_PENDING_AFTER:
                continue
            response.update(
                {
                    "recovery_state": "provider_result_unknown",
                    "ambiguous_since": observed_at.isoformat(),
                    "safe_to_retry": False,
                    "recovery_observations": _nonnegative_int(
                        response.get("recovery_observations")
                    )
                    + 1,
                }
            )
            record.status = "ambiguous"
            record.response_json = response
            record.error_detail = (
                record.error_detail
                or "Worker stopped before the provider delivery result was recorded"
            )
            record.updated_at = observed_at
            _add_recovery_audit(
                db=db,
                record=record,
                event_type="outbound_delivery_recovery_ambiguous",
                reason="stale_pending_provider_result_unknown",
            )
            pending_marked_ambiguous += 1
            continue

        if record.status == "ambiguous":
            ambiguous_since = _parse_datetime(response.get("ambiguous_since")) or updated_at
            if observed_at - ambiguous_since < _DEAD_LETTER_AFTER:
                continue
            _dead_letter(
                db=db,
                record=record,
                response=response,
                observed_at=observed_at,
                reason="ambiguous_delivery_not_retriable",
            )
            dead_lettered += 1
            continue

        attempt_count = _positive_int(
            response.get("attempt_count", response.get("attempt")),
            default=1,
        )
        max_attempts = _positive_int(
            response.get("max_attempts"),
            default=_DEFAULT_MAX_ATTEMPTS,
        )
        if attempt_count >= max_attempts:
            _dead_letter(
                db=db,
                record=record,
                response=response,
                observed_at=observed_at,
                reason="attempt_cap_reached",
            )
            dead_lettered += 1
            continue

        safe_to_retry = bool(response.get("safe_to_retry")) or (
            record.request_kind == "zapier_booking_webhook"
            and isinstance(response.get("delivery_payload"), dict)
            and bool(response.get("delivery_payload"))
        )
        if safe_to_retry and record.request_kind in _RECOVERABLE_KINDS:
            enqueued_at = _parse_datetime(response.get("recovery_enqueued_at"))
            if enqueued_at is not None and observed_at - enqueued_at < _REENQUEUE_AFTER:
                continue
            response["recovery_enqueued_at"] = observed_at.isoformat()
            response["max_attempts"] = max_attempts
            record.response_json = response
            record.updated_at = observed_at
            directives.append(
                OutboundRetryDirective(
                    request_id=record.id,
                    request_kind=record.request_kind,
                    lead_id=record.lead_id,
                    reason=str(response.get("reason") or ""),
                )
            )
            _add_recovery_audit(
                db=db,
                record=record,
                event_type="outbound_delivery_recovery_queued",
                reason="explicit_safe_retry",
                extra={"attempt_count": attempt_count, "max_attempts": max_attempts},
            )
            continue

        if observed_at - updated_at >= _DEAD_LETTER_AFTER:
            _dead_letter(
                db=db,
                record=record,
                response=response,
                observed_at=observed_at,
                reason=(
                    "retry_kind_not_supported"
                    if safe_to_retry
                    else "failure_not_marked_safe_to_retry"
                ),
            )
            dead_lettered += 1

    return OutboundRecoveryResult(
        pending_marked_ambiguous=pending_marked_ambiguous,
        dead_lettered=dead_lettered,
        retry_directives=tuple(directives),
    )


def clear_outbound_recovery_enqueue_marker(
    *,
    db: Session,
    request_id: int,
    detail: str,
) -> None:
    record = db.get(OutboundRequest, request_id)
    if record is None or record.status != "failed":
        return
    response = dict(record.response_json or {})
    response.pop("recovery_enqueued_at", None)
    response["recovery_enqueue_error"] = str(detail or "queue unavailable")[:500]
    record.response_json = response
    record.updated_at = datetime.now(timezone.utc)
    _add_recovery_audit(
        db=db,
        record=record,
        event_type="outbound_delivery_recovery_enqueue_failed",
        reason="queue_dispatch_failed",
    )


def _dead_letter(
    *,
    db: Session,
    record: OutboundRequest,
    response: dict[str, Any],
    observed_at: datetime,
    reason: str,
) -> None:
    response.pop("body", None)
    response.pop("outbound_payload", None)
    response.pop("delivery_payload", None)
    response.update(
        {
            "recovery_state": "dead_letter",
            "dead_letter_reason": reason,
            "dead_lettered_at": observed_at.isoformat(),
            "safe_to_retry": False,
        }
    )
    record.status = "dead_letter"
    record.response_json = response
    record.updated_at = observed_at
    _add_recovery_audit(
        db=db,
        record=record,
        event_type="outbound_delivery_dead_lettered",
        reason=reason,
    )


def _add_recovery_audit(
    *,
    db: Session,
    record: OutboundRequest,
    event_type: str,
    reason: str,
    extra: dict[str, Any] | None = None,
) -> None:
    db.add(
        AuditLog(
            client_id=record.client_id,
            lead_id=record.lead_id,
            event_type=event_type,
            decision={
                "request_id": record.id,
                "request_kind": record.request_kind,
                "reason": reason,
                **(extra or {}),
            },
        )
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _as_utc(parsed)


def _positive_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _nonnegative_int(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return max(parsed, 0)
