from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models import Lead, OutboundRequest


class OutboundRequestConflict(RuntimeError):
    """Raised when an idempotency key is reused for a different operation."""


@dataclass(frozen=True)
class OutboundReservation:
    request_id: int
    status: str
    should_send: bool
    response: dict[str, Any]


@dataclass(frozen=True)
class OutboundLeadState:
    lead_id: int
    phone: str
    consented: bool
    opted_out: bool


def lock_lead_for_outbound_delivery(*, db: Session, lead_id: int) -> OutboundLeadState | None:
    """Lock and re-read the minimum lead state needed immediately before send."""

    row = db.execute(
        select(Lead.id, Lead.phone, Lead.consented, Lead.opted_out)
        .where(Lead.id == lead_id)
        .with_for_update()
    ).one_or_none()
    if row is None:
        return None
    state = OutboundLeadState(
        lead_id=int(row.id),
        phone=str(row.phone or "").strip(),
        consented=bool(row.consented),
        opted_out=bool(row.opted_out),
    )
    if state.opted_out or not state.consented or not state.phone:
        return None
    return state


def fingerprint_payload(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def reserve_outbound_request(
    *,
    db: Session,
    lead: Lead,
    idempotency_key: str,
    request_kind: str,
    fingerprint_data: dict[str, Any],
    pending_response: dict[str, Any] | None = None,
    retry_failed: bool = False,
    require_safe_retry: bool = False,
) -> OutboundReservation:
    """Persist a reservation before an external side effect is attempted.

    The commit is intentional: an API or worker crash after the provider accepts
    the request must leave a durable row that prevents an automatic duplicate.
    Callers decide whether a known failed request is safe to retry.
    """

    key = str(idempotency_key or "").strip()
    kind = str(request_kind or "").strip()
    if not key or len(key) > 128:
        raise ValueError("Outbound idempotency key must be between 1 and 128 characters")
    if not kind or len(kind) > 64:
        raise ValueError("Outbound request kind must be between 1 and 64 characters")

    fingerprint = fingerprint_payload(fingerprint_data)

    # The reservation must be durable before the provider call, but committing
    # the caller's session would also commit unrelated lead/conversation memory.
    # Use a short independent transaction dedicated to the outbox row.
    reservation_db = Session(bind=db.get_bind(), expire_on_commit=False)

    def result_for(existing: OutboundRequest) -> OutboundReservation:
        if existing.request_kind != kind or existing.request_fingerprint != fingerprint:
            raise OutboundRequestConflict(
                "Outbound idempotency key was already used for a different operation"
            )
        existing_response = dict(existing.response_json or {})
        retry_is_safe = bool(existing_response.get("safe_to_retry"))
        attempt_count = _positive_int(existing_response.get("attempt_count"), default=1)
        max_attempts = _positive_int(existing_response.get("max_attempts"), default=0)
        below_attempt_cap = max_attempts <= 0 or attempt_count < max_attempts
        if (
            existing.status == "failed"
            and retry_failed
            and below_attempt_cap
            and (retry_is_safe or not require_safe_retry)
        ):
            existing.status = "pending"
            existing.error_detail = ""
            merged_response = dict(pending_response or {})
            # The originally reserved delivery payload is authoritative. A
            # retry must not substitute freshly generated message content.
            merged_response.update(existing_response)
            if "attempt_count" in merged_response:
                merged_response["attempt_count"] = attempt_count + 1
            merged_response.pop("safe_to_retry", None)
            merged_response.pop("failure_reason", None)
            merged_response.pop("provider_status", None)
            merged_response.pop("provider_code", None)
            merged_response.pop("last_failed_at", None)
            existing.response_json = merged_response
            existing.updated_at = datetime.now(timezone.utc)
            reservation_db.commit()
            return OutboundReservation(
                request_id=existing.id,
                status="pending",
                should_send=True,
                response=dict(existing.response_json or {}),
            )
        return OutboundReservation(
            request_id=existing.id,
            status=existing.status,
            should_send=False,
            response=dict(existing.response_json or {}),
        )

    try:
        existing = reservation_db.scalar(
            select(OutboundRequest)
            .where(
                OutboundRequest.client_id == lead.client_id,
                OutboundRequest.idempotency_key == key,
            )
            .with_for_update()
        )
        if existing is not None:
            return result_for(existing)

        record = OutboundRequest(
            client_id=lead.client_id,
            lead_id=lead.id,
            idempotency_key=key,
            request_kind=kind,
            request_fingerprint=fingerprint,
            status="pending",
            response_json=pending_response or {},
        )
        reservation_db.add(record)
        try:
            reservation_db.commit()
        except IntegrityError:
            reservation_db.rollback()
            existing = reservation_db.scalar(
                select(OutboundRequest)
                .where(
                    OutboundRequest.client_id == lead.client_id,
                    OutboundRequest.idempotency_key == key,
                )
                .with_for_update()
            )
            if existing is None:
                raise
            return result_for(existing)
        reservation_db.refresh(record)
        return OutboundReservation(
            request_id=record.id,
            status="pending",
            should_send=True,
            response=dict(record.response_json or {}),
        )
    finally:
        reservation_db.close()


def complete_outbound_request(
    *,
    db: Session,
    request_id: int,
    provider_reference: str = "",
    response: dict[str, Any] | None = None,
) -> None:
    record = db.get(OutboundRequest, request_id)
    if record is None:
        raise RuntimeError("Outbound reservation disappeared")
    record.status = "completed"
    record.provider_message_sid = str(provider_reference or "")[:255]
    record.response_json = response or {}
    record.error_detail = ""
    record.updated_at = datetime.now(timezone.utc)


def fail_outbound_request(
    *,
    db: Session,
    request_id: int,
    detail: Any,
    ambiguous: bool,
    response: dict[str, Any] | None = None,
    merge_response: bool = False,
) -> None:
    """Record a failed or ambiguous attempt after rolling back caller changes."""

    db.rollback()
    record = db.get(OutboundRequest, request_id)
    if record is None:
        return
    record.status = "ambiguous" if ambiguous else "failed"
    if response is not None:
        record.response_json = (
            {**dict(record.response_json or {}), **response}
            if merge_response
            else response
        )
    record.error_detail = str(detail or "Outbound delivery failed")[:500]
    record.updated_at = datetime.now(timezone.utc)
    db.commit()


def cancel_outbound_request(
    *,
    db: Session,
    request_id: int,
    reason: str,
    response: dict[str, Any] | None = None,
) -> None:
    """Cancel a reserved side effect when its send-time preconditions changed."""

    db.rollback()
    record = db.get(OutboundRequest, request_id)
    if record is None:
        return
    record.status = "cancelled"
    cancelled_response = {
        **dict(record.response_json or {}),
        **(response or {}),
        "cancel_reason": str(reason or "delivery_cancelled"),
    }
    cancelled_response.pop("body", None)
    cancelled_response.pop("outbound_payload", None)
    record.response_json = cancelled_response
    record.error_detail = ""
    record.updated_at = datetime.now(timezone.utc)
    db.commit()


def _positive_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default
