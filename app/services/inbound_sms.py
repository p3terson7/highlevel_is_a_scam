from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.metrics import incr
from app.db.models import (
    AuditLog,
    Client,
    ConversationState,
    ConversationStateEnum,
    Lead,
    Message,
    MessageDirection,
)
from app.services.booking import (
    BookingSelectionResult,
    BookingService,
    calendar_booking_confirmed,
    extract_email,
    handoff_suffix,
    looks_like_booking_commitment,
    looks_like_slot_selection_message,
)
from app.services.agent_control import should_suppress_ai_reply
from app.services.crm import (
    CRM_STAGE_CONTACTED,
    CRM_STAGE_MEETING_BOOKED,
    progress_crm_stage,
)
from app.services.handoff_policy import (
    HandoffDecision,
    build_handoff_state,
    evaluate_post_llm_handoff,
    evaluate_pre_llm_handoff,
)
from app.services.llm_agent import LLMAgent
from app.services.i18n import client_language, remember_lead_language
from app.services.outbound_requests import (
    cancel_outbound_request,
    complete_outbound_request,
    fail_outbound_request,
    lock_lead_for_outbound_delivery,
    reserve_outbound_request,
)
from app.services.sms_service import SMSService, classify_sms_delivery_failure
from app.services.zapier_booking import notify_zapier_booking_webhook

_PENDING_STEP_KEY = "pending_step"
_ACTIVE_BOOKING_OFFER_KEY = "active_booking_offer"
_PENDING_RESCHEDULE_KEY = "pending_reschedule_confirmation"
_RESCHEDULE_PENDING_STEP = "reschedule_confirmation_pending"
_DEFAULT_HISTORY_LIMIT = 40


def _booking_webhook_enabled_for_turn(lead: Lead) -> bool:
    raw_payload = lead.raw_payload if isinstance(lead.raw_payload, dict) else {}
    if raw_payload.get("created_from") != "ui_ai_sandbox":
        return True
    return str(raw_payload.get("test_configuration") or "").strip().lower() == "gpt_zapier"


def _store_outbound_message(
    db: Session,
    lead: Lead,
    body: str,
    provider_sid: str,
    raw_payload: dict[str, Any] | None = None,
    created_at: datetime | None = None,
    sms_service: SMSService | None = None,
) -> None:
    payload = raw_payload or {}
    if sms_service is not None:
        payload = sms_service.with_delivery_status(payload, provider_sid)
    values = {
        "lead_id": lead.id,
        "client_id": lead.client_id,
        "direction": MessageDirection.OUTBOUND,
        "body": body,
        "provider_message_sid": provider_sid,
        "raw_payload": payload,
    }
    if created_at is not None:
        values["created_at"] = created_at
    db.add(Message(**values))


def _send_inbound_reply_once(
    *,
    db: Session,
    client: Client,
    lead: Lead,
    sms_service: SMSService,
    body: str,
    inbound_message_id: int | None,
    retry_definitive_failure: bool = False,
) -> tuple[str, str] | None:
    if inbound_message_id is None:
        delivery_state = lock_lead_for_outbound_delivery(db=db, lead_id=lead.id)
        if delivery_state is None:
            db.rollback()
            db.add(
                AuditLog(
                    client_id=client.id,
                    lead_id=lead.id,
                    event_type="automated_reply_suppressed",
                    decision={"reason": "consent_withdrawn_before_send"},
                )
            )
            db.commit()
            return None
        provider_sid = sms_service.send_message(to_number=delivery_state.phone, body=body)
        return provider_sid, body

    reservation = reserve_outbound_request(
        db=db,
        lead=lead,
        idempotency_key=f"automated-inbound-reply:{inbound_message_id}",
        request_kind="automated_inbound_reply",
        fingerprint_data={
            "client_id": client.id,
            "lead_id": lead.id,
            "inbound_message_id": inbound_message_id,
        },
        pending_response={
            "inbound_message_id": inbound_message_id,
            "body": body,
            "attempt_count": 1,
            "max_attempts": 3,
        },
        retry_failed=retry_definitive_failure,
        require_safe_retry=retry_definitive_failure,
    )
    if not reservation.should_send:
        if reservation.status == "pending":
            fail_outbound_request(
                db=db,
                request_id=reservation.request_id,
                detail="A prior provider attempt did not record a definitive result",
                ambiguous=True,
                response={
                    "inbound_message_id": inbound_message_id,
                    "failure_reason": "delivery_result_unknown",
                    "safe_to_retry": False,
                },
                merge_response=True,
            )
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="automated_reply_suppressed",
                decision={
                    "reason": f"delivery_{reservation.status}",
                    "inbound_message_id": inbound_message_id,
                },
            )
        )
        db.commit()
        return None

    delivery_state = lock_lead_for_outbound_delivery(db=db, lead_id=lead.id)
    if delivery_state is None:
        cancel_outbound_request(
            db=db,
            request_id=reservation.request_id,
            reason="consent_withdrawn_before_send",
            response={"inbound_message_id": inbound_message_id},
        )
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="automated_reply_suppressed",
                decision={
                    "reason": "consent_withdrawn_before_send",
                    "inbound_message_id": inbound_message_id,
                },
            )
        )
        db.commit()
        return None

    body = str(reservation.response.get("body") or body)
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
                "inbound_message_id": inbound_message_id,
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
                event_type="automated_reply_failed",
                decision={
                    "reason": failure.reason,
                    "inbound_message_id": inbound_message_id,
                    "delivery_result_unknown": failure.ambiguous,
                    "safe_to_retry": failure.safe_to_retry,
                    "provider_status": failure.provider_status,
                    "provider_code": failure.provider_code,
                    "error": str(exc)[:500],
                },
            )
        )
        db.commit()
        return None

    complete_outbound_request(
        db=db,
        request_id=reservation.request_id,
        provider_reference=provider_sid,
        response={
            "inbound_message_id": inbound_message_id,
            "provider_sid": provider_sid,
            "attempt_count": reservation.response.get("attempt_count", 1),
        },
    )
    return provider_sid, body


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


def _current_pending_step(lead: Lead) -> str:
    raw_payload = lead.raw_payload if isinstance(lead.raw_payload, dict) else {}
    return str(raw_payload.get(_PENDING_STEP_KEY) or "").strip()


def _offer_has_slots(offer: Any) -> bool:
    return isinstance(offer, dict) and isinstance(offer.get("slots"), list) and bool(offer.get("slots"))


def _active_booking_offer(lead: Lead, history: list[Message] | None = None) -> dict[str, Any] | None:
    raw_payload = lead.raw_payload if isinstance(lead.raw_payload, dict) else {}
    active = raw_payload.get(_ACTIVE_BOOKING_OFFER_KEY)
    if _offer_has_slots(active):
        return active
    legacy = raw_payload.get("booking_offer")
    if _offer_has_slots(legacy):
        return legacy
    for message in reversed(history or []):
        raw = message.raw_payload if isinstance(message.raw_payload, dict) else {}
        offer = raw.get("booking_offer")
        if _offer_has_slots(offer):
            return offer
    return None


def _store_agent_memory(
    *,
    lead: Lead,
    agent_response,
    pending_step: str | None,
) -> None:
    payload = dict(lead.raw_payload or {})
    runtime_payload = dict(agent_response.runtime_payload or {})
    payload["qualification_memory"] = agent_response.collected_fields.model_dump(exclude_none=True)
    if agent_response.next_question_key:
        payload["last_question_key"] = agent_response.next_question_key
    else:
        payload.pop("last_question_key", None)
    if pending_step:
        payload[_PENDING_STEP_KEY] = pending_step
    else:
        payload.pop(_PENDING_STEP_KEY, None)
    if runtime_payload.get("booking_offer"):
        payload["booking_offer"] = runtime_payload["booking_offer"]
        payload[_ACTIVE_BOOKING_OFFER_KEY] = runtime_payload["booking_offer"]
    if runtime_payload.get("calendar_booking") or agent_response.next_state == ConversationStateEnum.BOOKED:
        payload.pop("booking_offer", None)
        payload.pop(_ACTIVE_BOOKING_OFFER_KEY, None)
    for key in (
        "cta_state",
        "intent_level",
        "intent_score",
        "intent_reasons",
        "important_missing_fields",
        "lead_summary",
        "recommended_follow_up",
        "calendar_booking",
        "booking_confirmation_unknown",
        "booking_provider_status",
    ):
        if key in runtime_payload:
            payload[key] = runtime_payload[key]
    if agent_response.next_state == ConversationStateEnum.HANDOFF:
        payload.pop("booking_offer", None)
        payload.pop(_ACTIVE_BOOKING_OFFER_KEY, None)
    lead.raw_payload = payload


def already_processed_inbound_message(*, db: Session, lead_id: int, inbound_message_id: int) -> bool:
    recent_outbound = db.scalars(
        select(Message)
        .where(Message.lead_id == lead_id, Message.direction == MessageDirection.OUTBOUND)
        .order_by(Message.created_at.desc())
        .limit(50)
    ).all()
    for message in recent_outbound:
        raw_payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
        if int(raw_payload.get("inbound_message_id") or 0) == int(inbound_message_id):
            return True
    return False


def _has_pending_reschedule(lead: Lead) -> bool:
    raw_payload = lead.raw_payload if isinstance(lead.raw_payload, dict) else {}
    return isinstance(raw_payload.get(_PENDING_RESCHEDULE_KEY), dict)


def _should_try_deterministic_slot_selection(*, lead: Lead, inbound_text: str) -> bool:
    pending_step = _current_pending_step(lead)
    if pending_step != "slot_selection_pending":
        return False
    return looks_like_slot_selection_message(inbound_text)


def _should_try_deterministic_commitment(*, lead: Lead, inbound_text: str) -> bool:
    pending_step = _current_pending_step(lead)
    if pending_step != "slot_selection_pending":
        return False
    return looks_like_booking_commitment(inbound_text)


def _merge_booking_flow_memory(*, lead: Lead, runtime_payload: dict[str, Any], next_state: ConversationStateEnum) -> None:
    payload = dict(lead.raw_payload or {})
    if "booking_offer" in runtime_payload and runtime_payload["booking_offer"]:
        payload["booking_offer"] = runtime_payload["booking_offer"]
        payload[_ACTIVE_BOOKING_OFFER_KEY] = runtime_payload["booking_offer"]
    if "calendar_booking" in runtime_payload and runtime_payload["calendar_booking"]:
        payload["calendar_booking"] = runtime_payload["calendar_booking"]
        payload.pop("booking_offer", None)
        payload.pop(_ACTIVE_BOOKING_OFFER_KEY, None)
    if _PENDING_RESCHEDULE_KEY in runtime_payload:
        pending = runtime_payload.get(_PENDING_RESCHEDULE_KEY)
        if isinstance(pending, dict) and pending:
            payload[_PENDING_RESCHEDULE_KEY] = pending
        else:
            payload.pop(_PENDING_RESCHEDULE_KEY, None)
    if runtime_payload.get("pending_step"):
        payload[_PENDING_STEP_KEY] = str(runtime_payload["pending_step"])
    elif "pending_step" in runtime_payload or next_state == ConversationStateEnum.BOOKED:
        payload.pop(_PENDING_STEP_KEY, None)
        if next_state == ConversationStateEnum.BOOKED:
            payload.pop("booking_offer", None)
            payload.pop(_ACTIVE_BOOKING_OFFER_KEY, None)
    if "booking_confirmation_unknown" in runtime_payload:
        payload["booking_confirmation_unknown"] = bool(runtime_payload["booking_confirmation_unknown"])
        payload["booking_provider_status"] = runtime_payload.get("booking_provider_status")
    if next_state == ConversationStateEnum.HANDOFF:
        payload.pop("booking_offer", None)
        payload.pop(_ACTIVE_BOOKING_OFFER_KEY, None)
    lead.raw_payload = payload


def _persist_and_deliver_confirmed_booking(
    *,
    db: Session,
    client: Client,
    lead: Lead,
    inbound_text: str,
    turn_time: datetime,
    sms_service: SMSService,
    reply_text: str,
    inbound_message_id: int | None,
    calendar_booking: dict[str, Any],
    outbound_raw_payload: dict[str, Any],
    transition_reason: str,
    provider: str,
    provider_error: str | None,
    booking_event_type: str,
    booking_decision: dict[str, Any],
    retry_definitive_failure: bool,
) -> None:
    """Commit the provider-confirmed outcome before best-effort notifications.

    A Calendly booking is an external side effect and remains true even if the
    confirmation SMS or Zapier notification fails. Persisting it first keeps
    CRM state, audit history, and booking memory consistent with the provider.
    """

    previous_state = lead.conversation_state
    lead.conversation_state = ConversationStateEnum.BOOKED
    _auto_update_crm_stage(
        db=db,
        lead=lead,
        client_id=client.id,
        target_stage=CRM_STAGE_CONTACTED,
        reason="booking_confirmed",
        inbound_text=inbound_text,
    )
    _auto_update_crm_stage(
        db=db,
        lead=lead,
        client_id=client.id,
        target_stage=CRM_STAGE_MEETING_BOOKED,
        reason="booking_confirmed",
        inbound_text=inbound_text,
    )
    if previous_state != lead.conversation_state:
        db.add(
            ConversationState(
                lead_id=lead.id,
                previous_state=previous_state,
                new_state=lead.conversation_state,
                reason=transition_reason,
                metadata_json={**outbound_raw_payload, "provider": provider},
            )
        )
    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=lead.id,
            event_type=booking_event_type,
            decision=booking_decision,
        )
    )
    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=lead.id,
            event_type="agent_decision",
            decision={
                "inbound": inbound_text,
                "outbound": reply_text,
                "next_state": lead.conversation_state.value,
                "provider": provider,
                "provider_error": provider_error,
                "confirmation_sms_status": "pending",
                **outbound_raw_payload,
            },
        )
    )
    db.commit()

    if _booking_webhook_enabled_for_turn(lead):
        try:
            notify_zapier_booking_webhook(
                db=db,
                client=client,
                lead=lead,
                calendar_booking=calendar_booking,
                trigger="sms_ai_calendar_booking_created",
            )
        except Exception as exc:
            db.rollback()
            db.add(
                AuditLog(
                    client_id=client.id,
                    lead_id=lead.id,
                    event_type="zapier_booking_webhook_dispatch_failed",
                    decision={"reason": "unexpected_dispatch_error", "error": str(exc)[:500]},
                )
            )
            db.commit()

    try:
        delivery = _send_inbound_reply_once(
            db=db,
            client=client,
            lead=lead,
            sms_service=sms_service,
            body=reply_text,
            inbound_message_id=inbound_message_id,
            retry_definitive_failure=retry_definitive_failure,
        )
    except Exception as exc:
        db.rollback()
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="booking_confirmation_sms_failed",
                decision={"reason": "provider_error", "error": str(exc)[:500]},
            )
        )
        db.commit()
        return

    if delivery is None:
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="booking_confirmation_sms_failed",
                decision={"reason": "delivery_not_confirmed"},
            )
        )
        db.commit()
        return
    sid, reply_text = delivery

    _store_outbound_message(
        db=db,
        lead=lead,
        body=reply_text,
        provider_sid=sid,
        raw_payload=outbound_raw_payload,
        created_at=turn_time,
        sms_service=sms_service,
    )
    lead.last_outbound_at = turn_time
    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=lead.id,
            event_type="booking_confirmation_sms_sent",
            decision={"provider_message_sid": sid},
        )
    )
    db.commit()
    incr("sms_outbound_total")


def _apply_booking_selection_result(
    *,
    db: Session,
    client: Client,
    lead: Lead,
    inbound_text: str,
    turn_time: datetime,
    sms_service: SMSService,
    result,
    inbound_message_id: int | None,
    pending_step_before: str,
    retry_definitive_failure: bool,
) -> None:
    reply_text = str(result.reply_text or "").strip()
    runtime_payload = dict(result.raw_payload or {})
    _merge_booking_flow_memory(lead=lead, runtime_payload=runtime_payload, next_state=result.next_state)

    outbound_raw_payload: dict[str, Any] = {
        "booking_flow": {
            "handled_before_llm": True,
            "event_type": result.audit_event_type,
            "transition_reason": result.transition_reason,
        },
        "pending_step_before": pending_step_before or None,
        "pending_step_after": runtime_payload.get("pending_step"),
    }
    if inbound_message_id is not None:
        outbound_raw_payload["inbound_message_id"] = int(inbound_message_id)
    for key in (
        "booking_offer",
        "calendar_booking",
        _PENDING_RESCHEDULE_KEY,
        "booking_confirmation_unknown",
        "booking_provider_status",
    ):
        if key in runtime_payload and runtime_payload[key]:
            outbound_raw_payload[key] = runtime_payload[key]

    calendar_booking = runtime_payload.get("calendar_booking")
    if result.next_state == ConversationStateEnum.BOOKED and isinstance(calendar_booking, dict) and calendar_booking:
        _persist_and_deliver_confirmed_booking(
            db=db,
            client=client,
            lead=lead,
            inbound_text=inbound_text,
            turn_time=turn_time,
            sms_service=sms_service,
            reply_text=reply_text,
            inbound_message_id=inbound_message_id,
            calendar_booking=calendar_booking,
            outbound_raw_payload=outbound_raw_payload,
            transition_reason=result.transition_reason,
            provider="deterministic_booking_flow",
            provider_error=None,
            booking_event_type=result.audit_event_type,
            booking_decision=result.audit_decision,
            retry_definitive_failure=retry_definitive_failure,
        )
        return

    delivery = _send_inbound_reply_once(
        db=db,
        client=client,
        lead=lead,
        sms_service=sms_service,
        body=reply_text,
        inbound_message_id=inbound_message_id,
        retry_definitive_failure=retry_definitive_failure,
    )
    if delivery is None:
        return
    sid, reply_text = delivery
    _store_outbound_message(
        db=db,
        lead=lead,
        body=reply_text,
        provider_sid=sid,
        raw_payload=outbound_raw_payload,
        created_at=turn_time,
        sms_service=sms_service,
    )

    previous_state = lead.conversation_state
    lead.conversation_state = result.next_state
    lead.last_outbound_at = turn_time
    _auto_update_crm_stage(
        db=db,
        lead=lead,
        client_id=client.id,
        target_stage=CRM_STAGE_CONTACTED,
        reason="outbound_sms_sent",
        inbound_text=inbound_text,
    )
    if result.next_state == ConversationStateEnum.BOOKED:
        _auto_update_crm_stage(
            db=db,
            lead=lead,
            client_id=client.id,
            target_stage=CRM_STAGE_MEETING_BOOKED,
            reason="booking_confirmed",
            inbound_text=inbound_text,
        )

    if previous_state != lead.conversation_state:
        db.add(
            ConversationState(
                lead_id=lead.id,
                previous_state=previous_state,
                new_state=lead.conversation_state,
                reason=result.transition_reason,
                metadata_json={
                    **outbound_raw_payload,
                    "provider": "deterministic_booking_flow",
                },
            )
        )

    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=lead.id,
            event_type=result.audit_event_type,
            decision=result.audit_decision,
        )
    )
    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=lead.id,
            event_type="agent_decision",
            decision={
                "inbound": inbound_text,
                "outbound": reply_text,
                "next_state": lead.conversation_state.value,
                "provider": "deterministic_booking_flow",
                **outbound_raw_payload,
            },
        )
    )
    db.commit()
    incr("sms_outbound_total")

    if runtime_payload.get("calendar_booking") and _booking_webhook_enabled_for_turn(lead):
        notify_zapier_booking_webhook(
            db=db,
            client=client,
            lead=lead,
            calendar_booking=runtime_payload["calendar_booking"],
            trigger="sms_ai_calendar_booking_created",
        )


def _booking_clarification_result(
    *,
    client: Client,
    lead: Lead,
    inbound_text: str,
    active_offer: dict[str, Any],
    resolution: dict[str, Any],
) -> BookingSelectionResult:
    language = client_language(client, lead=lead, inbound_text=inbound_text)
    reply_text = str(resolution.get("reply_text") or "").strip()
    if not reply_text:
        slots = active_offer.get("slots") if isinstance(active_offer.get("slots"), list) else []
        labels = []
        for slot in slots[:5]:
            if not isinstance(slot, dict):
                continue
            index = slot.get("index")
            display = str(slot.get("display_time") or "").strip()
            if index and display:
                labels.append(f"{index}) {display}")
        if language == "fr":
            reply_text = "Quel créneau voulez-vous réserver?" + ("\n" + "\n".join(labels) if labels else "")
        else:
            reply_text = "Which call time should I lock in?" + ("\n" + "\n".join(labels) if labels else "")
    return BookingSelectionResult(
        handled=True,
        reply_text=reply_text,
        next_state=ConversationStateEnum.BOOKING_SENT,
        raw_payload={
            "booking_offer": active_offer,
            _ACTIVE_BOOKING_OFFER_KEY: active_offer,
            "pending_step": "slot_selection_pending",
            "booking_resolution": resolution,
        },
        audit_event_type="calendar_booking_clarification_requested",
        audit_decision={"inbound": inbound_text, "booking_resolution": resolution},
        transition_reason="calendar_booking_clarification_requested",
    )


def _resolve_booking_selection_with_llm(
    *,
    client: Client,
    lead: Lead,
    inbound_text: str,
    history: list[Message],
    llm_agent: LLMAgent,
    booking_service: BookingService,
    active_offer: dict[str, Any] | None,
    db: Session,
) -> BookingSelectionResult | None:
    if not _offer_has_slots(active_offer):
        return None
    resolver = getattr(llm_agent, "resolve_booking_selection", None)
    if not callable(resolver):
        return None
    resolution = resolver(
        client=client,
        lead=lead,
        inbound_text=inbound_text,
        history=history,
        active_offer=active_offer,
    )
    if not isinstance(resolution, dict):
        return None
    decision = str(resolution.get("decision") or "").strip().lower()
    if decision == "select_slot":
        handle_slot_selection = getattr(booking_service, "handle_slot_selection", None)
        if not callable(handle_slot_selection):
            return None
        return handle_slot_selection(
            client=client,
            lead=lead,
            inbound_text=inbound_text,
            history=history,
            active_offer=active_offer,
            resolved_slot_index=resolution.get("selected_slot_index"),
            resolved_slot_start_time=resolution.get("selected_slot_start_time"),
            db=db,
        )
    if decision == "ask_clarification":
        return _booking_clarification_result(
            client=client,
            lead=lead,
            inbound_text=inbound_text,
            active_offer=active_offer,
            resolution=resolution,
        )
    return None


def _inbound_media_from_message(db: Session, inbound_message_id: int | None) -> list[dict[str, Any]]:
    if inbound_message_id is None:
        return []
    message = db.get(Message, inbound_message_id)
    if message is None:
        return []
    raw_payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
    attachments = raw_payload.get("attachments") if isinstance(raw_payload.get("attachments"), list) else []
    return [item for item in attachments if isinstance(item, dict)]


def _merge_policy_state_updates(*, lead: Lead, decision: HandoffDecision) -> None:
    if not decision.state_updates:
        return
    payload = dict(lead.raw_payload or {})
    for key, value in decision.state_updates.items():
        if value is None:
            payload.pop(key, None)
        else:
            payload[key] = value
    lead.raw_payload = payload


def _apply_handoff_decision(
    *,
    db: Session,
    client: Client,
    lead: Lead,
    inbound_text: str,
    turn_time: datetime,
    sms_service: SMSService,
    decision: HandoffDecision,
    inbound_message_id: int | None,
    source: str,
    provider: str = "handoff_policy",
    provider_error: str | None = None,
    retry_definitive_failure: bool = False,
) -> None:
    _merge_policy_state_updates(lead=lead, decision=decision)
    pending_step_before = _current_pending_step(lead) or None
    created_at = turn_time.isoformat()
    handoff_state = build_handoff_state(decision, created_at=created_at)
    payload = dict(lead.raw_payload or {})
    if handoff_state:
        payload["handoff"] = handoff_state
    payload.pop(_PENDING_STEP_KEY, None)
    lead.raw_payload = payload

    reply_text = f"{decision.reply_text}{handoff_suffix(client)}".strip()
    outbound_raw_payload: dict[str, Any] = {
        "agent": {
            "action": "handoff_to_human",
            "handoff_level": decision.level,
            "handoff_reason": decision.reason,
            "handoff_summary": decision.summary,
            "provider": provider,
            "provider_error": provider_error,
        },
        "actions": [{"type": "handoff_to_human", "payload": {"level": decision.level, "reason": decision.reason}}],
        "handoff": handoff_state,
        "pending_step_before": pending_step_before,
        "pending_step_after": None,
    }
    if inbound_message_id is not None:
        outbound_raw_payload["inbound_message_id"] = int(inbound_message_id)

    delivery = _send_inbound_reply_once(
        db=db,
        client=client,
        lead=lead,
        sms_service=sms_service,
        body=reply_text,
        inbound_message_id=inbound_message_id,
        retry_definitive_failure=retry_definitive_failure,
    )
    if delivery is None:
        return
    sid, reply_text = delivery
    _store_outbound_message(
        db=db,
        lead=lead,
        body=reply_text,
        provider_sid=sid,
        raw_payload=outbound_raw_payload,
        created_at=turn_time,
        sms_service=sms_service,
    )

    previous_state = lead.conversation_state
    lead.conversation_state = ConversationStateEnum.HANDOFF
    lead.last_outbound_at = turn_time
    _auto_update_crm_stage(
        db=db,
        lead=lead,
        client_id=client.id,
        target_stage=CRM_STAGE_CONTACTED,
        reason="agent_handoff_triggered",
        inbound_text=inbound_text,
    )

    if previous_state != lead.conversation_state:
        db.add(
            ConversationState(
                lead_id=lead.id,
                previous_state=previous_state,
                new_state=lead.conversation_state,
                reason="agent_handoff_triggered",
                metadata_json={
                    **outbound_raw_payload,
                    "source": source,
                    "provider": provider,
                },
                created_at=turn_time,
            )
        )

    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=lead.id,
            event_type="agent_handoff_triggered",
            decision={
                "source": source,
                "reason": decision.reason,
                "level": decision.level,
                "inbound": inbound_text,
                "outbound": reply_text,
                "provider": provider,
                "provider_error": provider_error,
                "summary": decision.summary,
                "actions": outbound_raw_payload["actions"],
            },
            created_at=turn_time,
        )
    )
    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=lead.id,
            event_type="agent_decision",
            decision={
                "inbound": inbound_text,
                "outbound": reply_text,
                "next_state": lead.conversation_state.value,
                "provider": provider,
                "provider_error": provider_error,
                **outbound_raw_payload,
            },
            created_at=turn_time,
        )
    )
    db.commit()
    incr("sms_outbound_total")


def process_inbound_turn(
    *,
    db: Session,
    client: Client,
    lead: Lead,
    inbound_text: str,
    now: datetime | None,
    sms_service: SMSService,
    booking_service: BookingService,
    llm_agent: LLMAgent,
    inbound_message_id: int | None = None,
    history_limit: int = _DEFAULT_HISTORY_LIMIT,
    media_attachments: list[dict[str, Any]] | None = None,
    retry_definitive_failure: bool = False,
) -> None:
    turn_time = now or datetime.now(timezone.utc)

    recent_desc = db.scalars(
        select(Message)
        .where(Message.lead_id == lead.id)
        .order_by(Message.created_at.desc())
        .limit(max(10, int(history_limit)))
    ).all()
    history = list(reversed(recent_desc))
    inbound_media_attachments = media_attachments if media_attachments is not None else _inbound_media_from_message(db, inbound_message_id)

    detected_email = extract_email(inbound_text)
    if detected_email and not lead.email:
        lead.email = detected_email

    remember_lead_language(client, lead, inbound_text=inbound_text)
    should_suppress, suppress_reason = should_suppress_ai_reply(lead)
    if should_suppress:
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="agent_reply_suppressed",
                decision={
                    "inbound": inbound_text,
                    "reason": suppress_reason,
                    "inbound_message_id": inbound_message_id,
                },
                created_at=turn_time,
            )
        )
        db.commit()
        return

    pending_step_before = _current_pending_step(lead)
    active_offer_before = _active_booking_offer(lead, history)

    deterministic_result = None
    if _has_pending_reschedule(lead):
        handle_reschedule = getattr(booking_service, "handle_reschedule_confirmation", None)
        if callable(handle_reschedule):
            deterministic_result = handle_reschedule(
                client=client,
                lead=lead,
                inbound_text=inbound_text,
                history=history,
                db=db,
            )
    if deterministic_result is None and _should_try_deterministic_slot_selection(lead=lead, inbound_text=inbound_text):
        handle_slot_selection = getattr(booking_service, "handle_slot_selection", None)
        if callable(handle_slot_selection):
            deterministic_result = handle_slot_selection(
                client=client,
                lead=lead,
                inbound_text=inbound_text,
                history=history,
                active_offer=active_offer_before,
                db=db,
            )
    if deterministic_result is not None and deterministic_result.handled:
        _apply_booking_selection_result(
            db=db,
            client=client,
            lead=lead,
            inbound_text=inbound_text,
            turn_time=turn_time,
            sms_service=sms_service,
            result=deterministic_result,
            inbound_message_id=inbound_message_id,
            pending_step_before=pending_step_before,
            retry_definitive_failure=retry_definitive_failure,
        )
        return

    pre_handoff = evaluate_pre_llm_handoff(
        client=client,
        lead=lead,
        inbound_text=inbound_text,
        history=history,
        media_attachments=inbound_media_attachments,
    )
    if pre_handoff.should_handoff:
        _apply_handoff_decision(
            db=db,
            client=client,
            lead=lead,
            inbound_text=inbound_text,
            turn_time=turn_time,
            sms_service=sms_service,
            decision=pre_handoff,
            inbound_message_id=inbound_message_id,
            source="pre_llm",
            retry_definitive_failure=retry_definitive_failure,
        )
        return

    deterministic_result = _resolve_booking_selection_with_llm(
        client=client,
        lead=lead,
        inbound_text=inbound_text,
        history=history,
        llm_agent=llm_agent,
        booking_service=booking_service,
        active_offer=active_offer_before,
        db=db,
    )
    if deterministic_result is not None and deterministic_result.handled:
        _apply_booking_selection_result(
            db=db,
            client=client,
            lead=lead,
            inbound_text=inbound_text,
            turn_time=turn_time,
            sms_service=sms_service,
            result=deterministic_result,
            inbound_message_id=inbound_message_id,
            pending_step_before=pending_step_before,
            retry_definitive_failure=retry_definitive_failure,
        )
        return

    if deterministic_result is None and _should_try_deterministic_commitment(lead=lead, inbound_text=inbound_text):
        handle_slot_selection = getattr(booking_service, "handle_slot_selection", None)
        if callable(handle_slot_selection):
            deterministic_result = handle_slot_selection(
                client=client,
                lead=lead,
                inbound_text=inbound_text,
                history=history,
                active_offer=active_offer_before,
                db=db,
            )
    if deterministic_result is not None and deterministic_result.handled:
        _apply_booking_selection_result(
            db=db,
            client=client,
            lead=lead,
            inbound_text=inbound_text,
            turn_time=turn_time,
            sms_service=sms_service,
            result=deterministic_result,
            inbound_message_id=inbound_message_id,
            pending_step_before=pending_step_before,
            retry_definitive_failure=retry_definitive_failure,
        )
        return

    run_turn = getattr(llm_agent, "run_turn", None)
    if callable(run_turn):
        agent_response = run_turn(
            client=client,
            lead=lead,
            inbound_text=inbound_text,
            history=history,
            booking_service=booking_service,
            db=db,
        )
    else:
        agent_response = llm_agent.next_reply(
            client=client,
            lead=lead,
            inbound_text=inbound_text,
            history=history,
        )

    reply_text = agent_response.reply_text.strip()
    next_state = agent_response.next_state
    action = agent_response.action
    runtime_payload = dict(agent_response.runtime_payload or {})
    next_pending_step: str | None = runtime_payload.get("pending_step", pending_step_before or None)
    has_calendar_booking = isinstance(runtime_payload.get("calendar_booking"), dict) and bool(runtime_payload.get("calendar_booking"))
    explicit_booked_confirmation = calendar_booking_confirmed(inbound_text)
    effective_action = action

    if not reply_text:
        language = client_language(client, lead=lead, inbound_text=inbound_text)
        reply_text = (
            "Je suis toujours là. Je vais vous envoyer de nouveaux créneaux."
            if language == "fr"
            else "I’m still with you. Let me send a fresh set of times."
        )
        runtime_payload.setdefault("pending_step", next_pending_step)
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="agent_empty_reply_fallback",
                decision={"inbound": inbound_text, "provider": agent_response.provider, "provider_error": agent_response.provider_error},
            )
        )

    if agent_response.provider_error:
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="agent_provider_fallback",
                decision={"inbound": inbound_text, "provider": agent_response.provider, "provider_error": agent_response.provider_error},
            )
        )

    post_handoff = evaluate_post_llm_handoff(
        client=client,
        lead=lead,
        inbound_text=inbound_text,
        reply_text=reply_text,
        history=history,
        runtime_payload=runtime_payload,
    )
    if post_handoff.should_handoff:
        _apply_handoff_decision(
            db=db,
            client=client,
            lead=lead,
            inbound_text=inbound_text,
            turn_time=turn_time,
            sms_service=sms_service,
            decision=post_handoff,
            inbound_message_id=inbound_message_id,
            source="post_llm",
            provider=agent_response.provider,
            provider_error=agent_response.provider_error,
            retry_definitive_failure=retry_definitive_failure,
        )
        return
    _merge_policy_state_updates(lead=lead, decision=post_handoff)

    if effective_action == "handoff_to_human":
        reply_text = f"{reply_text}{handoff_suffix(client)}".strip()
        next_state = ConversationStateEnum.HANDOFF
        next_pending_step = None
    elif effective_action == "mark_booked":
        if explicit_booked_confirmation or has_calendar_booking:
            next_state = ConversationStateEnum.BOOKED
            next_pending_step = None
        else:
            effective_action = "none"
            if next_state == ConversationStateEnum.BOOKED:
                if pending_step_before or runtime_payload.get("booking_offer"):
                    next_state = ConversationStateEnum.BOOKING_SENT
                else:
                    next_state = ConversationStateEnum.QUALIFYING
            if next_pending_step is None and (pending_step_before or runtime_payload.get("booking_offer")):
                next_pending_step = pending_step_before or "slot_selection_pending"
            language = client_language(client, lead=lead, inbound_text=inbound_text)
            if pending_step_before or runtime_payload.get("booking_offer"):
                reply_text = (
                    "Je peux réserver dès que vous choisissez un des créneaux proposés, ou vous pouvez m'envoyer l'heure exacte souhaitée."
                    if language == "fr"
                    else "I can lock that in once you pick one of the offered times, or send your exact preferred time."
                )
            else:
                reply_text = (
                    "Je peux réserver dès que vous m'envoyez la journée et l'heure qui vous conviennent."
                    if language == "fr"
                    else "I can lock that in as soon as you share your preferred day and time."
                )
    elif lead.conversation_state == ConversationStateEnum.BOOKED and next_state == ConversationStateEnum.QUALIFYING:
        next_state = ConversationStateEnum.BOOKED

    if has_calendar_booking:
        # A provider-confirmed booking is authoritative even if the model
        # returned an inconsistent state/action alongside the tool result.
        effective_action = "mark_booked"
        next_state = ConversationStateEnum.BOOKED
        next_pending_step = None

    action = effective_action
    agent_response.action = effective_action
    _store_agent_memory(lead=lead, agent_response=agent_response, pending_step=next_pending_step)

    outbound_raw_payload = {
        "agent": {
            "action": agent_response.action,
            "next_question_key": agent_response.next_question_key,
            "collected_fields": agent_response.collected_fields.model_dump(exclude_none=True),
            "provider": agent_response.provider,
            "provider_error": agent_response.provider_error,
            "intent_level": runtime_payload.get("intent_level"),
            "intent_score": runtime_payload.get("intent_score"),
            "cta_state": runtime_payload.get("cta_state"),
            "lead_summary": runtime_payload.get("lead_summary"),
        },
        "actions": [action_item.model_dump() for action_item in agent_response.actions],
        "pending_step_before": pending_step_before or None,
        "pending_step_after": next_pending_step,
    }
    if inbound_message_id is not None:
        outbound_raw_payload["inbound_message_id"] = int(inbound_message_id)
    if runtime_payload.get("booking_offer"):
        outbound_raw_payload["booking_offer"] = runtime_payload["booking_offer"]
    if runtime_payload.get("calendar_booking"):
        outbound_raw_payload["calendar_booking"] = runtime_payload["calendar_booking"]

    calendar_booking = runtime_payload.get("calendar_booking")
    if isinstance(calendar_booking, dict) and calendar_booking:
        _persist_and_deliver_confirmed_booking(
            db=db,
            client=client,
            lead=lead,
            inbound_text=inbound_text,
            turn_time=turn_time,
            sms_service=sms_service,
            reply_text=reply_text,
            inbound_message_id=inbound_message_id,
            calendar_booking=calendar_booking,
            outbound_raw_payload=outbound_raw_payload,
            transition_reason="calendar_booking_created",
            provider=agent_response.provider,
            provider_error=agent_response.provider_error,
            booking_event_type="calendar_booking_created",
            booking_decision={"inbound": inbound_text, "calendar_booking": calendar_booking},
            retry_definitive_failure=retry_definitive_failure,
        )
        return

    delivery = _send_inbound_reply_once(
        db=db,
        client=client,
        lead=lead,
        sms_service=sms_service,
        body=reply_text,
        inbound_message_id=inbound_message_id,
        retry_definitive_failure=retry_definitive_failure,
    )
    if delivery is None:
        return
    sid, reply_text = delivery
    _store_outbound_message(
        db=db,
        lead=lead,
        body=reply_text,
        provider_sid=sid,
        raw_payload=outbound_raw_payload,
        created_at=turn_time,
        sms_service=sms_service,
    )

    previous_state = lead.conversation_state
    lead.conversation_state = next_state
    lead.last_outbound_at = turn_time
    _auto_update_crm_stage(
        db=db,
        lead=lead,
        client_id=client.id,
        target_stage=CRM_STAGE_CONTACTED,
        reason="outbound_sms_sent",
        inbound_text=inbound_text,
    )
    if next_state == ConversationStateEnum.BOOKED:
        _auto_update_crm_stage(
            db=db,
            lead=lead,
            client_id=client.id,
            target_stage=CRM_STAGE_MEETING_BOOKED,
            reason="booking_confirmed",
            inbound_text=inbound_text,
        )

    if previous_state != lead.conversation_state:
        db.add(
            ConversationState(
                lead_id=lead.id,
                previous_state=previous_state,
                new_state=lead.conversation_state,
                reason="agent_transition" if not runtime_payload.get("calendar_booking") else "calendar_booking_created",
                metadata_json={
                    **outbound_raw_payload,
                    "provider": agent_response.provider,
                },
            )
        )

    if runtime_payload.get("booking_offer"):
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="calendar_booking_offer_sent",
                decision={"inbound": inbound_text, "booking_offer": runtime_payload["booking_offer"]},
            )
        )
    if runtime_payload.get("calendar_booking"):
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="calendar_booking_created",
                decision={"inbound": inbound_text, "calendar_booking": runtime_payload["calendar_booking"]},
            )
        )

    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=lead.id,
            event_type="agent_decision",
            decision={
                "inbound": inbound_text,
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

    if runtime_payload.get("calendar_booking") and _booking_webhook_enabled_for_turn(lead):
        notify_zapier_booking_webhook(
            db=db,
            client=client,
            lead=lead,
            calendar_booking=runtime_payload["calendar_booking"],
            trigger="sms_ai_calendar_booking_created",
        )
