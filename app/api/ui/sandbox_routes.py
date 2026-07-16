from fastapi import APIRouter

from app.services.sandbox_admission import admit_sandbox_action

from .shared import *

router = APIRouter()

_SANDBOX_MODES = {"gpt_only", "gpt_zapier"}
_ZAPIER_BOOKING_EVENTS = {"zapier_booking_webhook_sent", "zapier_booking_webhook_failed"}
_SANDBOX_START_LIMIT = 10
_SANDBOX_START_WINDOW = timedelta(minutes=10)
_SANDBOX_MESSAGE_LIMIT = 30
_SANDBOX_MESSAGE_WINDOW = timedelta(minutes=1)


def _enforce_sandbox_admission(
    *,
    settings: Settings,
    client_id: int,
    limit: int,
    window: timedelta,
    action: str,
) -> None:
    """Reserve a tenant-scoped sandbox budget before invoking the LLM."""

    window_seconds = max(1, int(window.total_seconds()))
    admission = admit_sandbox_action(
        settings=settings,
        client_id=client_id,
        action=action,
        limit=limit,
        window_seconds=window_seconds,
    )
    if admission.admitted:
        return

    if admission.reason == "coordination_unavailable":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"AI sandbox {action} is temporarily unavailable. Try again shortly.",
            headers={"Retry-After": "30"},
        )

    raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail=f"AI sandbox {action} limit reached for this workspace. Try again shortly.",
        headers={"Retry-After": str(window_seconds)},
    )


def _sandbox_booking_debug(message: Message | None) -> dict[str, Any] | None:
    if message is None:
        return None
    raw_payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
    offer = raw_payload.get("booking_offer") if isinstance(raw_payload.get("booking_offer"), dict) else None
    if not offer:
        return None
    slots = offer.get("slots") if isinstance(offer.get("slots"), list) else []
    return {
        "request": offer.get("request") if isinstance(offer.get("request"), dict) else {},
        "planner": offer.get("planner") if isinstance(offer.get("planner"), dict) else {},
        "match_mode": offer.get("match_mode"),
        "matched_preference": offer.get("matched_preference"),
        "selected_slots": [
            {
                "index": slot.get("index"),
                "display_time": slot.get("display_time"),
                "start_time": slot.get("start_time"),
                "end_time": slot.get("end_time"),
            }
            for slot in slots
            if isinstance(slot, dict)
        ],
    }


def _sandbox_zapier_result(db: Session, *, lead_id: int, mode: str, client: Client | None) -> dict[str, Any]:
    if mode != "gpt_zapier":
        return {
            "enabled": False,
            "status": "disabled",
            "message": "Zapier is off for this GPT-only sandbox.",
        }
    provider_config = client.provider_config if client is not None and isinstance(client.provider_config, dict) else {}
    if not str(provider_config.get("zapier_booking_webhook_url") or "").strip():
        return {
            "enabled": True,
            "status": "not_configured",
            "message": "Add a Zapier booking webhook URL for this client before booking in GPT + Zapier mode.",
        }

    latest = db.scalar(
        select(AuditLog)
        .where(AuditLog.lead_id == lead_id, AuditLog.event_type.in_(_ZAPIER_BOOKING_EVENTS))
        .order_by(desc(AuditLog.created_at), desc(AuditLog.id))
        .limit(1)
    )
    if latest is None:
        return {
            "enabled": True,
            "status": "waiting_for_booking",
            "message": "Zapier will fire after this sandbox lead books a meeting.",
        }

    decision = latest.decision if isinstance(latest.decision, dict) else {}
    return {
        "enabled": True,
        "status": "sent" if latest.event_type == "zapier_booking_webhook_sent" else "failed",
        "event_type": latest.event_type,
        "created_at": latest.created_at.isoformat(),
        "status_code": decision.get("status_code"),
        "endpoint_host": decision.get("endpoint_host"),
        "dedupe_key": decision.get("dedupe_key"),
        "error": decision.get("error"),
        "payload": decision.get("payload"),
    }


@router.post("/ui/api/owner/{client_key}/sandbox/start")
def ui_owner_start_ai_sandbox(
    client_key: str,
    payload: OwnerSandboxStartRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    client = actor.client if actor.role == "client" else _load_client_by_key(db, client_key)
    if client is None or client.client_key != client_key:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")

    mode = (payload.mode or "gpt_only").strip().lower()
    if mode not in _SANDBOX_MODES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only GPT only and GPT + Zapier sandbox modes are currently available")

    submitted_answers: dict[str, str] = {}
    original_answers: list[dict[str, str]] = []
    for row in payload.form_answers:
        question = row.question.strip()
        answer = row.answer.strip()
        if not question or not answer:
            continue
        submitted_answers[question] = answer
        original_answers.append({"question": question, "answer": answer})
    normalized_answers = normalize_form_answers(submitted_answers)
    if not normalized_answers:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="At least one form answer is required")

    provided_phone = (payload.phone or "").strip()
    normalized_phone = normalize_phone(provided_phone) if provided_phone else ""
    if provided_phone and not normalized_phone:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Phone number is invalid")

    _enforce_sandbox_admission(
        settings=settings,
        client_id=client.id,
        limit=_SANDBOX_START_LIMIT,
        window=_SANDBOX_START_WINDOW,
        action="start",
    )

    effective_runtime = get_effective_runtime_map_for_client(
        settings=settings,
        overrides=load_runtime_overrides(db),
        client=client,
    )
    llm_agent = build_llm_agent(settings=settings, runtime_overrides=effective_runtime)
    sandbox_sms_service = build_mock_sms_service()
    now = datetime.now(timezone.utc)
    lead_name = (payload.full_name or "Strategy Call Lead").strip() or "Strategy Call Lead"

    lead = Lead(
        client_id=client.id,
        external_lead_id=f"ui-sandbox-{uuid4().hex}",
        source=LeadSource.MANUAL,
        full_name=lead_name,
        phone=normalized_phone or "+10000000000",
        email=(payload.email or "").strip(),
        city=(payload.city or "").strip(),
        form_answers=normalized_answers,
        raw_payload={
            "created_from": "ui_ai_sandbox",
            "test_configuration": mode,
            "actor_role": actor.role,
            "delivery_mode": "sandbox",
            "twilio_bypassed": True,
            "submitted_form_answers": original_answers,
        },
        consented=True,
        opted_out=False,
        conversation_state=ConversationStateEnum.NEW,
    )
    db.add(lead)
    db.flush()
    if not normalized_phone:
        lead.phone = f"+1000{lead.id:07d}"
    db.add(LeadTag(lead_id=lead.id, client_id=client.id, tag="sandbox"))
    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=lead.id,
            event_type="ui_sandbox_started",
            decision={
                "source": "test_lab",
                "mode": mode,
                "actor_role": actor.role,
                "delivery_mode": "sandbox",
                "twilio_bypassed": True,
                "form_answer_count": len(normalized_answers),
            },
            created_at=now,
        )
    )

    ai_seed = _meta_initial_seed_text(lead)
    ai_response = llm_agent.next_reply(client=client, lead=lead, inbound_text=ai_seed, history=[])
    body = ai_response.reply_text.strip()
    if not body:
        first_name = lead.full_name.split(" ")[0] if lead.full_name else "there"
        body = f"Hi {first_name}, thanks for reaching out to {client.business_name}."

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
        "calendar_booking",
    ):
        if key in (ai_response.runtime_payload or {}):
            qualification_memory[key] = ai_response.runtime_payload[key]
    lead.raw_payload = qualification_memory

    outbound_payload = {
        "reason": "ui_sandbox_initial_ai_sms",
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
        "delivery_mode": "sandbox",
        "twilio_bypassed": True,
    }
    provider_sid, _ = _send_outbound_message(
        db=db,
        sms_service=sandbox_sms_service,
        lead=lead,
        body=body,
        created_at=now,
        raw_payload=outbound_payload,
        audit_event_type="ui_sandbox_initial_ai_sms",
        audit_decision={"source": "test_lab", "mode": mode, "actor_role": actor.role, "delivery_mode": "sandbox"},
        advance_new_to_greeted=False,
    )

    previous_state = lead.conversation_state
    previous_stage = lead.crm_stage
    lead.conversation_state = (
        ai_response.next_state if ai_response.next_state != ConversationStateEnum.NEW else ConversationStateEnum.QUALIFYING
    )
    lead.crm_stage = progress_crm_stage(lead.crm_stage, CRM_STAGE_CONTACTED)
    lead.initial_sms_sent_at = now
    lead.last_outbound_at = now
    lead.updated_at = now
    if previous_state != lead.conversation_state:
        db.add(
            ConversationState(
                lead_id=lead.id,
                previous_state=previous_state,
                new_state=lead.conversation_state,
                reason="ui_sandbox_initial_ai_sms",
                metadata_json=outbound_payload,
                created_at=now,
            )
        )
    if previous_stage != lead.crm_stage:
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="crm_stage_auto_updated",
                decision={
                    "previous_stage": previous_stage,
                    "new_stage": lead.crm_stage,
                    "reason": "ui_sandbox_initial_outbound",
                },
                created_at=now,
            )
        )
    db.commit()

    return {
        "status": "ok",
        "lead_id": lead.id,
        "mode": mode,
        "state": lead.conversation_state.value,
        "body": body,
        "provider_sid": provider_sid,
        "delivery_mode": "sandbox",
        "twilio_bypassed": True,
        "phone": lead.phone,
        "booking_debug": None,
        "zapier_booking_webhook": _sandbox_zapier_result(db, lead_id=lead.id, mode=mode, client=client),
    }


@router.post("/ui/api/conversations/{lead_id}/sandbox/messages")
def ui_send_ai_sandbox_message(
    lead_id: int,
    payload: OwnerSandboxMessageRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    booking_service: BookingService = Depends(get_booking_service),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    lead = _load_lead_for_actor(db, actor, lead_id)
    raw_payload = lead.raw_payload if isinstance(lead.raw_payload, dict) else {}
    if raw_payload.get("created_from") != "ui_ai_sandbox":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="This action is only available for AI sandbox threads")

    inbound_text = payload.body.strip()
    if not inbound_text:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Message body is required")
    if not lead.client:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")

    _enforce_sandbox_admission(
        settings=settings,
        client_id=lead.client_id,
        limit=_SANDBOX_MESSAGE_LIMIT,
        window=_SANDBOX_MESSAGE_WINDOW,
        action="message",
    )

    now = datetime.now(timezone.utc)
    inbound_message = Message(
        lead_id=lead.id,
        client_id=lead.client_id,
        direction=MessageDirection.INBOUND,
        body=inbound_text,
        provider_message_sid=f"SANDBOX-IN-{int(now.timestamp() * 1000)}",
        raw_payload={"source": "ui_ai_sandbox", "actor_role": actor.role, "twilio_bypassed": True},
        created_at=now,
    )
    db.add(inbound_message)
    db.flush()
    lead.last_inbound_at = now
    lead.updated_at = now

    if is_meaningful_inbound(inbound_text):
        _set_crm_stage(
            db=db,
            lead=lead,
            new_stage=CRM_STAGE_QUALIFIED,
            actor_role="system",
            reason="sandbox_meaningful_inbound",
            allow_backward=False,
            event_type="crm_stage_auto_updated",
            now=now,
        )
    db.add(
        AuditLog(
            client_id=lead.client_id,
            lead_id=lead.id,
            event_type="ui_sandbox_lead_message",
            decision={"inbound": inbound_text, "actor_role": actor.role, "twilio_bypassed": True},
            created_at=now,
        )
    )
    # Persist the inbound turn before any provider-side effect reserves work
    # in its independent outbox transaction (required for SQLite as well as a
    # coherent crash-recovery boundary).
    db.commit()

    effective_runtime = get_effective_runtime_map_for_client(
        settings=settings,
        overrides=load_runtime_overrides(db),
        client=lead.client,
    )
    process_inbound_turn(
        db=db,
        client=lead.client,
        lead=lead,
        inbound_text=inbound_text,
        now=now,
        sms_service=build_mock_sms_service(),
        booking_service=booking_service,
        llm_agent=build_llm_agent(settings=settings, runtime_overrides=effective_runtime),
        inbound_message_id=inbound_message.id,
    )

    latest_outbound = db.scalar(
        select(Message)
        .where(Message.lead_id == lead.id, Message.direction == MessageDirection.OUTBOUND)
        .order_by(desc(Message.created_at), desc(Message.id))
        .limit(1)
    )
    raw_payload_after = lead.raw_payload if isinstance(lead.raw_payload, dict) else {}
    mode = str(raw_payload_after.get("test_configuration") or raw_payload.get("test_configuration") or "gpt_only").strip().lower()
    return {
        "status": "ok",
        "lead_id": lead.id,
        "state": lead.conversation_state.value,
        "crm_stage": normalize_crm_stage(lead.crm_stage),
        "delivery_mode": "sandbox",
        "twilio_bypassed": True,
        "inbound_message_id": inbound_message.id,
        "reply": {
            "id": latest_outbound.id if latest_outbound else None,
            "body": latest_outbound.body if latest_outbound else "",
            "provider_message_sid": latest_outbound.provider_message_sid if latest_outbound else "",
        },
        "booking_debug": _sandbox_booking_debug(latest_outbound),
        "zapier_booking_webhook": _sandbox_zapier_result(db, lead_id=lead.id, mode=mode, client=lead.client),
    }
