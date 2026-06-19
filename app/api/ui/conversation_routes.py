from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import FileResponse

from .shared import *
from app.services.message_media import (
    MessageMediaError,
    attachment_file_path,
    attachment_public_url,
    create_message_attachment,
    provider_public_base_url,
    store_message_media,
)

router = APIRouter()


@router.get("/media/public/{public_token}")
def public_message_media(
    public_token: str,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
) -> FileResponse:
    attachment = db.scalar(select(MessageAttachment).where(MessageAttachment.public_token == public_token))
    if attachment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Media not found")
    try:
        path = attachment_file_path(settings, attachment)
    except MessageMediaError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Media file not found")
    return FileResponse(
        path,
        media_type=attachment.content_type or "application/octet-stream",
        filename=attachment.filename or None,
    )


@router.get("/ui/api/conversations")
def ui_conversations(
    client_key: str | None = Query(default=None),
    state: str | None = Query(default=None),
    q: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    state_filter = _parse_state_filter(state)
    from_filter = _parse_date_filter(date_from)
    to_filter = _parse_date_filter(date_to)

    stmt = select(Lead).options(selectinload(Lead.client)).join(Client)
    effective_client_key = _scoped_client_key(actor, client_key)
    if effective_client_key:
        stmt = stmt.where(Client.client_key == effective_client_key)
    if state_filter is not None:
        stmt = stmt.where(Lead.conversation_state == state_filter)
    if not q and from_filter is None and to_filter is None:
        stmt = stmt.limit(limit)
    leads = db.scalars(stmt.order_by(desc(Lead.updated_at), desc(Lead.created_at))).unique().all()

    items = _build_conversation_items(db, leads, limit=limit, date_from=from_filter, date_to=to_filter, query=q)
    counts = Counter(item["state"] for item in items)
    return {
        "items": items,
        "counts": dict(counts),
        "total": len(items),
    }


@router.get("/ui/api/conversations/{lead_id}/thread")
def ui_conversation_thread(
    lead_id: int,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    lead = _load_lead_for_actor(db, actor, lead_id)
    client = lead.client
    messages = db.scalars(
        select(Message)
        .where(Message.lead_id == lead.id)
        .order_by(Message.created_at.asc(), Message.id.asc())
    ).all()
    attachments_by_message = _attachments_by_message(db, [message.id for message in messages])
    state_history = db.scalars(
        select(ConversationState)
        .where(ConversationState.lead_id == lead.id)
        .order_by(ConversationState.created_at.asc(), ConversationState.id.asc())
    ).all()
    audit_logs = db.scalars(
        select(AuditLog)
        .where(AuditLog.lead_id == lead.id)
        .order_by(AuditLog.created_at.asc(), AuditLog.id.asc())
    ).all()
    notes = [log for log in audit_logs if log.event_type == "internal_note"]
    custom_tags = _custom_tags_by_lead(db, [lead.id]).get(lead.id, [])
    tags = _merged_tags(
        conversation_tags=_conversation_tags(lead, list(reversed(audit_logs))),
        custom_tags=custom_tags,
    )
    tasks = db.scalars(
        select(LeadTask)
        .where(LeadTask.lead_id == lead.id)
        .order_by(LeadTask.status.desc(), LeadTask.due_date.asc(), desc(LeadTask.created_at))
    ).all()

    timeline: list[dict[str, Any]] = []
    for message in messages:
        timeline.append(
            {
                "type": "message",
                "created_at": message.created_at.isoformat(),
                "direction": message.direction.value,
                "body": message.body,
                "provider_message_sid": message.provider_message_sid,
                "attachments": attachments_by_message.get(message.id, []),
            }
        )
    for state_row in state_history:
        timeline.append(
            {
                "type": "state",
                "created_at": state_row.created_at.isoformat(),
                "previous_state": state_row.previous_state.value,
                "new_state": state_row.new_state.value,
                "reason": state_row.reason,
                "metadata": state_row.metadata_json,
            }
        )
    for note in notes:
        timeline.append(
            {
                "type": "note",
                "created_at": note.created_at.isoformat(),
                "body": str(note.decision.get("note", "")).strip(),
            }
        )
    for log in audit_logs:
        if log.event_type not in {"conversation_archived", "conversation_unarchived"}:
            continue
        timeline.append(
            {
                "type": "archive",
                "created_at": log.created_at.isoformat(),
                "body": "Archived from inbox" if log.event_type == "conversation_archived" else "Restored to inbox",
            }
        )
    for log in audit_logs:
        if log.event_type not in {"crm_stage_changed", "crm_stage_auto_updated"}:
            continue
        timeline.append(
            {
                "type": "crm_stage",
                "created_at": log.created_at.isoformat(),
                "previous_stage": str(log.decision.get("previous_stage") or ""),
                "new_stage": str(log.decision.get("new_stage") or ""),
                "reason": str(log.decision.get("reason") or log.event_type),
            }
        )
    for task in tasks:
        timeline.append(
            {
                "type": "task",
                "created_at": task.created_at.isoformat(),
                "status": normalize_task_status(task.status),
                "title": task.title,
            }
        )
        if task.completed_at:
            timeline.append(
                {
                    "type": "task_completed",
                    "created_at": task.completed_at.isoformat(),
                    "status": normalize_task_status(task.status),
                    "title": task.title,
                }
            )
    timeline.sort(key=lambda item: item["created_at"])

    last_activity_at = _last_activity_at(lead, messages[-1] if messages else None)
    client_visible_audits = {
        "internal_note",
        "crm_stage_changed",
        "crm_stage_auto_updated",
        "admin_booking_link_sent",
        "portal_booking_link_sent",
        "calendar_booking_offer_sent",
        "calendar_booking_created",
        "booking_confirmed",
        "crm_task_created",
        "crm_task_completed",
        "crm_task_reopened",
        "crm_task_updated",
        "manual_outbound_sent",
        "portal_manual_outbound_sent",
        "agent_paused",
        "agent_resumed",
        "agent_reply_suppressed",
        "ui_sandbox_started",
        "ui_sandbox_initial_ai_sms",
        "ui_sandbox_lead_message",
        "admin_marked_handoff",
        "portal_marked_handoff",
        "conversation_archived",
        "conversation_unarchived",
    }
    visible_audits = (
        [log for log in audit_logs if log.event_type in client_visible_audits]
        if actor.role == "client"
        else [
            log
            for log in audit_logs
            if log.event_type not in {"lead_normalized", "meta_webhook_received", "linkedin_webhook_received"}
        ]
    )
    normalized_answers = normalize_form_answers(lead.form_answers or {})

    return {
        "lead": {
            "id": lead.id,
            "display_name": _lead_display_name(lead),
            "full_name": lead.full_name,
            "phone": lead.phone,
            "email": lead.email,
            "city": lead.city,
            "source": lead.source.value,
            "form_answers": normalized_answers,
            "summary": _lead_summary(lead),
            "summary_lines": _lead_summary_lines(lead),
            "agent_insights": _lead_agent_insights(lead),
            "agent_control": get_agent_control(lead),
            "current_state": lead.conversation_state.value,
            "crm_stage": normalize_crm_stage(lead.crm_stage),
            "opted_out": lead.opted_out,
            "created_at": lead.created_at.isoformat(),
            "updated_at": lead.updated_at.isoformat(),
            "last_activity_at": last_activity_at.isoformat(),
            "last_inbound_at": lead.last_inbound_at.isoformat() if lead.last_inbound_at else None,
            "last_outbound_at": lead.last_outbound_at.isoformat() if lead.last_outbound_at else None,
            "tags": tags,
        },
        "client": {
            "client_key": client.client_key,
            "business_name": client.business_name,
            "booking_url": client.booking_url,
            "fallback_handoff_number": client.fallback_handoff_number,
            "tone": client.tone,
        },
        "messages": [
            {
                "id": message.id,
                "direction": message.direction.value,
                "body": message.body,
                "provider_message_sid": message.provider_message_sid,
                "attachments": attachments_by_message.get(message.id, []),
                "created_at": message.created_at.isoformat(),
            }
            for message in messages
        ],
        "state_transitions": [
            {
                "id": row.id,
                "previous_state": row.previous_state.value,
                "new_state": row.new_state.value,
                "reason": row.reason,
                "created_at": row.created_at.isoformat(),
                "metadata": row.metadata_json,
            }
            for row in state_history
        ],
        "notes": [_serialize_note(log) for log in notes],
        "tasks": [_serialize_task(task) for task in tasks],
        "audit_events": [
            {
                "id": log.id,
                "event_type": log.event_type,
                "created_at": log.created_at.isoformat(),
                "decision": log.decision,
            }
            for log in visible_audits[-12:]
        ],
        "timeline": timeline,
    }


@router.post("/ui/api/conversations/{lead_id}/notes")
def ui_add_internal_note(
    lead_id: int,
    payload: InternalNoteRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    note = payload.note.strip()
    if not note:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Note is required")

    lead = _load_lead_for_actor(db, actor, lead_id)
    now = datetime.now(timezone.utc)
    log = _create_internal_note(
        db=db,
        lead=lead,
        note=note,
        actor_role=actor.role,
        created_at=now,
    )
    db.commit()
    db.refresh(log)
    return {"status": "ok", "note": _serialize_note(log)}


@router.post("/ui/api/conversations/{lead_id}/actions/booking-link")
def ui_send_booking_link(
    lead_id: int,
    payload: BookingLinkActionRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    sms_service: SMSService = Depends(get_sms_service),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    lead = _load_lead_for_actor(db, actor, lead_id)
    if lead.opted_out:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Lead has opted out")
    if not lead.phone:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Lead has no phone number")
    if not lead.client or not lead.client.booking_url:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Client booking URL is missing")
    resolved_sms_service = _sms_service_for_client(
        sms_service=sms_service,
        settings=settings,
        db=db,
        client=lead.client,
    )

    now = datetime.now(timezone.utc)
    intro = payload.message.strip() if payload.message else "Here is the booking link whenever you are ready."
    body = ensure_booking_link(intro, lead.client)
    provider_sid = _send_sms_or_http_error(sms_service=resolved_sms_service, to_number=lead.phone, body=body)
    db.add(
        Message(
            lead_id=lead.id,
            client_id=lead.client_id,
            direction=MessageDirection.OUTBOUND,
            body=body,
            provider_message_sid=provider_sid,
            raw_payload={"source": "ui_admin_action", "action": "send_booking_link"},
            created_at=now,
        )
    )

    previous_state = lead.conversation_state
    if lead.conversation_state not in {ConversationStateEnum.BOOKED, ConversationStateEnum.OPTED_OUT}:
        lead.conversation_state = ConversationStateEnum.BOOKING_SENT
    lead.last_outbound_at = now
    lead.updated_at = now
    _set_crm_stage(
        db=db,
        lead=lead,
        new_stage=CRM_STAGE_CONTACTED,
        actor_role="system",
        reason="booking_link_sent",
        allow_backward=False,
        event_type="crm_stage_auto_updated",
        now=now,
    )
    if previous_state != lead.conversation_state:
        db.add(
            ConversationState(
                lead_id=lead.id,
                previous_state=previous_state,
                new_state=lead.conversation_state,
                reason="admin_booking_link_sent",
                metadata_json={"source": "ui"},
                created_at=now,
            )
        )
    db.add(
        AuditLog(
            client_id=lead.client_id,
            lead_id=lead.id,
            event_type="admin_booking_link_sent" if actor.role == "admin" else "portal_booking_link_sent",
            decision={"body": body, "provider_sid": provider_sid, "actor_role": actor.role},
            created_at=now,
        )
    )
    db.commit()
    return {"status": "ok", "provider_sid": provider_sid, "body": body, "state": lead.conversation_state.value}


@router.post("/ui/api/conversations/{lead_id}/messages/manual")
def ui_send_manual_message(
    lead_id: int,
    payload: ManualMessageRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    sms_service: SMSService = Depends(get_sms_service),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    lead = _load_lead_for_actor(db, actor, lead_id)
    resolved_sms_service = _sms_service_for_client(
        sms_service=sms_service,
        settings=settings,
        db=db,
        client=lead.client,
    )
    now = datetime.now(timezone.utc)
    provider_sid, state_value = _send_outbound_message(
        db=db,
        sms_service=resolved_sms_service,
        lead=lead,
        body=payload.body,
        created_at=now,
        raw_payload={"source": "owner_workspace", "action": "manual_message", "actor_role": actor.role},
        audit_event_type="manual_outbound_sent" if actor.role == "admin" else "portal_manual_outbound_sent",
        audit_decision={"source": "owner_workspace", "actor_role": actor.role},
        advance_new_to_greeted=True,
    )
    if payload.pause_agent:
        set_agent_control(
            lead,
            paused=True,
            actor_role=actor.role,
            now=now,
            reason="manual_reply_takeover",
            note="Paused automatically after a manual outbound message.",
        )
        db.add(
            AuditLog(
                client_id=lead.client_id,
                lead_id=lead.id,
                event_type="agent_paused",
                decision={"actor_role": actor.role, "reason": "manual_reply_takeover"},
                created_at=now,
            )
        )
    db.commit()
    return {
        "status": "ok",
        "lead_id": lead.id,
        "provider_sid": provider_sid,
        "state": state_value.value,
        "delivery_mode": _manual_delivery_mode(settings, db, client=lead.client),
        "agent_control": get_agent_control(lead),
    }


@router.post("/ui/api/conversations/{lead_id}/messages/manual-media")
async def ui_send_manual_media_message(
    lead_id: int,
    request: Request,
    body: str = Form(default=""),
    media: UploadFile = File(...),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    sms_service: SMSService = Depends(get_sms_service),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    lead = _load_lead_for_actor(db, actor, lead_id)
    if lead.opted_out:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Lead has opted out")
    if not lead.phone:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Lead has no phone number")

    delivery_mode = _manual_delivery_mode(settings, db, client=lead.client)
    if delivery_mode == "twilio" and not provider_public_base_url(settings, lead.client.provider_config if lead.client else {}):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Public base URL is required before Twilio can send uploaded media.",
        )

    content = await media.read()
    now = datetime.now(timezone.utc)
    message = Message(
        lead_id=lead.id,
        client_id=lead.client_id,
        direction=MessageDirection.OUTBOUND,
        body=body.strip(),
        provider_message_sid="",
        raw_payload={"source": "owner_workspace", "action": "manual_media_message", "actor_role": actor.role},
        created_at=now,
    )
    db.add(message)
    db.flush()
    try:
        stored = store_message_media(
            settings=settings,
            client_id=lead.client_id,
            message_id=message.id,
            filename=media.filename or "",
            content_type=media.content_type or "",
            content=content,
            raw_payload={"source": "owner_upload", "actor_role": actor.role},
        )
    except MessageMediaError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    attachment = create_message_attachment(message=message, lead=lead, stored=stored)
    db.add(attachment)
    db.flush()
    provider_media_url = attachment_public_url(settings, attachment, lead.client.provider_config if lead.client else {})
    media_urls = [provider_media_url] if provider_media_url else []
    resolved_sms_service = _sms_service_for_client(
        sms_service=sms_service,
        settings=settings,
        db=db,
        client=lead.client,
    )
    provider_sid = _send_mms_or_http_error(
        sms_service=resolved_sms_service,
        to_number=lead.phone,
        body=message.body,
        media_urls=media_urls,
    )
    attachment.provider_media_url = provider_media_url
    message.provider_message_sid = provider_sid
    serialized_attachment = _serialize_attachment(attachment)
    message.raw_payload = {
        **(message.raw_payload or {}),
        "provider_media_urls": media_urls,
        "attachments": [serialized_attachment],
    }
    lead.last_outbound_at = now
    lead.updated_at = now
    if lead.initial_sms_sent_at is None:
        lead.initial_sms_sent_at = now
    _set_crm_stage(
        db=db,
        lead=lead,
        new_stage=CRM_STAGE_CONTACTED,
        actor_role="system",
        reason="manual_media_outbound_sent" if actor.role == "admin" else "portal_manual_media_outbound_sent",
        allow_backward=False,
        event_type="crm_stage_auto_updated",
        now=now,
    )
    if lead.conversation_state == ConversationStateEnum.NEW:
        _create_state_transition(
            db,
            lead=lead,
            new_state=ConversationStateEnum.GREETED,
            reason="owner_manual_media_outbound",
            created_at=now,
            metadata_json={"source": "owner_workspace"},
        )
    event_type = "manual_media_outbound_sent" if actor.role == "admin" else "portal_manual_media_outbound_sent"
    db.add(
        AuditLog(
            client_id=lead.client_id,
            lead_id=lead.id,
            event_type=event_type,
            decision={
                "body": message.body,
                "provider_sid": provider_sid,
                "actor_role": actor.role,
                "attachments": [serialized_attachment],
                "delivery_mode": delivery_mode,
                "request_url": str(request.url),
            },
            created_at=now,
        )
    )
    db.commit()
    return {
        "status": "ok",
        "lead_id": lead.id,
        "provider_sid": provider_sid,
        "state": lead.conversation_state.value,
        "delivery_mode": delivery_mode,
        "attachments": [serialized_attachment],
    }


@router.post("/ui/api/conversations/{lead_id}/actions/handoff")
def ui_mark_handoff(
    lead_id: int,
    payload: HandoffActionRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    lead = _load_lead_for_actor(db, actor, lead_id)
    previous_state = lead.conversation_state
    now = datetime.now(timezone.utc)
    lead.conversation_state = ConversationStateEnum.HANDOFF
    set_agent_control(
        lead,
        paused=True,
        actor_role=actor.role,
        now=now,
        reason="human_handoff",
        note=(payload.note or "").strip(),
    )
    lead.updated_at = now
    if previous_state != lead.conversation_state:
        db.add(
            ConversationState(
            lead_id=lead.id,
                previous_state=previous_state,
                new_state=lead.conversation_state,
                reason="admin_marked_handoff" if actor.role == "admin" else "portal_marked_handoff",
                metadata_json={"source": "ui", "actor_role": actor.role},
                created_at=now,
            )
        )
    db.add(
        AuditLog(
            client_id=lead.client_id,
            lead_id=lead.id,
            event_type="admin_marked_handoff" if actor.role == "admin" else "portal_marked_handoff",
            decision={"note": (payload.note or "").strip(), "actor_role": actor.role},
            created_at=now,
        )
    )
    db.commit()
    return {"status": "ok", "state": lead.conversation_state.value}


@router.patch("/ui/api/conversations/{lead_id}/agent-control")
def ui_update_agent_control(
    lead_id: int,
    payload: AgentControlRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    lead = _load_lead_for_actor(db, actor, lead_id)
    now = datetime.now(timezone.utc)
    reason = (payload.reason or "").strip() or ("operator_paused" if payload.paused else "operator_resumed")
    previous_state = lead.conversation_state
    if not payload.paused and lead.opted_out:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot resume AI for an opted-out contact")
    if not payload.paused and lead.conversation_state == ConversationStateEnum.HANDOFF and not lead.opted_out:
        _create_state_transition(
            db,
            lead=lead,
            new_state=ConversationStateEnum.QUALIFYING,
            reason="agent_resumed",
            created_at=now,
            metadata_json={"source": "ui", "actor_role": actor.role},
        )
    control = set_agent_control(
        lead,
        paused=payload.paused,
        actor_role=actor.role,
        now=now,
        reason=reason,
        note=(payload.note or "").strip(),
    )
    db.add(
        AuditLog(
            client_id=lead.client_id,
            lead_id=lead.id,
            event_type="agent_paused" if payload.paused else "agent_resumed",
            decision={"actor_role": actor.role, "reason": reason, "note": (payload.note or "").strip()},
            created_at=now,
        )
    )
    db.commit()
    if previous_state != lead.conversation_state and not payload.paused:
        control = get_agent_control(lead)
    return {"status": "ok", "lead_id": lead.id, "state": lead.conversation_state.value, "agent_control": control}


@router.patch("/ui/api/conversations/{lead_id}/archive")
def ui_archive_conversation(
    lead_id: int,
    payload: ConversationArchiveRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    lead = _load_lead_for_actor(db, actor, lead_id)
    changed = _set_lead_archived(
        db=db,
        lead=lead,
        archived=payload.archived,
        actor_role=actor.role,
        created_at=datetime.now(timezone.utc),
    )
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        changed = False
    tags = _custom_tags_by_lead(db, [lead.id]).get(lead.id, [])
    return {
        "status": "ok",
        "lead_id": lead.id,
        "archived": _has_tag(tags, _ARCHIVED_TAG),
        "changed": changed,
        "tags": tags,
    }


@router.delete("/ui/api/conversations/{lead_id}")
def ui_delete_conversation(
    lead_id: int,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    _require_admin_actor(actor)
    lead = _load_lead_for_actor(db, actor, lead_id)

    snapshot = {
        "lead_id": lead.id,
        "display_name": _lead_display_name(lead),
        "phone": lead.phone,
        "email": lead.email,
        "state": lead.conversation_state.value,
        "actor_role": actor.role,
    }
    db.add(
        AuditLog(
            client_id=lead.client_id,
            lead_id=None,
            event_type="conversation_deleted",
            decision=snapshot,
            created_at=datetime.now(timezone.utc),
        )
    )
    db.delete(lead)
    db.commit()
    return {"status": "ok", "deleted_lead_id": lead_id}
