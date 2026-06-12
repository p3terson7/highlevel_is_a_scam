from fastapi import APIRouter
from .shared import *

router = APIRouter()


def _lead_business_metrics(lead: Lead) -> dict[str, Any]:
    raw_payload = lead.raw_payload if isinstance(lead.raw_payload, dict) else {}
    summary = raw_payload.get("lead_summary") if isinstance(raw_payload.get("lead_summary"), dict) else {}

    def number_or_none(value: Any) -> int | float | None:
        try:
            if value is None or value == "":
                return None
            number = float(value)
        except (TypeError, ValueError):
            return None
        return int(number) if number.is_integer() else number

    return {
        "lead_score": number_or_none(raw_payload.get("lead_score") or raw_payload.get("intent_score")),
        "estimated_value": number_or_none(raw_payload.get("estimated_value")),
        "campaign_name": str(raw_payload.get("campaign_name") or "").strip(),
        "intent_level": str(summary.get("intent_level") or "").strip(),
        "recommended_follow_up": str(summary.get("recommended_follow_up") or "").strip(),
    }


@router.get("/ui/api/crm/leads")
def ui_crm_leads(
    client_key: str | None = Query(default=None),
    stage: str | None = Query(default=None),
    q: str | None = Query(default=None),
    archived: bool = Query(default=False),
    limit: int = Query(default=300, ge=1, le=1000),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    stage_filter = _parse_crm_stage_filter(stage)

    stmt = select(Lead).options(selectinload(Lead.client)).join(Client)
    effective_client_key = _scoped_client_key(actor, client_key)
    if effective_client_key:
        stmt = stmt.where(Client.client_key == effective_client_key)
    if stage_filter:
        stmt = stmt.where(Lead.crm_stage == stage_filter)
    archived_lead_ids = select(LeadTag.lead_id).where(LeadTag.tag == _ARCHIVED_TAG)
    stmt = stmt.where(Lead.id.in_(archived_lead_ids) if archived else ~Lead.id.in_(archived_lead_ids))
    if not q:
        stmt = stmt.limit(limit)
    leads = db.scalars(stmt.order_by(desc(Lead.updated_at), desc(Lead.created_at))).unique().all()

    lead_ids = [lead.id for lead in leads]
    latest_messages = _latest_messages_by_lead(db, lead_ids)
    logs_by_lead = _logs_by_lead(db, lead_ids)
    custom_tags_by_lead = _custom_tags_by_lead(db, lead_ids)
    next_tasks_by_lead: dict[int, LeadTask] = {}
    if lead_ids:
        open_tasks = db.scalars(
            select(LeadTask)
            .where(LeadTask.lead_id.in_(lead_ids), LeadTask.status == TASK_STATUS_OPEN)
            .order_by(LeadTask.due_date.asc(), LeadTask.created_at.asc(), LeadTask.id.asc())
        ).all()
        for task in open_tasks:
            next_tasks_by_lead.setdefault(task.lead_id, task)
    query_lower = (q or "").strip().lower()

    items: list[dict[str, Any]] = []
    for lead in leads:
        crm_stage = normalize_crm_stage(lead.crm_stage)
        if stage_filter and crm_stage != stage_filter:
            continue
        latest_message = latest_messages.get(lead.id)
        logs = logs_by_lead.get(lead.id, [])
        tags = _merged_tags(
            custom_tags=custom_tags_by_lead.get(lead.id, []),
            conversation_tags=_conversation_tags(lead, logs),
        )
        summary = _lead_summary(lead)
        metrics = _lead_business_metrics(lead)
        search_blob = " ".join(
            [
                _lead_search_blob(lead),
                crm_stage,
                lead.source.value,
                summary,
                " ".join(tags),
                str(metrics.get("campaign_name") or ""),
                str(metrics.get("intent_level") or ""),
                str(metrics.get("recommended_follow_up") or ""),
            ]
        ).lower()
        if query_lower and query_lower not in search_blob:
            continue

        last_activity_at = _last_activity_at(lead, latest_message)
        booked = crm_stage in {"Meeting Booked", "Meeting Completed", "Won"} or lead.conversation_state == ConversationStateEnum.BOOKED
        next_task = next_tasks_by_lead.get(lead.id)
        items.append(
            {
                "lead_id": lead.id,
                "lead_name": _lead_display_name(lead),
                "phone": lead.phone,
                "email": lead.email,
                "source": lead.source.value,
                "client_key": lead.client.client_key if lead.client else "",
                "client_name": lead.client.business_name if lead.client else "",
                "crm_stage": crm_stage,
                "conversation_state": lead.conversation_state.value,
                "last_message_snippet": _snippet(_message_preview_text(latest_message)),
                "last_message_direction": latest_message.direction.value if latest_message else "",
                "lead_summary": summary,
                "last_activity_at": last_activity_at.isoformat(),
                "created_at": lead.created_at.isoformat(),
                "tags": tags,
                "booked": booked,
                "archived": _has_tag(tags, _ARCHIVED_TAG),
                **metrics,
                "next_task_title": next_task.title if next_task else "",
                "next_task_due_date": next_task.due_date.isoformat() if next_task and next_task.due_date else "",
            }
        )

    items.sort(key=lambda item: item["last_activity_at"], reverse=True)
    limited = items[:limit]
    counts = Counter(item["crm_stage"] for item in limited)
    return {
        "items": limited,
        "counts": dict(counts),
        "total": len(limited),
        "stages": CRM_STAGES,
    }


@router.post("/ui/api/crm/leads")
def ui_crm_create_lead(
    payload: ManualLeadCreateRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    client = _client_for_actor(db, actor, payload.client_key)
    full_name = payload.full_name.strip()
    if not full_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Lead name is required")
    crm_stage = normalize_crm_stage(payload.crm_stage or "New Lead")
    if crm_stage not in CRM_STAGES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid CRM stage")

    now = datetime.now(timezone.utc)
    lead = Lead(
        client_id=client.id,
        external_lead_id=f"manual-{uuid4().hex}",
        source=LeadSource.MANUAL,
        full_name=full_name,
        phone=normalize_phone(payload.phone or ""),
        email=(payload.email or "").strip(),
        city=(payload.city or "").strip(),
        owner_name=(payload.owner_name or "").strip(),
        form_answers={},
        raw_payload={"source": "ui_manual_lead", "created_by": actor.role},
        consented=True,
        opted_out=False,
        conversation_state=ConversationStateEnum.NEW,
        crm_stage=crm_stage,
        created_at=now,
        updated_at=now,
    )
    db.add(lead)
    db.flush()
    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=lead.id,
            event_type="manual_lead_created",
            decision={"actor_role": actor.role, "crm_stage": crm_stage},
            created_at=now,
        )
    )
    note = (payload.notes or "").strip()
    if note:
        _create_internal_note(db=db, lead=lead, note=note, actor_role=actor.role, created_at=now)
    db.commit()
    db.refresh(lead)
    return {
        "status": "ok",
        "lead": {
            "id": lead.id,
            "lead_id": lead.id,
            "display_name": _lead_display_name(lead),
            "client_key": client.client_key,
            "crm_stage": normalize_crm_stage(lead.crm_stage),
            "conversation_state": lead.conversation_state.value,
        },
    }


@router.get("/ui/api/crm/leads/{lead_id}")
def ui_crm_lead_detail(
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
    tasks = db.scalars(
        select(LeadTask)
        .where(LeadTask.lead_id == lead.id)
        .order_by(LeadTask.status.desc(), LeadTask.due_date.asc(), desc(LeadTask.created_at))
    ).all()
    tags = _custom_tags_by_lead(db, [lead.id]).get(lead.id, [])
    notes = [log for log in audit_logs if log.event_type == "internal_note"]
    timeline: list[dict[str, Any]] = [
        {"type": "lead_created", "created_at": lead.created_at.isoformat(), "label": "Lead created"}
    ]

    for message in messages:
        timeline.append(
            {
                "type": "message",
                "created_at": message.created_at.isoformat(),
                "direction": message.direction.value,
                "body": message.body,
                "attachments": attachments_by_message.get(message.id, []),
            }
        )
    for state_row in state_history:
        timeline.append(
            {
                "type": "conversation_state",
                "created_at": state_row.created_at.isoformat(),
                "label": f"{state_row.previous_state.value} -> {state_row.new_state.value}",
                "reason": state_row.reason,
            }
        )
    for log in audit_logs:
        if log.event_type in {"crm_stage_changed", "crm_stage_auto_updated"}:
            timeline.append(
                {
                    "type": "crm_stage",
                    "created_at": log.created_at.isoformat(),
                    "label": f"{log.decision.get('previous_stage', '-') } -> {log.decision.get('new_stage', '-')}",
                    "reason": str(log.decision.get("reason") or log.event_type),
                }
            )
        elif log.event_type in {"booking_confirmed", "calendar_booking_created", "calendar_booking_offer_sent", "admin_booking_link_sent"}:
            timeline.append(
                {
                    "type": "booking_event",
                    "created_at": log.created_at.isoformat(),
                    "label": log.event_type,
                    "decision": log.decision,
                }
            )
        elif log.event_type == "internal_note":
            timeline.append(
                {
                    "type": "note",
                    "created_at": log.created_at.isoformat(),
                    "label": "Note added",
                    "body": str(log.decision.get("note") or ""),
                }
            )
        elif log.event_type in {"conversation_archived", "conversation_unarchived"}:
            timeline.append(
                {
                    "type": "archive_event",
                    "created_at": log.created_at.isoformat(),
                    "label": "Archived from inbox" if log.event_type == "conversation_archived" else "Restored to inbox",
                }
            )
    for task in tasks:
        timeline.append(
            {
                "type": "task_event",
                "created_at": task.created_at.isoformat(),
                "label": f"Task created: {task.title}",
            }
        )
        if task.completed_at:
            timeline.append(
                {
                    "type": "task_event",
                    "created_at": task.completed_at.isoformat(),
                    "label": f"Task completed: {task.title}",
                }
            )
    timeline.sort(key=lambda item: item["created_at"])

    latest_message = messages[-1] if messages else None
    last_activity_at = _last_activity_at(lead, latest_message)
    merged_tags = _merged_tags(
        custom_tags=tags,
        conversation_tags=_conversation_tags(lead, list(reversed(audit_logs))),
    )
    normalized_answers = normalize_form_answers(lead.form_answers or {})

    return {
        "lead": {
            "id": lead.id,
            "display_name": _lead_display_name(lead),
            "full_name": lead.full_name,
            "phone": lead.phone,
            "email": lead.email,
            "source": lead.source.value,
            "city": lead.city,
            "owner": lead.owner_name or None,
            "crm_stage": normalize_crm_stage(lead.crm_stage),
            "conversation_state": lead.conversation_state.value,
            "summary": _lead_summary(lead),
            "summary_lines": _lead_summary_lines(lead),
            "form_answers": normalized_answers,
            "created_at": lead.created_at.isoformat(),
            "last_activity_at": last_activity_at.isoformat(),
            "last_inbound_at": lead.last_inbound_at.isoformat() if lead.last_inbound_at else None,
            "last_outbound_at": lead.last_outbound_at.isoformat() if lead.last_outbound_at else None,
            "tags": merged_tags,
            **_lead_business_metrics(lead),
        },
        "client": {
            "client_key": client.client_key,
            "business_name": client.business_name,
            "timezone": client.timezone,
            "booking_url": client.booking_url,
            "fallback_handoff_number": client.fallback_handoff_number,
        },
        "messages": [
            {
                "id": msg.id,
                "direction": msg.direction.value,
                "body": msg.body,
                "provider_message_sid": msg.provider_message_sid,
                "attachments": attachments_by_message.get(msg.id, []),
                "created_at": msg.created_at.isoformat(),
            }
            for msg in messages
        ],
        "notes": [_serialize_note(log) for log in notes],
        "tasks": [_serialize_task(task) for task in tasks],
        "tags": tags,
        "timeline": timeline,
        "audit_events": [
            {
                "id": log.id,
                "event_type": log.event_type,
                "created_at": log.created_at.isoformat(),
                "decision": log.decision,
            }
            for log in audit_logs[-25:]
        ],
        "stages": CRM_STAGES,
    }


@router.patch("/ui/api/crm/leads/{lead_id}/stage")
def ui_crm_update_stage(
    lead_id: int,
    payload: CRMStageUpdateRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    lead = _load_lead_for_actor(db, actor, lead_id)
    now = datetime.now(timezone.utc)
    changed = _set_crm_stage(
        db=db,
        lead=lead,
        new_stage=payload.stage,
        actor_role=actor.role,
        reason="manual_update",
        allow_backward=True,
        event_type="crm_stage_changed",
        now=now,
    )
    if changed and normalize_crm_stage(lead.crm_stage) == "Meeting Booked":
        if lead.conversation_state != ConversationStateEnum.BOOKED:
            _create_state_transition(
                db,
                lead=lead,
                new_state=ConversationStateEnum.BOOKED,
                reason="crm_stage_marked_meeting_booked",
                created_at=now,
                metadata_json={"source": "ui_crm", "actor_role": actor.role},
            )
    elif changed and normalize_crm_stage(lead.crm_stage) == "Lost":
        if lead.conversation_state != ConversationStateEnum.OPTED_OUT:
            _create_state_transition(
                db,
                lead=lead,
                new_state=ConversationStateEnum.OPTED_OUT,
                reason="crm_stage_marked_lost",
                created_at=now,
                metadata_json={"source": "ui_crm", "actor_role": actor.role},
            )
    db.commit()
    return {
        "status": "ok",
        "lead_id": lead.id,
        "crm_stage": normalize_crm_stage(lead.crm_stage),
        "changed": changed,
    }


@router.post("/ui/api/crm/leads/{lead_id}/notes")
def ui_crm_add_note(
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
    log = _create_internal_note(
        db=db,
        lead=lead,
        note=note,
        actor_role=actor.role,
        created_at=datetime.now(timezone.utc),
    )
    db.commit()
    db.refresh(log)
    return {"status": "ok", "note": _serialize_note(log)}


@router.post("/ui/api/crm/leads/{lead_id}/tags")
def ui_crm_add_tag(
    lead_id: int,
    payload: CRMTagRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    lead = _load_lead_for_actor(db, actor, lead_id)
    tag = normalize_tag(payload.tag)
    if not tag:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Tag is required")

    existing = db.scalar(select(LeadTag).where(LeadTag.lead_id == lead.id, LeadTag.tag == tag))
    if existing is None:
        db.add(LeadTag(lead_id=lead.id, client_id=lead.client_id, tag=tag))
        now = datetime.now(timezone.utc)
        lead.updated_at = now
        db.add(
            AuditLog(
                client_id=lead.client_id,
                lead_id=lead.id,
                event_type="crm_tag_added",
                decision={"tag": tag, "actor_role": actor.role},
                created_at=now,
            )
        )
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
    tags = _custom_tags_by_lead(db, [lead.id]).get(lead.id, [])
    return {"status": "ok", "tags": tags}


@router.delete("/ui/api/crm/leads/{lead_id}/tags/{tag}")
def ui_crm_delete_tag(
    lead_id: int,
    tag: str,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    lead = _load_lead_for_actor(db, actor, lead_id)
    normalized_tag = normalize_tag(tag)
    row = db.scalar(
        select(LeadTag).where(LeadTag.lead_id == lead.id, LeadTag.tag == normalized_tag)
    )
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tag not found")
    db.delete(row)
    now = datetime.now(timezone.utc)
    lead.updated_at = now
    db.add(
        AuditLog(
            client_id=lead.client_id,
            lead_id=lead.id,
            event_type="crm_tag_removed",
            decision={"tag": normalized_tag, "actor_role": actor.role},
            created_at=now,
        )
    )
    db.commit()
    tags = _custom_tags_by_lead(db, [lead.id]).get(lead.id, [])
    return {"status": "ok", "tags": tags}


@router.get("/ui/api/crm/tasks")
def ui_crm_tasks(
    client_key: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    q: str | None = Query(default=None),
    limit: int = Query(default=300, ge=1, le=1000),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    task_status = _parse_task_status_filter(status_filter)
    stmt = (
        select(LeadTask)
        .join(Lead, LeadTask.lead_id == Lead.id)
        .join(Client, LeadTask.client_id == Client.id)
        .options(selectinload(LeadTask.lead).selectinload(Lead.client))
    )
    effective_client_key = _scoped_client_key(actor, client_key)
    if effective_client_key:
        stmt = stmt.where(Client.client_key == effective_client_key)
    if task_status:
        stmt = stmt.where(LeadTask.status == task_status)
    if not q:
        stmt = stmt.limit(limit)
    tasks = db.scalars(
        stmt.order_by(LeadTask.status.desc(), LeadTask.due_date.asc(), desc(LeadTask.created_at))
    ).unique().all()

    query_lower = (q or "").strip().lower()
    items: list[dict[str, Any]] = []
    for task in tasks:
        lead = task.lead
        if lead is None:
            continue
        blob = " ".join(
            [
                task.title,
                task.description or "",
                lead.full_name or "",
                lead.phone or "",
                lead.email or "",
                lead.client.business_name if lead.client else "",
            ]
        ).lower()
        if query_lower and query_lower not in blob:
            continue
        item = _serialize_task(task)
        item.update(
            {
                "lead_name": _lead_display_name(lead),
                "lead_phone": lead.phone,
                "lead_email": lead.email,
                "crm_stage": normalize_crm_stage(lead.crm_stage),
                "conversation_state": lead.conversation_state.value,
                "client_key": lead.client.client_key if lead.client else "",
                "client_name": lead.client.business_name if lead.client else "",
            }
        )
        items.append(item)

    limited = items[:limit]
    counts = Counter(item["status"] for item in limited)
    return {"items": limited, "counts": dict(counts), "total": len(limited)}


@router.post("/ui/api/crm/leads/{lead_id}/tasks")
def ui_crm_create_task(
    lead_id: int,
    payload: CRMTaskCreateRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    lead = _load_lead_for_actor(db, actor, lead_id)
    title = payload.title.strip()
    if not title:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Task title is required")
    try:
        due_date = parse_due_date(payload.due_date)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid due_date") from exc

    now = datetime.now(timezone.utc)
    task = LeadTask(
        lead_id=lead.id,
        client_id=lead.client_id,
        title=title,
        description=(payload.description or "").strip(),
        due_date=due_date,
        status=TASK_STATUS_OPEN,
        created_by=actor.role,
        created_at=now,
        updated_at=now,
    )
    db.add(task)
    lead.updated_at = now
    db.add(
        AuditLog(
            client_id=lead.client_id,
            lead_id=lead.id,
            event_type="crm_task_created",
            decision={"title": task.title, "due_date": task.due_date.isoformat() if task.due_date else None, "actor_role": actor.role},
            created_at=now,
        )
    )
    db.commit()
    db.refresh(task)
    return {"status": "ok", "task": _serialize_task(task)}


@router.patch("/ui/api/crm/tasks/{task_id}")
def ui_crm_update_task(
    task_id: int,
    payload: CRMTaskUpdateRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    task = _load_task_for_actor(db, actor, task_id)
    lead = _load_lead_for_actor(db, actor, task.lead_id)
    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        return {"status": "ok", "task": _serialize_task(task)}

    now = datetime.now(timezone.utc)
    event_type = "crm_task_updated"
    event_decision: dict[str, Any] = {"actor_role": actor.role, "task_id": task.id}

    if "title" in updates and updates["title"] is not None:
        title = str(updates["title"]).strip()
        if not title:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Task title cannot be empty")
        task.title = title
    if "description" in updates and updates["description"] is not None:
        task.description = str(updates["description"]).strip()
    if "due_date" in updates:
        try:
            task.due_date = parse_due_date(updates["due_date"])
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid due_date") from exc
    if "status" in updates and updates["status"] is not None:
        next_status = normalize_task_status(updates["status"])
        if next_status not in {TASK_STATUS_OPEN, TASK_STATUS_DONE}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid status")
        previous_status = normalize_task_status(task.status)
        task.status = next_status
        if previous_status != next_status:
            if next_status == TASK_STATUS_DONE:
                task.completed_at = now
                event_type = "crm_task_completed"
            else:
                task.completed_at = None
                event_type = "crm_task_reopened"
            event_decision["previous_status"] = previous_status
            event_decision["new_status"] = next_status
    task.updated_at = now
    lead.updated_at = now
    event_decision["title"] = task.title
    event_decision["due_date"] = task.due_date.isoformat() if task.due_date else None
    db.add(
        AuditLog(
            client_id=task.client_id,
            lead_id=task.lead_id,
            event_type=event_type,
            decision=event_decision,
            created_at=now,
        )
    )
    db.commit()
    db.refresh(task)
    return {"status": "ok", "task": _serialize_task(task)}
