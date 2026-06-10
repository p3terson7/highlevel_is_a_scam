from fastapi import APIRouter
from .shared import *

router = APIRouter()

@router.get("/ui/api/clients")
def ui_clients(
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> list[dict[str, Any]]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    _require_admin_actor(actor)
    clients = db.scalars(select(Client).order_by(Client.business_name.asc())).all()
    output: list[dict[str, Any]] = []
    for client in clients:
        leads = db.scalars(select(Lead).where(Lead.client_id == client.id)).all()
        last_activity = max(
            (_last_activity_at(lead, None) for lead in leads),
            default=client.updated_at,
        )
        last_webhook = db.scalar(
            select(AuditLog)
            .where(AuditLog.client_id == client.id, AuditLog.event_type.in_(_WEBHOOK_EVENT_TYPES))
            .order_by(desc(AuditLog.created_at), desc(AuditLog.id))
            .limit(1)
        )
        output.append(
            {
                "id": client.id,
                "client_key": client.client_key,
                "business_name": client.business_name,
                "tone": client.tone,
                "timezone": client.timezone,
                "booking_url": client.booking_url,
                "is_active": client.is_active,
                "portal_enabled": client.portal_enabled,
                "lead_count": len(leads),
                "open_conversations": sum(1 for lead in leads if lead.conversation_state not in _CLOSED_STATES),
                "last_activity_at": last_activity.isoformat() if last_activity else None,
                "last_webhook_received_at": last_webhook.created_at.isoformat() if last_webhook else None,
            }
        )
    return output


@router.get("/ui/api/clients/{client_key}")
def ui_client_detail(
    client_key: str,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    _require_admin_actor(actor)
    client = _load_client_by_key(db, client_key)
    return _client_preview_payload(db, settings, client)


@router.get("/ui/api/clients/{client_key}/booking-preview")
def ui_client_booking_preview(
    client_key: str,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    booking_service: BookingService = Depends(get_booking_service),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    client = actor.client if actor.role == "client" else _load_client_by_key(db, client_key)
    if actor.role == "client" and client.client_key != client_key:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    if not automated_booking_enabled(client):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Automated booking is not configured")
    try:
        offer = booking_service.preview_slots(client, db=db)
    except BookingProviderError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {
        "status": "ok",
        "booking_mode": client.booking_mode,
        "slots": [slot.__dict__ for slot in offer.slots],
        "reply_text": offer.reply_text,
    }


@router.get("/ui/api/clients/{client_key}/calendar")
def ui_client_calendar(
    client_key: str,
    limit: int = Query(default=200, ge=1, le=500),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    client = actor.client if actor.role == "client" else _load_client_by_key(db, client_key)
    if actor.role == "client" and client.client_key != client_key:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")

    rows = db.execute(
        select(CalendarBooking, Lead)
        .outerjoin(Lead, Lead.id == CalendarBooking.lead_id)
        .where(CalendarBooking.client_id == client.id, CalendarBooking.status == "scheduled")
        .order_by(CalendarBooking.start_at.asc(), CalendarBooking.id.asc())
        .limit(limit)
    ).all()
    items = [
        {
            "id": booking.id,
            "lead_id": booking.lead_id,
            "lead_name": _lead_display_name(lead) if lead else "",
            "phone": lead.phone if lead else "",
            "email": lead.email if lead else "",
            "provider": booking.provider,
            "source": booking.source,
            "status": booking.status,
            "start_at": booking.start_at.isoformat(),
            "end_at": booking.end_at.isoformat(),
            "timezone": booking.timezone,
            "title": booking.title,
            "notes": booking.notes,
            "created_at": booking.created_at.isoformat(),
        }
        for booking, lead in rows
    ]
    return {
        "client_key": client.client_key,
        "booking_mode": client.booking_mode,
        "timezone": client.timezone,
        "total": len(items),
        "items": items,
    }


@router.get("/ui/api/clients/{client_key}/zapier-results")
def ui_client_zapier_results(
    client_key: str,
    limit: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    if actor.role == "client":
        client = actor.client
        if client is None or client.client_key != client_key:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    else:
        client = _load_client_by_key(db, client_key)

    logs = db.scalars(
        select(AuditLog)
        .where(
            AuditLog.client_id == client.id,
            AuditLog.event_type.in_(_ZAPIER_CONSOLE_EVENTS),
        )
        .order_by(desc(AuditLog.created_at), desc(AuditLog.id))
        .limit(limit)
    ).all()
    return {
        "client_key": client.client_key,
        "webhook_url": _webhook_urls(client.client_key)["zapier_events"],
        "items": [
            {
                "id": log.id,
                "event_type": log.event_type,
                "lead_id": log.lead_id,
                "created_at": log.created_at.isoformat(),
                "decision": log.decision,
            }
            for log in logs
        ],
    }


@router.get("/ui/api/owner/{client_key}")
def ui_owner_workspace(
    client_key: str,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    if actor.role == "client":
        client = actor.client
        if client is None or client.client_key != client_key:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    else:
        client = _load_client_by_key(db, client_key)
    return _owner_workspace_payload(db, settings, client)


@router.patch("/ui/api/owner/{client_key}/ai-context")
def ui_owner_update_ai_context(
    client_key: str,
    payload: OwnerAIContextUpdateRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    if actor.role == "client":
        client = actor.client
        if client is None or client.client_key != client_key:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    else:
        client = _load_client_by_key(db, client_key)

    client.ai_context = payload.ai_context.strip()
    if payload.faq_context is not None:
        client.faq_context = payload.faq_context.strip()
    db.add(client)
    db.commit()
    db.refresh(client)
    return {
        "status": "ok",
        "client_key": client.client_key,
        "ai_context": client.ai_context,
        "faq_context": client.faq_context,
        "updated_at": client.updated_at.isoformat(),
    }


@router.get("/ui/api/owner/{client_key}/knowledge")
def ui_owner_knowledge(
    client_key: str,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    if actor.role == "client":
        client = actor.client
        if client is None or client.client_key != client_key:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    else:
        client = _load_client_by_key(db, client_key)

    return {
        "status": "ok",
        "client_key": client.client_key,
        **knowledge_payload(db, client_id=client.id),
    }


@router.post("/ui/api/owner/{client_key}/knowledge/ingest")
def ui_owner_ingest_knowledge(
    client_key: str,
    payload: OwnerKnowledgeIngestRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    if actor.role == "client":
        client = actor.client
        if client is None or client.client_key != client_key:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    else:
        client = _load_client_by_key(db, client_key)

    if not payload.urls:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="At least one URL is required")

    service = KnowledgeIngestionService()
    try:
        extraction = service.ingest_urls(
            db=db,
            client_id=client.id,
            urls=payload.urls,
            replace=payload.replace,
        )
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Website knowledge tables are not available yet. Run alembic upgrade head, then retry ingestion.",
        ) from exc
    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=None,
            event_type="knowledge_urls_ingested",
            decision={
                "urls": payload.urls,
                "replace": payload.replace,
                "total_pages": extraction["total_pages"],
                "total_chunks": extraction["total_chunks"],
                "actor_role": actor.role,
            },
        )
    )
    db.commit()
    return {
        "status": "ok",
        "client_key": client.client_key,
        "extraction": extraction,
        **knowledge_payload(db, client_id=client.id),
    }


@router.patch("/ui/api/owner/{client_key}/calendar")
def ui_owner_update_calendar(
    client_key: str,
    payload: OwnerCalendarUpdateRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    if actor.role == "client":
        client = actor.client
        if client is None or client.client_key != client_key:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    else:
        client = _load_client_by_key(db, client_key)

    rows: list[dict[str, Any]] = []
    for row in payload.availability:
        if row.day < 0 or row.day > 6:
            continue
        start = str(row.start).strip()
        end = str(row.end).strip()
        if not start or not end:
            continue
        rows.append(
            {
                "day": int(row.day),
                "start": start,
                "end": end,
                "enabled": bool(row.enabled),
            }
        )

    internal_calendar = {
        "slot_minutes": max(15, min(180, int(payload.slot_minutes))),
        "notice_minutes": max(0, min(24 * 60, int(payload.notice_minutes))),
        "horizon_days": max(1, min(60, int(payload.horizon_days))),
        "availability": rows,
    }
    booking_config = dict(client.booking_config or {}) if isinstance(client.booking_config, dict) else {}
    booking_config["internal_calendar"] = internal_calendar
    client.booking_config = booking_config
    client.booking_mode = "internal"
    db.add(client)
    db.commit()
    db.refresh(client)

    return {
        "status": "ok",
        "client_key": client.client_key,
        "booking_mode": client.booking_mode,
        "internal_calendar": internal_calendar_preview_config(client),
        "updated_at": client.updated_at.isoformat(),
    }

