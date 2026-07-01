from fastapi import APIRouter
from .shared import *

router = APIRouter()

@router.get("/ui/api/dashboard")
def ui_dashboard(
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    scoped_client = actor.client if actor.role == "client" else None
    runtime = _runtime_summary(settings, db, client=scoped_client)
    clients = [scoped_client] if scoped_client else db.scalars(select(Client).order_by(Client.business_name.asc())).all()

    now = datetime.now(timezone.utc)
    today = now.date()
    seven_days_ago = today - timedelta(days=6)
    thirty_days_ago = today - timedelta(days=29)
    one_day_ago = now - timedelta(days=1)
    scoped_client_ids = [client.id for client in clients if client is not None]
    lead_conditions: list[Any] = []
    if scoped_client is not None:
        lead_conditions.append(Lead.client_id == scoped_client.id)

    leads_stmt = select(Lead).options(selectinload(Lead.client))
    if lead_conditions:
        leads_stmt = leads_stmt.where(*lead_conditions)

    recent_conversation_leads = db.scalars(
        leads_stmt.order_by(desc(Lead.updated_at), desc(Lead.created_at), desc(Lead.id)).limit(80)
    ).unique().all()
    recent_conversations = _build_conversation_items(db, recent_conversation_leads, limit=8)
    conversations_total = _dashboard_lead_count(db, lead_conditions)

    last_webhook_query = select(AuditLog).where(AuditLog.event_type.in_(_WEBHOOK_EVENT_TYPES))
    last_inbound_query = select(Message).where(Message.direction == MessageDirection.INBOUND)
    last_outbound_query = select(Message).where(Message.direction == MessageDirection.OUTBOUND)
    last_ai_decision_query = select(AuditLog).where(AuditLog.event_type.in_(["agent_decision", "admin_test_ai_decision"]))
    if scoped_client_ids:
        last_webhook_query = last_webhook_query.where(AuditLog.client_id.in_(scoped_client_ids))
        last_inbound_query = last_inbound_query.where(Message.client_id.in_(scoped_client_ids))
        last_outbound_query = last_outbound_query.where(Message.client_id.in_(scoped_client_ids))
        last_ai_decision_query = last_ai_decision_query.where(AuditLog.client_id.in_(scoped_client_ids))

    last_webhook = db.scalar(last_webhook_query.order_by(desc(AuditLog.created_at), desc(AuditLog.id)).limit(1))
    last_inbound = db.scalar(last_inbound_query.order_by(desc(Message.created_at), desc(Message.id)).limit(1))
    last_outbound = db.scalar(last_outbound_query.order_by(desc(Message.created_at), desc(Message.id)).limit(1))
    last_ai_decision = db.scalar(last_ai_decision_query.order_by(desc(AuditLog.created_at), desc(AuditLog.id)).limit(1))

    if actor.role == "client" and scoped_client is not None:
        onboarding = [
            {
                "label": "SMS delivery ready",
                "done": runtime["twilio_configured"],
                "detail": runtime["twilio_from_number"] or "Add Twilio credentials to send live SMS replies.",
            },
            {
                "label": "AI assistant ready",
                "done": runtime["ai_configured"],
                "detail": runtime["openai_model"] if runtime["ai_configured"] else "OpenAI key missing; AI replies are unavailable.",
            },
            {
                "label": "Booking availability ready",
                "done": automated_booking_enabled(scoped_client),
                "detail": _booking_ready_detail(scoped_client),
            },
            {
                "label": "Lead intake active",
                "done": conversations_total > 0 or last_webhook is not None,
                "detail": last_webhook.created_at.isoformat() if last_webhook else "No webhook traffic recorded yet.",
            },
        ]
    else:
        clients_with_sms = sum(
            1
            for client in clients
            if all(client_runtime_overrides(client).get(key) for key in ("twilio_account_sid", "twilio_auth_token", "twilio_from_number"))
        )
        onboarding = [
            {
                "label": "Configure client SMS",
                "done": clients_with_sms > 0,
                "detail": f"{clients_with_sms} client(s) have Twilio credentials. Set these in Clients > Edit.",
            },
            {
                "label": "Configure AI",
                "done": runtime["ai_configured"],
                "detail": runtime["openai_model"] if runtime["ai_configured"] else "OpenAI key missing; AI replies are unavailable.",
            },
            {
                "label": "Create or seed clients",
                "done": bool(clients),
                "detail": f"{len(clients)} client(s) available.",
            },
            {
                "label": "Seed demo data",
                "done": demo_data_present(db),
                "detail": "Available in dev for populated onboarding." if can_seed_demo(settings) else "Disabled outside dev unless feature flag is enabled.",
            },
            {
                "label": "Receive a real webhook",
                "done": last_webhook is not None,
                "detail": last_webhook.created_at.isoformat() if last_webhook else "No webhook traffic recorded yet.",
            },
        ]

    recent_lead_rows = db.scalars(
        leads_stmt.order_by(desc(Lead.created_at), desc(Lead.id)).limit(8)
    ).unique().all()
    latest_messages = _latest_messages_by_lead(db, [lead.id for lead in recent_lead_rows])
    recent_leads = [
        {
            "lead_id": lead.id,
            "lead_name": _lead_display_name(lead),
            "phone": lead.phone,
            "email": lead.email,
            "source": lead.source.value,
            "client_key": lead.client.client_key if lead.client else "",
            "client_name": lead.client.business_name if lead.client else "",
            "crm_stage": normalize_crm_stage(lead.crm_stage),
            "conversation_state": lead.conversation_state.value,
            "created_at": lead.created_at.isoformat(),
            "last_message_snippet": _snippet(_message_preview_text(latest_messages.get(lead.id))),
            "last_message_direction": latest_messages.get(lead.id).direction.value if latest_messages.get(lead.id) else "",
            "last_message_delivery": delivery_status_for_message(latest_messages.get(lead.id)) if latest_messages.get(lead.id) else None,
        }
        for lead in recent_lead_rows
    ]
    source_counts = _dashboard_counter_rows(db, Lead.source, lead_conditions)
    stage_counts: Counter[str] = Counter()
    for stage, count in _dashboard_counter_rows(db, Lead.crm_stage, lead_conditions).items():
        stage_counts[normalize_crm_stage(stage)] += count
    task_summary, upcoming_tasks = _dashboard_open_tasks(db, actor, today=today, limit=5)
    meeting_summary, upcoming_meetings = _dashboard_upcoming_meetings(db, actor, now=now, limit=5)

    lead_trend: list[dict[str, Any]] = []
    current_week_start = today - timedelta(days=today.weekday())
    for offset in range(5, -1, -1):
        week_start = current_week_start - timedelta(days=offset * 7)
        week_end = week_start + timedelta(days=6)
        week_start_at = _start_of_day_utc(week_start)
        week_end_at = _start_of_day_utc(week_end + timedelta(days=1))
        lead_trend.append(
            {
                "week_start": week_start.isoformat(),
                "week_end": week_end.isoformat(),
                "count": _dashboard_lead_count(
                    db,
                    lead_conditions,
                    Lead.created_at >= week_start_at,
                    Lead.created_at < week_end_at,
                ),
            }
        )

    top_clients: list[dict[str, Any]] = []
    if actor.role == "admin":
        client_rows = db.execute(
            select(
                Client.id,
                Client.client_key,
                Client.business_name,
                Client.is_active,
                Client.updated_at,
                func.count(Lead.id).label("lead_count"),
                func.sum(case((Lead.conversation_state.notin_(list(_CLOSED_STATES)), 1), else_=0)).label("open_count"),
                func.sum(case((Lead.conversation_state == ConversationStateEnum.BOOKED, 1), else_=0)).label("booked_count"),
                func.max(Lead.updated_at).label("last_lead_activity"),
            )
            .outerjoin(Lead, Lead.client_id == Client.id)
            .group_by(Client.id, Client.client_key, Client.business_name, Client.is_active, Client.updated_at)
        ).all()
        for row in client_rows:
            last_activity = row.last_lead_activity or row.updated_at
            top_clients.append(
                {
                    "client_key": row.client_key,
                    "business_name": row.business_name,
                    "lead_count": int(row.lead_count or 0),
                    "open_conversations": int(row.open_count or 0),
                    "booked_total": int(row.booked_count or 0),
                    "last_activity_at": last_activity.isoformat() if last_activity else None,
                    "is_active": row.is_active,
                }
            )
        top_clients.sort(
            key=lambda item: (
                item["lead_count"],
                item["open_conversations"],
                item["last_activity_at"] or "",
            ),
            reverse=True,
        )
        top_clients = top_clients[:5]

    attention_count = _dashboard_lead_count(db, lead_conditions, Lead.conversation_state.notin_(list(_CLOSED_STATES)))
    booked_total = _dashboard_lead_count(db, lead_conditions, Lead.conversation_state == ConversationStateEnum.BOOKED)
    won_total = _dashboard_lead_count(db, lead_conditions, Lead.crm_stage == "Won")
    handoff_total = _dashboard_lead_count(db, lead_conditions, Lead.conversation_state == ConversationStateEnum.HANDOFF)
    new_last_7_days = _dashboard_lead_count(db, lead_conditions, Lead.created_at >= _start_of_day_utc(seven_days_ago))
    new_last_30_days = _dashboard_lead_count(db, lead_conditions, Lead.created_at >= _start_of_day_utc(thirty_days_ago))
    open_pipeline_total = _dashboard_lead_count(
        db,
        lead_conditions,
        Lead.crm_stage.notin_(["Won", "Lost"]),
        Lead.conversation_state != ConversationStateEnum.OPTED_OUT,
    )
    return {
        "scope": {
            "role": actor.role,
            "client_key": scoped_client.client_key if scoped_client else None,
            "client_name": scoped_client.business_name if scoped_client else None,
            "title": scoped_client.business_name if scoped_client else "Lead portfolio",
        },
        "runtime": runtime,
        "stats": {
            "clients_total": len(clients),
            "active_clients": sum(1 for client in clients if client.is_active),
            "conversations_total": conversations_total,
            "total_leads": conversations_total,
            "attention_needed": attention_count,
            "booked_total": booked_total,
            "handoff_total": handoff_total,
            "won_total": won_total,
            "new_last_24_hours": _dashboard_lead_count(db, lead_conditions, Lead.created_at >= one_day_ago),
            "new_last_7_days": new_last_7_days,
            "new_last_30_days": new_last_30_days,
            "open_pipeline_total": open_pipeline_total,
            "open_tasks_total": task_summary["total"],
            "overdue_tasks_total": task_summary["overdue"],
            "due_today_tasks": task_summary["due_today"],
            "upcoming_meetings_total": meeting_summary["total"],
            "upcoming_meetings_7d": meeting_summary["next_7_days"],
            "booked_rate": (booked_total / conversations_total) if conversations_total else 0,
            "won_rate": (won_total / conversations_total) if conversations_total else 0,
        },
        "lead_trend": lead_trend,
        "source_breakdown": _dashboard_breakdown_rows(
            source_counts,
            ordered_keys=[
                LeadSource.META.value,
                LeadSource.LINKEDIN.value,
                LeadSource.SMS.value,
                LeadSource.MANUAL.value,
            ],
        ),
        "campaign_performance": _dashboard_campaign_performance(clients),
        "stage_breakdown": _dashboard_breakdown_rows(stage_counts, ordered_keys=CRM_STAGES),
        "onboarding": onboarding,
        "top_clients": top_clients,
        "upcoming": {
            "tasks": upcoming_tasks,
            "meetings": upcoming_meetings,
        },
        "recent_leads": recent_leads,
        "recent_conversations": recent_conversations,
        "latest_activity": {
            "last_webhook_received_at": last_webhook.created_at.isoformat() if last_webhook else None,
            "last_sms_inbound_at": last_inbound.created_at.isoformat() if last_inbound else None,
            "last_sms_outbound_at": last_outbound.created_at.isoformat() if last_outbound else None,
            "last_ai_decision_at": last_ai_decision.created_at.isoformat() if last_ai_decision else None,
        },
    }
