from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from sqlalchemy import case, desc, func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, selectinload

from app.core.config import Settings
from app.core.deps import get_app_settings, get_booking_service, get_sms_service
from app.db.models import (
    AuditLog,
    CalendarBooking,
    Client,
    ConversationState,
    ConversationStateEnum,
    Lead,
    LeadTag,
    LeadTask,
    LeadSource,
    Message,
    MessageAttachment,
    MessageDirection,
)
from app.db.session import get_db
from app.services.booking import (
    BookingProviderError,
    BookingService,
    automated_booking_enabled,
    ensure_booking_link,
    internal_calendar_preview_config,
)
from app.services.agent_control import get_agent_control, set_agent_control
from app.services.crm import (
    CRM_STAGE_CONTACTED,
    CRM_STAGE_QUALIFIED,
    CRM_STAGES,
    TASK_STATUS_DONE,
    TASK_STATUS_OPEN,
    is_meaningful_inbound,
    normalize_crm_stage,
    normalize_tag,
    normalize_task_status,
    parse_due_date,
    progress_crm_stage,
)
from app.services.demo_seed import (
    can_seed_demo,
    demo_data_present,
    reset_demo_data,
    seed_demo_data,
    seed_showcase_client_data,
)
from app.services.lead_intake import normalize_phone
from app.services.knowledge import KnowledgeIngestionService, knowledge_payload
from app.services.inbound_sms import process_inbound_turn
from app.services.llm_agent import build_llm_agent
from app.services.lead_summary import build_lead_summary_lines, build_lead_summary_text, normalize_form_answers
from app.services.portal_auth import issue_portal_token, verify_portal_password, verify_portal_token
from app.services.runtime_config import (
    client_runtime_overrides,
    get_effective_runtime_map,
    get_effective_runtime_map_for_client,
    load_runtime_overrides,
)
from app.services.sms_service import SMSDeliveryError, SMSService, build_mock_sms_service, build_sms_service
from app.workers.tasks import _meta_initial_seed_text

_UI_FILE = Path(__file__).resolve().parents[1] / "templates" / "ui.html"
_WEBHOOK_EVENT_TYPES = {"meta_webhook_received", "linkedin_webhook_received", "zapier_webhook_received"}
_ZAPIER_CONSOLE_EVENTS = {
    "zapier_webhook_received",
    "lead_normalized",
    "initial_ai_sms_sent",
    "initial_sms_sent",
    "after_hours_initial_sms_sent",
    "initial_sms_skipped",
    "zapier_booking_webhook_sent",
    "zapier_booking_webhook_failed",
}
_BOOKING_STATES = {ConversationStateEnum.BOOKING_SENT, ConversationStateEnum.BOOKED}
_CLOSED_STATES = {ConversationStateEnum.BOOKED, ConversationStateEnum.OPTED_OUT}
_ARCHIVED_TAG = "archived"


def _booking_ready_detail(client: Client) -> str:
    mode = str(client.booking_mode or "link").strip().lower()
    if mode in {"internal", "calendar"}:
        if automated_booking_enabled(client):
            return "Internal calendar availability is configured."
        return "Set weekly availability in Clients > Edit > Internal calendar."
    if mode == "calendly":
        if automated_booking_enabled(client):
            return "Calendly automation is configured."
        return "Switch to internal calendar or configure Calendly token and event type URI."
    return "Link-only mode is active until automated booking is configured."


class InternalNoteRequest(BaseModel):
    note: str


class BookingLinkActionRequest(BaseModel):
    message: str | None = None


class HandoffActionRequest(BaseModel):
    note: str | None = None


class ManualMessageRequest(BaseModel):
    body: str
    pause_agent: bool = False


class AgentControlRequest(BaseModel):
    paused: bool
    reason: str | None = None
    note: str | None = None


class SandboxFormAnswer(BaseModel):
    question: str
    answer: str


class OwnerSandboxStartRequest(BaseModel):
    mode: str = "gpt_only"
    full_name: str | None = None
    phone: str | None = None
    city: str | None = None
    email: str | None = None
    form_answers: list[SandboxFormAnswer] = Field(default_factory=list)


class OwnerSandboxMessageRequest(BaseModel):
    body: str


class ClientPortalLoginRequest(BaseModel):
    email: str
    password: str


class OwnerAIContextUpdateRequest(BaseModel):
    ai_context: str
    faq_context: str | None = None


class OwnerKnowledgeIngestRequest(BaseModel):
    urls: list[str] = Field(default_factory=list)
    replace: bool = True


class OwnerCalendarAvailabilityRow(BaseModel):
    day: int
    start: str
    end: str
    enabled: bool = True


class OwnerCalendarUpdateRequest(BaseModel):
    slot_minutes: int = 30
    notice_minutes: int = 120
    horizon_days: int = 14
    availability: list[OwnerCalendarAvailabilityRow] = Field(default_factory=list)


class CRMStageUpdateRequest(BaseModel):
    stage: str


class ManualLeadCreateRequest(BaseModel):
    client_key: str | None = None
    full_name: str
    phone: str | None = None
    email: str | None = None
    city: str | None = None
    owner_name: str | None = None
    crm_stage: str | None = None
    notes: str | None = None


class CRMTagRequest(BaseModel):
    tag: str


class CRMTaskCreateRequest(BaseModel):
    title: str
    description: str | None = None
    due_date: str | None = None


class CRMTaskUpdateRequest(BaseModel):
    title: str | None = None
    description: str | None = None
    due_date: str | None = None
    status: str | None = None


class ConversationArchiveRequest(BaseModel):
    archived: bool = True


class ManualMeetingLeadCreateRequest(BaseModel):
    full_name: str
    phone: str | None = None
    email: str | None = None
    city: str | None = None


class ManualMeetingCreateRequest(BaseModel):
    lead_id: int | None = None
    new_lead: ManualMeetingLeadCreateRequest | None = None
    start_at: str
    duration_minutes: int = Field(default=30, ge=5, le=480)
    timezone: str
    title: str
    notes: str | None = None
    create_conference_link: bool = True
    send_email_invite: bool = True
    include_meeting_link: bool = True
    send_sms_reminders: bool = True


class ManualMeetingStatusRequest(BaseModel):
    status: str


@dataclass(frozen=True)
class UIActor:
    role: str
    client: Client | None = None


def _require_admin(settings: Settings, admin_token: str | None) -> None:
    if admin_token != settings.admin_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid admin token")


def _resolve_ui_actor(
    *,
    db: Session,
    settings: Settings,
    admin_token: str | None,
    portal_token: str | None,
) -> UIActor:
    if admin_token == settings.admin_token:
        return UIActor(role="admin")

    if portal_token:
        token_payload = verify_portal_token(settings, portal_token)
        if token_payload is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid portal session")
        client = db.scalar(select(Client).where(Client.id == token_payload.client_id))
        if client is None or not client.is_active:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Portal client is unavailable")
        if not client.portal_enabled or not client.portal_password_hash:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Portal access is disabled")
        if client.client_key != token_payload.client_key or client.portal_email.strip().lower() != token_payload.email:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Portal session is stale")
        return UIActor(role="client", client=client)

    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")


def _require_admin_actor(actor: UIActor) -> None:
    if actor.role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")


def _scoped_client_key(actor: UIActor, client_key: str | None) -> str | None:
    if actor.role == "client":
        return actor.client.client_key if actor.client else None
    return client_key


def _load_lead_for_actor(db: Session, actor: UIActor, lead_id: int) -> Lead:
    lead = _load_lead(db, lead_id)
    if actor.role == "client" and actor.client and lead.client_id != actor.client.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lead not found")
    return lead


def _client_for_actor(db: Session, actor: UIActor, client_key: str | None) -> Client:
    if actor.role == "client":
        if actor.client is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
        return actor.client
    if not client_key:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Client is required")
    return _load_client_by_key(db, client_key)


def _load_booking_for_actor(db: Session, actor: UIActor, booking_id: int) -> CalendarBooking:
    booking = db.scalar(select(CalendarBooking).where(CalendarBooking.id == booking_id))
    if booking is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Meeting not found")
    if actor.role == "client" and actor.client and booking.client_id != actor.client.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Meeting not found")
    return booking


def _load_task_for_actor(db: Session, actor: UIActor, task_id: int) -> LeadTask:
    task = db.scalar(select(LeadTask).where(LeadTask.id == task_id))
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
    if actor.role == "client" and actor.client and task.client_id != actor.client.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
    return task


def _session_payload(*, actor: UIActor, settings: Settings, db: Session) -> dict[str, Any]:
    payload = {
        "status": "ok",
        "role": actor.role,
        "app_name": settings.app_name,
        "env": settings.env,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "can_seed_demo": actor.role == "admin" and can_seed_demo(settings),
        "demo_data_present": demo_data_present(db) if actor.role == "admin" else False,
        "client_key": actor.client.client_key if actor.client else None,
        "client_name": actor.client.business_name if actor.client else None,
        "portal_display_name": actor.client.portal_display_name if actor.client else None,
    }
    return payload


def _load_client_by_key(db: Session, client_key: str) -> Client:
    client = db.scalar(select(Client).where(Client.client_key == client_key))
    if client is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    return client


def _load_lead(db: Session, lead_id: int) -> Lead:
    lead = db.scalar(select(Lead).options(selectinload(Lead.client)).where(Lead.id == lead_id))
    if lead is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lead not found")
    return lead


def _parse_local_datetime(value: str, timezone_name: str) -> datetime:
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid timezone") from exc
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid meeting date/time") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(timezone.utc)


def _manual_meeting_options(payload: ManualMeetingCreateRequest) -> dict[str, bool]:
    _ = payload
    return {
        "create_conference_link": True,
        "send_email_invite": True,
        "include_meeting_link": True,
        "send_sms_reminders": True,
        "zapier_pending": True,
    }


def _serialize_calendar_booking(booking: CalendarBooking, lead: Lead | None = None) -> dict[str, Any]:
    return {
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
        "updated_at": booking.updated_at.isoformat(),
    }


def _webhook_urls(client_key: str) -> dict[str, str]:
    return {
        "meta_verify": f"/webhooks/meta/{client_key}",
        "meta_events": f"/webhooks/meta/{client_key}",
        "zapier_events": f"/webhooks/zapier/{client_key}",
        "linkedin_events": f"/webhooks/linkedin/{client_key}",
        "twilio_sms": f"/sms/inbound/{client_key}",
    }


def _effective_runtime(settings: Settings, db: Session, client: Client | None = None) -> dict[str, str]:
    overrides = load_runtime_overrides(db)
    if client is not None:
        return get_effective_runtime_map_for_client(
            settings=settings,
            overrides=overrides,
            client=client,
        )
    return get_effective_runtime_map(settings=settings, overrides=overrides)


def _runtime_summary(settings: Settings, db: Session, client: Client | None = None) -> dict[str, Any]:
    effective = _effective_runtime(settings, db, client=client)
    has_client_overrides = bool(client_runtime_overrides(client))
    return {
        "twilio_configured": bool(
            effective["twilio_account_sid"] and effective["twilio_auth_token"] and effective["twilio_from_number"]
        ),
        "ai_configured": bool(effective["openai_api_key"]),
        "twilio_from_number": effective["twilio_from_number"],
        "openai_model": effective["openai_model"],
        "ai_provider_mode": effective["ai_provider_mode"],
        "public_base_url": effective["public_base_url"],
        "meta_verify_token_configured": bool(effective["meta_verify_token"]),
        "meta_access_token_configured": bool(effective["meta_access_token"]),
        "linkedin_verify_token_configured": bool(effective["linkedin_verify_token"]),
        "source": "client" if has_client_overrides else "global",
        "has_client_overrides": has_client_overrides,
    }


def _sms_service_for_client(
    *,
    sms_service: SMSService,
    settings: Settings,
    db: Session,
    client: Client | None,
) -> SMSService:
    provider_overrides = client_runtime_overrides(client)
    if not all(provider_overrides.get(key) for key in ("twilio_account_sid", "twilio_auth_token", "twilio_from_number")):
        return sms_service
    return build_sms_service(settings, runtime_overrides=_effective_runtime(settings, db, client=client))


def _lead_display_name(lead: Lead) -> str:
    return lead.full_name.strip() or lead.phone or f"Lead {lead.id}"


def _lead_summary(lead: Lead) -> str:
    return build_lead_summary_text(normalize_form_answers(lead.form_answers or {}))


def _lead_summary_lines(lead: Lead) -> list[dict[str, str]]:
    return build_lead_summary_lines(normalize_form_answers(lead.form_answers or {}))


def _lead_agent_insights(lead: Lead) -> dict[str, Any]:
    raw_payload = lead.raw_payload if isinstance(lead.raw_payload, dict) else {}
    lead_summary = raw_payload.get("lead_summary") if isinstance(raw_payload.get("lead_summary"), dict) else {}
    cta_state = raw_payload.get("cta_state") if isinstance(raw_payload.get("cta_state"), dict) else {}
    important_missing = raw_payload.get("important_missing_fields")
    if not isinstance(important_missing, list):
        important_missing = []
    return {
        "intent_level": raw_payload.get("intent_level") or lead_summary.get("intent_level") or "",
        "intent_score": raw_payload.get("intent_score") or 0,
        "intent_reasons": raw_payload.get("intent_reasons") or lead_summary.get("intent_reasons") or [],
        "qualification_level": lead_summary.get("qualification_level") or "",
        "meeting_status": cta_state.get("meeting_status") or lead_summary.get("meeting_status") or "",
        "meeting_suggested_count": cta_state.get("meeting_suggested_count") or lead_summary.get("meeting_suggested_count") or 0,
        "cta_state": cta_state,
        "important_missing_fields": important_missing,
        "recommended_follow_up": raw_payload.get("recommended_follow_up") or lead_summary.get("recommended_follow_up") or "",
    }


def _snippet(text: str, length: int = 90) -> str:
    compact = " ".join(text.split())
    if len(compact) <= length:
        return compact
    return compact[: length - 1].rstrip() + "..."


def _parse_state_filter(raw_state: str | None) -> ConversationStateEnum | None:
    if not raw_state or raw_state.lower() in {"", "all"}:
        return None
    try:
        return ConversationStateEnum(raw_state.upper())
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid state filter") from exc


def _parse_date_filter(raw_value: str | None) -> date | None:
    if not raw_value:
        return None
    try:
        return date.fromisoformat(raw_value)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid date filter") from exc


def _parse_crm_stage_filter(raw_stage: str | None) -> str | None:
    if not raw_stage or raw_stage.lower() in {"", "all"}:
        return None
    normalized = normalize_crm_stage(raw_stage)
    if normalized not in CRM_STAGES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid CRM stage filter")
    return normalized


def _parse_task_status_filter(raw_status: str | None) -> str | None:
    if not raw_status or raw_status.lower() in {"", "all"}:
        return None
    status_value = raw_status.strip().lower()
    if status_value not in {TASK_STATUS_OPEN, TASK_STATUS_DONE}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid task status filter")
    return status_value


def _latest_messages_by_lead(db: Session, lead_ids: list[int]) -> dict[int, Message]:
    if not lead_ids:
        return {}
    latest_rows = (
        select(
            Message.id.label("message_id"),
            func.row_number()
            .over(
                partition_by=Message.lead_id,
                order_by=(desc(Message.created_at), desc(Message.id)),
            )
            .label("row_number"),
        )
        .where(Message.lead_id.in_(lead_ids))
        .subquery()
    )
    messages = db.scalars(
        select(Message)
        .join(latest_rows, Message.id == latest_rows.c.message_id)
        .where(latest_rows.c.row_number == 1)
    ).all()
    return {message.lead_id: message for message in messages}


def _logs_by_lead(db: Session, lead_ids: list[int], *, per_lead_limit: int = 40) -> dict[int, list[AuditLog]]:
    grouped: dict[int, list[AuditLog]] = defaultdict(list)
    if not lead_ids:
        return grouped
    latest_rows = (
        select(
            AuditLog.id.label("log_id"),
            func.row_number()
            .over(
                partition_by=AuditLog.lead_id,
                order_by=(desc(AuditLog.created_at), desc(AuditLog.id)),
            )
            .label("row_number"),
        )
        .where(AuditLog.lead_id.in_(lead_ids))
        .subquery()
    )
    logs = db.scalars(
        select(AuditLog)
        .join(latest_rows, AuditLog.id == latest_rows.c.log_id)
        .where(latest_rows.c.row_number <= max(1, per_lead_limit))
        .order_by(desc(AuditLog.created_at), desc(AuditLog.id))
    ).all()
    for log in logs:
        if log.lead_id is not None:
            grouped[log.lead_id].append(log)
    return grouped


def _note_counts_by_lead(db: Session, lead_ids: list[int]) -> dict[int, int]:
    if not lead_ids:
        return {}
    rows = db.execute(
        select(AuditLog.lead_id, func.count(AuditLog.id))
        .where(AuditLog.lead_id.in_(lead_ids), AuditLog.event_type == "internal_note")
        .group_by(AuditLog.lead_id)
    ).all()
    return {int(lead_id): int(count) for lead_id, count in rows if lead_id is not None}


def _custom_tags_by_lead(db: Session, lead_ids: list[int]) -> dict[int, list[str]]:
    grouped: dict[int, list[str]] = defaultdict(list)
    if not lead_ids:
        return grouped
    rows = db.scalars(
        select(LeadTag)
        .where(LeadTag.lead_id.in_(lead_ids))
        .order_by(LeadTag.tag.asc(), LeadTag.id.asc())
    ).all()
    for row in rows:
        grouped[row.lead_id].append(row.tag)
    return grouped


def _last_activity_at(lead: Lead, latest_message: Message | None) -> datetime:
    candidates = [lead.created_at, lead.updated_at, lead.last_inbound_at, lead.last_outbound_at]
    if latest_message is not None:
        candidates.append(latest_message.created_at)
    return max(dt for dt in candidates if dt is not None)


def _actions_from_log(log: AuditLog) -> list[dict[str, Any]]:
    actions = log.decision.get("actions", []) if isinstance(log.decision, dict) else []
    if isinstance(actions, list):
        return [item for item in actions if isinstance(item, dict)]
    return []


def _conversation_tags(lead: Lead, logs: list[AuditLog]) -> list[str]:
    tags: list[str] = []
    if lead.opted_out or lead.conversation_state == ConversationStateEnum.OPTED_OUT:
        tags.append("Opted out")

    after_hours_at = max(
        (log.created_at for log in logs if log.event_type in {"after_hours_initial_sms_sent", "outside_operating_hours"}),
        default=None,
    )
    followup_at = max((log.created_at for log in logs if log.event_type == "follow_up_sms_sent"), default=None)
    if after_hours_at and (followup_at is None or followup_at < after_hours_at) and lead.conversation_state not in _CLOSED_STATES | {ConversationStateEnum.HANDOFF}:
        tags.append("After-hours pending")

    handoff_detected = lead.conversation_state == ConversationStateEnum.HANDOFF or any(
        log.event_type == "admin_marked_handoff" or any(action.get("type") == "handoff_to_human" for action in _actions_from_log(log))
        for log in logs
    )
    if handoff_detected:
        tags.append("Needs handoff")

    return tags


def _merged_tags(*, conversation_tags: list[str], custom_tags: list[str]) -> list[str]:
    combined: list[str] = []
    for tag in [*custom_tags, *conversation_tags]:
        text = str(tag).strip()
        if not text:
            continue
        if text not in combined:
            combined.append(text)
    return combined


def _has_tag(tags: list[str], tag: str) -> bool:
    needle = normalize_tag(tag)
    return any(normalize_tag(existing) == needle for existing in tags)


def _lead_search_blob(lead: Lead) -> str:
    client_name = lead.client.business_name if lead.client else ""
    parts = [lead.full_name, lead.phone, lead.email, lead.city, client_name]
    return " ".join(str(part or "") for part in parts).lower()


def _serialize_note(log: AuditLog) -> dict[str, Any]:
    actor = str(log.decision.get("actor_label") or log.decision.get("actor_role") or "operator").strip()
    return {
        "id": log.id,
        "created_at": log.created_at.isoformat(),
        "body": str(log.decision.get("note", "")).strip(),
        "event_type": log.event_type,
        "actor": actor,
    }


def _serialize_task(task: LeadTask) -> dict[str, Any]:
    return {
        "id": task.id,
        "lead_id": task.lead_id,
        "client_id": task.client_id,
        "title": task.title,
        "description": task.description,
        "due_date": task.due_date.isoformat() if task.due_date else None,
        "status": normalize_task_status(task.status),
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        "created_by": task.created_by,
        "created_at": task.created_at.isoformat(),
        "updated_at": task.updated_at.isoformat(),
    }


def _serialize_attachment(attachment: MessageAttachment) -> dict[str, Any]:
    return {
        "id": attachment.id,
        "filename": attachment.filename,
        "content_type": attachment.content_type,
        "media_kind": attachment.media_kind,
        "size_bytes": attachment.size_bytes,
        "url": f"/media/public/{attachment.public_token}",
        "created_at": attachment.created_at.isoformat(),
    }


def _attachments_by_message(db: Session, message_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    if not message_ids:
        return {}
    rows = db.scalars(
        select(MessageAttachment)
        .where(MessageAttachment.message_id.in_(message_ids))
        .order_by(MessageAttachment.created_at.asc(), MessageAttachment.id.asc())
    ).all()
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for attachment in rows:
        grouped[attachment.message_id].append(_serialize_attachment(attachment))
    return grouped


def _message_preview_text(message: Message | None) -> str:
    if message is None:
        return "No messages yet."
    body = str(message.body or "").strip()
    if body:
        return body
    raw_payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
    attachments = raw_payload.get("attachments") if isinstance(raw_payload.get("attachments"), list) else []
    first_kind = str(attachments[0].get("media_kind") or "") if attachments and isinstance(attachments[0], dict) else ""
    if first_kind == "image":
        return "Image attachment"
    if first_kind == "video":
        return "Video attachment"
    if attachments:
        return "Media attachment"
    return "No message body."


def _set_crm_stage(
    *,
    db: Session,
    lead: Lead,
    new_stage: str,
    actor_role: str,
    reason: str,
    allow_backward: bool = True,
    event_type: str = "crm_stage_changed",
    now: datetime | None = None,
) -> bool:
    target = normalize_crm_stage(new_stage)
    current = normalize_crm_stage(lead.crm_stage)
    if not allow_backward:
        target = progress_crm_stage(current, target)
    if current == target:
        return False
    if now is None:
        now = datetime.now(timezone.utc)
    lead.crm_stage = target
    lead.updated_at = now
    db.add(
        AuditLog(
            client_id=lead.client_id,
            lead_id=lead.id,
            event_type=event_type,
            decision={
                "previous_stage": current,
                "new_stage": target,
                "reason": reason,
                "actor_role": actor_role,
            },
            created_at=now,
        )
    )
    return True


def _create_internal_note(
    *,
    db: Session,
    lead: Lead,
    note: str,
    actor_role: str,
    created_at: datetime,
) -> AuditLog:
    actor_label = "client owner" if actor_role == "client" else "admin"
    log = AuditLog(
        client_id=lead.client_id,
        lead_id=lead.id,
        event_type="internal_note",
        decision={"note": note, "actor_role": actor_role, "actor_label": actor_label},
        created_at=created_at,
    )
    db.add(log)
    lead.updated_at = created_at
    return log


def _set_lead_archived(
    *,
    db: Session,
    lead: Lead,
    archived: bool,
    actor_role: str,
    created_at: datetime,
) -> bool:
    existing = db.scalar(select(LeadTag).where(LeadTag.lead_id == lead.id, LeadTag.tag == _ARCHIVED_TAG))
    if archived:
        if existing is not None:
            return False
        db.add(LeadTag(lead_id=lead.id, client_id=lead.client_id, tag=_ARCHIVED_TAG))
        event_type = "conversation_archived"
    else:
        if existing is None:
            return False
        db.delete(existing)
        event_type = "conversation_unarchived"
    lead.updated_at = created_at
    db.add(
        AuditLog(
            client_id=lead.client_id,
            lead_id=lead.id,
            event_type=event_type,
            decision={"tag": _ARCHIVED_TAG, "actor_role": actor_role},
            created_at=created_at,
        )
    )
    return True


def _create_state_transition(
    db: Session,
    *,
    lead: Lead,
    new_state: ConversationStateEnum,
    reason: str,
    created_at: datetime,
    metadata_json: dict[str, Any] | None = None,
) -> None:
    previous_state = lead.conversation_state
    if previous_state == new_state:
        return
    lead.conversation_state = new_state
    db.add(
        ConversationState(
            lead_id=lead.id,
            previous_state=previous_state,
            new_state=new_state,
            reason=reason,
            metadata_json=metadata_json or {},
            created_at=created_at,
        )
    )


def _manual_delivery_mode(settings: Settings, db: Session, client: Client | None = None) -> str:
    return "twilio" if _runtime_summary(settings, db, client=client)["twilio_configured"] else "mock"


def _send_outbound_message(
    *,
    db: Session,
    sms_service: SMSService,
    lead: Lead,
    body: str,
    created_at: datetime,
    raw_payload: dict[str, Any],
    audit_event_type: str,
    audit_decision: dict[str, Any] | None = None,
    advance_new_to_greeted: bool = False,
) -> tuple[str, ConversationStateEnum]:
    cleaned_body = body.strip()
    if not cleaned_body:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Message body is required")
    if lead.opted_out:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Lead has opted out")
    if not lead.phone:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Lead has no phone number")

    provider_sid = _send_sms_or_http_error(sms_service=sms_service, to_number=lead.phone, body=cleaned_body)
    db.add(
        Message(
            lead_id=lead.id,
            client_id=lead.client_id,
            direction=MessageDirection.OUTBOUND,
            body=cleaned_body,
            provider_message_sid=provider_sid,
            raw_payload=raw_payload,
            created_at=created_at,
        )
    )

    lead.last_outbound_at = created_at
    lead.updated_at = created_at
    if lead.initial_sms_sent_at is None:
        lead.initial_sms_sent_at = created_at
    _set_crm_stage(
        db=db,
        lead=lead,
        new_stage=CRM_STAGE_CONTACTED,
        actor_role="system",
        reason=audit_event_type,
        allow_backward=False,
        event_type="crm_stage_auto_updated",
        now=created_at,
    )
    if advance_new_to_greeted and lead.conversation_state == ConversationStateEnum.NEW:
        _create_state_transition(
            db,
            lead=lead,
            new_state=ConversationStateEnum.GREETED,
            reason="owner_manual_outbound",
            created_at=created_at,
            metadata_json={"source": raw_payload.get("source", "ui")},
        )

    db.add(
        AuditLog(
            client_id=lead.client_id,
            lead_id=lead.id,
            event_type=audit_event_type,
            decision={"body": cleaned_body, "provider_sid": provider_sid, **(audit_decision or {})},
            created_at=created_at,
        )
    )
    return provider_sid, lead.conversation_state


def _send_sms_or_http_error(*, sms_service: SMSService, to_number: str, body: str) -> str:
    try:
        return sms_service.send_message(to_number=to_number, body=body)
    except SMSDeliveryError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc


def _send_mms_or_http_error(*, sms_service: SMSService, to_number: str, body: str, media_urls: list[str]) -> str:
    try:
        return sms_service.send_message(to_number=to_number, body=body, media_urls=media_urls)
    except SMSDeliveryError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc


def _build_conversation_items(
    db: Session,
    leads: list[Lead],
    *,
    limit: int,
    date_from: date | None = None,
    date_to: date | None = None,
    query: str | None = None,
) -> list[dict[str, Any]]:
    lead_ids = [lead.id for lead in leads]
    latest_messages = _latest_messages_by_lead(db, lead_ids)
    logs_by_lead = _logs_by_lead(db, lead_ids)
    custom_tags_by_lead = _custom_tags_by_lead(db, lead_ids)
    note_counts_by_lead = _note_counts_by_lead(db, lead_ids)

    items: list[dict[str, Any]] = []
    query_lower = query.lower().strip() if query else ""
    for lead in leads:
        latest_message = latest_messages.get(lead.id)
        last_activity_at = _last_activity_at(lead, latest_message)
        if date_from and last_activity_at.date() < date_from:
            continue
        if date_to and last_activity_at.date() > date_to:
            continue
        if query_lower and query_lower not in _lead_search_blob(lead):
            continue

        logs = logs_by_lead.get(lead.id, [])
        custom_tags = custom_tags_by_lead.get(lead.id, [])
        tags = _merged_tags(
            conversation_tags=_conversation_tags(lead, logs),
            custom_tags=custom_tags,
        )
        notes_count = note_counts_by_lead.get(lead.id, 0)
        items.append(
            {
                "lead_id": lead.id,
                "lead_name": _lead_display_name(lead),
                "phone": lead.phone,
                "email": lead.email,
                "lead_summary": _lead_summary(lead),
                "client_key": lead.client.client_key if lead.client else "",
                "client_name": lead.client.business_name if lead.client else "",
                "state": lead.conversation_state.value,
                "crm_stage": normalize_crm_stage(lead.crm_stage),
                "agent_control": get_agent_control(lead),
                "opted_out": lead.opted_out,
                "tags": tags,
                "notes_count": notes_count,
                "last_message_snippet": _snippet(_message_preview_text(latest_message)),
                "last_message_direction": latest_message.direction.value if latest_message else None,
                "last_activity_at": last_activity_at.isoformat(),
                "created_at": lead.created_at.isoformat(),
            }
        )

    items.sort(key=lambda item: item["last_activity_at"], reverse=True)
    return items[:limit]


def _client_preview_payload(db: Session, settings: Settings, client: Client) -> dict[str, Any]:
    runtime = _runtime_summary(settings, db, client=client)
    leads = db.scalars(
        select(Lead)
        .options(selectinload(Lead.client))
        .where(Lead.client_id == client.id)
        .order_by(desc(Lead.updated_at), desc(Lead.created_at))
        .limit(80)
    ).all()
    recent_conversations = _build_conversation_items(db, leads, limit=10)
    recent_logs = db.scalars(
        select(AuditLog)
        .where(AuditLog.client_id == client.id)
        .order_by(desc(AuditLog.created_at), desc(AuditLog.id))
        .limit(12)
    ).all()
    last_webhook = next((log for log in recent_logs if log.event_type in _WEBHOOK_EVENT_TYPES), None)
    last_outbound = db.scalar(
        select(Message)
        .where(Message.client_id == client.id, Message.direction == MessageDirection.OUTBOUND)
        .order_by(desc(Message.created_at), desc(Message.id))
        .limit(1)
    )
    counts: Counter[str] = Counter()
    for raw_state, count in db.execute(
        select(Lead.conversation_state, func.count(Lead.id))
        .where(Lead.client_id == client.id)
        .group_by(Lead.conversation_state)
    ).all():
        key = raw_state.value if hasattr(raw_state, "value") else str(raw_state or "")
        if key:
            counts[key] = int(count)
    onboarding = [
        {
            "label": "Twilio configured",
            "done": runtime["twilio_configured"],
            "detail": runtime["twilio_from_number"] or "Add SID, token, and from number.",
        },
        {
            "label": "AI configured",
            "done": runtime["ai_configured"],
            "detail": runtime["openai_model"] if runtime["ai_configured"] else "OpenAI key missing; AI replies are unavailable.",
        },
        {
            "label": "Automated booking ready",
            "done": automated_booking_enabled(client),
            "detail": _booking_ready_detail(client),
        },
        {
            "label": "Webhook URLs ready",
            "done": True,
            "detail": "Copy the generated endpoints into your ad and SMS providers.",
        },
        {
            "label": "Last webhook received",
            "done": last_webhook is not None,
            "detail": last_webhook.created_at.isoformat() if last_webhook else "No provider webhook received yet.",
        },
        {
            "label": "Last SMS sent",
            "done": last_outbound is not None,
            "detail": last_outbound.created_at.isoformat() if last_outbound else "No outbound messages yet.",
        },
    ]
    return {
        "client": {
            "id": client.id,
            "client_key": client.client_key,
            "business_name": client.business_name,
            "tone": client.tone,
            "timezone": client.timezone,
            "qualification_questions": client.qualification_questions,
            "booking_url": client.booking_url,
            "booking_mode": client.booking_mode,
            "booking_config": client.booking_config,
            "provider_config": client.provider_config,
            "fallback_handoff_number": client.fallback_handoff_number,
            "consent_text": client.consent_text,
            "portal_display_name": client.portal_display_name,
            "portal_email": client.portal_email,
            "portal_enabled": client.portal_enabled,
            "portal_password_configured": bool(client.portal_password_hash),
            "operating_hours": client.operating_hours,
            "faq_context": client.faq_context,
            "ai_context": client.ai_context,
            "template_overrides": client.template_overrides,
            "is_active": client.is_active,
            "created_at": client.created_at.isoformat(),
            "updated_at": client.updated_at.isoformat(),
        },
        "webhook_urls": _webhook_urls(client.client_key),
        "provider_runtime": runtime,
        "onboarding": onboarding,
        "recent_conversations": recent_conversations,
        "recent_logs": [
            {
                "id": log.id,
                "event_type": log.event_type,
                "lead_id": log.lead_id,
                "created_at": log.created_at.isoformat(),
                "decision": log.decision,
            }
            for log in recent_logs
        ],
        "counts": dict(counts),
    }


def _owner_workspace_payload(db: Session, settings: Settings, client: Client) -> dict[str, Any]:
    runtime = _runtime_summary(settings, db, client=client)
    leads = db.scalars(
        select(Lead)
        .options(selectinload(Lead.client))
        .where(Lead.client_id == client.id)
        .order_by(desc(Lead.updated_at), desc(Lead.created_at))
        .limit(120)
    ).all()
    last_outbound = db.scalar(
        select(Message)
        .where(Message.client_id == client.id, Message.direction == MessageDirection.OUTBOUND)
        .order_by(desc(Message.created_at), desc(Message.id))
        .limit(1)
    )
    return {
        "client": {
            "client_key": client.client_key,
            "business_name": client.business_name,
            "booking_url": client.booking_url,
            "booking_mode": client.booking_mode,
            "booking_config": client.booking_config,
            "provider_config": client.provider_config,
            "fallback_handoff_number": client.fallback_handoff_number,
            "timezone": client.timezone,
            "tone": client.tone,
            "faq_context": client.faq_context,
            "ai_context": client.ai_context,
            "twilio_inbound_path": _webhook_urls(client.client_key)["twilio_sms"],
        },
        "runtime": runtime,
        "delivery_mode": "twilio" if runtime["twilio_configured"] else "mock",
        "knowledge": knowledge_payload(db, client_id=client.id),
        "live_test_checklist": [
            {
                "label": "Twilio configured",
                "done": runtime["twilio_configured"],
                "detail": runtime["twilio_from_number"] or "Set SID, auth token, and from number before live phone tests.",
            },
            {
                "label": "AI configured",
                "done": runtime["ai_configured"],
                "detail": runtime["openai_model"] if runtime["ai_configured"] else "OpenAI key missing; configure it for live AI replies.",
            },
            {
                "label": "Automated booking ready",
                "done": automated_booking_enabled(client),
                "detail": _booking_ready_detail(client),
            },
            {
                "label": "Twilio inbound webhook",
                "done": True,
                "detail": f"Point your Twilio number at {_webhook_urls(client.client_key)['twilio_sms']} for AI replies.",
            },
            {
                "label": "Initial outbound message sent",
                "done": last_outbound is not None,
                "detail": last_outbound.created_at.isoformat() if last_outbound else "Use the test contact form below to start a live thread.",
            },
        ],
        "conversations": _build_conversation_items(db, leads, limit=25),
    }


def _dashboard_breakdown_rows(
    counter: Counter[str],
    *,
    ordered_keys: list[str] | tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    total = sum(counter.values())
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    for key in ordered_keys or []:
        count = counter.get(key, 0)
        if count <= 0:
            continue
        rows.append({"key": key, "count": count, "share": (count / total) if total else 0})
        seen.add(key)

    for key, count in counter.most_common():
        if key in seen or count <= 0:
            continue
        rows.append({"key": key, "count": count, "share": (count / total) if total else 0})

    return rows


def _dashboard_campaign_performance(clients: list[Client]) -> dict[str, Any]:
    campaigns: list[dict[str, Any]] = []
    last_synced_at = ""
    source_label = "Zapier demo"
    report_range = "Last 30 days"

    for client in clients:
        provider_config = client.provider_config if isinstance(client.provider_config, dict) else {}
        report = provider_config.get("ad_campaign_reports") or provider_config.get("demo_ad_campaign_reports")
        if not isinstance(report, dict):
            continue
        if report.get("last_synced_at"):
            last_synced_at = max(last_synced_at, str(report.get("last_synced_at")))
        source_label = str(report.get("source_label") or report.get("source") or source_label)
        report_range = str(report.get("report_range") or report_range)
        for raw_campaign in report.get("campaigns") or []:
            if not isinstance(raw_campaign, dict):
                continue
            impressions = max(0, int(raw_campaign.get("impressions") or 0))
            clicks = max(0, int(raw_campaign.get("clicks") or 0))
            conversions = max(0, int(raw_campaign.get("conversions") or 0))
            reach = max(0, int(raw_campaign.get("reach") or 0))
            spend = max(0.0, float(raw_campaign.get("spend") or 0))
            cpc = float(raw_campaign.get("cpc") or (spend / clicks if clicks else 0))
            cost_per_conversion = float(
                raw_campaign.get("cost_per_conversion") or (spend / conversions if conversions else 0)
            )
            campaigns.append(
                {
                    "campaign_id": str(raw_campaign.get("campaign_id") or ""),
                    "campaign_name": str(raw_campaign.get("campaign_name") or "Untitled campaign"),
                    "client_key": client.client_key,
                    "client_name": client.business_name,
                    "platform": str(raw_campaign.get("platform") or report.get("platform") or "Facebook Lead Ads"),
                    "status": str(raw_campaign.get("status") or "active"),
                    "objective": str(raw_campaign.get("objective") or "Lead generation"),
                    "impressions": impressions,
                    "reach": reach,
                    "clicks": clicks,
                    "conversions": conversions,
                    "spend": round(spend, 2),
                    "cpc": round(cpc, 2),
                    "cost_per_conversion": round(cost_per_conversion, 2),
                    "ctr": (clicks / impressions) if impressions else 0,
                    "conversion_rate": (conversions / clicks) if clicks else 0,
                }
            )

    campaigns.sort(key=lambda item: (item["conversions"], -item["cost_per_conversion"]), reverse=True)
    total_impressions = sum(item["impressions"] for item in campaigns)
    total_reach = sum(item["reach"] for item in campaigns)
    total_clicks = sum(item["clicks"] for item in campaigns)
    total_conversions = sum(item["conversions"] for item in campaigns)
    total_spend = sum(float(item["spend"]) for item in campaigns)

    return {
        "source": source_label,
        "report_range": report_range,
        "last_synced_at": last_synced_at,
        "totals": {
            "campaigns": len(campaigns),
            "impressions": total_impressions,
            "reach": total_reach,
            "clicks": total_clicks,
            "conversions": total_conversions,
            "spend": round(total_spend, 2),
            "cpc": round((total_spend / total_clicks) if total_clicks else 0, 2),
            "cost_per_conversion": round((total_spend / total_conversions) if total_conversions else 0, 2),
            "ctr": (total_clicks / total_impressions) if total_impressions else 0,
            "conversion_rate": (total_conversions / total_clicks) if total_clicks else 0,
        },
        "campaigns": campaigns[:6],
    }


def _dashboard_open_tasks(
    db: Session,
    actor: UIActor,
    *,
    today: date,
    limit: int = 5,
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    stmt = (
        select(LeadTask)
        .join(Lead, LeadTask.lead_id == Lead.id)
        .join(Client, LeadTask.client_id == Client.id)
        .options(selectinload(LeadTask.lead).selectinload(Lead.client))
        .where(LeadTask.status == TASK_STATUS_OPEN)
    )
    count_stmt = (
        select(func.count(LeadTask.id))
        .join(Lead, LeadTask.lead_id == Lead.id)
        .join(Client, LeadTask.client_id == Client.id)
        .where(LeadTask.status == TASK_STATUS_OPEN)
    )
    overdue_stmt = count_stmt.where(LeadTask.due_date < today)
    due_today_stmt = count_stmt.where(LeadTask.due_date == today)
    if actor.role == "client" and actor.client:
        stmt = stmt.where(LeadTask.client_id == actor.client.id)
        count_stmt = count_stmt.where(LeadTask.client_id == actor.client.id)
        overdue_stmt = overdue_stmt.where(LeadTask.client_id == actor.client.id)
        due_today_stmt = due_today_stmt.where(LeadTask.client_id == actor.client.id)

    tasks = db.scalars(
        stmt.order_by(
            LeadTask.due_date.is_(None),
            LeadTask.due_date.asc(),
            LeadTask.created_at.asc(),
            LeadTask.id.asc(),
        ).limit(limit)
    ).unique().all()

    items: list[dict[str, Any]] = []
    for task in tasks:
        lead = task.lead
        item = _serialize_task(task)
        item.update(
            {
                "lead_name": _lead_display_name(lead) if lead else "",
                "lead_phone": lead.phone if lead else "",
                "client_name": lead.client.business_name if lead and lead.client else "",
                "crm_stage": normalize_crm_stage(lead.crm_stage) if lead else "",
            }
        )
        items.append(item)
    summary = {
        "total": int(db.scalar(count_stmt) or 0),
        "overdue": int(db.scalar(overdue_stmt) or 0),
        "due_today": int(db.scalar(due_today_stmt) or 0),
    }
    return summary, items


def _dashboard_upcoming_meetings(
    db: Session,
    actor: UIActor,
    *,
    now: datetime,
    limit: int = 5,
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    stmt = (
        select(CalendarBooking, Lead, Client)
        .join(Client, CalendarBooking.client_id == Client.id)
        .outerjoin(Lead, Lead.id == CalendarBooking.lead_id)
        .where(CalendarBooking.status == "scheduled", CalendarBooking.end_at >= now)
        .order_by(CalendarBooking.start_at.asc(), CalendarBooking.id.asc())
    )
    count_stmt = select(func.count(CalendarBooking.id)).where(
        CalendarBooking.status == "scheduled",
        CalendarBooking.end_at >= now,
    )
    seven_day_stmt = count_stmt.where(CalendarBooking.start_at < now + timedelta(days=8))
    if actor.role == "client" and actor.client:
        stmt = stmt.where(CalendarBooking.client_id == actor.client.id)
        count_stmt = count_stmt.where(CalendarBooking.client_id == actor.client.id)
        seven_day_stmt = seven_day_stmt.where(CalendarBooking.client_id == actor.client.id)

    rows = db.execute(stmt.limit(limit)).all()
    items = [
        {
            "id": booking.id,
            "lead_id": booking.lead_id,
            "lead_name": _lead_display_name(lead) if lead else "",
            "phone": lead.phone if lead else "",
            "email": lead.email if lead else "",
            "client_name": client.business_name if client else "",
            "start_at": booking.start_at.isoformat(),
            "end_at": booking.end_at.isoformat(),
            "timezone": booking.timezone,
            "title": booking.title,
            "provider": booking.provider,
            "source": booking.source,
        }
        for booking, lead, client in rows
    ]
    summary = {
        "total": int(db.scalar(count_stmt) or 0),
        "next_7_days": int(db.scalar(seven_day_stmt) or 0),
    }
    return summary, items


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _start_of_day_utc(value: date) -> datetime:
    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)


def _dashboard_lead_count(db: Session, conditions: list[Any], *extra_conditions: Any) -> int:
    stmt = select(func.count(Lead.id))
    all_conditions = [*conditions, *extra_conditions]
    if all_conditions:
        stmt = stmt.where(*all_conditions)
    return int(db.scalar(stmt) or 0)


def _dashboard_counter_rows(db: Session, column: Any, conditions: list[Any]) -> Counter[str]:
    stmt = select(column, func.count(Lead.id)).group_by(column)
    if conditions:
        stmt = stmt.where(*conditions)
    counter: Counter[str] = Counter()
    for raw_key, count in db.execute(stmt).all():
        key = raw_key.value if hasattr(raw_key, "value") else str(raw_key or "")
        if key:
            counter[key] = int(count)
    return counter


__all__ = [name for name in globals() if not name.startswith("__")]
