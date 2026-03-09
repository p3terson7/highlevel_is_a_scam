from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import desc, select
from sqlalchemy.orm import Session, selectinload

from app.core.config import Settings
from app.core.deps import get_app_settings, get_booking_service, get_sms_service
from app.db.models import (
    AuditLog,
    Client,
    ConversationState,
    ConversationStateEnum,
    Lead,
    LeadSource,
    Message,
    MessageDirection,
)
from app.db.session import get_db
from app.services.booking import BookingProviderError, BookingService, automated_booking_enabled, ensure_booking_link
from app.services.demo_seed import can_seed_demo, demo_data_present, reset_demo_data, seed_demo_data
from app.services.lead_intake import normalize_phone
from app.services.lead_summary import build_lead_summary_lines, build_lead_summary_text, normalize_form_answers
from app.services.portal_auth import issue_portal_token, verify_portal_password, verify_portal_token
from app.services.runtime_config import (
    client_runtime_overrides,
    get_effective_runtime_map,
    get_effective_runtime_map_for_client,
    load_runtime_overrides,
)
from app.services.sms_service import SMSService, build_sms_service

router = APIRouter(tags=["ui"])

_UI_FILE = Path(__file__).resolve().parents[1] / "templates" / "ui.html"
_WEBHOOK_EVENT_TYPES = {"meta_webhook_received", "linkedin_webhook_received", "zapier_webhook_received"}
_ZAPIER_CONSOLE_EVENTS = {
    "zapier_webhook_received",
    "lead_normalized",
    "initial_ai_sms_sent",
    "initial_sms_sent",
    "after_hours_initial_sms_sent",
    "initial_sms_skipped",
}
_BOOKING_STATES = {ConversationStateEnum.BOOKING_SENT, ConversationStateEnum.BOOKED}
_CLOSED_STATES = {ConversationStateEnum.BOOKED, ConversationStateEnum.OPTED_OUT}


class InternalNoteRequest(BaseModel):
    note: str


class BookingLinkActionRequest(BaseModel):
    message: str | None = None


class HandoffActionRequest(BaseModel):
    note: str | None = None


class ManualMessageRequest(BaseModel):
    body: str


class OwnerTestContactRequest(BaseModel):
    phone: str
    full_name: str | None = None
    email: str | None = None
    city: str | None = None
    first_message: str | None = None
    use_initial_template: bool = True


class ClientPortalLoginRequest(BaseModel):
    email: str
    password: str


class OwnerAIContextUpdateRequest(BaseModel):
    ai_context: str
    faq_context: str | None = None


@dataclass(frozen=True)
class UIActor:
    role: str
    client: Client | None = None


@router.get("/ui", response_class=HTMLResponse)
def ui_index() -> HTMLResponse:
    return HTMLResponse(_UI_FILE.read_text(encoding="utf-8"))


@router.get("/ui/", response_class=HTMLResponse)
def ui_index_slash() -> HTMLResponse:
    return HTMLResponse(_UI_FILE.read_text(encoding="utf-8"))


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
    if not client_runtime_overrides(client):
        return sms_service
    return build_sms_service(settings, runtime_overrides=_effective_runtime(settings, db, client=client))


def _lead_display_name(lead: Lead) -> str:
    return lead.full_name.strip() or lead.phone or f"Lead {lead.id}"


def _lead_summary(lead: Lead) -> str:
    return build_lead_summary_text(normalize_form_answers(lead.form_answers or {}))


def _lead_summary_lines(lead: Lead) -> list[dict[str, str]]:
    return build_lead_summary_lines(normalize_form_answers(lead.form_answers or {}))


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


def _latest_messages_by_lead(db: Session, lead_ids: list[int]) -> dict[int, Message]:
    if not lead_ids:
        return {}
    latest: dict[int, Message] = {}
    messages = db.scalars(
        select(Message)
        .where(Message.lead_id.in_(lead_ids))
        .order_by(desc(Message.created_at), desc(Message.id))
    ).all()
    for message in messages:
        latest.setdefault(message.lead_id, message)
    return latest


def _logs_by_lead(db: Session, lead_ids: list[int]) -> dict[int, list[AuditLog]]:
    grouped: dict[int, list[AuditLog]] = defaultdict(list)
    if not lead_ids:
        return grouped
    logs = db.scalars(
        select(AuditLog)
        .where(AuditLog.lead_id.in_(lead_ids))
        .order_by(desc(AuditLog.created_at), desc(AuditLog.id))
    ).all()
    for log in logs:
        if log.lead_id is not None:
            grouped[log.lead_id].append(log)
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

    booking_detected = lead.conversation_state in _BOOKING_STATES or any(
        log.event_type == "admin_booking_link_sent" or any(action.get("type") == "send_booking_link" for action in _actions_from_log(log))
        for log in logs
    )
    if booking_detected:
        tags.append("Booking link sent")

    return tags


def _lead_search_blob(lead: Lead) -> str:
    client_name = lead.client.business_name if lead.client else ""
    parts = [lead.full_name, lead.phone, lead.email, lead.city, client_name]
    return " ".join(str(part or "") for part in parts).lower()


def _serialize_note(log: AuditLog) -> dict[str, Any]:
    return {
        "id": log.id,
        "created_at": log.created_at.isoformat(),
        "body": str(log.decision.get("note", "")).strip(),
        "event_type": log.event_type,
    }


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


def _load_or_create_manual_lead(
    db: Session,
    *,
    client: Client,
    phone: str,
    full_name: str | None = None,
    email: str | None = None,
    city: str | None = None,
) -> tuple[Lead, bool]:
    normalized_phone = normalize_phone(phone)
    if not normalized_phone:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Valid phone number is required")

    lead = db.scalar(
        select(Lead)
        .where(Lead.client_id == client.id, Lead.phone == normalized_phone)
        .order_by(Lead.created_at.desc())
        .limit(1)
    )
    created = False
    if lead is None:
        lead = Lead(
            client_id=client.id,
            source=LeadSource.MANUAL,
            full_name=(full_name or "").strip(),
            phone=normalized_phone,
            email=(email or "").strip(),
            city=(city or "").strip(),
            form_answers={"created_from": "owner_workspace"},
            raw_payload={"created_from": "owner_workspace"},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.NEW,
        )
        db.add(lead)
        db.flush()
        created = True
    else:
        if full_name and not lead.full_name:
            lead.full_name = full_name.strip()
        if email and not lead.email:
            lead.email = email.strip()
        if city and not lead.city:
            lead.city = city.strip()
    return lead, created


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

    provider_sid = sms_service.send_message(to_number=lead.phone, body=cleaned_body)
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
        notes_count = sum(1 for log in logs if log.event_type == "internal_note")
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
                "opted_out": lead.opted_out,
                "tags": _conversation_tags(lead, logs),
                "notes_count": notes_count,
                "last_message_snippet": _snippet(latest_message.body if latest_message else "No messages yet."),
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
    counts = Counter(lead.conversation_state.value for lead in leads)
    onboarding = [
        {
            "label": "Twilio configured",
            "done": runtime["twilio_configured"],
            "detail": runtime["twilio_from_number"] or "Add SID, token, and from number.",
        },
        {
            "label": "AI configured",
            "done": runtime["ai_configured"],
            "detail": runtime["openai_model"] if runtime["ai_configured"] else "Using heuristic fallback.",
        },
        {
            "label": "Automated booking ready",
            "done": automated_booking_enabled(client),
            "detail": "Calendly token and event type are saved." if automated_booking_enabled(client) else "Link-only mode is active until Calendly is configured.",
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
        "live_test_checklist": [
            {
                "label": "Twilio configured",
                "done": runtime["twilio_configured"],
                "detail": runtime["twilio_from_number"] or "Set SID, auth token, and from number before live phone tests.",
            },
            {
                "label": "AI configured",
                "done": runtime["ai_configured"],
                "detail": runtime["openai_model"] if runtime["ai_configured"] else "Replies will use heuristic fallback until OpenAI is configured.",
            },
            {
                "label": "Automated booking ready",
                "done": automated_booking_enabled(client),
                "detail": "Calendly token and event type are saved." if automated_booking_enabled(client) else "Switch booking mode to calendly and save the Calendly token and event type URI.",
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


@router.get("/ui/api/session")
def ui_session(
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    return _session_payload(actor=actor, settings=settings, db=db)


@router.post("/ui/api/login/client")
def ui_client_login(
    payload: ClientPortalLoginRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
) -> dict[str, Any]:
    email = payload.email.strip().lower()
    password = payload.password
    if not email or not password:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email and password are required")

    client = db.scalar(
        select(Client)
        .where(
            Client.portal_email == email,
            Client.portal_enabled.is_(True),
            Client.is_active.is_(True),
        )
        .limit(1)
    )
    if client is None or not client.portal_password_hash or not verify_portal_password(password, client.portal_password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password")

    token = issue_portal_token(
        settings=settings,
        client_id=client.id,
        client_key=client.client_key,
        email=client.portal_email,
    )
    actor = UIActor(role="client", client=client)
    return {
        "status": "ok",
        "token": token,
        "session": _session_payload(actor=actor, settings=settings, db=db),
    }


@router.get("/ui/api/dashboard")
def ui_dashboard(
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    _require_admin_actor(actor)
    runtime = _runtime_summary(settings, db)
    clients = db.scalars(select(Client).order_by(Client.business_name.asc())).all()
    leads = db.scalars(select(Lead).options(selectinload(Lead.client))).all()
    recent_conversations = _build_conversation_items(db, leads, limit=8)
    last_webhook = db.scalar(
        select(AuditLog)
        .where(AuditLog.event_type.in_(_WEBHOOK_EVENT_TYPES))
        .order_by(desc(AuditLog.created_at), desc(AuditLog.id))
        .limit(1)
    )
    last_inbound = db.scalar(
        select(Message)
        .where(Message.direction == MessageDirection.INBOUND)
        .order_by(desc(Message.created_at), desc(Message.id))
        .limit(1)
    )
    last_outbound = db.scalar(
        select(Message)
        .where(Message.direction == MessageDirection.OUTBOUND)
        .order_by(desc(Message.created_at), desc(Message.id))
        .limit(1)
    )
    last_ai_decision = db.scalar(
        select(AuditLog)
        .where(AuditLog.event_type.in_(["agent_decision", "admin_test_ai_decision"]))
        .order_by(desc(AuditLog.created_at), desc(AuditLog.id))
        .limit(1)
    )
    onboarding = [
        {
            "label": "Configure Twilio",
            "done": runtime["twilio_configured"],
            "detail": runtime["twilio_from_number"] or "Required for real SMS sends.",
        },
        {
            "label": "Configure AI",
            "done": runtime["ai_configured"],
            "detail": runtime["openai_model"] if runtime["ai_configured"] else "OpenAI key missing; heuristic replies are active.",
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
    attention_count = sum(1 for lead in leads if lead.conversation_state not in _CLOSED_STATES)
    return {
        "runtime": runtime,
        "stats": {
            "clients_total": len(clients),
            "active_clients": sum(1 for client in clients if client.is_active),
            "conversations_total": len(leads),
            "attention_needed": attention_count,
            "booked_total": sum(1 for lead in leads if lead.conversation_state == ConversationStateEnum.BOOKED),
            "handoff_total": sum(1 for lead in leads if lead.conversation_state == ConversationStateEnum.HANDOFF),
        },
        "onboarding": onboarding,
        "recent_conversations": recent_conversations,
        "latest_activity": {
            "last_webhook_received_at": last_webhook.created_at.isoformat() if last_webhook else None,
            "last_sms_inbound_at": last_inbound.created_at.isoformat() if last_inbound else None,
            "last_sms_outbound_at": last_outbound.created_at.isoformat() if last_outbound else None,
            "last_ai_decision_at": last_ai_decision.created_at.isoformat() if last_ai_decision else None,
        },
    }


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
        offer = booking_service.preview_slots(client)
    except BookingProviderError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {
        "status": "ok",
        "booking_mode": client.booking_mode,
        "slots": [slot.__dict__ for slot in offer.slots],
        "reply_text": offer.reply_text,
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
    tags = _conversation_tags(lead, list(reversed(audit_logs)))

    timeline: list[dict[str, Any]] = []
    for message in messages:
        timeline.append(
            {
                "type": "message",
                "created_at": message.created_at.isoformat(),
                "direction": message.direction.value,
                "body": message.body,
                "provider_message_sid": message.provider_message_sid,
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
    timeline.sort(key=lambda item: item["created_at"])

    visible_audits = [
        log
        for log in audit_logs
        if log.event_type not in {"lead_normalized", "meta_webhook_received", "linkedin_webhook_received"}
    ]
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
            "current_state": lead.conversation_state.value,
            "opted_out": lead.opted_out,
            "created_at": lead.created_at.isoformat(),
            "updated_at": lead.updated_at.isoformat(),
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
    log = AuditLog(
        client_id=lead.client_id,
        lead_id=lead.id,
        event_type="internal_note",
        decision={"note": note, "actor_role": actor.role},
        created_at=now,
    )
    db.add(log)
    lead.updated_at = now
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
    provider_sid = resolved_sms_service.send_message(to_number=lead.phone, body=body)
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
    db.commit()
    return {
        "status": "ok",
        "lead_id": lead.id,
        "provider_sid": provider_sid,
        "state": state_value.value,
        "delivery_mode": _manual_delivery_mode(settings, db, client=lead.client),
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


@router.delete("/ui/api/conversations/{lead_id}")
def ui_delete_conversation(
    lead_id: int,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
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


@router.post("/ui/api/owner/{client_key}/test-contact")
def ui_owner_test_contact(
    client_key: str,
    payload: OwnerTestContactRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    sms_service: SMSService = Depends(get_sms_service),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> dict[str, Any]:
    _require_admin(settings, admin_token)
    client = _load_client_by_key(db, client_key)
    resolved_sms_service = _sms_service_for_client(
        sms_service=sms_service,
        settings=settings,
        db=db,
        client=client,
    )
    lead, created = _load_or_create_manual_lead(
        db,
        client=client,
        phone=payload.phone,
        full_name=payload.full_name,
        email=payload.email,
        city=payload.city,
    )

    now = datetime.now(timezone.utc)
    if payload.use_initial_template or not (payload.first_message or "").strip():
        first_name = lead.full_name.split(" ")[0] if lead.full_name else "there"
        body = sms_service.render_template(
            client,
            "initial_sms",
            context={
                "first_name": first_name,
                "business_name": client.business_name,
                "booking_url": client.booking_url,
                "consent_text": client.consent_text,
            },
        )
    else:
        body = payload.first_message or ""

    provider_sid, state_value = _send_outbound_message(
        db=db,
        sms_service=resolved_sms_service,
        lead=lead,
        body=body,
        created_at=now,
        raw_payload={"source": "owner_workspace", "action": "start_test_contact"},
        audit_event_type="owner_test_contact_started",
        audit_decision={"created": created},
        advance_new_to_greeted=True,
    )
    db.commit()
    return {
        "status": "ok",
        "created": created,
        "lead_id": lead.id,
        "provider_sid": provider_sid,
        "state": state_value.value,
        "delivery_mode": _manual_delivery_mode(settings, db, client=client),
        "phone": lead.phone,
        "body": body,
    }


@router.post("/ui/api/seed-demo")
def ui_seed_demo(
    reset: bool = Query(default=False),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> dict[str, Any]:
    _require_admin(settings, admin_token)
    if not can_seed_demo(settings):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Demo seed is disabled")
    result = seed_demo_data(db, reset=reset)
    db.commit()
    return result


@router.delete("/ui/api/seed-demo")
def ui_reset_demo(
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> dict[str, Any]:
    _require_admin(settings, admin_token)
    if not can_seed_demo(settings):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Demo seed is disabled")
    result = reset_demo_data(db)
    db.commit()
    return {**result, "status": "ok"}
