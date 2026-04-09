from __future__ import annotations

import secrets
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, status
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.core.deps import clear_dependency_caches, get_app_settings
from app.core.metrics import render_prometheus
from app.db.models import (
    AuditLog,
    Client,
    ConversationStateEnum,
    Lead,
    LeadSource,
    Message,
    MessageDirection,
)
from app.db.session import get_db
from app.services.llm_agent import build_llm_agent
from app.services.portal_auth import hash_portal_password
from app.services.runtime_config import (
    RUNTIME_KEYS,
    SECRET_KEYS,
    get_effective_runtime_map,
    get_effective_runtime_map_for_client,
    load_runtime_overrides,
    upsert_runtime_values,
)
from app.services.sms_service import build_sms_service

router = APIRouter(tags=["system"])


def _webhook_urls(client_key: str) -> dict[str, str]:
    return {
        "meta_verify": f"/webhooks/meta/{client_key}",
        "meta_events": f"/webhooks/meta/{client_key}",
        "zapier_events": f"/webhooks/zapier/{client_key}",
        "linkedin_events": f"/webhooks/linkedin/{client_key}",
        "twilio_sms": f"/sms/inbound/{client_key}",
    }


class ClientCreateRequest(BaseModel):
    business_name: str
    tone: str = "friendly"
    timezone: str = "America/New_York"
    qualification_questions: list[str] = Field(
        default_factory=lambda: [
            "What are you hoping to solve?",
            "When do you want to get started?",
        ]
    )
    booking_url: str = ""
    booking_mode: str = "link"
    booking_config: dict[str, Any] = Field(default_factory=dict)
    provider_config: dict[str, Any] = Field(default_factory=dict)
    fallback_handoff_number: str = ""
    consent_text: str = "Reply STOP to opt out. Msg/data rates may apply."
    operating_hours: dict[str, Any] = Field(
        default_factory=lambda: {"days": [0, 1, 2, 3, 4], "start": "09:00", "end": "18:00"}
    )
    faq_context: str = ""
    ai_context: str = ""
    template_overrides: dict[str, str] = Field(default_factory=dict)
    client_key: str | None = None
    portal_display_name: str = ""
    portal_email: str = ""
    portal_password: str | None = None
    portal_enabled: bool = False


class ClientUpdateRequest(BaseModel):
    business_name: str | None = None
    tone: str | None = None
    timezone: str | None = None
    qualification_questions: list[str] | None = None
    booking_url: str | None = None
    booking_mode: str | None = None
    booking_config: dict[str, Any] | None = None
    provider_config: dict[str, Any] | None = None
    fallback_handoff_number: str | None = None
    consent_text: str | None = None
    operating_hours: dict[str, Any] | None = None
    faq_context: str | None = None
    ai_context: str | None = None
    template_overrides: dict[str, str] | None = None
    is_active: bool | None = None
    portal_display_name: str | None = None
    portal_email: str | None = None
    portal_password: str | None = None
    portal_enabled: bool | None = None


class ClientCreateResponse(BaseModel):
    id: int
    client_key: str
    business_name: str
    webhook_urls: dict[str, str]


class AdminClientOut(BaseModel):
    id: int
    client_key: str
    business_name: str
    tone: str
    timezone: str
    booking_url: str
    booking_mode: str
    is_active: bool
    portal_enabled: bool
    created_at: datetime


class AdminClientDetailOut(BaseModel):
    id: int
    client_key: str
    business_name: str
    tone: str
    timezone: str
    qualification_questions: list[str]
    booking_url: str
    booking_mode: str
    booking_config: dict[str, Any]
    provider_config: dict[str, Any]
    fallback_handoff_number: str
    consent_text: str
    portal_display_name: str
    portal_email: str
    portal_enabled: bool
    portal_password_configured: bool
    operating_hours: dict[str, Any]
    faq_context: str
    ai_context: str
    template_overrides: dict[str, str]
    is_active: bool
    created_at: datetime
    updated_at: datetime
    webhook_urls: dict[str, str]


class AdminLeadOut(BaseModel):
    id: int
    external_lead_id: str | None
    full_name: str
    phone: str
    email: str
    city: str
    source: str
    conversation_state: str
    opted_out: bool
    created_at: datetime
    updated_at: datetime


class AdminMessageOut(BaseModel):
    id: int
    lead_id: int
    direction: str
    body: str
    provider_message_sid: str
    created_at: datetime


class RuntimeConfigUpdateRequest(BaseModel):
    twilio_account_sid: str | None = None
    twilio_auth_token: str | None = None
    twilio_from_number: str | None = None
    public_base_url: str | None = None
    openai_api_key: str | None = None
    openai_model: str | None = None
    ai_provider_mode: str | None = None
    meta_verify_token: str | None = None
    meta_access_token: str | None = None
    meta_graph_api_version: str | None = None
    linkedin_verify_token: str | None = None


class RuntimeConfigStatusOut(BaseModel):
    twilio_account_sid_configured: bool
    twilio_auth_token_configured: bool
    twilio_account_sid: str
    twilio_auth_token: str
    twilio_from_number: str
    public_base_url: str
    openai_api_key_configured: bool
    openai_api_key: str
    openai_model: str
    ai_provider_mode: str
    meta_verify_token_configured: bool
    meta_verify_token: str
    meta_access_token_configured: bool
    meta_access_token: str
    meta_graph_api_version: str
    linkedin_verify_token_configured: bool
    linkedin_verify_token: str


class RuntimeConfigUpdateResponse(BaseModel):
    updated_keys: list[str]
    secret_keys_updated: list[str]


class TestSMSRequest(BaseModel):
    client_key: str
    to_number: str
    body: str = "This is a test SMS from Lead Conversion SMS Agent."


class TestAIRequest(BaseModel):
    client_key: str
    inbound_text: str = "Can I book a consultation this week?"
    lead_name: str = "Test Lead"
    lead_city: str = "Test City"


class ClientEventSummaryOut(BaseModel):
    client_key: str
    last_lead_received_at: datetime | None
    last_sms_inbound_at: datetime | None
    last_sms_outbound_at: datetime | None
    last_ai_decision_at: datetime | None


class AdminAuditLogOut(BaseModel):
    id: int
    event_type: str
    lead_id: int | None
    created_at: datetime
    decision: dict[str, Any]


def _require_admin(
    settings: Settings,
    admin_token: str | None,
) -> None:
    if admin_token != settings.admin_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid admin token")


def _load_client_by_key(db: Session, client_key: str) -> Client:
    client = db.scalar(select(Client).where(Client.client_key == client_key))
    if client is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    return client


def _effective_runtime_config(settings: Settings, db: Session) -> dict[str, str]:
    return get_effective_runtime_map(settings=settings, overrides=load_runtime_overrides(db))


def _normalize_provider_config(raw: dict[str, Any] | None) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    output: dict[str, str] = {}
    for key in RUNTIME_KEYS:
        value = raw.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            output[key] = text
    return output


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@router.get("/metrics", response_class=PlainTextResponse)
def metrics() -> str:
    return render_prometheus()


@router.post("/admin/clients", status_code=status.HTTP_201_CREATED, response_model=ClientCreateResponse)
def create_client(
    payload: ClientCreateRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> ClientCreateResponse:
    _require_admin(settings, admin_token)

    client_key = payload.client_key or secrets.token_urlsafe(24)
    existing = db.scalar(select(Client).where(Client.client_key == client_key))
    if existing is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="client_key already exists")

    try:
        portal_password_hash = hash_portal_password(payload.portal_password) if payload.portal_password else ""
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    client = Client(
        client_key=client_key,
        business_name=payload.business_name,
        tone=payload.tone,
        timezone=payload.timezone,
        qualification_questions=payload.qualification_questions,
        booking_url=payload.booking_url,
        booking_mode=payload.booking_mode,
        booking_config=payload.booking_config,
        provider_config=_normalize_provider_config(payload.provider_config),
        fallback_handoff_number=payload.fallback_handoff_number,
        consent_text=payload.consent_text,
        portal_display_name=payload.portal_display_name.strip(),
        portal_email=payload.portal_email.strip().lower(),
        portal_password_hash=portal_password_hash,
        portal_enabled=payload.portal_enabled,
        operating_hours=payload.operating_hours,
        faq_context=payload.faq_context,
        ai_context=payload.ai_context,
        template_overrides=payload.template_overrides,
    )
    db.add(client)
    db.commit()
    db.refresh(client)

    return ClientCreateResponse(
        id=client.id,
        client_key=client.client_key,
        business_name=client.business_name,
        webhook_urls=_webhook_urls(client.client_key),
    )


@router.get("/admin/clients", response_model=list[AdminClientOut])
def list_clients(
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> list[AdminClientOut]:
    _require_admin(settings, admin_token)
    clients = db.scalars(select(Client).order_by(desc(Client.created_at))).all()
    return [
        AdminClientOut(
            id=client.id,
            client_key=client.client_key,
            business_name=client.business_name,
            tone=client.tone,
            timezone=client.timezone,
            booking_url=client.booking_url,
            booking_mode=client.booking_mode,
            is_active=client.is_active,
            portal_enabled=client.portal_enabled,
            created_at=client.created_at,
        )
        for client in clients
    ]


@router.get("/admin/clients/{client_key}", response_model=AdminClientDetailOut)
def get_client(
    client_key: str,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> AdminClientDetailOut:
    _require_admin(settings, admin_token)
    client = _load_client_by_key(db, client_key)
    return AdminClientDetailOut(
        id=client.id,
        client_key=client.client_key,
        business_name=client.business_name,
        tone=client.tone,
        timezone=client.timezone,
        qualification_questions=client.qualification_questions,
        booking_url=client.booking_url,
        booking_mode=client.booking_mode,
        booking_config=client.booking_config,
        provider_config=client.provider_config,
        fallback_handoff_number=client.fallback_handoff_number,
        consent_text=client.consent_text,
        portal_display_name=client.portal_display_name,
        portal_email=client.portal_email,
        portal_enabled=client.portal_enabled,
        portal_password_configured=bool(client.portal_password_hash),
        operating_hours=client.operating_hours,
        faq_context=client.faq_context,
        ai_context=client.ai_context,
        template_overrides=client.template_overrides,
        is_active=client.is_active,
        created_at=client.created_at,
        updated_at=client.updated_at,
        webhook_urls=_webhook_urls(client.client_key),
    )


@router.patch("/admin/clients/{client_key}", response_model=AdminClientDetailOut)
def update_client(
    client_key: str,
    payload: ClientUpdateRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> AdminClientDetailOut:
    _require_admin(settings, admin_token)
    client = _load_client_by_key(db, client_key)

    changes = payload.model_dump(exclude_unset=True)
    portal_password = changes.pop("portal_password", None)
    for key, value in changes.items():
        if key == "provider_config":
            merged = dict(client.provider_config or {})
            merged.update(_normalize_provider_config(value))
            value = merged
        if key == "portal_email" and value is not None:
            value = value.strip().lower()
        if key == "portal_display_name" and value is not None:
            value = value.strip()
        setattr(client, key, value)
    if portal_password is not None:
        try:
            client.portal_password_hash = hash_portal_password(portal_password) if portal_password.strip() else ""
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    db.commit()
    db.refresh(client)

    return AdminClientDetailOut(
        id=client.id,
        client_key=client.client_key,
        business_name=client.business_name,
        tone=client.tone,
        timezone=client.timezone,
        qualification_questions=client.qualification_questions,
        booking_url=client.booking_url,
        booking_mode=client.booking_mode,
        booking_config=client.booking_config,
        provider_config=client.provider_config,
        fallback_handoff_number=client.fallback_handoff_number,
        consent_text=client.consent_text,
        portal_display_name=client.portal_display_name,
        portal_email=client.portal_email,
        portal_enabled=client.portal_enabled,
        portal_password_configured=bool(client.portal_password_hash),
        operating_hours=client.operating_hours,
        faq_context=client.faq_context,
        ai_context=client.ai_context,
        template_overrides=client.template_overrides,
        is_active=client.is_active,
        created_at=client.created_at,
        updated_at=client.updated_at,
        webhook_urls=_webhook_urls(client.client_key),
    )


@router.get("/admin/clients/{client_key}/leads", response_model=list[AdminLeadOut])
def list_client_leads(
    client_key: str,
    limit: int = 50,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> list[AdminLeadOut]:
    _require_admin(settings, admin_token)
    client = _load_client_by_key(db, client_key)

    clamped_limit = max(1, min(limit, 200))
    leads = db.scalars(
        select(Lead)
        .where(Lead.client_id == client.id)
        .order_by(desc(Lead.created_at))
        .limit(clamped_limit)
    ).all()

    return [
        AdminLeadOut(
            id=lead.id,
            external_lead_id=lead.external_lead_id,
            full_name=lead.full_name,
            phone=lead.phone,
            email=lead.email,
            city=lead.city,
            source=lead.source.value,
            conversation_state=lead.conversation_state.value,
            opted_out=lead.opted_out,
            created_at=lead.created_at,
            updated_at=lead.updated_at,
        )
        for lead in leads
    ]


@router.get("/admin/leads/{lead_id}/messages", response_model=list[AdminMessageOut])
def list_lead_messages(
    lead_id: int,
    limit: int = 100,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> list[AdminMessageOut]:
    _require_admin(settings, admin_token)
    lead = db.get(Lead, lead_id)
    if lead is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lead not found")

    clamped_limit = max(1, min(limit, 500))
    messages = db.scalars(
        select(Message)
        .where(Message.lead_id == lead_id)
        .order_by(desc(Message.created_at))
        .limit(clamped_limit)
    ).all()
    messages = list(reversed(messages))

    return [
        AdminMessageOut(
            id=msg.id,
            lead_id=msg.lead_id,
            direction=msg.direction.value,
            body=msg.body,
            provider_message_sid=msg.provider_message_sid,
            created_at=msg.created_at,
        )
        for msg in messages
    ]


@router.get("/admin/runtime-config/status", response_model=RuntimeConfigStatusOut)
def runtime_config_status(
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> RuntimeConfigStatusOut:
    _require_admin(settings, admin_token)
    effective = _effective_runtime_config(settings, db)
    return RuntimeConfigStatusOut(
        twilio_account_sid_configured=bool(effective["twilio_account_sid"]),
        twilio_auth_token_configured=bool(effective["twilio_auth_token"]),
        twilio_account_sid=effective["twilio_account_sid"],
        twilio_auth_token=effective["twilio_auth_token"],
        twilio_from_number=effective["twilio_from_number"],
        public_base_url=effective["public_base_url"],
        openai_api_key_configured=bool(effective["openai_api_key"]),
        openai_api_key=effective["openai_api_key"],
        openai_model=effective["openai_model"],
        ai_provider_mode=effective["ai_provider_mode"],
        meta_verify_token_configured=bool(effective["meta_verify_token"]),
        meta_verify_token=effective["meta_verify_token"],
        meta_access_token_configured=bool(effective["meta_access_token"]),
        meta_access_token=effective["meta_access_token"],
        meta_graph_api_version=effective["meta_graph_api_version"],
        linkedin_verify_token_configured=bool(effective["linkedin_verify_token"]),
        linkedin_verify_token=effective["linkedin_verify_token"],
    )


@router.put("/admin/runtime-config", response_model=RuntimeConfigUpdateResponse)
def update_runtime_config(
    payload: RuntimeConfigUpdateRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> RuntimeConfigUpdateResponse:
    _require_admin(settings, admin_token)
    updates = payload.model_dump(exclude_unset=True)
    upsert_runtime_values(db, updates)
    db.commit()
    clear_dependency_caches()

    updated_keys = sorted(updates.keys())
    secret_keys_updated = sorted([key for key in updates.keys() if key in SECRET_KEYS])
    return RuntimeConfigUpdateResponse(updated_keys=updated_keys, secret_keys_updated=secret_keys_updated)


@router.post("/admin/test/sms")
def admin_test_sms(
    payload: TestSMSRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> dict[str, Any]:
    _require_admin(settings, admin_token)
    client = _load_client_by_key(db, payload.client_key)
    effective = get_effective_runtime_map_for_client(
        settings=settings,
        overrides=load_runtime_overrides(db),
        client=client,
    )

    if not (
        effective["twilio_account_sid"]
        and effective["twilio_auth_token"]
        and effective["twilio_from_number"]
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Twilio is not fully configured for this client.",
        )

    sms_service = build_sms_service(settings, runtime_overrides=effective)
    provider_sid = sms_service.send_message(to_number=payload.to_number, body=payload.body)

    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=None,
            event_type="admin_test_sms_sent",
            decision={"to_number": payload.to_number, "provider_sid": provider_sid},
        )
    )
    db.commit()

    return {"status": "ok", "provider_sid": provider_sid}


@router.post("/admin/test/ai")
def admin_test_ai(
    payload: TestAIRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> dict[str, Any]:
    _require_admin(settings, admin_token)
    client = _load_client_by_key(db, payload.client_key)

    effective = get_effective_runtime_map_for_client(
        settings=settings,
        overrides=load_runtime_overrides(db),
        client=client,
    )
    llm_agent = build_llm_agent(settings=settings, runtime_overrides=effective)

    fake_lead = Lead(
        client_id=client.id,
        source=LeadSource.MANUAL,
        full_name=payload.lead_name,
        phone="",
        email="",
        city=payload.lead_city,
        form_answers={},
        raw_payload={},
        consented=True,
        opted_out=False,
        conversation_state=ConversationStateEnum.QUALIFYING,
    )

    result = llm_agent.next_reply(
        client=client,
        lead=fake_lead,
        inbound_text=payload.inbound_text,
        history=[],
    )

    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=None,
            event_type="admin_test_ai_decision",
            decision={
                "inbound_text": payload.inbound_text,
                "reply_text": result.reply_text,
                "next_state": result.next_state.value,
                "action": result.action,
                "next_question_key": result.next_question_key,
                "collected_fields": result.collected_fields.model_dump(exclude_none=True),
                "provider": result.provider,
                "provider_error": result.provider_error,
                "actions": [action.model_dump() for action in result.actions],
            },
        )
    )
    db.commit()

    return {
        "status": "ok",
        "provider": result.provider,
        "provider_error": result.provider_error,
        "reply_text": result.reply_text,
        "next_state": result.next_state.value,
        "action": result.action,
        "next_question_key": result.next_question_key,
        "collected_fields": result.collected_fields.model_dump(exclude_none=True),
        "actions": [action.model_dump() for action in result.actions],
    }


@router.get("/admin/clients/{client_key}/events", response_model=ClientEventSummaryOut)
def client_event_summary(
    client_key: str,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> ClientEventSummaryOut:
    _require_admin(settings, admin_token)
    client = _load_client_by_key(db, client_key)

    last_lead = db.scalar(
        select(Lead)
        .where(Lead.client_id == client.id)
        .order_by(desc(Lead.created_at))
        .limit(1)
    )
    last_inbound = db.scalar(
        select(Message)
        .where(Message.client_id == client.id, Message.direction == MessageDirection.INBOUND)
        .order_by(desc(Message.created_at))
        .limit(1)
    )
    last_outbound = db.scalar(
        select(Message)
        .where(Message.client_id == client.id, Message.direction == MessageDirection.OUTBOUND)
        .order_by(desc(Message.created_at))
        .limit(1)
    )
    last_ai_decision = db.scalar(
        select(AuditLog)
        .where(AuditLog.client_id == client.id, AuditLog.event_type.in_(["agent_decision", "admin_test_ai_decision"]))
        .order_by(desc(AuditLog.created_at))
        .limit(1)
    )

    return ClientEventSummaryOut(
        client_key=client_key,
        last_lead_received_at=last_lead.created_at if last_lead else None,
        last_sms_inbound_at=last_inbound.created_at if last_inbound else None,
        last_sms_outbound_at=last_outbound.created_at if last_outbound else None,
        last_ai_decision_at=last_ai_decision.created_at if last_ai_decision else None,
    )


@router.get("/admin/clients/{client_key}/audit-logs", response_model=list[AdminAuditLogOut])
def list_client_audit_logs(
    client_key: str,
    limit: int = 50,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> list[AdminAuditLogOut]:
    _require_admin(settings, admin_token)
    client = _load_client_by_key(db, client_key)

    clamped_limit = max(1, min(limit, 200))
    logs = db.scalars(
        select(AuditLog)
        .where(AuditLog.client_id == client.id)
        .order_by(desc(AuditLog.created_at))
        .limit(clamped_limit)
    ).all()

    return [
        AdminAuditLogOut(
            id=log.id,
            event_type=log.event_type,
            lead_id=log.lead_id,
            created_at=log.created_at,
            decision=log.decision,
        )
        for log in logs
    ]
