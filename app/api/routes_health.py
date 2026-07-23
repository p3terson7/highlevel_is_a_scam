from __future__ import annotations

import secrets
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, status
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from sqlalchemy import desc, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.core.deps import clear_dependency_caches, get_app_settings
from app.core.metrics import render_prometheus
from app.core.security import verify_admin_token
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
from app.services.config_visibility import browser_safe_booking_config, browser_safe_provider_config
from app.services.runtime_config import (
    CLIENT_PROVIDER_KEYS,
    GLOBAL_SECRET_KEYS,
    RETIRED_CLIENT_PROVIDER_KEYS,
    get_effective_runtime_map,
    get_effective_runtime_map_for_client,
    load_runtime_overrides,
    normalize_client_provider_config,
    upsert_runtime_values,
)
from app.services.sms_service import build_sms_service
from app.services.secret_storage import protect_mapping
from app.services.ui_session_auth import current_ui_session_token, verify_ui_session_token

router = APIRouter(tags=["system"])
_BOOKING_SECRET_KEYS = {"calendly_personal_access_token"}


def _portal_email_in_use(
    db: Session,
    *,
    email: str,
    exclude_client_id: int | None = None,
) -> bool:
    normalized = str(email or "").strip().lower()
    if not normalized:
        return False
    query = select(Client.id).where(
        Client.portal_enabled.is_(True),
        func.lower(func.trim(Client.portal_email)) == normalized,
    )
    if exclude_client_id is not None:
        query = query.where(Client.id != exclude_client_id)
    return db.scalar(query.limit(1)) is not None


def _webhook_urls(client_key: str) -> dict[str, str]:
    return {
        "zapier_events": f"/webhooks/zapier/{client_key}",
        "website_form": f"/webhooks/form/{client_key}",
        "twilio_sms": f"/sms/inbound/{client_key}",
    }


class ClientCreateRequest(BaseModel):
    business_name: str = Field(max_length=255)
    tone: str = Field(default="friendly", max_length=200)
    timezone: str = Field(default="America/New_York", max_length=64)
    qualification_questions: list[str] = Field(
        default_factory=lambda: [
            "What are you hoping to solve?",
            "When do you want to get started?",
        ],
        max_length=50,
    )
    booking_url: str = Field(default="", max_length=2_048)
    booking_mode: str = Field(default="link", max_length=32)
    booking_config: dict[str, Any] = Field(default_factory=dict, max_length=100)
    provider_config: dict[str, Any] = Field(default_factory=dict, max_length=100)
    fallback_handoff_number: str = Field(default="", max_length=32)
    consent_text: str = Field(default="Reply STOP to opt out. Msg/data rates may apply.", max_length=1_000)
    operating_hours: dict[str, Any] = Field(
        default_factory=lambda: {"days": [0, 1, 2, 3, 4], "start": "09:00", "end": "18:00"},
        max_length=32,
    )
    faq_context: str = Field(default="", max_length=12_000)
    ai_context: str = Field(default="", max_length=12_000)
    template_overrides: dict[str, str] = Field(default_factory=dict, max_length=100)
    client_key: str | None = Field(default=None, max_length=128)
    portal_display_name: str = Field(default="", max_length=255)
    portal_email: str = Field(default="", max_length=320)
    portal_password: str | None = Field(default=None, max_length=512)
    portal_enabled: bool = False


class ClientUpdateRequest(BaseModel):
    business_name: str | None = Field(default=None, max_length=255)
    tone: str | None = Field(default=None, max_length=200)
    timezone: str | None = Field(default=None, max_length=64)
    qualification_questions: list[str] | None = Field(default=None, max_length=50)
    booking_url: str | None = Field(default=None, max_length=2_048)
    booking_mode: str | None = Field(default=None, max_length=32)
    booking_config: dict[str, Any] | None = Field(default=None, max_length=100)
    provider_config: dict[str, Any] | None = Field(default=None, max_length=100)
    provider_config_clear_keys: list[str] = Field(default_factory=list, max_length=100)
    fallback_handoff_number: str | None = Field(default=None, max_length=32)
    consent_text: str | None = Field(default=None, max_length=1_000)
    operating_hours: dict[str, Any] | None = Field(default=None, max_length=32)
    faq_context: str | None = Field(default=None, max_length=12_000)
    ai_context: str | None = Field(default=None, max_length=12_000)
    template_overrides: dict[str, str] | None = Field(default=None, max_length=100)
    is_active: bool | None = None
    portal_display_name: str | None = Field(default=None, max_length=255)
    portal_email: str | None = Field(default=None, max_length=320)
    portal_password: str | None = Field(default=None, max_length=512)
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
    openai_api_key: str | None = Field(default=None, max_length=512)
    openai_model: str | None = Field(default=None, max_length=128)
    ai_provider_mode: str | None = Field(default=None, max_length=32)


class RuntimeConfigStatusOut(BaseModel):
    openai_api_key_configured: bool
    openai_model: str
    ai_provider_mode: str


class RuntimeConfigUpdateResponse(BaseModel):
    updated_keys: list[str]
    secret_keys_updated: list[str]


class TestSMSRequest(BaseModel):
    client_key: str = Field(max_length=128)
    to_number: str = Field(max_length=32)
    body: str = Field(default="This is a test SMS from Lead Conversion SMS Agent.", max_length=1_600)


class TestAIRequest(BaseModel):
    client_key: str = Field(max_length=128)
    inbound_text: str = Field(default="Can I book a consultation this week?", max_length=2_000)
    lead_name: str = Field(default="Test Lead", max_length=255)
    lead_city: str = Field(default="Test City", max_length=128)


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
    cookie_session = verify_ui_session_token(settings, current_ui_session_token())
    if not verify_admin_token(admin_token, settings.admin_token) and not (
        cookie_session is not None and cookie_session.role == "admin"
    ):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid admin token")


def _load_client_by_key(db: Session, client_key: str) -> Client:
    client = db.scalar(select(Client).where(Client.client_key == client_key))
    if client is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    return client


def _effective_runtime_config(settings: Settings, db: Session) -> dict[str, str]:
    return get_effective_runtime_map(settings=settings, overrides=load_runtime_overrides(db))


def _normalize_provider_config(raw: dict[str, Any] | None) -> dict[str, str]:
    return normalize_client_provider_config(raw)


def _drop_client_disallowed_provider_keys(provider_config: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(provider_config or {})
    for key in ("openai_api_key", "openai_model", "ai_provider_mode", *RETIRED_CLIENT_PROVIDER_KEYS):
        cleaned.pop(key, None)
    return cleaned


def _deep_merge_config(current: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = dict(current)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_config(dict(merged[key]), value)
        else:
            merged[key] = value
    return merged


def _protect_booking_config(raw: dict[str, Any] | None) -> dict[str, Any]:
    return protect_mapping(raw, secret_keys=_BOOKING_SECRET_KEYS)


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@router.get("/metrics", response_class=PlainTextResponse)
def metrics(
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> str:
    if settings.env.strip().lower() in {"prod", "production"}:
        _require_admin(settings, admin_token)
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

    portal_email = payload.portal_email.strip().lower()
    if payload.portal_enabled and _portal_email_in_use(db, email=portal_email):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Portal email is already assigned to another enabled client",
        )

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
        booking_config=_protect_booking_config(payload.booking_config),
        provider_config=_normalize_provider_config(payload.provider_config),
        fallback_handoff_number=payload.fallback_handoff_number,
        consent_text=payload.consent_text,
        portal_display_name=payload.portal_display_name.strip(),
        portal_email=portal_email,
        portal_password_hash=portal_password_hash,
        portal_enabled=payload.portal_enabled,
        operating_hours=payload.operating_hours,
        faq_context=payload.faq_context,
        ai_context=payload.ai_context,
        template_overrides=payload.template_overrides,
    )
    db.add(client)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Client key or enabled portal email already exists",
        ) from exc
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
        booking_config=browser_safe_booking_config(client.booking_config),
        provider_config=browser_safe_provider_config(client.provider_config),
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
    clear_provider_keys = {
        str(key).strip()
        for key in changes.pop("provider_config_clear_keys", [])
        if str(key).strip()
    }
    invalid_clear_keys = clear_provider_keys - CLIENT_PROVIDER_KEYS
    if invalid_clear_keys:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported provider configuration keys: {', '.join(sorted(invalid_clear_keys))}",
        )

    provider_patch_present = "provider_config" in changes
    provider_patch = changes.pop("provider_config", None)
    normalized_provider_patch = _normalize_provider_config(provider_patch) if provider_patch_present else {}
    overlapping_provider_keys = clear_provider_keys & normalized_provider_patch.keys()
    if overlapping_provider_keys:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Provider keys cannot be updated and cleared together: {', '.join(sorted(overlapping_provider_keys))}",
        )
    if provider_patch_present or clear_provider_keys:
        merged_provider_config = _drop_client_disallowed_provider_keys(dict(client.provider_config or {}))
        for key in clear_provider_keys:
            merged_provider_config.pop(key, None)
        merged_provider_config.update(normalized_provider_patch)
        client.provider_config = merged_provider_config

    candidate_portal_email = str(
        changes.get("portal_email", client.portal_email) or ""
    ).strip().lower()
    candidate_portal_enabled = bool(
        changes.get("portal_enabled", client.portal_enabled)
    )
    if candidate_portal_enabled and _portal_email_in_use(
        db,
        email=candidate_portal_email,
        exclude_client_id=client.id,
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Portal email is already assigned to another enabled client",
        )

    for key, value in changes.items():
        if key == "booking_config":
            value = _protect_booking_config(
                _deep_merge_config(dict(client.booking_config or {}), value or {})
            )
        if key == "portal_email":
            value = str(value or "").strip().lower()
        if key == "portal_display_name" and value is not None:
            value = value.strip()
        setattr(client, key, value)
    if portal_password is not None:
        try:
            client.portal_password_hash = hash_portal_password(portal_password) if portal_password.strip() else ""
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Enabled portal email is already assigned to another client",
        ) from exc
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
        booking_config=browser_safe_booking_config(client.booking_config),
        provider_config=browser_safe_provider_config(client.provider_config),
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
        openai_api_key_configured=bool(effective["openai_api_key"]),
        openai_model=effective["openai_model"],
        ai_provider_mode=effective["ai_provider_mode"],
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
    secret_keys_updated = sorted([key for key in updates.keys() if key in GLOBAL_SECRET_KEYS])
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
