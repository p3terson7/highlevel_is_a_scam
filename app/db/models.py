from __future__ import annotations

import enum
from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def default_operating_hours() -> dict[str, Any]:
    return {"days": [0, 1, 2, 3, 4], "start": "09:00", "end": "18:00"}


def default_questions() -> list[str]:
    return [
        "What are you looking to solve right now?",
        "What timeline are you aiming for?",
        "What outcome would make this successful?",
    ]


def default_dict() -> dict[str, Any]:
    return {}


class Base(DeclarativeBase):
    pass


class ConversationStateEnum(str, enum.Enum):
    NEW = "NEW"
    GREETED = "GREETED"
    QUALIFYING = "QUALIFYING"
    BOOKING_SENT = "BOOKING_SENT"
    BOOKED = "BOOKED"
    HANDOFF = "HANDOFF"
    OPTED_OUT = "OPTED_OUT"


class MessageDirection(str, enum.Enum):
    INBOUND = "INBOUND"
    OUTBOUND = "OUTBOUND"


class LeadSource(str, enum.Enum):
    META = "meta"
    LINKEDIN = "linkedin"
    SMS = "sms"
    MANUAL = "manual"


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class Client(Base, TimestampMixin):
    __tablename__ = "clients"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    client_key: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    business_name: Mapped[str] = mapped_column(String(255), nullable=False)
    tone: Mapped[str] = mapped_column(String(128), default="friendly", nullable=False)
    timezone: Mapped[str] = mapped_column(String(64), default="America/New_York", nullable=False)
    qualification_questions: Mapped[list[str]] = mapped_column(
        JSON, default=default_questions, nullable=False
    )
    booking_url: Mapped[str] = mapped_column(String(1024), default="", nullable=False)
    booking_mode: Mapped[str] = mapped_column(String(32), default="link", nullable=False)
    booking_config: Mapped[dict[str, Any]] = mapped_column(JSON, default=default_dict, nullable=False)
    provider_config: Mapped[dict[str, Any]] = mapped_column(JSON, default=default_dict, nullable=False)
    fallback_handoff_number: Mapped[str] = mapped_column(String(32), default="", nullable=False)
    consent_text: Mapped[str] = mapped_column(
        String(512),
        default="Reply STOP to opt out. Msg/data rates may apply.",
        nullable=False,
    )
    portal_display_name: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    portal_email: Mapped[str] = mapped_column(String(255), default="", nullable=False, index=True)
    portal_password_hash: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    portal_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    operating_hours: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=default_operating_hours, nullable=False
    )
    faq_context: Mapped[str] = mapped_column(Text, default="", nullable=False)
    ai_context: Mapped[str] = mapped_column(Text, default="", nullable=False)
    template_overrides: Mapped[dict[str, str]] = mapped_column(
        JSON, default=default_dict, nullable=False
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    leads: Mapped[list["Lead"]] = relationship(back_populates="client", cascade="all, delete-orphan")
    calendar_bookings: Mapped[list["CalendarBooking"]] = relationship(
        back_populates="client",
        cascade="all, delete-orphan",
    )
    knowledge_sources: Mapped[list["KnowledgeSource"]] = relationship(
        back_populates="client",
        cascade="all, delete-orphan",
    )


class Lead(Base, TimestampMixin):
    __tablename__ = "leads"
    __table_args__ = (
        UniqueConstraint("client_id", "external_lead_id", name="uq_leads_client_external"),
        Index("ix_leads_client_updated_created", "client_id", "updated_at", "created_at"),
        Index("ix_leads_client_state_updated", "client_id", "conversation_state", "updated_at"),
        Index("ix_leads_client_stage_updated", "client_id", "crm_stage", "updated_at"),
        Index("ix_leads_created_id", "created_at", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("clients.id", ondelete="CASCADE"), index=True)
    external_lead_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source: Mapped[LeadSource] = mapped_column(
        Enum(
            LeadSource,
            values_callable=lambda enum_cls: [item.value for item in enum_cls],
            name="leadsource",
        ),
        nullable=False,
    )

    full_name: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    phone: Mapped[str] = mapped_column(String(32), index=True, default="", nullable=False)
    email: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    city: Mapped[str] = mapped_column(String(128), default="", nullable=False)

    form_answers: Mapped[dict[str, Any]] = mapped_column(JSON, default=default_dict, nullable=False)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=default_dict, nullable=False)

    consented: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    opted_out: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    conversation_state: Mapped[ConversationStateEnum] = mapped_column(
        Enum(ConversationStateEnum),
        default=ConversationStateEnum.NEW,
        nullable=False,
    )
    crm_stage: Mapped[str] = mapped_column(String(32), default="New Lead", nullable=False, index=True)
    owner_name: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    initial_sms_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_inbound_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_outbound_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    client: Mapped[Client] = relationship(back_populates="leads")
    messages: Mapped[list["Message"]] = relationship(back_populates="lead", cascade="all, delete-orphan")
    state_history: Mapped[list["ConversationState"]] = relationship(
        back_populates="lead", cascade="all, delete-orphan"
    )
    audit_logs: Mapped[list["AuditLog"]] = relationship(back_populates="lead", cascade="all, delete-orphan")
    tasks: Mapped[list["LeadTask"]] = relationship(back_populates="lead", cascade="all, delete-orphan")
    tags: Mapped[list["LeadTag"]] = relationship(back_populates="lead", cascade="all, delete-orphan")
    calendar_bookings: Mapped[list["CalendarBooking"]] = relationship(
        back_populates="lead",
        cascade="all, delete-orphan",
    )


class Message(Base):
    __tablename__ = "messages"
    __table_args__ = (
        Index("ix_messages_lead_created_id", "lead_id", "created_at", "id"),
        Index("ix_messages_client_direction_created_id", "client_id", "direction", "created_at", "id"),
        Index("ix_messages_client_direction_sid", "client_id", "direction", "provider_message_sid"),
        Index(
            "uq_messages_client_direction_provider_sid_not_empty",
            "client_id",
            "direction",
            "provider_message_sid",
            unique=True,
            postgresql_where=text("provider_message_sid <> ''"),
            sqlite_where=text("provider_message_sid <> ''"),
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lead_id: Mapped[int] = mapped_column(ForeignKey("leads.id", ondelete="CASCADE"), index=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("clients.id", ondelete="CASCADE"), index=True)
    direction: Mapped[MessageDirection] = mapped_column(Enum(MessageDirection), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    provider_message_sid: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=default_dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    lead: Mapped[Lead] = relationship(back_populates="messages")
    attachments: Mapped[list["MessageAttachment"]] = relationship(
        back_populates="message",
        cascade="all, delete-orphan",
    )


class MessageAttachment(Base):
    __tablename__ = "message_attachments"
    __table_args__ = (
        Index("ix_message_attachments_message_id", "message_id"),
        Index("ix_message_attachments_lead_created", "lead_id", "created_at"),
        Index("ix_message_attachments_public_token", "public_token", unique=True),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    message_id: Mapped[int] = mapped_column(ForeignKey("messages.id", ondelete="CASCADE"))
    lead_id: Mapped[int] = mapped_column(ForeignKey("leads.id", ondelete="CASCADE"))
    client_id: Mapped[int] = mapped_column(ForeignKey("clients.id", ondelete="CASCADE"))
    filename: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    content_type: Mapped[str] = mapped_column(String(128), default="", nullable=False)
    media_kind: Mapped[str] = mapped_column(String(16), default="", nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    storage_path: Mapped[str] = mapped_column(String(1024), default="", nullable=False)
    provider_media_url: Mapped[str] = mapped_column(String(2048), default="", nullable=False)
    public_token: Mapped[str] = mapped_column(String(64), nullable=False)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=default_dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    message: Mapped[Message] = relationship(back_populates="attachments")


class ConversationState(Base):
    __tablename__ = "conversation_states"
    __table_args__ = (
        Index("ix_conversation_states_lead_created_id", "lead_id", "created_at", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lead_id: Mapped[int] = mapped_column(ForeignKey("leads.id", ondelete="CASCADE"), index=True)
    previous_state: Mapped[ConversationStateEnum] = mapped_column(
        Enum(ConversationStateEnum), nullable=False
    )
    new_state: Mapped[ConversationStateEnum] = mapped_column(Enum(ConversationStateEnum), nullable=False)
    reason: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=default_dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    lead: Mapped[Lead] = relationship(back_populates="state_history")


class AuditLog(Base):
    __tablename__ = "audit_logs"
    __table_args__ = (
        Index("ix_audit_logs_client_event_created_id", "client_id", "event_type", "created_at", "id"),
        Index("ix_audit_logs_lead_created_id", "lead_id", "created_at", "id"),
        Index("ix_audit_logs_lead_event_created_id", "lead_id", "event_type", "created_at", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    client_id: Mapped[int | None] = mapped_column(ForeignKey("clients.id", ondelete="CASCADE"), index=True)
    lead_id: Mapped[int | None] = mapped_column(ForeignKey("leads.id", ondelete="CASCADE"), index=True)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    decision: Mapped[dict[str, Any]] = mapped_column(JSON, default=default_dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    lead: Mapped[Lead | None] = relationship(back_populates="audit_logs")


class RuntimeSetting(Base):
    __tablename__ = "runtime_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class LeadTag(Base):
    __tablename__ = "lead_tags"
    __table_args__ = (
        UniqueConstraint("lead_id", "tag", name="uq_lead_tags_lead_tag"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lead_id: Mapped[int] = mapped_column(ForeignKey("leads.id", ondelete="CASCADE"), index=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("clients.id", ondelete="CASCADE"), index=True)
    tag: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    lead: Mapped[Lead] = relationship(back_populates="tags")


class LeadTask(Base, TimestampMixin):
    __tablename__ = "lead_tasks"
    __table_args__ = (
        Index("ix_lead_tasks_client_status_due_created", "client_id", "status", "due_date", "created_at"),
        Index("ix_lead_tasks_status_due_created", "status", "due_date", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lead_id: Mapped[int] = mapped_column(ForeignKey("leads.id", ondelete="CASCADE"), index=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("clients.id", ondelete="CASCADE"), index=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, default="", nullable=False)
    due_date: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(16), default="open", nullable=False, index=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_by: Mapped[str] = mapped_column(String(64), default="", nullable=False)

    lead: Mapped[Lead] = relationship(back_populates="tasks")


class CalendarBooking(Base, TimestampMixin):
    __tablename__ = "calendar_bookings"
    __table_args__ = (
        Index("ix_calendar_bookings_client_status_start", "client_id", "status", "start_at"),
        Index("ix_calendar_bookings_status_end_start", "status", "end_at", "start_at"),
        Index(
            "ix_calendar_bookings_client_provider_status_start_end",
            "client_id",
            "provider",
            "status",
            "start_at",
            "end_at",
        ),
        Index(
            "uq_calendar_bookings_client_provider_start_end_scheduled",
            "client_id",
            "provider",
            "start_at",
            "end_at",
            unique=True,
            postgresql_where=text("status = 'scheduled'"),
            sqlite_where=text("status = 'scheduled'"),
        ),
        Index(
            "uq_calendar_bookings_client_lead_provider_scheduled",
            "client_id",
            "lead_id",
            "provider",
            unique=True,
            postgresql_where=text("status = 'scheduled' AND lead_id IS NOT NULL"),
            sqlite_where=text("status = 'scheduled' AND lead_id IS NOT NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("clients.id", ondelete="CASCADE"), index=True)
    lead_id: Mapped[int | None] = mapped_column(ForeignKey("leads.id", ondelete="SET NULL"), index=True, nullable=True)
    provider: Mapped[str] = mapped_column(String(32), default="internal", nullable=False)
    source: Mapped[str] = mapped_column(String(32), default="sms_ai", nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="scheduled", nullable=False, index=True)
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    timezone: Mapped[str] = mapped_column(String(64), default="UTC", nullable=False)
    title: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    notes: Mapped[str] = mapped_column(Text, default="", nullable=False)

    client: Mapped[Client] = relationship(back_populates="calendar_bookings")
    lead: Mapped[Lead | None] = relationship(back_populates="calendar_bookings")


class KnowledgeSource(Base, TimestampMixin):
    __tablename__ = "knowledge_sources"
    __table_args__ = (
        UniqueConstraint("client_id", "normalized_url", name="uq_knowledge_sources_client_url"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("clients.id", ondelete="CASCADE"), index=True)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    normalized_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    title: Mapped[str] = mapped_column(String(512), default="", nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False, index=True)
    content_hash: Mapped[str] = mapped_column(String(64), default="", nullable=False)
    extracted_text: Mapped[str] = mapped_column(Text, default="", nullable=False)
    text_excerpt: Mapped[str] = mapped_column(Text, default="", nullable=False)
    error_message: Mapped[str] = mapped_column(Text, default="", nullable=False)
    last_crawled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    client: Mapped[Client] = relationship(back_populates="knowledge_sources")
    chunks: Mapped[list["KnowledgeChunk"]] = relationship(
        back_populates="source",
        cascade="all, delete-orphan",
        order_by="KnowledgeChunk.chunk_index",
    )


class KnowledgeChunk(Base, TimestampMixin):
    __tablename__ = "knowledge_chunks"
    __table_args__ = (
        UniqueConstraint("source_id", "chunk_index", name="uq_knowledge_chunks_source_index"),
        Index("ix_knowledge_chunks_client_source_index", "client_id", "source_id", "chunk_index"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("clients.id", ondelete="CASCADE"), index=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("knowledge_sources.id", ondelete="CASCADE"), index=True)
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    search_text: Mapped[str] = mapped_column(Text, default="", nullable=False)

    source: Mapped[KnowledgeSource] = relationship(back_populates="chunks")
