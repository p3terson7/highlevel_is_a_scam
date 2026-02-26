from __future__ import annotations

import enum
from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def default_operating_hours() -> dict[str, Any]:
    return {"days": [0, 1, 2, 3, 4], "start": "09:00", "end": "18:00"}


def default_questions() -> list[str]:
    return [
        "What are you looking to solve right now?",
        "What timeline are you aiming for?",
        "What is your budget range?",
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
    fallback_handoff_number: Mapped[str] = mapped_column(String(32), default="", nullable=False)
    consent_text: Mapped[str] = mapped_column(
        String(512),
        default="Reply STOP to opt out. Msg/data rates may apply.",
        nullable=False,
    )
    operating_hours: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=default_operating_hours, nullable=False
    )
    faq_context: Mapped[str] = mapped_column(Text, default="", nullable=False)
    template_overrides: Mapped[dict[str, str]] = mapped_column(
        JSON, default=default_dict, nullable=False
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    leads: Mapped[list["Lead"]] = relationship(back_populates="client", cascade="all, delete-orphan")


class Lead(Base, TimestampMixin):
    __tablename__ = "leads"
    __table_args__ = (
        UniqueConstraint("client_id", "external_lead_id", name="uq_leads_client_external"),
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
    initial_sms_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_inbound_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_outbound_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    client: Mapped[Client] = relationship(back_populates="leads")
    messages: Mapped[list["Message"]] = relationship(back_populates="lead", cascade="all, delete-orphan")
    state_history: Mapped[list["ConversationState"]] = relationship(
        back_populates="lead", cascade="all, delete-orphan"
    )
    audit_logs: Mapped[list["AuditLog"]] = relationship(back_populates="lead", cascade="all, delete-orphan")


class Message(Base):
    __tablename__ = "messages"

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


class ConversationState(Base):
    __tablename__ = "conversation_states"

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
