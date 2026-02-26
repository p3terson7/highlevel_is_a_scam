"""initial schema

Revision ID: 20260226_0001
Revises:
Create Date: 2026-02-26 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260226_0001"
down_revision = None
branch_labels = None
depends_on = None


conversation_state_enum = sa.Enum(
    "NEW",
    "GREETED",
    "QUALIFYING",
    "BOOKING_SENT",
    "BOOKED",
    "HANDOFF",
    "OPTED_OUT",
    name="conversationstateenum",
)
message_direction_enum = sa.Enum("INBOUND", "OUTBOUND", name="messagedirection")
lead_source_enum = sa.Enum("meta", "linkedin", "sms", "manual", name="leadsource")


def upgrade() -> None:
    op.create_table(
        "clients",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("client_key", sa.String(length=64), nullable=False),
        sa.Column("business_name", sa.String(length=255), nullable=False),
        sa.Column("tone", sa.String(length=128), nullable=False, server_default="friendly"),
        sa.Column("timezone", sa.String(length=64), nullable=False, server_default="America/New_York"),
        sa.Column("qualification_questions", sa.JSON(), nullable=False),
        sa.Column("booking_url", sa.String(length=1024), nullable=False, server_default=""),
        sa.Column("fallback_handoff_number", sa.String(length=32), nullable=False, server_default=""),
        sa.Column(
            "consent_text",
            sa.String(length=512),
            nullable=False,
            server_default="Reply STOP to opt out. Msg/data rates may apply.",
        ),
        sa.Column("operating_hours", sa.JSON(), nullable=False),
        sa.Column("faq_context", sa.Text(), nullable=False, server_default=""),
        sa.Column("template_overrides", sa.JSON(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("ix_clients_client_key", "clients", ["client_key"], unique=True)

    op.create_table(
        "leads",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("client_id", sa.Integer(), nullable=False),
        sa.Column("external_lead_id", sa.String(length=255), nullable=True),
        sa.Column("source", lead_source_enum, nullable=False),
        sa.Column("full_name", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("phone", sa.String(length=32), nullable=False, server_default=""),
        sa.Column("email", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("city", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("form_answers", sa.JSON(), nullable=False),
        sa.Column("raw_payload", sa.JSON(), nullable=False),
        sa.Column("consented", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("opted_out", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("conversation_state", conversation_state_enum, nullable=False, server_default="NEW"),
        sa.Column("initial_sms_sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_inbound_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_outbound_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("client_id", "external_lead_id", name="uq_leads_client_external"),
    )
    op.create_index("ix_leads_client_id", "leads", ["client_id"], unique=False)
    op.create_index("ix_leads_phone", "leads", ["phone"], unique=False)

    op.create_table(
        "messages",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("lead_id", sa.Integer(), nullable=False),
        sa.Column("client_id", sa.Integer(), nullable=False),
        sa.Column("direction", message_direction_enum, nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("provider_message_sid", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("raw_payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_messages_lead_id", "messages", ["lead_id"], unique=False)
    op.create_index("ix_messages_client_id", "messages", ["client_id"], unique=False)

    op.create_table(
        "conversation_states",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("lead_id", sa.Integer(), nullable=False),
        sa.Column("previous_state", conversation_state_enum, nullable=False),
        sa.Column("new_state", conversation_state_enum, nullable=False),
        sa.Column("reason", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_conversation_states_lead_id", "conversation_states", ["lead_id"], unique=False)

    op.create_table(
        "audit_logs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("client_id", sa.Integer(), nullable=True),
        sa.Column("lead_id", sa.Integer(), nullable=True),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("decision", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_audit_logs_client_id", "audit_logs", ["client_id"], unique=False)
    op.create_index("ix_audit_logs_lead_id", "audit_logs", ["lead_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_audit_logs_lead_id", table_name="audit_logs")
    op.drop_index("ix_audit_logs_client_id", table_name="audit_logs")
    op.drop_table("audit_logs")

    op.drop_index("ix_conversation_states_lead_id", table_name="conversation_states")
    op.drop_table("conversation_states")

    op.drop_index("ix_messages_client_id", table_name="messages")
    op.drop_index("ix_messages_lead_id", table_name="messages")
    op.drop_table("messages")

    op.drop_index("ix_leads_phone", table_name="leads")
    op.drop_index("ix_leads_client_id", table_name="leads")
    op.drop_table("leads")

    op.drop_index("ix_clients_client_key", table_name="clients")
    op.drop_table("clients")

    bind = op.get_bind()
    lead_source_enum.drop(bind, checkfirst=True)
    message_direction_enum.drop(bind, checkfirst=True)
    conversation_state_enum.drop(bind, checkfirst=True)
