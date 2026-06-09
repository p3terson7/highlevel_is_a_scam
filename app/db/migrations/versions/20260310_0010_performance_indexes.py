"""performance indexes

Revision ID: 20260310_0010
Revises: 20260310_0009
Create Date: 2026-03-10 18:00:00.000000
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "20260310_0010"
down_revision = "20260310_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_leads_client_updated_created", "leads", ["client_id", "updated_at", "created_at"], unique=False)
    op.create_index("ix_leads_client_state_updated", "leads", ["client_id", "conversation_state", "updated_at"], unique=False)
    op.create_index("ix_leads_client_stage_updated", "leads", ["client_id", "crm_stage", "updated_at"], unique=False)
    op.create_index("ix_leads_created_id", "leads", ["created_at", "id"], unique=False)

    op.create_index("ix_messages_lead_created_id", "messages", ["lead_id", "created_at", "id"], unique=False)
    op.create_index(
        "ix_messages_client_direction_created_id",
        "messages",
        ["client_id", "direction", "created_at", "id"],
        unique=False,
    )
    op.create_index(
        "ix_messages_client_direction_sid",
        "messages",
        ["client_id", "direction", "provider_message_sid"],
        unique=False,
    )

    op.create_index(
        "ix_conversation_states_lead_created_id",
        "conversation_states",
        ["lead_id", "created_at", "id"],
        unique=False,
    )

    op.create_index(
        "ix_audit_logs_client_event_created_id",
        "audit_logs",
        ["client_id", "event_type", "created_at", "id"],
        unique=False,
    )
    op.create_index("ix_audit_logs_lead_created_id", "audit_logs", ["lead_id", "created_at", "id"], unique=False)
    op.create_index(
        "ix_audit_logs_lead_event_created_id",
        "audit_logs",
        ["lead_id", "event_type", "created_at", "id"],
        unique=False,
    )

    op.create_index(
        "ix_lead_tasks_client_status_due_created",
        "lead_tasks",
        ["client_id", "status", "due_date", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_lead_tasks_status_due_created",
        "lead_tasks",
        ["status", "due_date", "created_at"],
        unique=False,
    )

    op.create_index(
        "ix_calendar_bookings_client_status_start",
        "calendar_bookings",
        ["client_id", "status", "start_at"],
        unique=False,
    )
    op.create_index(
        "ix_calendar_bookings_status_end_start",
        "calendar_bookings",
        ["status", "end_at", "start_at"],
        unique=False,
    )
    op.create_index(
        "ix_calendar_bookings_client_provider_status_start_end",
        "calendar_bookings",
        ["client_id", "provider", "status", "start_at", "end_at"],
        unique=False,
    )

    op.create_index(
        "ix_knowledge_chunks_client_source_index",
        "knowledge_chunks",
        ["client_id", "source_id", "chunk_index"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_knowledge_chunks_client_source_index", table_name="knowledge_chunks")

    op.drop_index("ix_calendar_bookings_client_provider_status_start_end", table_name="calendar_bookings")
    op.drop_index("ix_calendar_bookings_status_end_start", table_name="calendar_bookings")
    op.drop_index("ix_calendar_bookings_client_status_start", table_name="calendar_bookings")

    op.drop_index("ix_lead_tasks_status_due_created", table_name="lead_tasks")
    op.drop_index("ix_lead_tasks_client_status_due_created", table_name="lead_tasks")

    op.drop_index("ix_audit_logs_lead_event_created_id", table_name="audit_logs")
    op.drop_index("ix_audit_logs_lead_created_id", table_name="audit_logs")
    op.drop_index("ix_audit_logs_client_event_created_id", table_name="audit_logs")

    op.drop_index("ix_conversation_states_lead_created_id", table_name="conversation_states")

    op.drop_index("ix_messages_client_direction_sid", table_name="messages")
    op.drop_index("ix_messages_client_direction_created_id", table_name="messages")
    op.drop_index("ix_messages_lead_created_id", table_name="messages")

    op.drop_index("ix_leads_created_id", table_name="leads")
    op.drop_index("ix_leads_client_stage_updated", table_name="leads")
    op.drop_index("ix_leads_client_state_updated", table_name="leads")
    op.drop_index("ix_leads_client_updated_created", table_name="leads")
