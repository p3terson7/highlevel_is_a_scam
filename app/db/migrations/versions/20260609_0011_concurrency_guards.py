"""concurrency guards

Revision ID: 20260609_0011
Revises: 20260310_0010
Create Date: 2026-06-09 16:45:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260609_0011"
down_revision = "20260310_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "uq_messages_client_direction_provider_sid_not_empty",
        "messages",
        ["client_id", "direction", "provider_message_sid"],
        unique=True,
        postgresql_where=sa.text("provider_message_sid <> ''"),
        sqlite_where=sa.text("provider_message_sid <> ''"),
    )
    op.create_index(
        "uq_calendar_bookings_client_provider_start_end_scheduled",
        "calendar_bookings",
        ["client_id", "provider", "start_at", "end_at"],
        unique=True,
        postgresql_where=sa.text("status = 'scheduled'"),
        sqlite_where=sa.text("status = 'scheduled'"),
    )
    op.create_index(
        "uq_calendar_bookings_client_lead_provider_scheduled",
        "calendar_bookings",
        ["client_id", "lead_id", "provider"],
        unique=True,
        postgresql_where=sa.text("status = 'scheduled' AND lead_id IS NOT NULL"),
        sqlite_where=sa.text("status = 'scheduled' AND lead_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_calendar_bookings_client_lead_provider_scheduled",
        table_name="calendar_bookings",
    )
    op.drop_index(
        "uq_calendar_bookings_client_provider_start_end_scheduled",
        table_name="calendar_bookings",
    )
    op.drop_index(
        "uq_messages_client_direction_provider_sid_not_empty",
        table_name="messages",
    )
