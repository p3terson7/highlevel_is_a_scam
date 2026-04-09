"""internal calendar bookings

Revision ID: 20260309_0008
Revises: 20260309_0007
Create Date: 2026-03-09 19:15:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260309_0008"
down_revision = "20260309_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "calendar_bookings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("client_id", sa.Integer(), nullable=False),
        sa.Column("lead_id", sa.Integer(), nullable=True),
        sa.Column("provider", sa.String(length=32), nullable=False, server_default="internal"),
        sa.Column("source", sa.String(length=32), nullable=False, server_default="sms_ai"),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="scheduled"),
        sa.Column("start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("timezone", sa.String(length=64), nullable=False, server_default="UTC"),
        sa.Column("title", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("notes", sa.Text(), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"], ondelete="SET NULL"),
    )
    op.create_index("ix_calendar_bookings_client_id", "calendar_bookings", ["client_id"], unique=False)
    op.create_index("ix_calendar_bookings_lead_id", "calendar_bookings", ["lead_id"], unique=False)
    op.create_index("ix_calendar_bookings_status", "calendar_bookings", ["status"], unique=False)
    op.create_index("ix_calendar_bookings_start_at", "calendar_bookings", ["start_at"], unique=False)
    op.create_index("ix_calendar_bookings_end_at", "calendar_bookings", ["end_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_calendar_bookings_end_at", table_name="calendar_bookings")
    op.drop_index("ix_calendar_bookings_start_at", table_name="calendar_bookings")
    op.drop_index("ix_calendar_bookings_status", table_name="calendar_bookings")
    op.drop_index("ix_calendar_bookings_lead_id", table_name="calendar_bookings")
    op.drop_index("ix_calendar_bookings_client_id", table_name="calendar_bookings")
    op.drop_table("calendar_bookings")
