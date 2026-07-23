"""add durable inbound SMS work state

Revision ID: 20260714_0019
Revises: 20260713_0018
Create Date: 2026-07-14
"""

from alembic import op
import sqlalchemy as sa


revision = "20260714_0019"
down_revision = "20260713_0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "messages",
        sa.Column(
            "inbound_work_status",
            sa.String(length=24),
            nullable=False,
            server_default=sa.text("''"),
        ),
    )
    op.add_column(
        "messages",
        sa.Column(
            "inbound_work_attempt_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.add_column(
        "messages",
        sa.Column(
            "inbound_work_error",
            sa.String(length=128),
            nullable=False,
            server_default=sa.text("''"),
        ),
    )
    op.add_column(
        "messages",
        sa.Column("inbound_work_updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_messages_inbound_work_status_updated",
        "messages",
        ["inbound_work_status", "inbound_work_updated_at", "id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_messages_inbound_work_status_updated", table_name="messages")
    op.drop_column("messages", "inbound_work_updated_at")
    op.drop_column("messages", "inbound_work_error")
    op.drop_column("messages", "inbound_work_attempt_count")
    op.drop_column("messages", "inbound_work_status")
