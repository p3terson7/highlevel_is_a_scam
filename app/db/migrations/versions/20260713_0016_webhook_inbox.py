"""add durable authenticated webhook inbox

Revision ID: 20260713_0016
Revises: 20260713_0015
Create Date: 2026-07-13
"""

from alembic import op
import sqlalchemy as sa


revision = "20260713_0016"
down_revision = "20260713_0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "inbound_webhook_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("client_id", sa.Integer(), nullable=False),
        sa.Column("endpoint", sa.String(length=32), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("event_key", sa.String(length=128), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("payload_json", sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column("status", sa.String(length=24), server_default="pending", nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("error_detail", sa.String(length=500), server_default="", nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("client_id", "event_key", name="uq_inbound_webhook_events_client_key"),
    )
    op.create_index(
        "ix_inbound_webhook_events_client_id",
        "inbound_webhook_events",
        ["client_id"],
        unique=False,
    )
    op.create_index(
        "ix_inbound_webhook_events_status_created",
        "inbound_webhook_events",
        ["status", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_inbound_webhook_events_client_fingerprint_created",
        "inbound_webhook_events",
        ["client_id", "payload_sha256", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_inbound_webhook_events_client_fingerprint_created",
        table_name="inbound_webhook_events",
    )
    op.drop_index(
        "ix_inbound_webhook_events_status_created",
        table_name="inbound_webhook_events",
    )
    op.drop_index("ix_inbound_webhook_events_client_id", table_name="inbound_webhook_events")
    op.drop_table("inbound_webhook_events")
