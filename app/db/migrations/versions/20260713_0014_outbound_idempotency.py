"""add durable outbound request idempotency

Revision ID: 20260713_0014
Revises: 20260713_0013
Create Date: 2026-07-13
"""

from alembic import op
import sqlalchemy as sa


revision = "20260713_0014"
down_revision = "20260713_0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "outbound_requests",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("client_id", sa.Integer(), nullable=False),
        sa.Column("lead_id", sa.Integer(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("request_kind", sa.String(length=64), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False, server_default="pending"),
        sa.Column("provider_message_sid", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("response_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("error_detail", sa.String(length=500), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("client_id", "idempotency_key", name="uq_outbound_requests_client_key"),
    )
    op.create_index("ix_outbound_requests_client_id", "outbound_requests", ["client_id"], unique=False)
    op.create_index("ix_outbound_requests_lead_id", "outbound_requests", ["lead_id"], unique=False)
    op.create_index("ix_outbound_requests_lead_created", "outbound_requests", ["lead_id", "created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_outbound_requests_lead_created", table_name="outbound_requests")
    op.drop_index("ix_outbound_requests_lead_id", table_name="outbound_requests")
    op.drop_index("ix_outbound_requests_client_id", table_name="outbound_requests")
    op.drop_table("outbound_requests")
