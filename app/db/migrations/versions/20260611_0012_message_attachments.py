"""add message attachments

Revision ID: 20260611_0012
Revises: 20260609_0011
Create Date: 2026-06-11
"""

from alembic import op
import sqlalchemy as sa


revision = "20260611_0012"
down_revision = "20260609_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "message_attachments",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("message_id", sa.Integer(), nullable=False),
        sa.Column("lead_id", sa.Integer(), nullable=False),
        sa.Column("client_id", sa.Integer(), nullable=False),
        sa.Column("filename", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("content_type", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("media_kind", sa.String(length=16), nullable=False, server_default=""),
        sa.Column("size_bytes", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("storage_path", sa.String(length=1024), nullable=False, server_default=""),
        sa.Column("provider_media_url", sa.String(length=2048), nullable=False, server_default=""),
        sa.Column("public_token", sa.String(length=64), nullable=False),
        sa.Column("raw_payload", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["message_id"], ["messages.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_message_attachments_message_id", "message_attachments", ["message_id"], unique=False)
    op.create_index("ix_message_attachments_lead_created", "message_attachments", ["lead_id", "created_at"], unique=False)
    op.create_index("ix_message_attachments_public_token", "message_attachments", ["public_token"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_message_attachments_public_token", table_name="message_attachments")
    op.drop_index("ix_message_attachments_lead_created", table_name="message_attachments")
    op.drop_index("ix_message_attachments_message_id", table_name="message_attachments")
    op.drop_table("message_attachments")
