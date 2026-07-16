"""harden consent defaults and expire public media links

Revision ID: 20260713_0015
Revises: 20260713_0014
Create Date: 2026-07-13
"""

from alembic import op
import sqlalchemy as sa


revision = "20260713_0015"
down_revision = "20260713_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Preserve historical consent values, but every new row must now provide
    # affirmative evidence before outbound marketing automation can run.
    # SQLite cannot alter a column default in place. Alembic's batch context
    # recreates the table on SQLite while retaining normal ALTER behavior on
    # databases that support it.
    with op.batch_alter_table("leads") as batch_op:
        batch_op.alter_column(
            "consented",
            existing_type=sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        )

    with op.batch_alter_table("message_attachments") as batch_op:
        batch_op.add_column(
            sa.Column("public_expires_at", sa.DateTime(timezone=True), nullable=True),
        )
        batch_op.create_index(
            "ix_message_attachments_public_expires_at",
            ["public_expires_at"],
            unique=False,
        )


def downgrade() -> None:
    # Batch mode also keeps the downgrade compatible with SQLite versions
    # that cannot drop columns directly.
    with op.batch_alter_table("message_attachments") as batch_op:
        batch_op.drop_index("ix_message_attachments_public_expires_at")
        batch_op.drop_column("public_expires_at")

    with op.batch_alter_table("leads") as batch_op:
        batch_op.alter_column(
            "consented",
            existing_type=sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        )
