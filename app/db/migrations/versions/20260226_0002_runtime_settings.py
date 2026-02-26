"""runtime settings table

Revision ID: 20260226_0002
Revises: 20260226_0001
Create Date: 2026-02-26 00:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260226_0002"
down_revision = "20260226_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "runtime_settings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("key", sa.String(length=128), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("ix_runtime_settings_key", "runtime_settings", ["key"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_runtime_settings_key", table_name="runtime_settings")
    op.drop_table("runtime_settings")
