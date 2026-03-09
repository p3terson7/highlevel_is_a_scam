"""client portal auth fields

Revision ID: 20260304_0003
Revises: 20260226_0002
Create Date: 2026-03-04 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260304_0003"
down_revision = "20260226_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("clients", sa.Column("portal_display_name", sa.String(length=255), nullable=False, server_default=""))
    op.add_column("clients", sa.Column("portal_email", sa.String(length=255), nullable=False, server_default=""))
    op.add_column("clients", sa.Column("portal_password_hash", sa.String(length=255), nullable=False, server_default=""))
    op.add_column("clients", sa.Column("portal_enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")))
    op.create_index("ix_clients_portal_email", "clients", ["portal_email"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_clients_portal_email", table_name="clients")
    op.drop_column("clients", "portal_enabled")
    op.drop_column("clients", "portal_password_hash")
    op.drop_column("clients", "portal_email")
    op.drop_column("clients", "portal_display_name")
