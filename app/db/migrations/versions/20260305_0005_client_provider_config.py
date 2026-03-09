"""client provider config

Revision ID: 20260305_0005
Revises: 20260304_0004
Create Date: 2026-03-05 01:15:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260305_0005"
down_revision = "20260304_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("clients", sa.Column("provider_config", sa.JSON(), nullable=False, server_default=sa.text("'{}'")))


def downgrade() -> None:
    op.drop_column("clients", "provider_config")
