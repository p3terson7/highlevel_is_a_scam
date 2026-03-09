"""booking automation fields

Revision ID: 20260304_0004
Revises: 20260304_0003
Create Date: 2026-03-04 13:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260304_0004"
down_revision = "20260304_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("clients", sa.Column("booking_mode", sa.String(length=32), nullable=False, server_default="link"))
    op.add_column("clients", sa.Column("booking_config", sa.JSON(), nullable=False, server_default=sa.text("'{}'")))


def downgrade() -> None:
    op.drop_column("clients", "booking_config")
    op.drop_column("clients", "booking_mode")
