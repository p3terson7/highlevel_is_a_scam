"""client ai context

Revision ID: 20260307_0006
Revises: 20260305_0005
Create Date: 2026-03-07 16:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260307_0006"
down_revision = "20260305_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("clients", sa.Column("ai_context", sa.Text(), nullable=False, server_default=""))


def downgrade() -> None:
    op.drop_column("clients", "ai_context")
