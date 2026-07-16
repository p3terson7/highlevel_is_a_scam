"""index outbound requests for bounded recovery scans

Revision ID: 20260713_0017
Revises: 20260713_0016
Create Date: 2026-07-13
"""

from alembic import op


revision = "20260713_0017"
down_revision = "20260713_0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_outbound_requests_status_updated",
        "outbound_requests",
        ["status", "updated_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_outbound_requests_status_updated",
        table_name="outbound_requests",
    )
