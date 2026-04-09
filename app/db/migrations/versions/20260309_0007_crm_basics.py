"""crm basics

Revision ID: 20260309_0007
Revises: 20260307_0006
Create Date: 2026-03-09 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260309_0007"
down_revision = "20260307_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("leads", sa.Column("crm_stage", sa.String(length=32), nullable=False, server_default="New Lead"))
    op.add_column("leads", sa.Column("owner_name", sa.String(length=255), nullable=False, server_default=""))
    op.create_index("ix_leads_crm_stage", "leads", ["crm_stage"], unique=False)

    op.create_table(
        "lead_tags",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("lead_id", sa.Integer(), nullable=False),
        sa.Column("client_id", sa.Integer(), nullable=False),
        sa.Column("tag", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("lead_id", "tag", name="uq_lead_tags_lead_tag"),
    )
    op.create_index("ix_lead_tags_lead_id", "lead_tags", ["lead_id"], unique=False)
    op.create_index("ix_lead_tags_client_id", "lead_tags", ["client_id"], unique=False)
    op.create_index("ix_lead_tags_tag", "lead_tags", ["tag"], unique=False)

    op.create_table(
        "lead_tasks",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("lead_id", sa.Integer(), nullable=False),
        sa.Column("client_id", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=False, server_default=""),
        sa.Column("due_date", sa.Date(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="open"),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by", sa.String(length=64), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_lead_tasks_lead_id", "lead_tasks", ["lead_id"], unique=False)
    op.create_index("ix_lead_tasks_client_id", "lead_tasks", ["client_id"], unique=False)
    op.create_index("ix_lead_tasks_due_date", "lead_tasks", ["due_date"], unique=False)
    op.create_index("ix_lead_tasks_status", "lead_tasks", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_lead_tasks_status", table_name="lead_tasks")
    op.drop_index("ix_lead_tasks_due_date", table_name="lead_tasks")
    op.drop_index("ix_lead_tasks_client_id", table_name="lead_tasks")
    op.drop_index("ix_lead_tasks_lead_id", table_name="lead_tasks")
    op.drop_table("lead_tasks")

    op.drop_index("ix_lead_tags_tag", table_name="lead_tags")
    op.drop_index("ix_lead_tags_client_id", table_name="lead_tags")
    op.drop_index("ix_lead_tags_lead_id", table_name="lead_tags")
    op.drop_table("lead_tags")

    op.drop_index("ix_leads_crm_stage", table_name="leads")
    op.drop_column("leads", "owner_name")
    op.drop_column("leads", "crm_stage")
