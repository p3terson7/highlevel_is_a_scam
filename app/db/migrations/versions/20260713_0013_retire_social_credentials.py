"""retire unused Meta and LinkedIn credentials

Revision ID: 20260713_0013
Revises: 20260611_0012
Create Date: 2026-07-13
"""

from alembic import op
import sqlalchemy as sa


revision = "20260713_0013"
down_revision = "20260611_0012"
branch_labels = None
depends_on = None


_RETIRED_KEYS = (
    "meta_verify_token",
    "meta_access_token",
    "meta_graph_api_version",
    "linkedin_verify_token",
)


def upgrade() -> None:
    bind = op.get_bind()
    op.execute(
        sa.text("DELETE FROM runtime_settings WHERE key IN :keys").bindparams(
            sa.bindparam("keys", expanding=True, value=_RETIRED_KEYS)
        )
    )

    if bind.dialect.name == "postgresql":
        op.execute(
            sa.text(
                """
                UPDATE clients
                SET provider_config = (
                    provider_config::jsonb
                    - 'meta_verify_token'
                    - 'meta_access_token'
                    - 'meta_graph_api_version'
                    - 'linkedin_verify_token'
                )::json
                """
            )
        )
    elif bind.dialect.name == "sqlite":
        op.execute(
            sa.text(
                """
                UPDATE clients
                SET provider_config = json_remove(
                    provider_config,
                    '$.meta_verify_token',
                    '$.meta_access_token',
                    '$.meta_graph_api_version',
                    '$.linkedin_verify_token'
                )
                """
            )
        )


def downgrade() -> None:
    # Retired credentials are intentionally not recoverable once purged.
    pass
