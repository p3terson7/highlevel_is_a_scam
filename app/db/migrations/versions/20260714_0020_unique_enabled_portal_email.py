"""make enabled portal identities globally unambiguous

Revision ID: 20260714_0020
Revises: 20260714_0019
Create Date: 2026-07-14
"""

from alembic import op
import sqlalchemy as sa


revision = "20260714_0020"
down_revision = "20260714_0019"
branch_labels = None
depends_on = None


_INDEX_NAME = "uq_clients_enabled_portal_email"


def _disable_ambiguous_portal_identities() -> None:
    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            """
            SELECT id, lower(trim(portal_email)) AS normalized_email
            FROM clients
            WHERE portal_enabled AND trim(portal_email) <> ''
            ORDER BY
                lower(trim(portal_email)),
                updated_at DESC,
                created_at DESC,
                id DESC
            """
        )
    ).mappings()
    seen: set[str] = set()
    ambiguous_ids: list[int] = []
    for row in rows:
        normalized_email = str(row["normalized_email"])
        if normalized_email in seen:
            ambiguous_ids.append(int(row["id"]))
        else:
            seen.add(normalized_email)
    if ambiguous_ids:
        bind.execute(
            sa.text(
                """
                UPDATE clients
                SET portal_enabled = false, updated_at = CURRENT_TIMESTAMP
                WHERE id IN :client_ids
                """
            ).bindparams(
                sa.bindparam("client_ids", expanding=True, value=ambiguous_ids)
            )
        )


def upgrade() -> None:
    # Preserve every account and credential, but disable older ambiguous portal
    # identities before enforcing one enabled tenant per case-insensitive email.
    op.execute(
        sa.text(
            """
            UPDATE clients
            SET portal_email = lower(trim(portal_email))
            WHERE portal_email <> lower(trim(portal_email))
            """
        )
    )
    _disable_ambiguous_portal_identities()
    op.execute(
        sa.text(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {_INDEX_NAME}
            ON clients (portal_email)
            WHERE portal_enabled AND trim(portal_email) <> ''
            """
        )
    )


def downgrade() -> None:
    op.drop_index(_INDEX_NAME, table_name="clients")
