"""repair the scheduled-booking-per-lead concurrency guard

Revision ID: 20260713_0018
Revises: 20260713_0017
Create Date: 2026-07-13

The guard belongs to revision 0011, but some databases applied an earlier
version of that migration before the index was present.  Reasserting the
invariant with ``IF NOT EXISTS`` repairs those databases without failing on a
fresh migration chain where 0011 already created it.
"""

from alembic import op
import sqlalchemy as sa


revision = "20260713_0018"
down_revision = "20260713_0017"
branch_labels = None
depends_on = None


_INDEX_NAME = "uq_calendar_bookings_client_lead_provider_scheduled"


def _cancel_duplicate_scheduled_bookings() -> None:
    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            """
            SELECT id, client_id, lead_id, provider
            FROM calendar_bookings
            WHERE status = 'scheduled' AND lead_id IS NOT NULL
            ORDER BY
                client_id,
                lead_id,
                provider,
                updated_at DESC,
                created_at DESC,
                id DESC
            """
        )
    ).mappings()
    seen: set[tuple[int, int, str]] = set()
    superseded_ids: list[int] = []
    for row in rows:
        key = (int(row["client_id"]), int(row["lead_id"]), str(row["provider"]))
        if key in seen:
            superseded_ids.append(int(row["id"]))
        else:
            seen.add(key)
    if superseded_ids:
        bind.execute(
            sa.text(
                """
                UPDATE calendar_bookings
                SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP
                WHERE id IN :booking_ids
                """
            ).bindparams(
                sa.bindparam("booking_ids", expanding=True, value=superseded_ids)
            )
        )


def upgrade() -> None:
    # Preserve every historical row, but mark older conflicting appointments
    # as superseded before enforcing the invariant.  The latest operator action
    # (updated/created/id ordering) remains scheduled.
    _cancel_duplicate_scheduled_bookings()
    op.execute(
        sa.text(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {_INDEX_NAME}
            ON calendar_bookings (client_id, lead_id, provider)
            WHERE status = 'scheduled' AND lead_id IS NOT NULL
            """
        )
    )


def downgrade() -> None:
    # Revision 0011 owns this invariant.  Dropping it here would make a
    # downgrade to 0017 less safe than the historical schema it represents.
    pass
