from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa
import pytest
from alembic import command
from alembic.config import Config

from app.core.config import get_settings


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def _alembic_config() -> Config:
    config = Config(str(REPOSITORY_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(REPOSITORY_ROOT / "app" / "db" / "migrations"))
    return config


def _column_default(engine: sa.Engine, table: str, column: str) -> str:
    columns = {item["name"]: item for item in sa.inspect(engine).get_columns(table)}
    default = str(columns[column]["default"] or "").strip().strip("()'").lower()
    return default


def _connection_revision(engine: sa.Engine) -> str:
    with engine.connect() as connection:
        return str(connection.scalar(sa.text("SELECT version_num FROM alembic_version")))


def test_sqlite_security_migration_upgrade_and_downgrade(
    tmp_path, monkeypatch, request: pytest.FixtureRequest
) -> None:
    database_path = tmp_path / "migration-chain.db"
    database_url = f"sqlite+pysqlite:///{database_path.as_posix()}"
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("ADMIN_TOKEN", "test-admin-token-32-characters-long!")
    get_settings.cache_clear()
    request.addfinalizer(get_settings.cache_clear)

    config = _alembic_config()
    command.upgrade(config, "20260713_0014")

    engine = sa.create_engine(database_url)
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                INSERT INTO clients (
                    client_key, business_name, qualification_questions,
                    operating_hours, template_overrides
                ) VALUES (
                    'migration-client', 'Migration Client', '[]', '{}', '{}'
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO clients (
                    client_key, business_name, qualification_questions,
                    portal_email, portal_enabled,
                    operating_hours, template_overrides,
                    created_at, updated_at
                ) VALUES
                    (
                        'portal-older', 'Older Portal Client', '[]',
                        'Owner@Example.com', true, '{}', '{}',
                        '2026-07-13 10:00:00', '2026-07-13 10:00:00'
                    ),
                    (
                        'portal-current', 'Current Portal Client', '[]',
                        'owner@example.com', true, '{}', '{}',
                        '2026-07-13 11:00:00', '2026-07-13 11:00:00'
                    )
                """
            )
        )
        # Simulate a database that applied the original 0011 before this guard
        # was added to that historical migration.  The head repair must restore
        # the invariant without duplicating it on a fresh chain.
        connection.execute(
            sa.text(
                "DROP INDEX uq_calendar_bookings_client_lead_provider_scheduled"
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO leads (
                    client_id, external_lead_id, source, form_answers,
                    raw_payload, consented
                ) VALUES (
                    1, 'historical-lead', 'manual', '{}', '{}', true
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO calendar_bookings (
                    client_id, lead_id, provider, source, status,
                    start_at, end_at, timezone, title, notes,
                    created_at, updated_at
                ) VALUES
                    (
                        1, 1, 'manual', 'manual', 'scheduled',
                        '2026-07-20 10:00:00', '2026-07-20 10:30:00',
                        'UTC', 'Superseded meeting', '',
                        '2026-07-13 10:00:00', '2026-07-13 10:00:00'
                    ),
                    (
                        1, 1, 'manual', 'manual', 'scheduled',
                        '2026-07-21 10:00:00', '2026-07-21 10:30:00',
                        'UTC', 'Current meeting', '',
                        '2026-07-13 11:00:00', '2026-07-13 11:00:00'
                    )
                """
            )
        )
    engine.dispose()

    command.upgrade(config, "head")

    engine = sa.create_engine(database_url)
    attachment_columns = {column["name"] for column in sa.inspect(engine).get_columns("message_attachments")}
    attachment_indexes = {index["name"] for index in sa.inspect(engine).get_indexes("message_attachments")}
    outbound_indexes = {index["name"] for index in sa.inspect(engine).get_indexes("outbound_requests")}
    message_columns = {column["name"] for column in sa.inspect(engine).get_columns("messages")}
    message_indexes = {index["name"] for index in sa.inspect(engine).get_indexes("messages")}
    booking_indexes = {index["name"] for index in sa.inspect(engine).get_indexes("calendar_bookings")}
    client_indexes = {index["name"] for index in sa.inspect(engine).get_indexes("clients")}
    assert "public_expires_at" in attachment_columns
    assert "ix_message_attachments_public_expires_at" in attachment_indexes
    assert "ix_outbound_requests_status_updated" in outbound_indexes
    assert {
        "inbound_work_status",
        "inbound_work_attempt_count",
        "inbound_work_error",
        "inbound_work_updated_at",
    }.issubset(message_columns)
    assert "ix_messages_inbound_work_status_updated" in message_indexes
    assert "uq_calendar_bookings_client_lead_provider_scheduled" in booking_indexes
    assert "uq_clients_enabled_portal_email" in client_indexes
    assert _column_default(engine, "leads", "consented") in {"0", "false"}

    with engine.connect() as connection:
        booking_statuses = connection.execute(
            sa.text(
                """
                SELECT title, status
                FROM calendar_bookings
                WHERE client_id = 1 AND lead_id = 1 AND provider = 'manual'
                ORDER BY id
                """
            )
        ).all()
    assert booking_statuses == [
        ("Superseded meeting", "cancelled"),
        ("Current meeting", "scheduled"),
    ]

    with engine.connect() as connection:
        portal_states = connection.execute(
            sa.text(
                """
                SELECT client_key, portal_enabled
                FROM clients
                WHERE client_key IN ('portal-older', 'portal-current')
                ORDER BY client_key
                """
            )
        ).all()
    assert portal_states == [
        ("portal-current", 1),
        ("portal-older", 0),
    ]

    with engine.begin() as connection:
        historical_consent = connection.scalar(
            sa.text("SELECT consented FROM leads WHERE external_lead_id = 'historical-lead'")
        )
        assert historical_consent == 1
        connection.execute(
            sa.text(
                """
                INSERT INTO leads (
                    client_id, external_lead_id, source, form_answers, raw_payload
                ) VALUES (
                    1, 'new-secure-default', 'manual', '{}', '{}'
                )
                """
            )
        )
        assert connection.scalar(
            sa.text("SELECT consented FROM leads WHERE external_lead_id = 'new-secure-default'")
        ) == 0
    engine.dispose()

    command.downgrade(config, "20260713_0014")

    engine = sa.create_engine(database_url)
    attachment_columns = {column["name"] for column in sa.inspect(engine).get_columns("message_attachments")}
    attachment_indexes = {index["name"] for index in sa.inspect(engine).get_indexes("message_attachments")}
    assert "public_expires_at" not in attachment_columns
    assert "ix_message_attachments_public_expires_at" not in attachment_indexes
    assert _column_default(engine, "leads", "consented") in {"1", "true"}

    with engine.begin() as connection:
        assert connection.scalar(
            sa.text("SELECT consented FROM leads WHERE external_lead_id = 'historical-lead'")
        ) == 1
        assert connection.scalar(
            sa.text("SELECT consented FROM leads WHERE external_lead_id = 'new-secure-default'")
        ) == 0
    engine.dispose()

    command.upgrade(config, "head")
    engine = sa.create_engine(database_url)
    assert _connection_revision(engine) == "20260716_0022"
    engine.dispose()
