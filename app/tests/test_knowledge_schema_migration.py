from __future__ import annotations

import json
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from app.core.config import get_settings
from app.db.models import KnowledgeChunk


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def _alembic_config() -> Config:
    config = Config(str(REPOSITORY_ROOT / "alembic.ini"))
    config.set_main_option(
        "script_location",
        str(REPOSITORY_ROOT / "app" / "db" / "migrations"),
    )
    return config


def test_postgres_knowledge_search_index_matches_the_runtime_query_shape() -> None:
    search_index = next(
        index
        for index in KnowledgeChunk.__table__.indexes
        if index.name == "ix_knowledge_chunks_search_tsv"
    )

    compiled = str(
        CreateIndex(search_index).compile(dialect=postgresql.dialect())
    )

    assert "USING gin" in compiled
    assert "to_tsvector('simple', search_text)" in compiled


def test_knowledge_search_migration_rebuilds_legacy_french_search_text(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    database_path = tmp_path / "knowledge-search-reindex.db"
    database_url = f"sqlite+pysqlite:///{database_path.as_posix()}"
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("ADMIN_TOKEN", "test-admin-token-32-characters-long!")
    get_settings.cache_clear()
    request.addfinalizer(get_settings.cache_clear)

    config = _alembic_config()
    command.upgrade(config, "20260716_0021")

    engine = sa.create_engine(database_url)
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                INSERT INTO clients (
                    client_key, business_name, qualification_questions,
                    operating_hours, template_overrides
                ) VALUES ('preciscan', '3D PreciScan', '[]', '{}', '{}')
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO knowledge_sources (client_id, url, normalized_url)
                VALUES (1, 'https://example.com/services', 'https://example.com/services')
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO knowledge_chunks (
                    client_id, source_id, chunk_index, content, search_text
                ) VALUES (
                    1,
                    1,
                    0,
                    'MÉTROLOGIE industrielle et RÉTRO-ingénierie pour les pièces.',
                    'legacy trologie r tro ing nierie pi ces'
                )
                """
            )
        )
    engine.dispose()

    command.upgrade(config, "head")

    engine = sa.create_engine(database_url)
    with engine.connect() as connection:
        assert connection.scalar(
            sa.text("SELECT search_text FROM knowledge_chunks WHERE id = 1")
        ) == "metrologie industrielle retro ingenierie pieces"
    engine.dispose()


def test_knowledge_schema_migration_scrubs_deduplicates_and_enforces_tenant_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    database_path = tmp_path / "knowledge-schema.db"
    database_url = f"sqlite+pysqlite:///{database_path.as_posix()}"
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("ADMIN_TOKEN", "test-admin-token-32-characters-long!")
    get_settings.cache_clear()
    request.addfinalizer(get_settings.cache_clear)

    config = _alembic_config()
    command.upgrade(config, "20260714_0020")

    engine = sa.create_engine(database_url)
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                INSERT INTO clients (
                    client_key, business_name, qualification_questions,
                    operating_hours, template_overrides, provider_config
                ) VALUES
                    (
                        'tenant-a', 'Tenant A', '[]', '{}', '{}',
                        :tenant_a_provider_config
                    ),
                    ('tenant-b', 'Tenant B', '[]', '{}', '{}', '{}')
                """
            ),
            {
                "tenant_a_provider_config": json.dumps(
                    {
                        "business_profile_context": (
                            "Legacy configured profile for Tenant A. " + "A" * 2_000
                        )
                    }
                )
            },
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO knowledge_sources (
                    client_id, url, normalized_url, title, status,
                    content_hash, extracted_text, text_excerpt,
                    last_crawled_at, created_at, updated_at
                ) VALUES
                    (
                        2,
                        'https://viewer:legacy-password@example.com/services?token=legacy-secret',
                        'https://viewer:legacy-password@example.com/services?token=legacy-secret',
                        'Weak duplicate', 'error', '', '', '',
                        '2026-07-16 12:00:00',
                        '2026-07-16 12:00:00',
                        '2026-07-16 12:00:00'
                    ),
                    (
                        2,
                        'https://example.com/services?signature=second-secret',
                        'https://example.com/services?signature=second-secret',
                        'Precision services', 'ok', 'strong-hash',
                        'Strong precision services and dimensional validation.',
                        'Strong precision services and dimensional validation.',
                        '2026-07-15 12:00:00',
                        '2026-07-15 12:00:00',
                        '2026-07-15 12:00:00'
                    ),
                    (
                        2,
                        'https://example.com/about?token=about-secret',
                        'https://example.com/about?token=about-secret',
                        'About the company', 'stale', 'about-hash',
                        'Legacy company expertise and service area.',
                        'Legacy company expertise and service area.',
                        '2026-07-14 12:00:00',
                        '2026-07-14 12:00:00',
                        '2026-07-14 12:00:00'
                    )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                    INSERT INTO knowledge_chunks (
                        client_id, source_id, chunk_index, content
                    ) VALUES
                        (2, 1, 0, 'delete the weaker duplicate chunk'),
                        (2, 2, 0, 'preserve the strongest source chunk'),
                        (1, 3, 0, 'delete ambiguous cross-tenant content')
                """
            )
        )
    engine.dispose()

    command.upgrade(config, "head")

    engine = sa.create_engine(database_url)
    inspector = sa.inspect(engine)
    client_columns = {column["name"] for column in inspector.get_columns("clients")}
    source_columns = {
        column["name"] for column in inspector.get_columns("knowledge_sources")
    }
    source_unique_constraints = {
        constraint["name"]: constraint["column_names"]
        for constraint in inspector.get_unique_constraints("knowledge_sources")
    }
    chunk_foreign_keys = {
        constraint["name"]: constraint
        for constraint in inspector.get_foreign_keys("knowledge_chunks")
    }

    assert "knowledge_profile_context" in client_columns
    assert {"final_url", "structured_data", "last_success_at"}.issubset(
        source_columns
    )
    assert source_unique_constraints["uq_knowledge_sources_client_id_id"] == [
        "client_id",
        "id",
    ]
    composite_foreign_key = chunk_foreign_keys["fk_knowledge_chunks_client_source"]
    assert composite_foreign_key["constrained_columns"] == ["client_id", "source_id"]
    assert composite_foreign_key["referred_columns"] == ["client_id", "id"]

    with engine.connect() as connection:
        source_rows = connection.execute(
            sa.text(
                """
                    SELECT id, client_id, url, normalized_url, final_url,
                           status, last_success_at
                    FROM knowledge_sources
                    ORDER BY id
                """
            )
        ).all()
        assert source_rows == [
            (
                2,
                2,
                "https://example.com/services",
                "https://example.com/services",
                "https://example.com/services",
                "ok",
                "2026-07-15 12:00:00",
            ),
            (
                3,
                2,
                "https://example.com/about",
                "https://example.com/about",
                "https://example.com/about",
                "stale",
                "2026-07-14 12:00:00",
            ),
        ]
        serialized_sources = repr(source_rows)
        for secret in (
            "legacy-password",
            "legacy-secret",
            "second-secret",
            "about-secret",
        ):
            assert secret not in serialized_sources

        assert connection.execute(
            sa.text(
                """
                    SELECT source_id, client_id, content
                    FROM knowledge_chunks
                    ORDER BY id
                """
            )
        ).all() == [(2, 2, "preserve the strongest source chunk")]

        profiles = dict(
            connection.execute(
                sa.text(
                    """
                    SELECT id, knowledge_profile_context
                    FROM clients
                    ORDER BY id
                    """
                )
            ).all()
        )
        assert profiles[1].startswith("Legacy configured profile for Tenant A.")
        assert len(profiles[1]) == 1_800
        assert "Strong precision services and dimensional validation." in profiles[2]
        assert "Legacy company expertise and service area." not in profiles[2]
        assert "Weak duplicate" not in profiles[2]
        assert len(profiles[2]) <= 1_800
    engine.dispose()

    enforcing_engine = sa.create_engine(database_url)

    @sa.event.listens_for(enforcing_engine, "connect")
    def _enable_sqlite_foreign_keys(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    with pytest.raises(sa.exc.IntegrityError):
        with enforcing_engine.begin() as connection:
            connection.execute(
                sa.text(
                    """
                    INSERT INTO knowledge_chunks (
                        client_id, source_id, chunk_index, content
                    ) VALUES (1, 2, 1, 'must be rejected')
                    """
                )
            )
    enforcing_engine.dispose()

    command.downgrade(config, "20260714_0020")

    engine = sa.create_engine(database_url)
    inspector = sa.inspect(engine)
    client_columns = {column["name"] for column in inspector.get_columns("clients")}
    source_columns = {
        column["name"] for column in inspector.get_columns("knowledge_sources")
    }
    assert "knowledge_profile_context" not in client_columns
    assert not {"final_url", "structured_data", "last_success_at"}.intersection(
        source_columns
    )
    with engine.connect() as connection:
        assert connection.scalars(
            sa.text("SELECT content FROM knowledge_chunks ORDER BY id")
        ).all() == ["preserve the strongest source chunk"]
    engine.dispose()
