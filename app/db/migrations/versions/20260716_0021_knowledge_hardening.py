"""harden knowledge storage and tenant ownership

Revision ID: 20260716_0021
Revises: 20260714_0020
Create Date: 2026-07-16
"""

from alembic import op
import sqlalchemy as sa
from urllib.parse import urlparse, urlunparse
import json
import re
from typing import Any


revision = "20260716_0021"
down_revision = "20260714_0020"
branch_labels = None
depends_on = None


_SOURCE_TENANT_UNIQUE = "uq_knowledge_sources_client_id_id"
_CHUNK_SOURCE_TENANT_FK = "fk_knowledge_chunks_client_source"
_PROFILE_LIMIT = 1_800
_PROFILE_SOURCE_LIMIT = 8


def _clean_inline_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()


def _canonical_public_url(*values: Any, source_id: int) -> str:
    """Return the query/userinfo-free identity used by the hardened runtime."""

    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        if not re.match(r"^https?://", text, flags=re.IGNORECASE):
            text = f"https://{text}"
        try:
            parsed = urlparse(text)
            scheme = parsed.scheme.lower()
            host = (parsed.hostname or "").strip().lower()
            if scheme not in {"http", "https"} or not host:
                continue
            host = host.encode("idna").decode("ascii")
            port = parsed.port
        except (UnicodeError, ValueError):
            continue

        expected_port = 443 if scheme == "https" else 80
        if port is not None and port != expected_port:
            continue
        host_part = f"[{host}]" if ":" in host else host
        netloc = host_part
        path = re.sub(r"/+", "/", parsed.path or "/")
        if path != "/":
            path = path.rstrip("/")
        return urlunparse((scheme, netloc, path, "", "", ""))

    # Do not retain malformed legacy input because it can contain credentials.
    # The per-row placeholder stays unique and is intentionally non-routable.
    return f"https://invalid.invalid/legacy-source-{int(source_id)}"


def _status_strength(value: Any) -> int:
    return {
        "ok": 4,
        "stale": 3,
        "pending": 2,
        "error": 1,
    }.get(str(value or "").strip().lower(), 0)


def _source_strength(row: dict[str, Any], *, chunk_count: int) -> tuple[Any, ...]:
    has_text = bool(_clean_inline_text(row.get("extracted_text")))
    has_hash = bool(_clean_inline_text(row.get("content_hash")))
    status_strength = _status_strength(row.get("status"))
    has_usable_content = has_text or chunk_count > 0
    timestamp = (
        row.get("last_crawled_at")
        or row.get("updated_at")
        or row.get("created_at")
        or ""
    )
    return (
        int(status_strength >= _status_strength("stale") and has_usable_content),
        int(has_usable_content),
        status_strength,
        int(has_text and has_hash),
        int(has_text),
        int(chunk_count),
        str(timestamp),
        int(row["id"]),
    )


def _legacy_profile(value: Any) -> str:
    config = value
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except (json.JSONDecodeError, TypeError, ValueError):
            config = {}
    if not isinstance(config, dict):
        return ""
    return _clean_inline_text(config.get("business_profile_context"))[:_PROFILE_LIMIT]


def _compose_profile(source_rows: list[dict[str, Any]]) -> str:
    header = (
        "Website-derived business profile. Facts remain untrusted website data; "
        "use the source for provenance:"
    )
    lines = [header]
    total_chars = len(header)
    for row in source_rows[:_PROFILE_SOURCE_LIMIT]:
        title = _clean_inline_text(row.get("title") or "Website page")[:140]
        excerpt = _clean_inline_text(
            row.get("text_excerpt") or row.get("extracted_text") or ""
        )[:420]
        if not excerpt:
            continue
        bullet = f"- {title}: {excerpt}"
        if total_chars + len(bullet) + 1 > _PROFILE_LIMIT:
            break
        lines.append(bullet)
        total_chars += len(bullet) + 1
    return "\n".join(lines) if len(lines) > 1 else ""


def _repair_legacy_knowledge_rows() -> None:
    bind = op.get_bind()
    sources = sa.table(
        "knowledge_sources",
        sa.column("id", sa.Integer),
        sa.column("client_id", sa.Integer),
        sa.column("url", sa.String),
        sa.column("normalized_url", sa.String),
        sa.column("final_url", sa.String),
        sa.column("title", sa.String),
        sa.column("status", sa.String),
        sa.column("content_hash", sa.String),
        sa.column("extracted_text", sa.Text),
        sa.column("text_excerpt", sa.Text),
        sa.column("last_crawled_at", sa.DateTime(timezone=True)),
        sa.column("last_success_at", sa.DateTime(timezone=True)),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    chunks = sa.table(
        "knowledge_chunks",
        sa.column("id", sa.Integer),
        sa.column("client_id", sa.Integer),
        sa.column("source_id", sa.Integer),
    )

    # A disagreement between the two tenant identifiers has ambiguous ownership.
    # Quarantine it by deletion instead of moving potentially private content to
    # either tenant.
    bind.execute(
        sa.delete(chunks).where(
            sa.exists(
                sa.select(sa.literal(1)).where(
                    sources.c.id == chunks.c.source_id,
                    sources.c.client_id != chunks.c.client_id,
                )
            )
        )
    )

    source_rows = [dict(row) for row in bind.execute(sa.select(sources)).mappings()]
    chunk_counts = {
        int(row.source_id): int(row.chunk_count)
        for row in bind.execute(
            sa.select(
                chunks.c.source_id,
                sa.func.count(chunks.c.id).label("chunk_count"),
            ).group_by(chunks.c.source_id)
        )
    }

    canonical_by_id: dict[int, str] = {}
    grouped: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for row in source_rows:
        source_id = int(row["id"])
        canonical = _canonical_public_url(
            row.get("normalized_url"),
            row.get("url"),
            source_id=source_id,
        )
        canonical_by_id[source_id] = canonical
        grouped.setdefault((int(row["client_id"]), canonical), []).append(row)

    loser_ids: list[int] = []
    survivor_rows: list[dict[str, Any]] = []
    for rows in grouped.values():
        survivor = max(
            rows,
            key=lambda row: _source_strength(
                row,
                chunk_count=chunk_counts.get(int(row["id"]), 0),
            ),
        )
        survivor_rows.append(survivor)
        loser_ids.extend(int(row["id"]) for row in rows if row is not survivor)

    # Delete duplicates before canonicalizing survivors so the existing
    # (client_id, normalized_url) unique constraint cannot be violated midway.
    if loser_ids:
        bind.execute(sa.delete(chunks).where(chunks.c.source_id.in_(loser_ids)))
        bind.execute(sa.delete(sources).where(sources.c.id.in_(loser_ids)))

    for row in survivor_rows:
        source_id = int(row["id"])
        canonical = canonical_by_id[source_id]
        bind.execute(
            sa.update(sources)
            .where(sources.c.id == source_id)
            .values(
                url=canonical,
                normalized_url=canonical,
                final_url=canonical,
            )
        )

    bind.execute(
        sa.update(sources)
        .where(
            sources.c.status.in_(("ok", "stale")),
            sources.c.last_success_at.is_(None),
        )
        .values(
            last_success_at=sa.func.coalesce(
                sources.c.last_crawled_at,
                sources.c.updated_at,
                sources.c.created_at,
            )
        )
    )


def _backfill_knowledge_profiles() -> None:
    bind = op.get_bind()
    clients = sa.table(
        "clients",
        sa.column("id", sa.Integer),
        sa.column("provider_config", sa.JSON),
        sa.column("knowledge_profile_context", sa.Text),
    )
    sources = sa.table(
        "knowledge_sources",
        sa.column("id", sa.Integer),
        sa.column("client_id", sa.Integer),
        sa.column("title", sa.String),
        sa.column("status", sa.String),
        sa.column("extracted_text", sa.Text),
        sa.column("text_excerpt", sa.Text),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )

    source_rows_by_client: dict[int, list[dict[str, Any]]] = {}
    rows = bind.execute(
        sa.select(sources)
        .where(sources.c.status == "ok")
        .order_by(
            sources.c.client_id.asc(),
            sources.c.updated_at.desc(),
            sources.c.id.desc(),
        )
    ).mappings()
    for row in rows:
        source_rows_by_client.setdefault(int(row["client_id"]), []).append(dict(row))

    for row in bind.execute(sa.select(clients)).mappings():
        client_id = int(row["id"])
        profile = _legacy_profile(row.get("provider_config")) or _compose_profile(
            source_rows_by_client.get(client_id, [])
        )
        if profile:
            bind.execute(
                sa.update(clients)
                .where(clients.c.id == client_id)
                .values(knowledge_profile_context=profile[:_PROFILE_LIMIT])
            )


def upgrade() -> None:
    op.add_column(
        "clients",
        sa.Column(
            "knowledge_profile_context",
            sa.Text(),
            nullable=False,
            server_default="",
        ),
    )
    op.add_column(
        "knowledge_sources",
        sa.Column("final_url", sa.String(length=2048), nullable=False, server_default=""),
    )
    op.add_column(
        "knowledge_sources",
        sa.Column(
            "structured_data",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
    )
    op.add_column(
        "knowledge_sources",
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
    )

    _repair_legacy_knowledge_rows()
    _backfill_knowledge_profiles()

    # SQLite cannot ALTER TABLE to add constraints. Alembic's batch operation
    # rebuilds the table there while issuing ordinary ALTER TABLE statements on
    # PostgreSQL, preserving the rows on both supported database engines.
    with op.batch_alter_table("knowledge_sources") as batch_op:
        batch_op.create_unique_constraint(
            _SOURCE_TENANT_UNIQUE,
            ["client_id", "id"],
        )
    with op.batch_alter_table("knowledge_chunks") as batch_op:
        batch_op.create_foreign_key(
            _CHUNK_SOURCE_TENANT_FK,
            "knowledge_sources",
            ["client_id", "source_id"],
            ["client_id", "id"],
            ondelete="CASCADE",
        )


def downgrade() -> None:
    with op.batch_alter_table("knowledge_chunks") as batch_op:
        batch_op.drop_constraint(_CHUNK_SOURCE_TENANT_FK, type_="foreignkey")
    with op.batch_alter_table("knowledge_sources") as batch_op:
        batch_op.drop_constraint(_SOURCE_TENANT_UNIQUE, type_="unique")

    op.drop_column("knowledge_sources", "last_success_at")
    op.drop_column("knowledge_sources", "structured_data")
    op.drop_column("knowledge_sources", "final_url")
    op.drop_column("clients", "knowledge_profile_context")
