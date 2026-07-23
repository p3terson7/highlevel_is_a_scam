"""add PostgreSQL full-text index for website knowledge

Revision ID: 20260716_0022
Revises: 20260716_0021
Create Date: 2026-07-16
"""

import re
import unicodedata

from alembic import op
import sqlalchemy as sa


revision = "20260716_0022"
down_revision = "20260716_0021"
branch_labels = None
depends_on = None


_INDEX_NAME = "ix_knowledge_chunks_search_tsv"
_REINDEX_BATCH_SIZE = 500
# Historical copy of the v2 application tokenizer. Migrations must remain
# reproducible if the runtime tokenizer changes in a later release.
_STOPWORDS = {
    "a",
    "about",
    "after",
    "all",
    "also",
    "am",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "can",
    "do",
    "for",
    "from",
    "have",
    "how",
    "i",
    "in",
    "is",
    "it",
    "me",
    "my",
    "need",
    "of",
    "on",
    "or",
    "our",
    "that",
    "the",
    "this",
    "to",
    "we",
    "what",
    "when",
    "with",
    "you",
    "your",
    "alors",
    "au",
    "aux",
    "avec",
    "ce",
    "ces",
    "comment",
    "dans",
    "de",
    "des",
    "du",
    "elle",
    "en",
    "est",
    "et",
    "il",
    "je",
    "la",
    "le",
    "les",
    "mais",
    "nous",
    "ou",
    "par",
    "pas",
    "pour",
    "que",
    "qui",
    "quoi",
    "se",
    "sur",
    "un",
    "une",
    "vous",
}


def _normalize_search_text(value: object) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or "")).casefold()
    folded = "".join(
        character
        for character in normalized
        if not unicodedata.combining(character)
    )
    tokens = re.findall(r"[^\W_]+", folded, flags=re.UNICODE)
    return " ".join(
        token for token in tokens if len(token) >= 2 and token not in _STOPWORDS
    )


def _rebuild_existing_search_text() -> None:
    bind = op.get_bind()
    chunks = sa.table(
        "knowledge_chunks",
        sa.column("id", sa.Integer()),
        sa.column("content", sa.Text()),
        sa.column("search_text", sa.Text()),
    )
    update_statement = (
        chunks.update()
        .where(chunks.c.id == sa.bindparam("chunk_row_id"))
        .values(search_text=sa.bindparam("normalized_search_text"))
    )
    last_id: int | None = None
    while True:
        batch_query = sa.select(chunks.c.id, chunks.c.content).order_by(
            chunks.c.id.asc()
        )
        if last_id is not None:
            batch_query = batch_query.where(chunks.c.id > last_id)
        rows = bind.execute(batch_query.limit(_REINDEX_BATCH_SIZE)).mappings().all()
        if not rows:
            return
        bind.execute(
            update_statement,
            [
                {
                    "chunk_row_id": int(row["id"]),
                    "normalized_search_text": _normalize_search_text(row["content"]),
                }
                for row in rows
            ],
        )
        last_id = int(rows[-1]["id"])


def upgrade() -> None:
    _rebuild_existing_search_text()
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    op.execute(
        """
        CREATE INDEX ix_knowledge_chunks_search_tsv
        ON knowledge_chunks
        USING gin (to_tsvector('simple', search_text))
        """
    )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    op.drop_index(_INDEX_NAME, table_name="knowledge_chunks")
