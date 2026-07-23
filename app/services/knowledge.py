from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import socket
import unicodedata
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import httpx
from sqlalchemy import Table, and_, delete, func, inspect, literal_column, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.models import Client, KnowledgeChunk, KnowledgeSource

_DEFAULT_TIMEOUT_SECONDS = 15
_MAX_HTML_BYTES = 1_500_000
_MAX_URLS_PER_INGEST = 12
_MAX_SOURCES_PER_CLIENT = 48
_MAX_REDIRECTS = 5
_MAX_URL_LENGTH = 2048
_REDIRECT_STATUS_CODES = {301, 302, 303, 307, 308}
_CHUNK_TARGET_CHARS = 900
_CHUNK_OVERLAP_SENTENCES = 1
_KNOWLEDGE_INDEX_VERSION = 2
_PROFILE_SOURCE_LIMIT = 8
_STALE_SOURCE_MAX_AGE_DAYS = 30
_MAX_CONTEXT_SOURCE_URL_CHARS = 240
_SCHEMA_SHAPE_CACHE_KEY = "knowledge_schema_shape"
_DISCOVERY_PATH_TERMS = {
    "about",
    "a-propos",
    "application",
    "capabil",
    "expertise",
    "faq",
    "industries",
    "process",
    "product",
    "services",
    "solutions",
    "secteurs",
    "scan",
    "technology",
    "metrolog",
    "ingenier",
}
_DISCOVERY_EXCLUDED_PATH_TERMS = {
    "account",
    "admin",
    "cart",
    "checkout",
    "login",
    "logout",
    "privacy",
    "terms",
}
_BOILERPLATE_PATTERNS = (
    re.compile(
        r"^(home|about|services|industries|portfolio|blog|contact)$", re.IGNORECASE
    ),
    re.compile(
        r"\b(subscribe to our newsletter|be the first to know|powered by|all rights reserved|privacy policy|terms\s*&\s*conditions)\b",
        re.IGNORECASE,
    ),
)
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
    # Common French conversational words. Domain terms are deliberately absent.
    "alors",
    "au",
    "aux",
    "avez",
    "avons",
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
    "fait",
    "il",
    "je",
    "la",
    "le",
    "les",
    "mais",
    "moi",
    "notre",
    "nous",
    "ou",
    "par",
    "parlez",
    "pas",
    "plus",
    "pour",
    "que",
    "qui",
    "quoi",
    "se",
    "serait",
    "sur",
    "un",
    "une",
    "votre",
    "vous",
}

# Small, explicit bilingual/domain expansions keep lexical retrieval useful for
# mixed-language leads without adding a model call or a heavyweight embedding
# dependency.  These are concepts, not arbitrary translations: expanding a term
# only within its group should still point at the same kind of source material.
_QUERY_EXPANSION_GROUPS: tuple[tuple[str, ...], ...] = (
    ("engine", "motor", "moteur"),
    ("block", "bloc"),
    ("project", "projet", "realisation", "realisations", "mandat"),
    ("scan", "scanning", "scanner", "numerisation", "numeriser"),
    ("engineering", "ingenierie"),
    ("reverse", "retro"),
    ("inspection", "controle"),
    ("drawing", "dessin", "plan"),
    ("part", "piece"),
    ("gear", "engrenage"),
    ("foundry", "fonderie"),
)
_QUERY_EXPANSIONS = {
    token: tuple(candidate for candidate in group if candidate != token)
    for group in _QUERY_EXPANSION_GROUPS
    for token in group
}
_CURRENT_QUERY_WEIGHT = 8.0
_CURRENT_EXPANSION_WEIGHT = 6.0
_HISTORY_QUERY_WEIGHT = 1.0
_HISTORY_EXPANSION_WEIGHT = 0.75
_FORM_QUERY_WEIGHT = 0.35
_FORM_EXPANSION_WEIGHT = 0.25
_MAX_RETRIEVAL_CANDIDATE_TOKENS = 12


@dataclass(frozen=True)
class FetchResult:
    url: str
    status_code: int
    html: str


@dataclass(frozen=True)
class KnowledgeRetrievalQuery:
    """Weighted retrieval inputs.

    ``current`` is the lead's current turn. ``history`` only disambiguates a
    short follow-up, while ``form`` is weak background evidence.  Plain strings
    remain supported by :func:`retrieve_knowledge_snippets`; for the multiline
    query emitted by Agent V3, the first line is treated as current and the
    remaining lines as progressively weaker supporting context.
    """

    current: str
    history: tuple[str, ...] = ()
    form: tuple[str, ...] = ()


@dataclass(frozen=True)
class KnowledgeContextSource:
    """Audit-safe provenance for one source actually included in context."""

    source_id: int
    title: str
    score: float
    status: str


@dataclass(frozen=True)
class KnowledgeContextResult:
    text: str
    sources: tuple[KnowledgeContextSource, ...] = ()


@dataclass(frozen=True)
class _WeightedRetrievalQuery:
    current_weights: tuple[tuple[str, float], ...]
    history_weights: tuple[tuple[str, float], ...]
    form_weights: tuple[tuple[str, float], ...]
    candidate_tokens: tuple[str, ...]


@dataclass(frozen=True)
class ExtractedPage:
    url: str
    normalized_url: str
    title: str
    text: str
    text_excerpt: str
    content_hash: str
    chunks: list[str]
    links: list[str]
    structured_data: dict[str, Any]


@dataclass(frozen=True)
class _PageOutcome:
    fetch_url: str
    normalized_url: str
    extracted: ExtractedPage | None = None
    error_message: str = ""


class KnowledgeIngestionError(RuntimeError):
    pass


class TransientKnowledgeIngestionError(KnowledgeIngestionError):
    """A network/provider failure that should reach the RQ retry boundary."""

    pass


class KnowledgeIngestionService:
    def __init__(
        self,
        timeout_seconds: int = _DEFAULT_TIMEOUT_SECONDS,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._timeout_seconds = timeout_seconds
        self._transport = transport
        self._active_clients: dict[str, httpx.Client] | None = None
        self._active_client_stack: ExitStack | None = None

    def ingest_urls(
        self,
        *,
        db: Session,
        client_id: int,
        urls: list[str],
        replace: bool = True,
    ) -> dict[str, Any]:
        requested_urls = _dedupe_urls(urls)[:_MAX_URLS_PER_INGEST]
        if not requested_urls:
            # An empty replace request must never be interpreted as "delete every
            # source". The API rejects this already, but the service is also used
            # by workers and tests and therefore owns the destructive-operation
            # invariant as a second line of defence.
            raise KnowledgeIngestionError("At least one valid URL is required")
        outcomes: list[_PageOutcome] = []
        pending: list[tuple[str, str, int, str]] = [
            (fetch_url, normalized_url, 0, (urlparse(fetch_url).hostname or "").lower())
            for fetch_url, normalized_url in requested_urls
        ]
        seen_urls = {normalized_url for _, normalized_url in requested_urls}

        # Complete all remote work before opening a database transaction. A slow
        # website must not hold an application DB connection or a stale Client row.
        with self._shared_http_clients():
            cursor = 0
            while cursor < len(pending) and len(outcomes) < _MAX_URLS_PER_INGEST:
                fetch_url, normalized_url, depth, root_host = pending[cursor]
                cursor += 1
                try:
                    fetched = self._fetch_url(fetch_url)
                    extracted = extract_page_text(
                        fetched.html, url=fetched.url or fetch_url
                    )
                    outcomes.append(
                        _PageOutcome(
                            fetch_url=fetch_url,
                            normalized_url=normalized_url,
                            extracted=extracted,
                        )
                    )
                    if depth == 0 and len(seen_urls) < _MAX_URLS_PER_INGEST:
                        for discovered_url in _prioritized_discovered_urls(
                            extracted.links,
                            root_host=root_host,
                            root_scheme=urlparse(fetch_url).scheme.lower(),
                        ):
                            discovered_identity = public_source_url(discovered_url)
                            if (
                                not discovered_identity
                                or discovered_identity in seen_urls
                            ):
                                continue
                            seen_urls.add(discovered_identity)
                            pending.append(
                                (discovered_url, discovered_identity, 1, root_host)
                            )
                            if len(seen_urls) >= _MAX_URLS_PER_INGEST:
                                break
                except TransientKnowledgeIngestionError:
                    if depth > 0:
                        # Auto-discovered pages are optional. A flaky secondary
                        # link must not prevent a valid owner-requested root page
                        # from being stored after every retry.
                        continue
                    # Let RQ apply its configured retry policy. No database state
                    # has been touched, so the last-known-good snapshot remains live.
                    raise
                except KnowledgeIngestionError as exc:
                    if depth > 0:
                        # Broken optional links are ignored rather than persisted
                        # as owner-managed error sources or blocking replacement.
                        continue
                    outcomes.append(
                        _PageOutcome(
                            fetch_url=fetch_url,
                            normalized_url=normalized_url,
                            error_message=_safe_ingestion_error(exc),
                        )
                    )

        now = datetime.now(timezone.utc)
        existing_sources = {
            public_source_url(source.normalized_url): source
            for source in db.scalars(
                select(KnowledgeSource).where(KnowledgeSource.client_id == client_id)
            ).all()
        }

        pages: list[dict[str, Any]] = []
        successful_refresh = all(outcome.extracted is not None for outcome in outcomes)
        replacing_complete_snapshot = bool(replace and successful_refresh)
        remaining_source_capacity = max(
            0,
            _MAX_SOURCES_PER_CLIENT - len(existing_sources),
        )
        keep = {outcome.normalized_url for outcome in outcomes}
        for outcome in outcomes:
            source = existing_sources.get(outcome.normalized_url)
            if source is None:
                if not replacing_complete_snapshot and remaining_source_capacity <= 0:
                    pages.append(
                        {
                            "url": public_source_url(outcome.normalized_url),
                            "normalized_url": public_source_url(outcome.normalized_url),
                            "final_url": "",
                            "title": "",
                            "status": "error",
                            "text_excerpt": "",
                            "structured_data": {},
                            "error_message": (
                                "Website knowledge source limit reached; replace or clear "
                                "existing sources before adding more."
                            ),
                            "last_success_at": None,
                            "chunk_count": 0,
                            "chunks": [],
                        }
                    )
                    continue
                source = KnowledgeSource(
                    client_id=client_id,
                    url=outcome.normalized_url,
                    normalized_url=outcome.normalized_url,
                )
                db.add(source)
                db.flush()
                if not replacing_complete_snapshot:
                    remaining_source_capacity -= 1

            source.url = outcome.normalized_url
            source.normalized_url = outcome.normalized_url
            source.last_crawled_at = now
            extracted = outcome.extracted
            if extracted is not None:
                previous_structured = (
                    source.structured_data
                    if isinstance(source.structured_data, dict)
                    else {}
                )
                content_changed = (
                    source.content_hash != extracted.content_hash
                    or previous_structured.get("_index_version")
                    != _KNOWLEDGE_INDEX_VERSION
                )
                source.status = "ok"
                source.title = extracted.title
                source.content_hash = extracted.content_hash
                source.extracted_text = extracted.text
                source.text_excerpt = extracted.text_excerpt
                source.error_message = ""
                source.final_url = public_source_url(extracted.url)
                source.structured_data = {
                    **extracted.structured_data,
                    "_index_version": _KNOWLEDGE_INDEX_VERSION,
                }
                source.last_crawled_at = now
                source.last_success_at = now
                db.add(source)
                db.flush()
                if content_changed:
                    db.execute(
                        delete(KnowledgeChunk).where(
                            KnowledgeChunk.source_id == source.id
                        )
                    )
                    for index, chunk in enumerate(extracted.chunks):
                        db.add(
                            KnowledgeChunk(
                                client_id=client_id,
                                source_id=source.id,
                                chunk_index=index,
                                content=chunk,
                                search_text=_normalize_search_text(chunk),
                            )
                        )
                pages.append(
                    _source_extraction_payload(source=source, chunks=extracted.chunks)
                )
            else:
                had_working_content = bool(
                    source.content_hash and source.extracted_text
                )
                source.status = "stale" if had_working_content else "error"
                source.error_message = outcome.error_message
                source.last_crawled_at = now
                db.add(source)
                db.flush()
                existing_chunks = db.scalars(
                    select(KnowledgeChunk)
                    .where(
                        KnowledgeChunk.client_id == client_id,
                        KnowledgeChunk.source_id == source.id,
                    )
                    .order_by(KnowledgeChunk.chunk_index.asc())
                ).all()
                pages.append(
                    _source_extraction_payload(
                        source=source,
                        chunks=[chunk.content for chunk in existing_chunks],
                    )
                )

        # `replace` means replace the active snapshot, not delete good data before
        # its replacement exists. A partial/failed refresh therefore retains old
        # omitted sources until a complete run succeeds.
        if replace and successful_refresh:
            for normalized, source in list(existing_sources.items()):
                if normalized not in keep:
                    db.delete(source)

        db.flush()
        business_profile_context = refresh_business_profile_context(
            db, client_id=client_id
        )
        return {
            "pages": pages,
            "total_pages": len(pages),
            "total_chunks": sum(int(page.get("chunk_count") or 0) for page in pages),
            "business_profile_context": business_profile_context,
        }

    @contextmanager
    def _shared_http_clients(self) -> Iterator[None]:
        if self._active_clients is not None:
            yield
            return
        with ExitStack() as stack:
            self._active_clients = {}
            self._active_client_stack = stack
            try:
                yield
            finally:
                self._active_clients = None
                self._active_client_stack = None

    def _client_for_url(self, url: str) -> httpx.Client:
        host = (urlparse(url).hostname or "").strip().lower()
        if self._active_clients is None or self._active_client_stack is None:
            raise RuntimeError("Knowledge HTTP client pool is not active")
        client = self._active_clients.get(host)
        if client is None:
            # Each logical hostname gets its own cookie jar and connection pool.
            # Requests connect to pinned IPs, so sharing one client across hosts
            # could otherwise reuse cookies or a TLS connection by IP origin.
            client = self._active_client_stack.enter_context(
                httpx.Client(**self._http_client_kwargs())
            )
            self._active_clients[host] = client
        return client

    def _http_client_kwargs(self) -> dict[str, Any]:
        client_kwargs: dict[str, Any] = {
            "timeout": self._timeout_seconds,
            "follow_redirects": False,
            "headers": {"User-Agent": "LeadOpsKnowledgeBot/1.0"},
            # Environment proxy settings can otherwise bypass the public-address
            # checks and let an untrusted URL reach the proxy's private network.
            "trust_env": False,
        }
        if self._transport is not None:
            client_kwargs["transport"] = self._transport
        return client_kwargs

    def _fetch_url(self, url: str) -> FetchResult:
        current_url = normalize_source_url(url)
        initial_url = current_url

        def fetch_with_pool() -> FetchResult:
            nonlocal current_url
            for redirect_count in range(_MAX_REDIRECTS + 1):
                addresses = _assert_fetchable_public_url(current_url)
                client = self._client_for_url(current_url)
                request_url, request_headers, request_extensions = (
                    _pinned_request_target(
                        current_url,
                        addresses[0],
                    )
                )
                try:
                    with client.stream(
                        "GET",
                        request_url,
                        headers=request_headers,
                        extensions=request_extensions,
                    ) as response:
                        if response.status_code in _REDIRECT_STATUS_CODES:
                            if redirect_count >= _MAX_REDIRECTS:
                                raise KnowledgeIngestionError(
                                    "Too many redirects while fetching website knowledge"
                                )
                            location = response.headers.get("location", "").strip()
                            if not location:
                                raise KnowledgeIngestionError(
                                    "Redirect response did not include a Location header"
                                )
                            redirected_url = normalize_source_url(
                                urljoin(current_url, location)
                            )
                            if not _safe_redirect(
                                initial_url=initial_url,
                                current_url=current_url,
                                redirected_url=redirected_url,
                            ):
                                raise KnowledgeIngestionError(
                                    "Website redirects must remain on the same site and may not downgrade HTTPS"
                                )
                            current_url = redirected_url
                            continue

                        if (
                            response.status_code in {408, 425, 429}
                            or response.status_code >= 500
                        ):
                            raise TransientKnowledgeIngestionError(
                                f"Temporary website fetch failure (HTTP {response.status_code})"
                            )
                        if response.status_code >= 400:
                            raise KnowledgeIngestionError(
                                f"Fetch failed with HTTP {response.status_code}"
                            )

                        content_type = response.headers.get("content-type", "").lower()
                        if (
                            "text/html" not in content_type
                            and "application/xhtml" not in content_type
                            and content_type
                        ):
                            raise KnowledgeIngestionError(
                                f"Unsupported content type: {content_type[:160]}"
                            )

                        content_length = response.headers.get(
                            "content-length", ""
                        ).strip()
                        if content_length:
                            try:
                                if int(content_length) > _MAX_HTML_BYTES:
                                    raise KnowledgeIngestionError(
                                        "Website content exceeds the ingestion size limit"
                                    )
                            except ValueError:
                                pass

                        chunks: list[bytes] = []
                        total_bytes = 0
                        for chunk in response.iter_bytes():
                            total_bytes += len(chunk)
                            if total_bytes > _MAX_HTML_BYTES:
                                raise KnowledgeIngestionError(
                                    "Website content exceeds the ingestion size limit"
                                )
                            chunks.append(chunk)
                        raw = b"".join(chunks)
                        return FetchResult(
                            url=current_url,
                            status_code=response.status_code,
                            html=raw.decode(
                                response.encoding or "utf-8", errors="replace"
                            ),
                        )
                except httpx.RequestError as exc:
                    raise TransientKnowledgeIngestionError(
                        "Website could not be reached temporarily"
                    ) from exc

            raise KnowledgeIngestionError("Website fetch did not return a response")

        if self._active_clients is not None:
            return fetch_with_pool()
        with self._shared_http_clients():
            return fetch_with_pool()


def extract_page_text(html: str, *, url: str) -> ExtractedPage:
    normalized_url = normalize_source_url(url)
    parser = _ReadableHTMLParser()
    parser.feed(html or "")
    parser.close()
    title = _clean_inline_text(parser.title)[:512]
    structured_data = _extract_structured_facts(parser.json_ld_payloads)
    metadata_lines: list[str] = []
    if parser.meta_description:
        metadata_lines.append(f"Page description: {parser.meta_description}")
    metadata_lines.extend(_structured_fact_lines(structured_data))
    text = _clean_extracted_lines([*metadata_lines, *parser.lines])
    if not text:
        raise KnowledgeIngestionError("No readable page text found")
    text = text[:120_000]
    chunks = chunk_text(text)
    content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return ExtractedPage(
        url=url,
        normalized_url=normalized_url,
        title=title,
        text=text,
        text_excerpt=summarize_excerpt(text, limit=900),
        content_hash=content_hash,
        chunks=chunks,
        links=_normalize_page_links(parser.links, base_url=url),
        structured_data=structured_data,
    )


def _required_tables_available(db: Session, *tables: Table) -> bool:
    """Preflight optional knowledge reads without risking the caller transaction.

    A query against an unmigrated table aborts a PostgreSQL transaction. Using a
    ``Session.begin_nested()`` guard is not safe either: SQLAlchemy flushes every
    pending caller-owned change before opening the savepoint, even when autoflush
    is disabled. That can also take a SQLite write lock before the outbound outbox
    opens its independent transaction.

    Inspector catalog reads do not reference the optional relations themselves,
    and ``db.no_autoflush`` protects the subsequent read queries. Cache the result
    for this short-lived session so an agent turn pays the schema check only once.
    """

    cache = db.info.setdefault(_SCHEMA_SHAPE_CACHE_KEY, {})
    inspector = None
    try:
        for table in tables:
            cache_key = (table.schema or "", table.name)
            available = cache.get(cache_key)
            if available is None:
                if inspector is None:
                    inspector = inspect(db.connection())
                available = inspector.has_table(table.name, schema=table.schema)
                if available:
                    existing_columns = {
                        str(column["name"])
                        for column in inspector.get_columns(
                            table.name,
                            schema=table.schema,
                        )
                    }
                    available = all(
                        column.name in existing_columns for column in table.columns
                    )
                cache[cache_key] = available
            if not available:
                return False
    except SQLAlchemyError:
        return False
    return True


def knowledge_payload(db: Session, *, client_id: int) -> dict[str, Any]:
    if not _required_tables_available(
        db,
        KnowledgeSource.__table__,
        KnowledgeChunk.__table__,
    ):
        return {
            "sources": [],
            "total_sources": 0,
            "total_chunks": 0,
            "status": "unavailable",
            "error": "Knowledge tables are not migrated yet. Run alembic upgrade head.",
        }
    with db.no_autoflush:
        sources = db.scalars(
            select(KnowledgeSource)
            .where(KnowledgeSource.client_id == client_id)
            .order_by(KnowledgeSource.updated_at.desc(), KnowledgeSource.id.desc())
        ).all()
        source_ids = [source.id for source in sources]
        chunks_by_source: dict[int, list[KnowledgeChunk]] = {
            source.id: [] for source in sources
        }
        chunk_counts_by_source: dict[int, int] = {source.id: 0 for source in sources}
        if source_ids:
            count_rows = db.execute(
                select(
                    KnowledgeChunk.source_id,
                    func.count(KnowledgeChunk.id),
                )
                .where(
                    KnowledgeChunk.client_id == client_id,
                    KnowledgeChunk.source_id.in_(source_ids),
                )
                .group_by(KnowledgeChunk.source_id)
            ).all()
            for source_id, chunk_count in count_rows:
                chunk_counts_by_source[int(source_id)] = int(chunk_count)
            chunks = db.scalars(
                select(KnowledgeChunk)
                .where(
                    KnowledgeChunk.client_id == client_id,
                    KnowledgeChunk.source_id.in_(source_ids),
                    KnowledgeChunk.chunk_index < 8,
                )
                .order_by(
                    KnowledgeChunk.source_id.asc(),
                    KnowledgeChunk.chunk_index.asc(),
                )
            ).all()
            for chunk in chunks:
                chunks_by_source.setdefault(chunk.source_id, []).append(chunk)

    business_profile_context = build_business_profile_context(db, client_id=client_id)
    return {
        "sources": [
            _source_payload(
                source=source,
                chunks=chunks_by_source.get(source.id, []),
                chunk_count=chunk_counts_by_source.get(source.id, 0),
            )
            for source in sources
        ],
        "total_sources": len(sources),
        "total_chunks": sum(chunk_counts_by_source.values()),
        "business_profile_context": business_profile_context,
    }


def retrieve_knowledge_snippets(
    db: Session,
    *,
    client_id: int,
    query: str | KnowledgeRetrievalQuery,
    limit: int = 4,
) -> list[dict[str, Any]]:
    weighted_query = _prepare_weighted_query(query)
    if not weighted_query.candidate_tokens:
        return []
    candidate_tokens = list(
        weighted_query.candidate_tokens[:_MAX_RETRIEVAL_CANDIDATE_TOKENS]
    )
    if not _required_tables_available(
        db,
        KnowledgeSource.__table__,
        KnowledgeChunk.__table__,
    ):
        return []
    with db.no_autoflush:
        base_statement = (
            select(KnowledgeChunk, KnowledgeSource)
            .join(KnowledgeSource, KnowledgeSource.id == KnowledgeChunk.source_id)
            .where(
                KnowledgeChunk.client_id == client_id,
                KnowledgeSource.client_id == client_id,
                _active_knowledge_source_condition(),
            )
        )
        bind = db.get_bind()
        source_location = func.coalesce(
            KnowledgeSource.final_url,
            KnowledgeSource.normalized_url,
            KnowledgeSource.url,
            "",
        )
        metadata_conditions = [
            condition
            for token in _candidate_search_fragments(candidate_tokens)
            for condition in (
                KnowledgeSource.title.ilike(
                    f"%{_escape_like(token)}%",
                    escape="\\",
                ),
                source_location.ilike(
                    f"%{_escape_like(token)}%",
                    escape="\\",
                ),
            )
        ]
        if bind is not None and bind.dialect.name == "postgresql":
            search_config = literal_column("'simple'")
            search_vector = func.to_tsvector(search_config, KnowledgeChunk.search_text)
            search_terms = _fts_query_terms(candidate_tokens)
            search_query = func.to_tsquery(search_config, " | ".join(search_terms))
            rank = func.ts_rank_cd(search_vector, search_query)
            body_statement = (
                base_statement.where(search_vector.op("@@")(search_query))
                .order_by(
                    rank.desc(),
                    KnowledgeSource.id.asc(),
                    KnowledgeChunk.chunk_index.asc(),
                )
                .limit(500)
            )
        else:
            candidate_fragments = _candidate_search_fragments(candidate_tokens)
            body_conditions = [
                KnowledgeChunk.search_text.ilike(
                    f"%{_escape_like(token)}%",
                    escape="\\",
                )
                for token in candidate_fragments
            ]
            body_statement = base_statement.where(or_(*body_conditions)).order_by(
                KnowledgeSource.id.asc(),
                KnowledgeChunk.chunk_index.asc(),
            )
        body_rows = db.execute(body_statement).all()

        # Title/path matches must not fan out to every chunk before the
        # PostgreSQL body-search cap. Fetch one representative chunk per
        # matching source separately, then let the common Python scorer rerank
        # body and metadata candidates together. This also keeps SQLite and
        # PostgreSQL candidate semantics aligned.
        metadata_source_ids = db.scalars(
            select(KnowledgeSource.id)
            .where(
                KnowledgeSource.client_id == client_id,
                _active_knowledge_source_condition(),
                or_(*metadata_conditions),
            )
            .order_by(KnowledgeSource.id.asc())
            .limit(_MAX_SOURCES_PER_CLIENT)
        ).all()
        metadata_rows: list[tuple[KnowledgeChunk, KnowledgeSource]] = []
        if metadata_source_ids:
            representative_chunks = (
                select(
                    KnowledgeChunk.source_id.label("source_id"),
                    func.min(KnowledgeChunk.chunk_index).label("chunk_index"),
                )
                .where(
                    KnowledgeChunk.client_id == client_id,
                    KnowledgeChunk.source_id.in_(metadata_source_ids),
                )
                .group_by(KnowledgeChunk.source_id)
                .subquery()
            )
            metadata_rows = db.execute(
                select(KnowledgeChunk, KnowledgeSource)
                .join(
                    KnowledgeSource,
                    KnowledgeSource.id == KnowledgeChunk.source_id,
                )
                .join(
                    representative_chunks,
                    and_(
                        representative_chunks.c.source_id
                        == KnowledgeChunk.source_id,
                        representative_chunks.c.chunk_index
                        == KnowledgeChunk.chunk_index,
                    ),
                )
                .where(
                    KnowledgeChunk.client_id == client_id,
                    KnowledgeSource.client_id == client_id,
                    _active_knowledge_source_condition(),
                )
                .order_by(KnowledgeSource.id.asc())
            ).all()

        rows: list[tuple[KnowledgeChunk, KnowledgeSource]] = []
        seen_chunk_ids: set[int] = set()
        for chunk, source in [*body_rows, *metadata_rows]:
            if chunk.id in seen_chunk_ids:
                continue
            seen_chunk_ids.add(chunk.id)
            rows.append((chunk, source))
    scored: list[tuple[float, KnowledgeChunk, KnowledgeSource]] = []
    for chunk, source in rows:
        score = _score_knowledge_candidate(
            chunk=chunk,
            source=source,
            query=weighted_query,
        )
        if score <= 0:
            continue
        scored.append((score, chunk, source))
    scored.sort(key=lambda item: (-item[0], item[1].chunk_index))
    requested_limit = max(1, min(int(limit), 12))
    selected: list[tuple[float, KnowledgeChunk, KnowledgeSource]] = []
    selected_chunk_ids: set[int] = set()
    selected_source_ids: set[int] = set()
    # Avoid letting a repetitive page crowd every other source out of context.
    for item in scored:
        _, chunk, source = item
        if source.id in selected_source_ids:
            continue
        selected.append(item)
        selected_chunk_ids.add(chunk.id)
        selected_source_ids.add(source.id)
        if len(selected) >= requested_limit:
            break
    if len(selected) < requested_limit:
        for item in scored:
            _, chunk, _ = item
            if chunk.id in selected_chunk_ids:
                continue
            selected.append(item)
            if len(selected) >= requested_limit:
                break
    return [
        {
            "source_id": source.id,
            "source_url": public_source_url(source.final_url or source.url),
            "source_title": source.title,
            "source_status": source.status,
            "last_success_at": (
                source.last_success_at.isoformat() if source.last_success_at else None
            ),
            "chunk_index": chunk.chunk_index,
            "content": chunk.content,
            "score": round(score, 3),
        }
        for score, chunk, source in selected
    ]


def build_knowledge_context_result(
    db: Session | None,
    *,
    client_id: int,
    query: str | KnowledgeRetrievalQuery,
    limit: int = 4,
) -> KnowledgeContextResult:
    """Build model context and safe trace metadata with a single retrieval."""

    if db is None:
        return KnowledgeContextResult(text="")
    snippets = retrieve_knowledge_snippets(
        db, client_id=client_id, query=query, limit=limit
    )
    if not snippets:
        return KnowledgeContextResult(text="")
    lines: list[str] = []
    sources: list[KnowledgeContextSource] = []
    total_chars = 0
    for snippet in snippets:
        title = str(snippet.get("source_title") or "Website page").strip()
        url = summarize_excerpt(
            str(snippet.get("source_url") or "").strip(),
            limit=_MAX_CONTEXT_SOURCE_URL_CHARS,
        )
        content = summarize_excerpt(str(snippet.get("content") or ""), limit=700)
        stale_note = ""
        if snippet.get("source_status") == "stale":
            last_success = str(snippet.get("last_success_at") or "unknown date")[:32]
            stale_note = f"; last successful crawl {last_success}"
        block = f"Source: {title} ({url}){stale_note}\n{content}".strip()
        if total_chars + len(block) > 2600:
            continue
        lines.append(block)
        sources.append(
            KnowledgeContextSource(
                source_id=int(snippet.get("source_id") or 0),
                title=summarize_excerpt(title, limit=180),
                score=float(snippet.get("score") or 0.0),
                status=str(snippet.get("source_status") or "")[:24],
            )
        )
        total_chars += len(block)
    return KnowledgeContextResult(
        text="\n\n".join(lines),
        sources=tuple(sources),
    )


def build_knowledge_context(
    db: Session | None,
    *,
    client_id: int,
    query: str | KnowledgeRetrievalQuery,
    limit: int = 4,
) -> str:
    """Backward-compatible text-only wrapper around context retrieval."""

    return build_knowledge_context_result(
        db,
        client_id=client_id,
        query=query,
        limit=limit,
    ).text


def build_business_profile_context(
    db: Session | None, *, client_id: int, fallback: str = ""
) -> str:
    if db is None:
        return fallback.strip()
    if not _required_tables_available(db, Client.__table__):
        return fallback.strip()
    with db.no_autoflush:
        client = db.get(Client, client_id)
        if client is not None:
            stored = _stored_business_profile(client)
            if stored:
                return stored
        if not _required_tables_available(
            db,
            KnowledgeSource.__table__,
            KnowledgeChunk.__table__,
        ):
            return fallback.strip()
        return _compose_business_profile_from_sources(db, client_id=client_id)


def refresh_business_profile_context(db: Session, *, client_id: int) -> str:
    profile = _compose_business_profile_from_sources(db, client_id=client_id)
    client = db.get(Client, client_id)
    if client is not None:
        # Derived crawl output has its own column. Never rewrite the shared
        # provider_config JSON from a background crawl: it also carries encrypted
        # integration settings and may be updated concurrently.
        client.knowledge_profile_context = profile
        db.add(client)
    return profile


def _stored_business_profile(client: Client) -> str:
    stored = getattr(client, "knowledge_profile_context", "")
    return " ".join(str(stored or "").split()).strip()


def _compose_business_profile_from_sources(
    db: Session, *, client_id: int, limit: int = 1800
) -> str:
    sources = db.scalars(
        select(KnowledgeSource)
        .where(
            KnowledgeSource.client_id == client_id,
            # Stale last-known-good content remains available only through
            # query-specific retrieval, where it carries an explicit warning and
            # expires after 30 days. It must not become silent always-on memory.
            KnowledgeSource.status == "ok",
        )
        .order_by(
            KnowledgeSource.last_success_at.desc(),
            KnowledgeSource.id.desc(),
        )
    ).all()
    if not sources:
        return ""
    fallback_source_ids = [
        source.id
        for source in sources
        if not _clean_inline_text(source.text_excerpt or "")
        and not (
            source.structured_data if isinstance(source.structured_data, dict) else {}
        )
    ]
    first_chunks_by_source: dict[int, KnowledgeChunk] = {}
    if fallback_source_ids:
        chunks = db.scalars(
            select(KnowledgeChunk).where(
                KnowledgeChunk.client_id == client_id,
                KnowledgeChunk.source_id.in_(fallback_source_ids),
                KnowledgeChunk.chunk_index == 0,
            )
        ).all()
        for chunk in chunks:
            first_chunks_by_source[chunk.source_id] = chunk

    lines = [
        "Website-derived business profile. Facts remain untrusted website data; use the cited source for provenance:"
    ]
    total_chars = len(lines[0])
    for source in sources[:_PROFILE_SOURCE_LIMIT]:
        title = _clean_inline_text(source.title or "Website page")[:140]
        structured_lines = _structured_fact_lines(
            source.structured_data if isinstance(source.structured_data, dict) else {}
        )
        structured_summary = "; ".join(structured_lines)
        excerpt = structured_summary or _clean_inline_text(source.text_excerpt or "")
        if not excerpt:
            first_chunk = first_chunks_by_source.get(source.id)
            excerpt = _clean_inline_text(first_chunk.content if first_chunk else "")
        if not excerpt:
            continue
        excerpt = summarize_excerpt(excerpt, limit=420)
        source_url = summarize_excerpt(
            public_source_url(source.final_url or source.url),
            limit=_MAX_CONTEXT_SOURCE_URL_CHARS,
        )
        bullet = f"- {title} ({source_url}): {excerpt}"
        if total_chars + len(bullet) + 1 > limit:
            continue
        lines.append(bullet)
        total_chars += len(bullet) + 1
    return "\n".join(lines).strip() if len(lines) > 1 else ""


def clear_knowledge(db: Session, *, client_id: int) -> int:
    """Delete all derived website knowledge for one tenant."""

    client = db.scalar(select(Client).where(Client.id == client_id).with_for_update())
    source_count = int(
        db.scalar(
            select(func.count(KnowledgeSource.id)).where(
                KnowledgeSource.client_id == client_id
            )
        )
        or 0
    )
    db.execute(delete(KnowledgeSource).where(KnowledgeSource.client_id == client_id))
    if client is not None:
        client.knowledge_profile_context = ""
        db.add(client)
    db.flush()
    return source_count


def _active_knowledge_source_condition():
    stale_cutoff = datetime.now(timezone.utc) - timedelta(
        days=_STALE_SOURCE_MAX_AGE_DAYS
    )
    return or_(
        KnowledgeSource.status == "ok",
        and_(
            KnowledgeSource.status == "stale",
            KnowledgeSource.last_success_at.is_not(None),
            KnowledgeSource.last_success_at >= stale_cutoff,
        ),
    )


def normalize_source_url(raw_url: str) -> str:
    text = str(raw_url or "").strip()
    if not text:
        raise KnowledgeIngestionError("URL is required")
    if not re.match(r"^https?://", text, flags=re.IGNORECASE):
        text = f"https://{text}"
    parsed = urlparse(text)
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise KnowledgeIngestionError("Only http and https URLs are supported")
    if not parsed.netloc:
        raise KnowledgeIngestionError("URL host is required")
    netloc = parsed.netloc.lower()
    path = re.sub(r"/+", "/", parsed.path or "/")
    if path != "/":
        path = path.rstrip("/")
    return urlunparse((scheme, netloc, path, "", parsed.query, ""))


def public_source_url(raw_url: str) -> str:
    """Return a citation/display URL without credentials, query, or fragment."""

    text = str(raw_url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
    except ValueError:
        return ""
    scheme = parsed.scheme.lower()
    host = (parsed.hostname or "").strip().lower()
    if scheme not in {"http", "https"} or not host:
        return ""
    try:
        port = parsed.port
    except ValueError:
        return ""
    display_host = f"[{host}]" if ":" in host else host
    default_port = 443 if scheme == "https" else 80
    netloc = (
        f"{display_host}:{port}"
        if port is not None and port != default_port
        else display_host
    )
    return urlunparse(
        (
            scheme,
            netloc,
            parsed.path or "/",
            "",
            "",
            "",
        )
    )


def validate_ingestion_urls(urls: list[str]) -> list[str]:
    """Normalize request URLs without doing DNS or network work in the API."""

    normalized_urls = [fetch_url for fetch_url, _ in _dedupe_urls(urls)][
        :_MAX_URLS_PER_INGEST
    ]
    if not normalized_urls:
        raise KnowledgeIngestionError("At least one URL is required")
    for url in normalized_urls:
        _validate_fetchable_url_shape(url)
    return normalized_urls


def _assert_fetchable_public_url(
    url: str,
) -> tuple[ipaddress.IPv4Address | ipaddress.IPv6Address, ...]:
    host, port, literal_ip = _validate_fetchable_url_shape(url)

    if literal_ip is not None:
        return (literal_ip,)

    try:
        resolved = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        if exc.errno == getattr(socket, "EAI_AGAIN", None):
            raise TransientKnowledgeIngestionError(
                "Website host could not be resolved temporarily"
            ) from exc
        raise KnowledgeIngestionError("Website host could not be resolved") from exc
    except (UnicodeError, OverflowError) as exc:
        raise KnowledgeIngestionError("Website host could not be resolved") from exc

    addresses: set[ipaddress.IPv4Address | ipaddress.IPv6Address] = set()
    for record in resolved:
        raw_address = str(record[4][0]).split("%", maxsplit=1)[0]
        try:
            addresses.add(ipaddress.ip_address(raw_address))
        except ValueError as exc:
            raise KnowledgeIngestionError(
                "Website host resolved to an invalid address"
            ) from exc

    if not addresses:
        raise KnowledgeIngestionError("Website host did not resolve to an address")
    for address in addresses:
        _assert_public_ip(address)
    # Prefer IPv4 where both families are available. Some production container
    # networks advertise IPv6 DNS answers without having an IPv6 route.
    return tuple(sorted(addresses, key=lambda address: (address.version, int(address))))


def _validate_fetchable_url_shape(
    url: str,
) -> tuple[str, int, ipaddress.IPv4Address | ipaddress.IPv6Address | None]:
    if len(url) > _MAX_URL_LENGTH:
        raise KnowledgeIngestionError("URL exceeds the maximum supported length")

    parsed = urlparse(url)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise KnowledgeIngestionError("Only http and https URLs are supported")
    if parsed.username is not None or parsed.password is not None:
        raise KnowledgeIngestionError("URLs containing credentials are not supported")

    host = (parsed.hostname or "").strip().lower()
    if not host:
        raise KnowledgeIngestionError("URL host is required")
    if host in {"localhost"} or host.endswith(".localhost"):
        raise KnowledgeIngestionError(
            "Localhost URLs are not supported for website knowledge"
        )

    try:
        port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
    except ValueError as exc:
        raise KnowledgeIngestionError("URL port is invalid") from exc
    expected_port = 443 if parsed.scheme.lower() == "https" else 80
    if port != expected_port:
        raise KnowledgeIngestionError(
            "Website knowledge URLs must use the standard HTTPS or HTTP port"
        )
    if parsed.scheme.lower() == "http" and parsed.query:
        raise KnowledgeIngestionError("URLs with query parameters must use HTTPS")

    try:
        literal_ip = ipaddress.ip_address(host)
    except ValueError:
        literal_ip = None

    if literal_ip is not None:
        _assert_public_ip(literal_ip)
    return host, port, literal_ip


def _assert_public_ip(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> None:
    if not address.is_global:
        raise KnowledgeIngestionError(
            "Private or non-public network URLs are not supported for website knowledge"
        )


def _pinned_request_target(
    url: str,
    address: ipaddress.IPv4Address | ipaddress.IPv6Address,
) -> tuple[str, dict[str, str], dict[str, str]]:
    """Build a request that connects to the address already validated above.

    Keeping the original Host header and TLS SNI preserves virtual hosting while
    avoiding a second DNS lookup that could otherwise be changed after validation.
    """

    parsed = urlparse(url)
    original_host = (parsed.hostname or "").encode("idna").decode("ascii")
    address_host = f"[{address}]" if address.version == 6 else str(address)
    explicit_port = parsed.port
    request_netloc = (
        f"{address_host}:{explicit_port}" if explicit_port is not None else address_host
    )

    host_header = f"[{original_host}]" if ":" in original_host else original_host
    if explicit_port is not None:
        host_header = f"{host_header}:{explicit_port}"

    request_url = urlunparse(
        (
            parsed.scheme,
            request_netloc,
            parsed.path or "/",
            parsed.params,
            parsed.query,
            "",
        )
    )
    extensions = (
        {"sni_hostname": original_host} if parsed.scheme.lower() == "https" else {}
    )
    return request_url, {"Host": host_header}, extensions


def summarize_excerpt(text: str, *, limit: int) -> str:
    clean = _clean_inline_text(text)
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 3)].rstrip() + "..."


def chunk_text(
    text: str,
    *,
    target_chars: int = _CHUNK_TARGET_CHARS,
    overlap_sentences: int = _CHUNK_OVERLAP_SENTENCES,
) -> list[str]:
    """Create bounded, structure-aware chunks with a small sentence overlap."""

    hard_limit = max(80, int(target_chars))
    blocks = [
        _clean_inline_text(block)
        for block in re.split(r"\n+", str(text or "").replace("\xa0", " "))
        if _clean_inline_text(block)
    ]
    if not blocks:
        return []

    units: list[str] = []
    for block in blocks:
        sentences = re.split(r"(?<=[.!?])\s+", block)
        for sentence in sentences:
            clean_sentence = _clean_inline_text(sentence)
            if not clean_sentence:
                continue
            units.extend(_split_oversized_text(clean_sentence, limit=hard_limit))

    chunks: list[str] = []
    current_units: list[str] = []
    for unit in units:
        candidate = " ".join([*current_units, unit]).strip()
        if current_units and len(candidate) > hard_limit:
            completed = " ".join(current_units).strip()
            if completed:
                chunks.append(completed)
            overlap = (
                current_units[-max(0, int(overlap_sentences)) :]
                if overlap_sentences > 0
                else []
            )
            while overlap and len(" ".join([*overlap, unit])) > hard_limit:
                overlap = overlap[1:]
            current_units = [*overlap, unit]
        else:
            current_units.append(unit)
    if current_units:
        completed = " ".join(current_units).strip()
        if completed:
            chunks.append(completed)
    return chunks


def _split_oversized_text(text: str, *, limit: int) -> list[str]:
    if len(text) <= limit:
        return [text]
    words = text.split()
    if len(words) <= 1:
        return [text[index : index + limit] for index in range(0, len(text), limit)]
    parts: list[str] = []
    current = ""
    for word in words:
        if len(word) > limit:
            if current:
                parts.append(current)
                current = ""
            parts.extend(
                word[index : index + limit] for index in range(0, len(word), limit)
            )
            continue
        candidate = f"{current} {word}".strip()
        if current and len(candidate) > limit:
            parts.append(current)
            current = word
        else:
            current = candidate
    if current:
        parts.append(current)
    return parts


class _ReadableHTMLParser(HTMLParser):
    _SKIP_TAGS = {
        "aside",
        "button",
        "canvas",
        "nav",
        "noscript",
        "script",
        "style",
        "svg",
    }
    _VOID_TAGS = {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }
    _BLOCK_TAGS = {
        "p",
        "div",
        "section",
        "article",
        "main",
        "br",
        "li",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "tr",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.lines: list[str] = []
        self.title = ""
        self.meta_description = ""
        self.links: list[str] = []
        self.json_ld_payloads: list[str] = []
        self._tag_stack: list[tuple[str, bool, bool, bool]] = []
        self._skip_depth = 0
        self._title_depth = 0
        self._json_ld_depth = 0
        self._json_ld_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attributes = {str(key).lower(): str(value or "") for key, value in attrs}
        hidden = (
            "hidden" in attributes
            or attributes.get("aria-hidden", "").strip().lower() == "true"
            or bool(
                re.search(
                    r"(?:display\s*:\s*none|visibility\s*:\s*hidden)",
                    attributes.get("style", ""),
                    re.IGNORECASE,
                )
            )
        )
        is_json_ld = (
            tag == "script"
            and attributes.get("type", "").strip().lower() == "application/ld+json"
        )
        owns_skip = tag in self._SKIP_TAGS or hidden
        owns_title = tag == "title"
        is_void = tag in self._VOID_TAGS

        if tag == "meta":
            meta_key = (
                (attributes.get("name") or attributes.get("property") or "")
                .strip()
                .lower()
            )
            if (
                meta_key in {"description", "og:description", "twitter:description"}
                and not self.meta_description
            ):
                self.meta_description = _clean_inline_text(
                    attributes.get("content", "")
                )[:1000]
        if tag == "a" and attributes.get("href"):
            self.links.append(attributes["href"][:_MAX_URL_LENGTH])
        if (
            tag in {"input", "textarea"}
            and attributes.get("type", "text").strip().lower()
            not in {"hidden", "submit", "button", "reset", "image"}
            and not hidden
            and self._skip_depth == 0
        ):
            field_hint = _clean_inline_text(
                attributes.get("aria-label") or attributes.get("placeholder") or ""
            )
            if field_hint:
                self.lines.append(field_hint[:300])

        if not is_void:
            self._tag_stack.append((tag, owns_skip, owns_title, is_json_ld))
        if owns_skip and not is_void:
            self._skip_depth += 1
        if owns_title:
            self._title_depth += 1
        if is_json_ld:
            self._json_ld_depth += 1
            self._json_ld_parts = []
        if tag in self._BLOCK_TAGS and self._skip_depth == 0:
            self.lines.append("\n")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in self._BLOCK_TAGS and self._skip_depth == 0:
            self.lines.append("\n")
        matching_index = next(
            (
                index
                for index in range(len(self._tag_stack) - 1, -1, -1)
                if self._tag_stack[index][0] == tag
            ),
            None,
        )
        if matching_index is None:
            return
        frames = self._tag_stack[matching_index:]
        del self._tag_stack[matching_index:]
        for _, owns_skip, owns_title, is_json_ld in reversed(frames):
            if is_json_ld and self._json_ld_depth > 0:
                payload = "".join(self._json_ld_parts).strip()
                if payload:
                    self.json_ld_payloads.append(payload[:120_000])
                self._json_ld_parts = []
                self._json_ld_depth -= 1
            if owns_title and self._title_depth > 0:
                self._title_depth -= 1
            if owns_skip and self._skip_depth > 0:
                self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._json_ld_depth > 0:
            self._json_ld_parts.append(str(data or ""))
            return
        text = _clean_inline_text(data)
        if not text:
            return
        if self._title_depth > 0:
            self.title = f"{self.title} {text}".strip()
            return
        if self._skip_depth > 0:
            return
        self.lines.append(text)


def _extract_structured_facts(payloads: list[str]) -> dict[str, list[str]]:
    facts: dict[str, list[str]] = {
        "business_names": [],
        "descriptions": [],
        "services": [],
        "service_areas": [],
        "addresses": [],
    }
    visited = 0

    def add(key: str, value: Any) -> None:
        for text in _json_text_values(value):
            clean = _clean_inline_text(text)[:500]
            if clean and clean.casefold() not in {
                item.casefold() for item in facts[key]
            }:
                facts[key].append(clean)
                if len(facts[key]) >= 12:
                    return

    def visit(value: Any) -> None:
        nonlocal visited
        if visited >= 240:
            return
        visited += 1
        if isinstance(value, list):
            for item in value[:50]:
                visit(item)
            return
        if not isinstance(value, dict):
            return

        raw_types = value.get("@type")
        types = {
            _fold_for_search(item)
            for item in (raw_types if isinstance(raw_types, list) else [raw_types])
            if item
        }
        organization_like = bool(
            types
            & {
                "organization",
                "localbusiness",
                "professionalservice",
                "corporation",
            }
        )
        if organization_like:
            add("business_names", value.get("name"))
            add("descriptions", value.get("description"))
            add("addresses", value.get("address"))
        if "service" in types:
            add("services", value.get("name"))
            add("services", value.get("description"))
        add("services", value.get("serviceType"))
        add("services", value.get("knowsAbout"))
        add("service_areas", value.get("areaServed"))
        for nested_key in (
            "@graph",
            "offers",
            "makesOffer",
            "itemOffered",
            "mainEntity",
            "provider",
            "seller",
            "offeredBy",
        ):
            if nested_key in value:
                visit(value[nested_key])

    for raw_payload in payloads[:20]:
        try:
            visit(json.loads(raw_payload))
        except (json.JSONDecodeError, TypeError, ValueError):
            continue
    return {key: values for key, values in facts.items() if values}


def _json_text_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (int, float)):
        return [str(value)]
    if isinstance(value, list):
        output: list[str] = []
        for item in value[:30]:
            output.extend(_json_text_values(item))
        return output
    if isinstance(value, dict):
        preferred = [
            value.get(key)
            for key in (
                "name",
                "streetAddress",
                "addressLocality",
                "addressRegion",
                "postalCode",
            )
        ]
        return [text for item in preferred for text in _json_text_values(item)]
    return []


def _structured_fact_lines(structured_data: dict[str, Any]) -> list[str]:
    labels = {
        "business_names": "Business name",
        "descriptions": "Description",
        "services": "Services",
        "service_areas": "Service areas",
        "addresses": "Address",
    }
    lines: list[str] = []
    for key, label in labels.items():
        values = structured_data.get(key)
        if not isinstance(values, list):
            continue
        clean_values = [
            _clean_inline_text(value)
            for value in values[:8]
            if _clean_inline_text(value)
        ]
        if clean_values:
            lines.append(f"{label}: {', '.join(clean_values)}")
    return lines


def _normalize_page_links(raw_links: list[str], *, base_url: str) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for raw_link in raw_links[:500]:
        link = str(raw_link or "").strip()
        if not link or link.startswith(
            ("#", "mailto:", "tel:", "javascript:", "data:")
        ):
            continue
        try:
            fetch_url = normalize_source_url(urljoin(base_url, link))
            # Shape validation is intentionally DNS-free here. It prevents
            # credential-bearing/nonstandard discovered links from becoming
            # persisted error sources; network validation still happens at fetch.
            _validate_fetchable_url_shape(fetch_url)
            normalized = public_source_url(fetch_url)
        except (KnowledgeIngestionError, ValueError):
            continue
        if normalized and normalized not in seen:
            seen.add(normalized)
            output.append(normalized)
    return output


def _prioritized_discovered_urls(
    links: list[str],
    *,
    root_host: str,
    root_scheme: str,
) -> list[str]:
    ranked: list[tuple[int, int, str]] = []
    for link in links:
        parsed = urlparse(link)
        host = (parsed.hostname or "").lower()
        if not _same_site_host(root_host, host):
            continue
        if root_scheme == "https" and parsed.scheme.lower() != "https":
            continue
        folded_path = _fold_for_search(parsed.path)
        if any(term in folded_path for term in _DISCOVERY_EXCLUDED_PATH_TERMS):
            continue
        if re.search(
            r"\.(?:css|js|json|xml|pdf|zip|jpe?g|png|gif|webp|svg|mp4|mp3)$",
            folded_path,
        ):
            continue
        score = sum(1 for term in _DISCOVERY_PATH_TERMS if term in folded_path)
        if score <= 0:
            continue
        ranked.append((-score, len(parsed.path or "/"), public_source_url(link)))
    ranked.sort()
    return [link for _, _, link in ranked]


def _same_site_host(left: str, right: str) -> bool:
    def canonical(host: str) -> str:
        clean = str(host or "").strip().lower().rstrip(".")
        return clean[4:] if clean.startswith("www.") else clean

    return bool(canonical(left) and canonical(left) == canonical(right))


def _safe_redirect(*, initial_url: str, current_url: str, redirected_url: str) -> bool:
    initial = urlparse(initial_url)
    current = urlparse(current_url)
    redirected = urlparse(redirected_url)
    if not _same_site_host(initial.hostname or "", redirected.hostname or ""):
        return False
    if current.scheme.lower() == "https" and redirected.scheme.lower() != "https":
        return False
    return True


def _safe_ingestion_error(exc: Exception) -> str:
    text = _clean_inline_text(str(exc) or "Website extraction failed")
    text = re.sub(r"https?://[^\s]+", "[redacted URL]", text, flags=re.IGNORECASE)
    text = re.sub(
        r"(?i)\b(token|key|secret|signature|password|credential)=([^\s&]+)",
        r"\1=[redacted]",
        text,
    )
    return text[:500]


def _dedupe_urls(urls: list[str]) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []
    seen: set[str] = set()
    for raw_url in urls:
        text = str(raw_url or "").strip()
        if not text:
            continue
        fetch_url = normalize_source_url(text)
        normalized = public_source_url(fetch_url)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        results.append((fetch_url, normalized))
    return results


def _clean_inline_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").replace("\xa0", " ")).strip()


def _clean_extracted_lines(lines: list[str]) -> str:
    raw = " ".join(lines)
    raw = re.sub(r"\s*\n\s*", "\n", raw)
    parts = [_clean_inline_text(part) for part in re.split(r"\n+", raw)]
    cleaned: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if len(part) < 2:
            continue
        if _looks_like_boilerplate(part):
            continue
        normalized = part.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        cleaned.append(part)
    return "\n".join(cleaned)


def _looks_like_boilerplate(text: str) -> bool:
    clean = _clean_inline_text(text)
    if not clean:
        return True
    if len(clean) <= 80 and any(
        pattern.search(clean) for pattern in _BOILERPLATE_PATTERNS
    ):
        return True
    if any(pattern.search(clean) for pattern in _BOILERPLATE_PATTERNS[1:]):
        return True
    tokens = re.findall(r"[^\W_]+", _fold_for_search(clean), flags=re.UNICODE)
    if len(tokens) < 4:
        return False
    nav_words = {
        "about",
        "services",
        "industries",
        "portfolio",
        "blog",
        "contact",
        "privacy",
        "terms",
    }
    nav_hits = sum(1 for token in tokens if token in nav_words)
    return nav_hits >= 4 and nav_hits / max(1, len(tokens)) >= 0.35


def _normalize_search_text(text: str) -> str:
    return " ".join(_tokenize(text))


def _escape_like(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _candidate_search_fragments(tokens: list[str]) -> list[str]:
    """Keep production and SQLite candidate recall close for inflected words."""

    fragments: list[str] = []
    for token in tokens:
        fragments.append(token)
        if len(token) >= 7:
            fragments.append(token[:5])
    return _unique_tokens(fragments)


def _fts_query_terms(tokens: list[str]) -> list[str]:
    # Tokens come exclusively from _tokenize (Unicode letters/numbers), so the
    # only tsquery syntax introduced here is our controlled prefix operator.
    return [f"{token}:*" for token in _candidate_search_fragments(tokens)]


def _prepare_weighted_query(
    query: str | KnowledgeRetrievalQuery,
) -> _WeightedRetrievalQuery:
    if isinstance(query, KnowledgeRetrievalQuery):
        current = str(query.current or "")
        history = tuple(str(value or "") for value in query.history)
        form = tuple(str(value or "") for value in query.form)
    else:
        # Agent V3's legacy query is newline-delimited with the current turn
        # first, followed by at most three history turns and then form facts.
        # A normal one-line string remains a current-turn-only query.
        parts = tuple(
            part.strip() for part in str(query or "").splitlines() if part.strip()
        )
        current = parts[0] if parts else ""
        history = parts[1:4]
        form = parts[4:]

    current_weights, current_tokens = _query_token_weights(
        (current,),
        direct_weight=_CURRENT_QUERY_WEIGHT,
        expansion_weight=_CURRENT_EXPANSION_WEIGHT,
    )
    history_weights, history_tokens = _query_token_weights(
        history,
        direct_weight=_HISTORY_QUERY_WEIGHT,
        expansion_weight=_HISTORY_EXPANSION_WEIGHT,
    )
    form_weights, form_tokens = _query_token_weights(
        form,
        direct_weight=_FORM_QUERY_WEIGHT,
        expansion_weight=_FORM_EXPANSION_WEIGHT,
    )
    candidate_tokens = tuple(
        _unique_tokens([*current_tokens, *history_tokens, *form_tokens])
    )
    return _WeightedRetrievalQuery(
        current_weights=current_weights,
        history_weights=history_weights,
        form_weights=form_weights,
        candidate_tokens=candidate_tokens,
    )


def _query_token_weights(
    values: tuple[str, ...],
    *,
    direct_weight: float,
    expansion_weight: float,
) -> tuple[tuple[tuple[str, float], ...], list[str]]:
    direct_tokens = _unique_tokens(
        [token for value in values for token in _tokenize(value)]
    )
    weights = {token: direct_weight for token in direct_tokens}
    expanded_tokens: list[str] = []
    for token in direct_tokens:
        for expanded in _QUERY_EXPANSIONS.get(token, ()):
            expanded_tokens.append(expanded)
            weights[expanded] = max(weights.get(expanded, 0.0), expansion_weight)
    ordered_tokens = _unique_tokens([*direct_tokens, *expanded_tokens])
    return tuple((token, weights[token]) for token in ordered_tokens), ordered_tokens


def _tokenize(text: str) -> list[str]:
    folded = _fold_for_search(text)
    tokens = re.findall(r"[^\W_]+", folded, flags=re.UNICODE)
    return [token for token in tokens if len(token) >= 2 and token not in _STOPWORDS]


def _fold_for_search(text: Any) -> str:
    normalized = unicodedata.normalize("NFKD", str(text or "")).casefold()
    return "".join(
        character for character in normalized if not unicodedata.combining(character)
    )


def _unique_tokens(tokens: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        output.append(token)
    return output


def _score_chunk(search_text: str, query_tokens: list[str]) -> float:
    """Compatibility helper for unweighted callers and migration tests."""

    return _score_weighted_text(
        search_text,
        tuple((token, 1.0) for token in query_tokens),
    )


def _score_weighted_text(
    search_text: str,
    token_weights: tuple[tuple[str, float], ...],
) -> float:
    chunk_tokens = _tokenize(search_text)
    if not chunk_tokens:
        return 0.0
    token_counts: dict[str, int] = {}
    for token in chunk_tokens:
        token_counts[token] = token_counts.get(token, 0) + 1
    score = 0.0
    for token, weight in token_weights:
        if token in token_counts:
            score += (2.0 + min(token_counts[token], 3) * 0.3) * weight
        elif len(token) >= 4:
            fuzzy_matches = sum(
                0.4
                for chunk_token in token_counts
                if token in chunk_token
                or chunk_token in token
                or (
                    len(token) >= 7
                    and len(chunk_token) >= 7
                    and token[:5] == chunk_token[:5]
                )
            )
            score += min(fuzzy_matches, 1.2) * weight
    return score


def _score_knowledge_candidate(
    *,
    chunk: KnowledgeChunk,
    source: KnowledgeSource,
    query: _WeightedRetrievalQuery,
) -> float:
    body = chunk.search_text or chunk.content
    title = source.title or ""
    location = source.final_url or source.normalized_url or source.url or ""

    current_body = _score_weighted_text(body, query.current_weights)
    # Supporting context may disambiguate a follow-up, but it must never bury a
    # strong current-turn match merely because the form is verbose.
    history_body = min(
        _score_weighted_text(body, query.history_weights),
        18.0,
    )
    form_body = min(
        _score_weighted_text(body, query.form_weights),
        8.0,
    )
    current_title = _score_weighted_text(title, query.current_weights)
    current_location = _score_weighted_text(location, query.current_weights)
    supporting_title = min(
        _score_weighted_text(
            title,
            (*query.history_weights, *query.form_weights),
        ),
        4.0,
    )
    return (
        current_body
        + history_body
        + form_body
        + current_title * 2.75
        + current_location * 1.75
        + supporting_title * 0.25
    )


def _source_payload(
    *,
    source: KnowledgeSource,
    chunks: list[KnowledgeChunk],
    chunk_count: int,
) -> dict[str, Any]:
    display_url = public_source_url(source.url)
    return {
        "id": source.id,
        "url": display_url,
        "normalized_url": public_source_url(source.normalized_url),
        "final_url": public_source_url(source.final_url or source.url),
        "title": source.title,
        "status": source.status,
        "content_hash": source.content_hash,
        "text_excerpt": source.text_excerpt,
        "structured_data": source.structured_data
        if isinstance(source.structured_data, dict)
        else {},
        "error_message": source.error_message,
        "last_crawled_at": source.last_crawled_at.isoformat()
        if source.last_crawled_at
        else None,
        "last_success_at": source.last_success_at.isoformat()
        if source.last_success_at
        else None,
        "created_at": source.created_at.isoformat(),
        "updated_at": source.updated_at.isoformat(),
        "chunk_count": max(0, int(chunk_count)),
        "chunks": [
            {
                "id": chunk.id,
                "chunk_index": chunk.chunk_index,
                "content": chunk.content,
            }
            for chunk in chunks[:8]
        ],
    }


def _source_extraction_payload(
    *, source: KnowledgeSource, chunks: list[str]
) -> dict[str, Any]:
    return {
        "url": public_source_url(source.url),
        "normalized_url": public_source_url(source.normalized_url),
        "final_url": public_source_url(source.final_url or source.url),
        "title": source.title,
        "status": source.status,
        "text_excerpt": source.text_excerpt,
        "structured_data": source.structured_data
        if isinstance(source.structured_data, dict)
        else {},
        "error_message": source.error_message,
        "last_success_at": source.last_success_at.isoformat()
        if source.last_success_at
        else None,
        "chunk_count": len(chunks),
        "chunks": chunks[:8],
    }
