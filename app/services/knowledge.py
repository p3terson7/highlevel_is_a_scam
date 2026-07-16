from __future__ import annotations

import hashlib
import ipaddress
import re
import socket
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import httpx
from sqlalchemy import delete, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.models import Client, KnowledgeChunk, KnowledgeSource

_DEFAULT_TIMEOUT_SECONDS = 15
_MAX_HTML_BYTES = 1_500_000
_MAX_URLS_PER_INGEST = 12
_MAX_REDIRECTS = 5
_MAX_URL_LENGTH = 2048
_REDIRECT_STATUS_CODES = {301, 302, 303, 307, 308}
_CHUNK_TARGET_CHARS = 900
_CHUNK_OVERLAP_CHARS = 0
_BOILERPLATE_PATTERNS = (
    re.compile(r"^(home|about|services|industries|portfolio|blog|contact)(\s+|$)", re.IGNORECASE),
    re.compile(r"\b(subscribe to our newsletter|be the first to know|powered by|all rights reserved|privacy policy|terms\s*&\s*conditions)\b", re.IGNORECASE),
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
}


@dataclass(frozen=True)
class FetchResult:
    url: str
    status_code: int
    html: str


@dataclass(frozen=True)
class ExtractedPage:
    url: str
    normalized_url: str
    title: str
    text: str
    text_excerpt: str
    content_hash: str
    chunks: list[str]


class KnowledgeIngestionError(RuntimeError):
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

    def ingest_urls(
        self,
        *,
        db: Session,
        client_id: int,
        urls: list[str],
        replace: bool = True,
    ) -> dict[str, Any]:
        normalized_urls = _dedupe_urls(urls)[:_MAX_URLS_PER_INGEST]
        now = datetime.now(timezone.utc)
        existing_sources = {
            source.normalized_url: source
            for source in db.scalars(select(KnowledgeSource).where(KnowledgeSource.client_id == client_id)).all()
        }

        if replace:
            keep = {normalized for _, normalized in normalized_urls}
            for normalized, source in list(existing_sources.items()):
                if normalized not in keep:
                    db.delete(source)

        pages: list[dict[str, Any]] = []
        for display_url, normalized_url in normalized_urls:
            source = existing_sources.get(normalized_url)
            if source is None:
                source = KnowledgeSource(
                    client_id=client_id,
                    url=display_url,
                    normalized_url=normalized_url,
                )
                db.add(source)
                db.flush()

            source.url = display_url
            source.normalized_url = normalized_url
            source.last_crawled_at = now
            try:
                fetched = self._fetch_url(display_url)
                extracted = extract_page_text(fetched.html, url=fetched.url or display_url)
                source.status = "ok"
                source.title = extracted.title
                source.content_hash = extracted.content_hash
                source.extracted_text = extracted.text
                source.text_excerpt = extracted.text_excerpt
                source.error_message = ""
                source.last_crawled_at = now
                db.add(source)
                db.flush()
                db.execute(delete(KnowledgeChunk).where(KnowledgeChunk.source_id == source.id))
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
                pages.append(_source_extraction_payload(source=source, chunks=extracted.chunks))
            except Exception as exc:
                source.status = "error"
                source.title = ""
                source.content_hash = ""
                source.extracted_text = ""
                source.text_excerpt = ""
                source.error_message = str(exc)[:2000]
                source.last_crawled_at = now
                db.add(source)
                db.flush()
                db.execute(delete(KnowledgeChunk).where(KnowledgeChunk.source_id == source.id))
                pages.append(_source_extraction_payload(source=source, chunks=[]))

        db.flush()
        business_profile_context = refresh_business_profile_context(db, client_id=client_id)
        return {
            "pages": pages,
            "total_pages": len(pages),
            "total_chunks": sum(len(page.get("chunks", [])) for page in pages),
            "business_profile_context": business_profile_context,
        }

    def _fetch_url(self, url: str) -> FetchResult:
        current_url = normalize_source_url(url)
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

        with httpx.Client(**client_kwargs) as client:
            for redirect_count in range(_MAX_REDIRECTS + 1):
                addresses = _assert_fetchable_public_url(current_url)
                request_url, request_headers, request_extensions = _pinned_request_target(
                    current_url,
                    addresses[0],
                )
                with client.stream(
                    "GET",
                    request_url,
                    headers=request_headers,
                    extensions=request_extensions,
                ) as response:
                    if response.status_code in _REDIRECT_STATUS_CODES:
                        if redirect_count >= _MAX_REDIRECTS:
                            raise KnowledgeIngestionError("Too many redirects while fetching website knowledge")
                        location = response.headers.get("location", "").strip()
                        if not location:
                            raise KnowledgeIngestionError("Redirect response did not include a Location header")
                        current_url = normalize_source_url(urljoin(current_url, location))
                        continue

                    if response.status_code >= 400:
                        raise KnowledgeIngestionError(f"Fetch failed with HTTP {response.status_code}")

                    content_type = response.headers.get("content-type", "")
                    if (
                        "text/html" not in content_type
                        and "application/xhtml" not in content_type
                        and content_type
                    ):
                        raise KnowledgeIngestionError(f"Unsupported content type: {content_type}")

                    content_length = response.headers.get("content-length", "").strip()
                    if content_length:
                        try:
                            if int(content_length) > _MAX_HTML_BYTES:
                                raise KnowledgeIngestionError("Website content exceeds the ingestion size limit")
                        except ValueError:
                            pass

                    chunks: list[bytes] = []
                    total_bytes = 0
                    for chunk in response.iter_bytes():
                        total_bytes += len(chunk)
                        if total_bytes > _MAX_HTML_BYTES:
                            raise KnowledgeIngestionError("Website content exceeds the ingestion size limit")
                        chunks.append(chunk)
                    raw = b"".join(chunks)
                    return FetchResult(
                        url=current_url,
                        status_code=response.status_code,
                        html=raw.decode(response.encoding or "utf-8", errors="ignore"),
                    )

        raise KnowledgeIngestionError("Website fetch did not return a response")


def extract_page_text(html: str, *, url: str) -> ExtractedPage:
    normalized_url = normalize_source_url(url)
    parser = _ReadableHTMLParser()
    parser.feed(html or "")
    title = _clean_inline_text(parser.title)[:512]
    text = _clean_extracted_lines(parser.lines)
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
    )


def knowledge_payload(db: Session, *, client_id: int) -> dict[str, Any]:
    try:
        sources = db.scalars(
            select(KnowledgeSource)
            .where(KnowledgeSource.client_id == client_id)
            .order_by(KnowledgeSource.updated_at.desc(), KnowledgeSource.id.desc())
        ).all()
        source_ids = [source.id for source in sources]
        chunks_by_source: dict[int, list[KnowledgeChunk]] = {source.id: [] for source in sources}
        if source_ids:
            chunks = db.scalars(
                select(KnowledgeChunk)
                .where(KnowledgeChunk.source_id.in_(source_ids))
                .order_by(KnowledgeChunk.source_id.asc(), KnowledgeChunk.chunk_index.asc())
            ).all()
            for chunk in chunks:
                chunks_by_source.setdefault(chunk.source_id, []).append(chunk)
    except SQLAlchemyError:
        db.rollback()
        return {
            "sources": [],
            "total_sources": 0,
            "total_chunks": 0,
            "status": "unavailable",
            "error": "Knowledge tables are not migrated yet. Run alembic upgrade head.",
        }

    business_profile_context = build_business_profile_context(db, client_id=client_id)
    return {
        "sources": [
            _source_payload(source=source, chunks=chunks_by_source.get(source.id, []))
            for source in sources
        ],
        "total_sources": len(sources),
        "total_chunks": sum(len(items) for items in chunks_by_source.values()),
        "business_profile_context": business_profile_context,
    }


def retrieve_knowledge_snippets(
    db: Session,
    *,
    client_id: int,
    query: str,
    limit: int = 4,
) -> list[dict[str, Any]]:
    query_tokens = _tokenize(query)
    if not query_tokens:
        return []
    candidate_tokens = query_tokens[:8]
    search_conditions = [KnowledgeChunk.search_text.ilike(f"%{_escape_like(token)}%", escape="\\") for token in candidate_tokens]
    try:
        rows = db.execute(
            select(KnowledgeChunk, KnowledgeSource)
            .join(KnowledgeSource, KnowledgeSource.id == KnowledgeChunk.source_id)
            .where(
                KnowledgeChunk.client_id == client_id,
                KnowledgeSource.status == "ok",
                or_(*search_conditions),
            )
            .limit(250)
        ).all()
    except SQLAlchemyError:
        db.rollback()
        return []
    scored: list[tuple[float, KnowledgeChunk, KnowledgeSource]] = []
    for chunk, source in rows:
        score = _score_chunk(chunk.search_text or chunk.content, query_tokens)
        if score <= 0:
            continue
        scored.append((score, chunk, source))
    scored.sort(key=lambda item: (-item[0], item[1].chunk_index))
    return [
        {
            "source_url": public_source_url(source.url),
            "source_title": source.title,
            "chunk_index": chunk.chunk_index,
            "content": chunk.content,
            "score": round(score, 3),
        }
        for score, chunk, source in scored[: max(1, limit)]
    ]


def build_knowledge_context(db: Session | None, *, client_id: int, query: str, limit: int = 4) -> str:
    if db is None:
        return ""
    snippets = retrieve_knowledge_snippets(db, client_id=client_id, query=query, limit=limit)
    if not snippets:
        return ""
    lines: list[str] = []
    total_chars = 0
    for snippet in snippets:
        title = str(snippet.get("source_title") or "Website page").strip()
        url = str(snippet.get("source_url") or "").strip()
        content = summarize_excerpt(str(snippet.get("content") or ""), limit=700)
        block = f"Source: {title} ({url})\n{content}".strip()
        if total_chars + len(block) > 2600:
            break
        lines.append(block)
        total_chars += len(block)
    return "\n\n".join(lines)


def build_business_profile_context(db: Session | None, *, client_id: int, fallback: str = "") -> str:
    if db is None:
        return fallback.strip()
    try:
        client = db.get(Client, client_id)
        if client is not None:
            stored = _stored_business_profile(client)
            if stored:
                return stored
        return _compose_business_profile_from_sources(db, client_id=client_id)
    except SQLAlchemyError:
        db.rollback()
        return fallback.strip()


def refresh_business_profile_context(db: Session, *, client_id: int) -> str:
    profile = _compose_business_profile_from_sources(db, client_id=client_id)
    client = db.get(Client, client_id)
    if client is not None:
        provider_config = dict(client.provider_config or {})
        if profile:
            provider_config["business_profile_context"] = profile
        else:
            provider_config.pop("business_profile_context", None)
        client.provider_config = provider_config
        db.add(client)
    return profile


def _stored_business_profile(client: Client) -> str:
    provider_config = client.provider_config if isinstance(client.provider_config, dict) else {}
    return " ".join(str(provider_config.get("business_profile_context") or "").split()).strip()


def _compose_business_profile_from_sources(db: Session, *, client_id: int, limit: int = 1800) -> str:
    sources = db.scalars(
        select(KnowledgeSource)
        .where(KnowledgeSource.client_id == client_id, KnowledgeSource.status == "ok")
        .order_by(KnowledgeSource.updated_at.desc(), KnowledgeSource.id.desc())
    ).all()
    if not sources:
        return ""
    source_ids = [source.id for source in sources]
    chunks_by_source: dict[int, list[KnowledgeChunk]] = {source.id: [] for source in sources}
    if source_ids:
        chunks = db.scalars(
            select(KnowledgeChunk)
            .where(KnowledgeChunk.source_id.in_(source_ids))
            .order_by(KnowledgeChunk.source_id.asc(), KnowledgeChunk.chunk_index.asc())
        ).all()
        for chunk in chunks:
            chunks_by_source.setdefault(chunk.source_id, []).append(chunk)

    lines = ["Website-derived business profile. Use this as always-on business memory:"]
    total_chars = len(lines[0])
    for source in sources[:8]:
        title = _clean_inline_text(source.title or "Website page")[:140]
        excerpt = _clean_inline_text(source.text_excerpt or "")
        if not excerpt:
            first_chunk = next(iter(chunks_by_source.get(source.id, [])), None)
            excerpt = _clean_inline_text(first_chunk.content if first_chunk else "")
        if not excerpt:
            continue
        excerpt = summarize_excerpt(excerpt, limit=420)
        bullet = f"- {title}: {excerpt}"
        if total_chars + len(bullet) + 1 > limit:
            break
        lines.append(bullet)
        total_chars += len(bullet) + 1
    return "\n".join(lines).strip() if len(lines) > 1 else ""


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
    """Return a citation/display URL without query credentials or fragments."""

    text = str(raw_url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
    except ValueError:
        return ""
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return ""
    return urlunparse(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path or "/",
            "",
            "",
            "",
        )
    )


def validate_ingestion_urls(urls: list[str]) -> list[str]:
    """Normalize request URLs without doing DNS or network work in the API."""

    normalized_urls = [normalized for _, normalized in _dedupe_urls(urls)][
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
    except (socket.gaierror, UnicodeError, OverflowError) as exc:
        raise KnowledgeIngestionError("Website host could not be resolved") from exc

    addresses: set[ipaddress.IPv4Address | ipaddress.IPv6Address] = set()
    for record in resolved:
        raw_address = str(record[4][0]).split("%", maxsplit=1)[0]
        try:
            addresses.add(ipaddress.ip_address(raw_address))
        except ValueError as exc:
            raise KnowledgeIngestionError("Website host resolved to an invalid address") from exc

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
        raise KnowledgeIngestionError("Localhost URLs are not supported for website knowledge")

    try:
        port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
    except ValueError as exc:
        raise KnowledgeIngestionError("URL port is invalid") from exc
    expected_port = 443 if parsed.scheme.lower() == "https" else 80
    if port != expected_port:
        raise KnowledgeIngestionError(
            "Website knowledge URLs must use the standard HTTPS or HTTP port"
        )

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
    request_netloc = f"{address_host}:{explicit_port}" if explicit_port is not None else address_host

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
    extensions = {"sni_hostname": original_host} if parsed.scheme.lower() == "https" else {}
    return request_url, {"Host": host_header}, extensions


def summarize_excerpt(text: str, *, limit: int) -> str:
    clean = _clean_inline_text(text)
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 3)].rstrip() + "..."


def chunk_text(text: str, *, target_chars: int = _CHUNK_TARGET_CHARS, overlap_sentences: int = _CHUNK_OVERLAP_CHARS) -> list[str]:
    clean = _clean_inline_text(text)
    if not clean:
        return []
    sentences = re.split(r"(?<=[.!?])\s+", clean)
    chunks: list[str] = []
    current_sentences: list[str] = []
    current = ""
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
        if current and len(current) + len(sentence) + 1 > target_chars:
            chunks.append(current)
            current_sentences = current_sentences[-overlap_sentences:] if overlap_sentences > 0 else []
            current_sentences.append(sentence)
            current = " ".join(current_sentences).strip()
        else:
            current_sentences.append(sentence)
            current = " ".join(current_sentences).strip()
    if current:
        chunks.append(current)
    return chunks or [clean[:target_chars]]


class _ReadableHTMLParser(HTMLParser):
    _SKIP_TAGS = {
        "aside",
        "button",
        "canvas",
        "footer",
        "form",
        "input",
        "nav",
        "noscript",
        "script",
        "select",
        "style",
        "svg",
        "textarea",
    }
    _BLOCK_TAGS = {"p", "div", "section", "article", "main", "br", "li", "h1", "h2", "h3", "h4", "h5", "h6", "tr"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.lines: list[str] = []
        self.title = ""
        self._tag_stack: list[str] = []
        self._skip_depth = 0
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        self._tag_stack.append(tag)
        if tag in self._SKIP_TAGS:
            self._skip_depth += 1
        if tag == "title":
            self._in_title = True
        if tag in self._BLOCK_TAGS:
            self.lines.append("\n")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in self._SKIP_TAGS and self._skip_depth > 0:
            self._skip_depth -= 1
        if tag == "title":
            self._in_title = False
        if tag in self._BLOCK_TAGS:
            self.lines.append("\n")
        if self._tag_stack:
            self._tag_stack.pop()

    def handle_data(self, data: str) -> None:
        text = _clean_inline_text(data)
        if not text:
            return
        if self._in_title:
            self.title = f"{self.title} {text}".strip()
            return
        if self._skip_depth > 0:
            return
        self.lines.append(text)


def _dedupe_urls(urls: list[str]) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []
    seen: set[str] = set()
    for raw_url in urls:
        text = str(raw_url or "").strip()
        if not text:
            continue
        normalized = normalize_source_url(text)
        if normalized in seen:
            continue
        seen.add(normalized)
        results.append((normalized, normalized))
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
    if len(clean) <= 80 and any(pattern.search(clean) for pattern in _BOILERPLATE_PATTERNS):
        return True
    if any(pattern.search(clean) for pattern in _BOILERPLATE_PATTERNS[1:]):
        return True
    tokens = re.findall(r"[a-z0-9]+", clean.lower())
    if len(tokens) < 4:
        return False
    nav_words = {"about", "services", "industries", "portfolio", "blog", "contact", "privacy", "terms"}
    nav_hits = sum(1 for token in tokens if token in nav_words)
    return nav_hits >= 4 and nav_hits / max(1, len(tokens)) >= 0.35


def _normalize_search_text(text: str) -> str:
    return " ".join(_tokenize(text))


def _escape_like(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _tokenize(text: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9][a-z0-9+\-]{1,}", str(text or "").lower())
    return [token for token in tokens if token not in _STOPWORDS]


def _score_chunk(search_text: str, query_tokens: list[str]) -> float:
    chunk_tokens = _tokenize(search_text)
    if not chunk_tokens:
        return 0.0
    token_counts: dict[str, int] = {}
    for token in chunk_tokens:
        token_counts[token] = token_counts.get(token, 0) + 1
    score = 0.0
    for token in query_tokens:
        if token in token_counts:
            score += 2.0 + min(token_counts[token], 3) * 0.3
        else:
            score += sum(0.4 for chunk_token in token_counts if token in chunk_token or chunk_token in token)
    return score


def _source_payload(*, source: KnowledgeSource, chunks: list[KnowledgeChunk]) -> dict[str, Any]:
    display_url = public_source_url(source.url)
    return {
        "id": source.id,
        "url": display_url,
        "normalized_url": public_source_url(source.normalized_url),
        "title": source.title,
        "status": source.status,
        "content_hash": source.content_hash,
        "text_excerpt": source.text_excerpt,
        "error_message": source.error_message,
        "last_crawled_at": source.last_crawled_at.isoformat() if source.last_crawled_at else None,
        "created_at": source.created_at.isoformat(),
        "updated_at": source.updated_at.isoformat(),
        "chunk_count": len(chunks),
        "chunks": [
            {
                "id": chunk.id,
                "chunk_index": chunk.chunk_index,
                "content": chunk.content,
            }
            for chunk in chunks[:8]
        ],
    }


def _source_extraction_payload(*, source: KnowledgeSource, chunks: list[str]) -> dict[str, Any]:
    return {
        "url": public_source_url(source.url),
        "normalized_url": public_source_url(source.normalized_url),
        "title": source.title,
        "status": source.status,
        "text_excerpt": source.text_excerpt,
        "error_message": source.error_message,
        "chunk_count": len(chunks),
        "chunks": chunks[:8],
    }
