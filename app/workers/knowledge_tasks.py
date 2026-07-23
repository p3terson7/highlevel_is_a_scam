from __future__ import annotations

import hashlib
import json
import re
import secrets
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

from redis.exceptions import RedisError
from rq import Queue, Retry, get_current_job
from rq.exceptions import NoSuchJobError
from rq.job import Job
from sqlalchemy import select

from app.core.config import get_settings
from app.core.logging import get_logger
from app.db.models import AuditLog, Client
from app.db.session import get_session_factory
from app.services.knowledge import KnowledgeIngestionService, validate_ingestion_urls
from app.services.secret_storage import protect_secret, reveal_secret
from app.workers.tasks import get_redis_connection

logger = get_logger(__name__)

_KNOWLEDGE_QUEUE_NAME = "knowledge"
_KNOWLEDGE_JOB_TIMEOUT_SECONDS = 300
_KNOWLEDGE_ADMISSION_SECONDS = 30 * 60
_KNOWLEDGE_CANCELLATION_SECONDS = 24 * 60 * 60
_KNOWLEDGE_JOB_META_KEY = "knowledge_ingestion"
_KNOWLEDGE_JOB_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


class KnowledgeIngestionBusy(RuntimeError):
    pass


class KnowledgeIngestionQueueUnavailable(RuntimeError):
    pass


class KnowledgeIngestionJobNotFound(RuntimeError):
    pass


@lru_cache
def get_knowledge_queue() -> Queue | None:
    redis_conn = get_redis_connection()
    if redis_conn is None:
        return None
    return Queue(_KNOWLEDGE_QUEUE_NAME, connection=redis_conn)


def enqueue_knowledge_ingestion(
    *,
    client_id: int,
    urls: list[str],
    replace: bool,
    actor_role: str,
) -> str:
    """Admit at most one queued/running crawl per tenant and enqueue it."""

    normalized_urls = validate_ingestion_urls(urls)
    settings = get_settings()
    if settings.rq_eager:
        raise KnowledgeIngestionQueueUnavailable(
            "Knowledge ingestion requires the dedicated background worker."
        )

    redis_conn = get_redis_connection()
    queue = get_knowledge_queue()
    if redis_conn is None or queue is None:
        raise KnowledgeIngestionQueueUnavailable(
            "Knowledge ingestion queue is unavailable."
        )

    admission_token = secrets.token_urlsafe(24)
    admission_key = _knowledge_admission_key(client_id)
    try:
        admitted = bool(
            redis_conn.set(
                admission_key,
                admission_token,
                nx=True,
                ex=_KNOWLEDGE_ADMISSION_SECONDS,
            )
        )
    except RedisError as exc:
        raise KnowledgeIngestionQueueUnavailable(
            "Knowledge ingestion admission is unavailable."
        ) from exc
    if not admitted:
        raise KnowledgeIngestionBusy(
            "A knowledge ingestion is already queued or running for this client."
        )

    try:
        # RQ persists positional arguments in Redis. Protect the complete URL,
        # including query-string credentials, before it leaves this process.
        protected_urls = [protect_secret(url, settings=settings) for url in normalized_urls]
        job = queue.enqueue(
            process_knowledge_ingestion_task,
            client_id,
            protected_urls,
            bool(replace),
            str(actor_role or "owner")[:32],
            admission_token,
            meta={
                _KNOWLEDGE_JOB_META_KEY: {
                    "client_id": int(client_id),
                    "stage": "queued",
                    "total_pages": len(normalized_urls),
                    "failed_pages": 0,
                    "total_chunks": 0,
                }
            },
            retry=Retry(max=2, interval=[60, 180]),
            job_timeout=_KNOWLEDGE_JOB_TIMEOUT_SECONDS,
            result_ttl=24 * 60 * 60,
            failure_ttl=7 * 24 * 60 * 60,
        )
    except Exception as exc:
        _release_knowledge_admission(
            client_id=client_id,
            admission_token=admission_token,
        )
        raise KnowledgeIngestionQueueUnavailable(
            "Knowledge ingestion could not be queued."
        ) from exc
    return str(job.id)


def process_knowledge_ingestion_task(
    client_id: int,
    urls: list[str],
    replace: bool,
    actor_role: str,
    admission_token: str,
) -> dict[str, Any]:
    """Fetch and extract owner-managed URLs outside the API process."""

    # Plaintext arguments remain accepted for direct calls and jobs queued by
    # an older release. New jobs contain Fernet-protected values.
    urls = [reveal_secret(url) for url in urls]
    _update_current_knowledge_job_meta(
        stage="running",
        total_pages=len(urls),
        failed_pages=0,
        total_chunks=0,
    )

    if _knowledge_ingestion_cancelled(admission_token) or not _claim_or_renew_knowledge_admission(
        client_id=client_id,
        admission_token=admission_token,
    ):
        result = {
            "status": "skipped",
            "reason": "superseded_knowledge_ingestion",
            "client_id": client_id,
        }
        _update_current_knowledge_job_meta(stage="skipped")
        return result

    completed = False
    try:
        SessionLocal = get_session_factory()
        with SessionLocal() as db:
            client = db.get(Client, client_id)
            if client is None or not client.is_active:
                completed = True
                result = {
                    "status": "skipped",
                    "reason": "client_not_found_or_inactive",
                    "client_id": client_id,
                }
                _update_current_knowledge_job_meta(stage="skipped")
                return result
            resolved_client_id = int(client.id)
            # End the read transaction before any remote I/O. The ingestion
            # service intentionally performs all fetching before its first DB
            # query, so the worker does not hold a connection while a site is slow.
            db.rollback()

            extraction = KnowledgeIngestionService().ingest_urls(
                db=db,
                client_id=resolved_client_id,
                urls=validate_ingestion_urls(urls),
                replace=bool(replace),
            )
            # Serialize the final cancellation decision with an explicit purge.
            # The clear endpoint locks the same tenant row, so either this crawl
            # commits first and is then deleted, or the purge wins and this
            # transaction rolls back without repopulating knowledge.
            db.scalar(
                select(Client.id)
                .where(Client.id == resolved_client_id)
                .with_for_update()
            )
            if _knowledge_ingestion_cancelled(
                admission_token
            ) or not _claim_or_renew_knowledge_admission(
                client_id=resolved_client_id,
                admission_token=admission_token,
            ):
                db.rollback()
                completed = True
                result = {
                    "status": "skipped",
                    "reason": "superseded_knowledge_ingestion",
                    "client_id": resolved_client_id,
                }
                _update_current_knowledge_job_meta(stage="skipped")
                return result
            pages = list(extraction.get("pages") or [])
            failed_pages = sum(1 for page in pages if page.get("status") != "ok")
            result_status = (
                "ok"
                if failed_pages == 0
                else ("partial" if failed_pages < len(pages) else "failed")
            )
            db.add(
                AuditLog(
                    client_id=resolved_client_id,
                    lead_id=None,
                    event_type="knowledge_urls_ingested",
                    decision={
                        "status": result_status,
                        "url_count": len(urls),
                        "url_hosts": _knowledge_url_hosts(urls),
                        "request_sha256": _knowledge_request_fingerprint(urls),
                        "replace": bool(replace),
                        "total_pages": int(extraction.get("total_pages") or 0),
                        "failed_pages": failed_pages,
                        "total_chunks": int(extraction.get("total_chunks") or 0),
                        "actor_role": str(actor_role or "owner")[:32],
                    },
                )
            )
            db.commit()
            completed = True
            result = {
                "status": result_status,
                "client_id": resolved_client_id,
                # Keep the persisted RQ result free of source URLs and page
                # exception messages. The database remains the source of truth
                # for the extracted content shown by the existing knowledge API.
                "extraction": {
                    "total_pages": int(extraction.get("total_pages") or 0),
                    "failed_pages": failed_pages,
                    "total_chunks": int(extraction.get("total_chunks") or 0),
                },
            }
            _update_current_knowledge_job_meta(
                stage=result_status,
                total_pages=result["extraction"]["total_pages"],
                failed_pages=failed_pages,
                total_chunks=result["extraction"]["total_chunks"],
            )
            return result
    except Exception as exc:
        # Do not attach the traceback here: HTTP client exceptions can include
        # the full requested URL, including query-string credentials.
        logger.error(
            "knowledge_ingestion_worker_failed",
            extra={"client_id": client_id, "error_type": type(exc).__name__},
        )
        _update_current_knowledge_job_meta(stage="failed")
        raise
    finally:
        # Keep the token across RQ retries. A terminal worker failure expires
        # automatically, while successful/intentional completion frees the
        # tenant immediately.
        if completed:
            _release_knowledge_admission(
                client_id=client_id,
                admission_token=admission_token,
            )


def get_knowledge_ingestion_job_status(*, client_id: int, job_id: str) -> dict[str, Any]:
    """Return a tenant-scoped, redacted summary for one RQ ingestion job."""

    clean_job_id = str(job_id or "").strip()
    if not _KNOWLEDGE_JOB_ID_RE.fullmatch(clean_job_id):
        raise KnowledgeIngestionJobNotFound("Knowledge ingestion job not found.")

    redis_conn = get_redis_connection()
    if redis_conn is None:
        raise KnowledgeIngestionQueueUnavailable(
            "Knowledge ingestion queue is unavailable."
        )
    try:
        job = Job.fetch(clean_job_id, connection=redis_conn)
        raw_status = job.get_status(refresh=True)
    except NoSuchJobError as exc:
        raise KnowledgeIngestionJobNotFound(
            "Knowledge ingestion job not found."
        ) from exc
    except RedisError as exc:
        raise KnowledgeIngestionQueueUnavailable(
            "Knowledge ingestion queue is unavailable."
        ) from exc

    metadata = _knowledge_job_metadata(job)
    if _safe_nonnegative_int(metadata.get("client_id"), default=-1) != int(client_id):
        # Missing metadata and cross-tenant lookups deliberately have the same
        # response so a caller cannot enumerate another tenant's jobs.
        raise KnowledgeIngestionJobNotFound("Knowledge ingestion job not found.")

    rq_status = str(getattr(raw_status, "value", raw_status) or "").lower()
    status, terminal = _public_knowledge_job_state(rq_status=rq_status, job=job)
    counts = _knowledge_job_counts(metadata=metadata, job=job if terminal else None)
    return {
        "job_id": clean_job_id,
        "status": status,
        "terminal": terminal,
        **counts,
    }


def _knowledge_admission_key(client_id: int) -> str:
    return f"knowledge-ingestion:client:{int(client_id)}"


def _knowledge_cancellation_key(admission_token: str) -> str:
    digest = hashlib.sha256(str(admission_token or "").encode("utf-8")).hexdigest()
    return f"knowledge-ingestion:cancelled:{digest}"


def cancel_knowledge_ingestion(*, client_id: int) -> bool:
    """Cancel a queued/running tenant crawl without blocking a future crawl."""

    redis_conn = get_redis_connection()
    if redis_conn is None:
        return False
    try:
        current = redis_conn.get(_knowledge_admission_key(client_id))
        if current is None:
            return False
        token = current.decode("utf-8") if isinstance(current, bytes) else str(current)
        script = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            redis.call('SET', KEYS[2], '1', 'EX', ARGV[2])
            redis.call('DEL', KEYS[1])
            return 1
        end
        return 0
        """
        return bool(
            redis_conn.eval(
                script,
                2,
                _knowledge_admission_key(client_id),
                _knowledge_cancellation_key(token),
                token,
                _KNOWLEDGE_CANCELLATION_SECONDS,
            )
        )
    except RedisError as exc:
        logger.warning(
            "knowledge_ingestion_cancel_failed",
            extra={"client_id": client_id, "error_type": type(exc).__name__},
        )
        return False


def _knowledge_ingestion_cancelled(admission_token: str) -> bool:
    redis_conn = get_redis_connection()
    if redis_conn is None:
        return False
    try:
        return bool(redis_conn.exists(_knowledge_cancellation_key(admission_token)))
    except RedisError:
        return False


def _claim_or_renew_knowledge_admission(*, client_id: int, admission_token: str) -> bool:
    redis_conn = get_redis_connection()
    if redis_conn is None:
        raise KnowledgeIngestionQueueUnavailable(
            "Knowledge ingestion admission is unavailable."
        )
    script = """
    local current = redis.call('GET', KEYS[1])
    if not current then
        redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
        return 1
    end
    if current == ARGV[1] then
        redis.call('EXPIRE', KEYS[1], ARGV[2])
        return 1
    end
    return 0
    """
    try:
        return bool(
            redis_conn.eval(
                script,
                1,
                _knowledge_admission_key(client_id),
                admission_token,
                _KNOWLEDGE_ADMISSION_SECONDS,
            )
        )
    except RedisError as exc:
        raise KnowledgeIngestionQueueUnavailable(
            "Knowledge ingestion admission is unavailable."
        ) from exc


def _release_knowledge_admission(*, client_id: int, admission_token: str) -> None:
    redis_conn = get_redis_connection()
    if redis_conn is None:
        return
    script = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    end
    return 0
    """
    try:
        redis_conn.eval(
            script,
            1,
            _knowledge_admission_key(client_id),
            admission_token,
        )
    except RedisError as exc:
        logger.warning(
            "knowledge_ingestion_admission_release_failed",
            extra={"client_id": client_id, "error_type": type(exc).__name__},
        )


def _update_current_knowledge_job_meta(
    *,
    stage: str,
    total_pages: int | None = None,
    failed_pages: int | None = None,
    total_chunks: int | None = None,
) -> None:
    """Best-effort progress metadata; observability must not fail ingestion."""

    try:
        job = get_current_job()
        if job is None:
            return
        metadata = _knowledge_job_metadata(job)
        metadata["stage"] = str(stage or "running")[:32]
        for key, value in (
            ("total_pages", total_pages),
            ("failed_pages", failed_pages),
            ("total_chunks", total_chunks),
        ):
            if value is not None:
                metadata[key] = max(0, int(value))
        job.meta[_KNOWLEDGE_JOB_META_KEY] = metadata
        job.save_meta()
    except Exception as exc:
        # Job metadata is diagnostic only. It must never make a successful
        # extraction fail, and logs include only the exception type.
        logger.warning(
            "knowledge_ingestion_metadata_update_failed",
            extra={"error_type": type(exc).__name__},
        )


def _knowledge_job_metadata(job: Job) -> dict[str, Any]:
    raw = job.meta.get(_KNOWLEDGE_JOB_META_KEY) if isinstance(job.meta, dict) else None
    return dict(raw) if isinstance(raw, dict) else {}


def _public_knowledge_job_state(*, rq_status: str, job: Job) -> tuple[str, bool]:
    if rq_status in {"created", "queued", "deferred", "scheduled"}:
        return "queued", False
    if rq_status == "started":
        return "running", False
    if rq_status == "finished":
        result = job.return_value(refresh=False)
        result_status = str(result.get("status") or "") if isinstance(result, dict) else ""
        if result_status in {"ok", "partial", "failed", "skipped"}:
            return result_status, True
        return "completed", True
    if rq_status in {"stopped", "canceled"}:
        return "cancelled", True
    if rq_status == "failed":
        return "failed", True
    return "queued", False


def _knowledge_job_counts(*, metadata: dict[str, Any], job: Job | None) -> dict[str, int]:
    source: dict[str, Any] = metadata
    if job is not None:
        result = job.return_value(refresh=False)
        extraction = result.get("extraction") if isinstance(result, dict) else None
        if isinstance(extraction, dict):
            source = {**metadata, **extraction}
    return {
        "total_pages": _safe_nonnegative_int(source.get("total_pages")),
        "failed_pages": _safe_nonnegative_int(source.get("failed_pages")),
        "total_chunks": _safe_nonnegative_int(source.get("total_chunks")),
    }


def _safe_nonnegative_int(value: Any, *, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError, OverflowError):
        return default


def _knowledge_url_hosts(urls: list[str]) -> list[str]:
    return sorted(
        {
            str(urlparse(url).hostname or "").lower()
            for url in urls
            if str(urlparse(url).hostname or "").strip()
        }
    )


def _knowledge_request_fingerprint(urls: list[str]) -> str:
    payload = json.dumps(list(urls), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
