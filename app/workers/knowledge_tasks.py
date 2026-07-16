from __future__ import annotations

import hashlib
import json
import secrets
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

from redis.exceptions import RedisError
from rq import Queue, Retry

from app.core.config import get_settings
from app.core.logging import get_logger
from app.db.models import AuditLog, Client
from app.db.session import get_session_factory
from app.services.knowledge import KnowledgeIngestionService, validate_ingestion_urls
from app.workers.tasks import get_redis_connection

logger = get_logger(__name__)

_KNOWLEDGE_QUEUE_NAME = "knowledge"
_KNOWLEDGE_JOB_TIMEOUT_SECONDS = 300
_KNOWLEDGE_ADMISSION_SECONDS = 30 * 60


class KnowledgeIngestionBusy(RuntimeError):
    pass


class KnowledgeIngestionQueueUnavailable(RuntimeError):
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
        job = queue.enqueue(
            process_knowledge_ingestion_task,
            client_id,
            normalized_urls,
            bool(replace),
            str(actor_role or "owner")[:32],
            admission_token,
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

    if not _claim_or_renew_knowledge_admission(
        client_id=client_id,
        admission_token=admission_token,
    ):
        return {
            "status": "skipped",
            "reason": "superseded_knowledge_ingestion",
            "client_id": client_id,
        }

    completed = False
    try:
        SessionLocal = get_session_factory()
        with SessionLocal() as db:
            client = db.get(Client, client_id)
            if client is None or not client.is_active:
                completed = True
                return {
                    "status": "skipped",
                    "reason": "client_not_found_or_inactive",
                    "client_id": client_id,
                }

            extraction = KnowledgeIngestionService().ingest_urls(
                db=db,
                client_id=client.id,
                urls=validate_ingestion_urls(urls),
                replace=bool(replace),
            )
            pages = list(extraction.get("pages") or [])
            failed_pages = sum(1 for page in pages if page.get("status") != "ok")
            result_status = (
                "ok"
                if failed_pages == 0
                else ("partial" if failed_pages < len(pages) else "failed")
            )
            db.add(
                AuditLog(
                    client_id=client.id,
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
            return {
                "status": result_status,
                "client_id": client.id,
                "extraction": extraction,
            }
    except Exception as exc:
        # Do not attach the traceback here: HTTP client exceptions can include
        # the full requested URL, including query-string credentials.
        logger.error(
            "knowledge_ingestion_worker_failed",
            extra={"client_id": client_id, "error_type": type(exc).__name__},
        )
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


def _knowledge_admission_key(client_id: int) -> str:
    return f"knowledge-ingestion:client:{int(client_id)}"


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
