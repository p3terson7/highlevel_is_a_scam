from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import re
import secrets
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.core.deps import get_app_settings
from app.core.logging import get_logger
from app.core.metrics import incr
from app.db.models import AuditLog, Client, InboundWebhookEvent
from app.db.session import get_db
from app.services.lead_intake import normalize_webhook_payload, validate_webhook_candidates
from app.services.lead_summary import filter_question_form_answers, normalize_form_answers
from app.services.runtime_config import get_effective_runtime_map_for_client, load_runtime_overrides
from app.workers.tasks import enqueue_process_webhook_event, get_redis_connection

router = APIRouter(prefix="/webhooks", tags=["webhooks"])
logger = get_logger(__name__)

_MAX_WEBHOOK_BODY_BYTES = 128 * 1024
_MAX_JSON_DEPTH = 10
_MAX_JSON_NODES = 1_000
_MAX_OBJECT_FIELDS = 100
_MAX_LIST_ITEMS = 100
_MAX_FIELD_BYTES = 8 * 1024
_HMAC_REPLAY_WINDOW_SECONDS = 300
_REPLAY_CACHE_SECONDS = 10 * 60
_RATE_WINDOW_SECONDS = 60
_RATE_REQUEST_COUNT = 60
_REPLAY_CACHE_MAX_ENTRIES = 10_000
_REPLAY_CACHE_TRIM_TO = 9_000
_LOCAL_ENVS = {"dev", "development", "local", "test"}
_CONSENT_KEYS = ("sms_consent", "consent_sms", "consent_to_sms", "sms_opt_in", "contact_by_sms")

_ADMISSION_LOCK = Lock()
_RATE_EVENTS: dict[tuple[int, str], deque[float]] = {}
_REPLAY_EXPIRY: dict[tuple[int, str, str], float] = {}


def _redis_client():
    """Indirection keeps shared admission control replaceable in tests."""

    return get_redis_connection()


def _load_client(db: Session, client_key: str) -> Client:
    client = db.scalar(select(Client).where(Client.client_key == client_key, Client.is_active.is_(True)))
    if client is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    return client


def _verify_webhook_authentication(
    *,
    request: Request,
    raw_body: bytes,
    effective_runtime: dict[str, Any],
    settings: Settings,
    now: float | None = None,
) -> str:
    webhook_secret = str(
        effective_runtime.get("crm_webhook_secret")
        or effective_runtime.get("zapier_webhook_secret")
        or ""
    ).strip()
    if not webhook_secret:
        local_environment = settings.env.strip().lower() in _LOCAL_ENVS
        if settings.allow_unsigned_crm_webhooks and local_environment:
            if _request_origin_is_loopback(request):
                return "unsigned-loopback-dev"
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Unsigned webhook requests are restricted to loopback",
            )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook authentication is not configured",
        )

    supplied_timestamp = request.headers.get("X-CRM-Webhook-Timestamp", "").strip()
    supplied_signature = request.headers.get("X-CRM-Webhook-Signature", "").strip()
    if supplied_timestamp or supplied_signature:
        if not supplied_timestamp or not supplied_signature:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Incomplete webhook signature")
        if re.fullmatch(r"[0-9]{1,12}", supplied_timestamp) is None:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid webhook timestamp")
        try:
            timestamp = int(supplied_timestamp)
        except (TypeError, ValueError, UnicodeError) as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid webhook timestamp") from exc
        current_time = int(time.time() if now is None else now)
        if abs(current_time - timestamp) > _HMAC_REPLAY_WINDOW_SECONDS:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Expired webhook signature")
        signature_hex = supplied_signature.removeprefix("sha256=").strip().lower()
        expected = hmac.new(
            webhook_secret.encode("utf-8"),
            supplied_timestamp.encode("ascii") + b"." + raw_body,
            hashlib.sha256,
        ).hexdigest()
        if not secrets.compare_digest(signature_hex, expected):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid webhook signature")
        return "hmac-sha256"

    # Compatibility for existing server-to-server integrations. Secrets are
    # deliberately header-only so they do not leak through URLs or proxy logs.
    provided_secret = (
        request.headers.get("X-CRM-Webhook-Secret")
        or request.headers.get("X-Zapier-Webhook-Secret")
        or request.headers.get("X-Zapier-Token")
        or ""
    ).strip()
    if not secrets.compare_digest(provided_secret, webhook_secret):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid webhook secret")
    return "legacy-header"


def _request_origin_is_loopback(request: Request) -> bool:
    client = getattr(request, "client", None)
    host = str(getattr(client, "host", "") or "").strip()
    if not host:
        return False
    try:
        address = ipaddress.ip_address(host.split("%", 1)[0])
    except ValueError:
        return False
    if address.is_loopback:
        return True
    mapped = getattr(address, "ipv4_mapped", None)
    return bool(mapped and mapped.is_loopback)


async def _bounded_request_body(request: Request) -> bytes:
    content_length = request.headers.get("content-length", "").strip()
    if content_length:
        try:
            declared_length = int(content_length)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Content-Length") from exc
        if declared_length < 0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Content-Length")
        if declared_length > _MAX_WEBHOOK_BODY_BYTES:
            raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="Webhook payload is too large")

    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > _MAX_WEBHOOK_BODY_BYTES:
            raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="Webhook payload is too large")
        chunks.append(chunk)
    return b"".join(chunks)


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise ValueError(f"Duplicate JSON field: {key}")
        output[key] = value
    return output


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"Non-standard JSON value: {value}")


def _parse_bounded_json(raw_body: bytes) -> dict[str, Any]:
    if not raw_body:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="JSON payload is required")
    try:
        payload = json.loads(
            raw_body,
            object_pairs_hook=_strict_json_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Payload must be a JSON object")

    nodes = 0
    stack: list[tuple[Any, int]] = [(payload, 0)]
    while stack:
        value, depth = stack.pop()
        nodes += 1
        if nodes > _MAX_JSON_NODES:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Webhook payload has too many fields")
        if depth > _MAX_JSON_DEPTH:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Webhook payload is nested too deeply")
        if isinstance(value, dict):
            if len(value) > _MAX_OBJECT_FIELDS:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Webhook object has too many fields")
            for key, item in value.items():
                if len(str(key).encode("utf-8")) > 128:
                    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Webhook field name is too long")
                stack.append((item, depth + 1))
        elif isinstance(value, list):
            if len(value) > _MAX_LIST_ITEMS:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Webhook list has too many items")
            stack.extend((item, depth + 1) for item in value)
        elif isinstance(value, str) and len(value.encode("utf-8")) > _MAX_FIELD_BYTES:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Webhook field value is too long")
    return payload


def _check_webhook_rate(*, client_id: int, endpoint: str, now: float | None = None) -> None:
    if now is None:
        redis_conn = _redis_client()
        if redis_conn is not None:
            current_ms = int(time.time() * 1000)
            key = f"webhook-admission:rate:{client_id}:{endpoint}"
            member = f"{current_ms}:{secrets.token_hex(8)}"
            try:
                pipeline = redis_conn.pipeline(transaction=True)
                pipeline.zremrangebyscore(key, 0, current_ms - (_RATE_WINDOW_SECONDS * 1000))
                pipeline.zadd(key, {member: current_ms})
                pipeline.zcard(key)
                pipeline.expire(key, _RATE_WINDOW_SECONDS * 2)
                _, _, request_count, _ = pipeline.execute()
                if int(request_count) > _RATE_REQUEST_COUNT:
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail="Webhook request rate exceeded",
                        headers={"Retry-After": str(_RATE_WINDOW_SECONDS)},
                    )
                return
            except HTTPException:
                raise
            except RedisError as exc:
                logger.warning(
                    "webhook_shared_rate_limit_unavailable",
                    extra={"client_id": client_id, "endpoint": endpoint, "error_type": type(exc).__name__},
                )

    current = time.monotonic() if now is None else now
    key = (client_id, endpoint)
    with _ADMISSION_LOCK:
        events = _RATE_EVENTS.setdefault(key, deque())
        cutoff = current - _RATE_WINDOW_SECONDS
        while events and events[0] <= cutoff:
            events.popleft()
        if len(events) >= _RATE_REQUEST_COUNT:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Webhook request rate exceeded",
                headers={"Retry-After": str(_RATE_WINDOW_SECONDS)},
            )
        events.append(current)


def _reserve_webhook_replay(
    *, client_id: int, endpoint: str, raw_body: bytes, now: float | None = None
) -> tuple[str, bool]:
    fingerprint = hashlib.sha256(raw_body).hexdigest()
    if now is None:
        redis_conn = _redis_client()
        if redis_conn is not None:
            redis_key = f"webhook-admission:replay:{client_id}:{endpoint}:{fingerprint}"
            try:
                reserved = redis_conn.set(
                    redis_key,
                    "1",
                    nx=True,
                    ex=_REPLAY_CACHE_SECONDS,
                )
                return fingerprint, not bool(reserved)
            except RedisError as exc:
                logger.warning(
                    "webhook_shared_replay_cache_unavailable",
                    extra={"client_id": client_id, "endpoint": endpoint, "error_type": type(exc).__name__},
                )

    current = time.monotonic() if now is None else now
    key = (client_id, endpoint, fingerprint)
    with _ADMISSION_LOCK:
        expiry = _REPLAY_EXPIRY.get(key, 0)
        if expiry > current:
            return fingerprint, True
        _REPLAY_EXPIRY[key] = current + _REPLAY_CACHE_SECONDS
        if len(_REPLAY_EXPIRY) > _REPLAY_CACHE_MAX_ENTRIES:
            expired = [cache_key for cache_key, cached_expiry in _REPLAY_EXPIRY.items() if cached_expiry <= current]
            for cache_key in expired:
                _REPLAY_EXPIRY.pop(cache_key, None)
            overflow = len(_REPLAY_EXPIRY) - _REPLAY_CACHE_TRIM_TO
            if overflow > 0:
                oldest = sorted(_REPLAY_EXPIRY, key=_REPLAY_EXPIRY.get)[:overflow]
                for cache_key in oldest:
                    _REPLAY_EXPIRY.pop(cache_key, None)
    return fingerprint, False


def _release_webhook_replay(*, client_id: int, endpoint: str, fingerprint: str) -> None:
    redis_conn = _redis_client()
    if redis_conn is not None:
        try:
            redis_conn.delete(f"webhook-admission:replay:{client_id}:{endpoint}:{fingerprint}")
        except RedisError as exc:
            logger.warning(
                "webhook_shared_replay_release_failed",
                extra={"client_id": client_id, "endpoint": endpoint, "error_type": type(exc).__name__},
            )
    with _ADMISSION_LOCK:
        _REPLAY_EXPIRY.pop((client_id, endpoint, fingerprint), None)


def _reset_webhook_admission_state() -> None:
    """Clear process-local admission state for isolated tests."""
    with _ADMISSION_LOCK:
        _RATE_EVENTS.clear()
        _REPLAY_EXPIRY.clear()


def _question_answer_rows(answers: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"question": key, "answer": value} for key, value in answers.items()]


def _consent_fields(*sources: Any) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for source in sources:
        if not isinstance(source, dict):
            continue
        if isinstance(source.get("consent"), dict):
            fields.setdefault("consent", source["consent"])
        for key in _CONSENT_KEYS:
            if key in source:
                fields.setdefault(key, source[key])
    return fields


def _merge_dicts(*values: Any) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for value in values:
        if isinstance(value, dict):
            merged.update({str(key): item for key, item in value.items()})
    return merged


def _deep_merge_dicts(*values: Any) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for value in values:
        if not isinstance(value, dict):
            continue
        for raw_key, item in value.items():
            key = str(raw_key)
            if isinstance(item, dict) and isinstance(merged.get(key), dict):
                merged[key] = _deep_merge_dicts(merged[key], item)
            else:
                merged[key] = item
    return merged


def _assign_dotted_value(target: dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = [part.strip() for part in dotted_key.split(".") if part.strip()]
    if not parts:
        return
    cursor = target
    for part in parts[:-1]:
        existing = cursor.get(part)
        if not isinstance(existing, dict):
            existing = {}
            cursor[part] = existing
        cursor = existing
    cursor[parts[-1]] = value


def _expand_dotted_payload(payload: dict[str, Any]) -> dict[str, Any]:
    expanded: dict[str, Any] = {}
    for raw_key, value in payload.items():
        key = str(raw_key).strip()
        if "." in key:
            _assign_dotted_value(expanded, key, value)
        else:
            expanded[key] = value
    return expanded


def _parse_key_value_blob(blob: Any) -> dict[str, Any]:
    if not isinstance(blob, str):
        return {}

    parsed: dict[str, Any] = {}
    for raw_line in blob.splitlines():
        line = raw_line.strip()
        if not line or "=" not in line:
            continue
        raw_key, raw_value = line.split("=", maxsplit=1)
        key = raw_key.strip()
        value = raw_value.strip()
        if not key:
            continue
        if "." in key:
            _assign_dotted_value(parsed, key, value)
        else:
            parsed[key] = value
    return parsed


def _parse_json_object_blob(blob: Any) -> dict[str, Any]:
    if not isinstance(blob, str) or not blob.strip().startswith("{"):
        return {}
    try:
        parsed = json.loads(blob)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _tracking_from_payload(payload: dict[str, Any], answers: dict[str, Any]) -> dict[str, Any]:
    tracking = _merge_dicts(payload.get("tracking"), payload.get("utm"), payload.get("utms"))
    for key, value in payload.items():
        if str(key).startswith("utm_") or key in {"gclid", "fbclid", "li_fat_id", "msclkid", "ad_id"}:
            tracking[str(key)] = value
    for key, value in answers.items():
        if str(key).startswith("utm_") or key in {"gclid", "fbclid", "li_fat_id", "msclkid", "ad_id"}:
            tracking.setdefault(str(key), value)
    return normalize_form_answers(tracking)


def _source_from_tracking(payload: dict[str, Any], tracking: dict[str, Any]) -> str:
    raw_source = str(
        payload.get("source")
        or payload.get("lead_source")
        or tracking.get("utm_source")
        or tracking.get("source")
        or ""
    ).strip().lower()
    if "linkedin" in raw_source or raw_source in {"li", "linkedin_ads", "linkedin ads"}:
        return "linkedin"
    if raw_source in {"meta", "facebook", "fb", "instagram", "ig"} or "facebook" in raw_source or "instagram" in raw_source:
        return "meta"
    return "manual"


def _first_payload_value(*sources: Any, keys: tuple[str, ...]) -> str:
    for source in sources:
        if not isinstance(source, dict):
            continue
        for key in keys:
            value = source.get(key)
            if value not in (None, ""):
                return str(value).strip()
    return ""


def _lead_identity_from_payload(
    payload: dict[str, Any],
    lead_payload: dict[str, Any],
    raw_answers: dict[str, Any],
) -> dict[str, Any]:
    first_name = _first_payload_value(lead_payload, payload, raw_answers, keys=("first_name",))
    last_name = _first_payload_value(lead_payload, payload, raw_answers, keys=("last_name",))
    full_name = _first_payload_value(
        lead_payload,
        payload,
        raw_answers,
        keys=("full_name", "name", "contact_name"),
    )
    if full_name and last_name and last_name.lower() not in full_name.lower():
        full_name = f"{full_name} {last_name}".strip()
    elif not full_name:
        full_name = " ".join(part for part in (first_name, last_name) if part).strip()

    identity = {
        "full_name": full_name,
        "phone": _first_payload_value(
            lead_payload,
            payload,
            raw_answers,
            keys=("phone", "phone_number", "mobile_phone", "cell", "mobile"),
        ),
        "email": _first_payload_value(lead_payload, payload, raw_answers, keys=("email", "email_address")),
        "city": _first_payload_value(lead_payload, payload, raw_answers, keys=("city", "location_city", "location")),
    }
    return {key: value for key, value in identity.items() if value}


def _external_lead_id_from_payload(
    payload: dict[str, Any],
    lead_payload: dict[str, Any],
    raw_answers: dict[str, Any],
) -> str:
    return _first_payload_value(
        lead_payload,
        payload,
        raw_answers,
        keys=(
            "id",
            "lead_id",
            "external_lead_id",
            "leadgen_id",
            "lead_gen_id",
            "linkedin_lead_id",
            "lead_gen_form_response_id",
            "lead_gen_form_response",
        ),
    )


def _coerce_website_form_payload(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    payload = _deep_merge_dicts(
        _parse_json_object_blob(payload.get("")),
        _parse_key_value_blob(payload.get("")),
        _expand_dotted_payload(payload),
    )
    lead_payload = payload.get("lead") if isinstance(payload.get("lead"), dict) else {}
    form_answers = _merge_dicts(
        payload.get("form_answers"),
        payload.get("fields"),
        payload.get("answers"),
        lead_payload.get("form_answers"),
    )
    raw_form_answers = normalize_form_answers(form_answers)
    tracking = _tracking_from_payload(payload, raw_form_answers)
    clean_form_answers = filter_question_form_answers(raw_form_answers)
    external_lead_id = _external_lead_id_from_payload(payload, lead_payload, raw_form_answers)
    lead_identity = _lead_identity_from_payload(payload, lead_payload, raw_form_answers)

    source = _source_from_tracking(payload, tracking)
    normalized = {
        "lead": {
            "id": external_lead_id or None,
            **lead_identity,
            **_consent_fields(lead_payload, payload, raw_form_answers),
            "form_answers": clean_form_answers,
            "submitted_form_answers": _question_answer_rows(clean_form_answers),
            "tracking": tracking,
            "source_page_url": payload.get("source_page_url") or payload.get("page_url") or payload.get("url") or "",
            "referrer": payload.get("referrer") or payload.get("referrer_url") or "",
            "raw_website_payload": payload,
        }
    }
    return source, normalized


def _parse_zapier_label_blob(blob: Any) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    answers: dict[str, Any] = {}
    rows: list[dict[str, Any]] = []
    if not isinstance(blob, str):
        return answers, rows
    for raw_key, raw_value in re.findall(r'([^:,][^:]*?)\s*:\s*"([^"]*)"', blob):
        key = raw_key.strip()
        value = raw_value.strip()
        if not key or value == "":
            continue
        answers.setdefault(key, value)
        rows.append({"question": key, "answer": value})
        normalized_key = re.sub(r"[^a-z0-9]+", "_", key.lower()).strip("_")
        if normalized_key:
            answers.setdefault(normalized_key, value)
    return answers, rows


def _coerce_zapier_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if any(key in payload for key in ("entry", "lead", "leads")):
        return payload

    leadgen_id = str(
        payload.get("leadgen_id")
        or payload.get("lead_id")
        or payload.get("id")
        or payload.get("external_lead_id")
        or ""
    ).strip()

    if isinstance(payload.get("field_data"), list):
        return {
            "entry": [
                {
                    "changes": [
                        {
                            "value": {
                                "leadgen_id": leadgen_id,
                                "field_data": payload["field_data"],
                                **_consent_fields(payload),
                            }
                        }
                    ]
                }
            ]
        }

    form_answers: dict[str, Any] = {}
    original_answers: list[dict[str, Any]] = []

    # Some Zapier setups send a single text blob under an empty key:
    # {"": "Full Name : \"...\", Email : \"...\", ..."}
    blob_answers, blob_rows = _parse_zapier_label_blob(payload.get(""))
    form_answers.update(blob_answers)
    original_answers.extend(blob_rows)

    if isinstance(payload.get("form_answers"), dict):
        form_answers.update(payload["form_answers"])
        original_answers.extend(
            {"question": key, "answer": value} for key, value in payload["form_answers"].items()
        )
    if isinstance(payload.get("fields"), dict):
        form_answers.update(payload["fields"])
        original_answers.extend({"question": key, "answer": value} for key, value in payload["fields"].items())

    for key in (
        "full_name",
        "name",
        "first_name",
        "phone_number",
        "phone",
        "mobile_phone",
        "email",
        "email_address",
        "city",
        "location_city",
    ):
        value = payload.get(key)
        if value not in (None, ""):
            form_answers.setdefault(key, value)
            original_answers.append({"question": key, "answer": value})

    if not form_answers:
        for key, value in payload.items():
            if key in {"leadgen_id", "lead_id", "id", "external_lead_id"}:
                continue
            if isinstance(value, (str, int, float, bool)):
                form_answers[key] = value
                original_answers.append({"question": key, "answer": value})

    raw_form_answers = normalize_form_answers(form_answers)
    clean_form_answers = filter_question_form_answers(raw_form_answers)
    clean_original_answers = [
        row
        for row in original_answers
        if filter_question_form_answers({row.get("question"): row.get("answer")})
    ]

    lead_payload: dict[str, Any] = {
        **_lead_identity_from_payload(payload, {}, raw_form_answers),
        **_consent_fields(payload, raw_form_answers),
        "form_answers": clean_form_answers,
        "submitted_form_answers": clean_original_answers or _question_answer_rows(clean_form_answers),
    }
    if leadgen_id:
        lead_payload["id"] = leadgen_id
    elif external_lead_id := _external_lead_id_from_payload(payload, {}, raw_form_answers):
        lead_payload["id"] = external_lead_id
    return {"lead": lead_payload}


def _zapier_lead_source(payload: dict[str, Any]) -> str:
    expanded = _expand_dotted_payload(payload)
    lead_payload = expanded.get("lead") if isinstance(expanded.get("lead"), dict) else {}
    blob_answers, _ = _parse_zapier_label_blob(payload.get(""))
    mappings = [
        expanded,
        lead_payload,
        expanded.get("form_answers"),
        expanded.get("fields"),
        expanded.get("answers"),
        lead_payload.get("form_answers"),
        lead_payload.get("fields"),
        lead_payload.get("answers"),
        blob_answers,
        _parse_key_value_blob(payload.get("")),
        _parse_json_object_blob(payload.get("")),
    ]
    candidates: list[Any] = []
    for mapping in mappings:
        normalized = normalize_form_answers(mapping) if isinstance(mapping, dict) else {}
        tracking = _tracking_from_payload(normalized, normalized)
        candidates.extend(
            (
                normalized.get("source"),
                normalized.get("lead_source"),
                tracking.get("utm_source"),
                tracking.get("source"),
            )
        )
    for candidate in candidates:
        detected = _source_from_tracking({"source": candidate}, {})
        if detected in {"meta", "linkedin"}:
            return detected
    # Historic Zapier intake used the Meta-shaped normalizer. Preserve that
    # compatibility when the payload carries no recognizable attribution.
    return "meta"


async def _authenticated_webhook_payload(
    *,
    request: Request,
    client: Client,
    endpoint: str,
    effective_runtime: dict[str, Any],
    settings: Settings,
) -> tuple[dict[str, Any], bytes, str]:
    raw_body = await _bounded_request_body(request)
    authentication = _verify_webhook_authentication(
        request=request,
        raw_body=raw_body,
        effective_runtime=effective_runtime,
        settings=settings,
    )
    _check_webhook_rate(client_id=client.id, endpoint=endpoint)
    return _parse_bounded_json(raw_body), raw_body, authentication


def _validated_candidate_stats(source: str, normalized_payload: dict[str, Any]) -> dict[str, int]:
    try:
        return validate_webhook_candidates(normalize_webhook_payload(source=source, payload=normalized_payload))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc


def _latest_inbound_event(
    *,
    db: Session,
    client_id: int,
    endpoint: str,
    fingerprint: str,
) -> InboundWebhookEvent | None:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=_REPLAY_CACHE_SECONDS)
    return db.scalar(
        select(InboundWebhookEvent)
        .where(
            InboundWebhookEvent.client_id == client_id,
            InboundWebhookEvent.endpoint == endpoint,
            InboundWebhookEvent.payload_sha256 == fingerprint,
            InboundWebhookEvent.created_at >= cutoff,
        )
        .order_by(InboundWebhookEvent.created_at.desc(), InboundWebhookEvent.id.desc())
        .limit(1)
    )


def _persist_inbound_event(
    *,
    db: Session,
    client: Client,
    endpoint: str,
    source: str,
    normalized_payload: dict[str, Any],
    fingerprint: str,
    authentication: str,
    payload_bytes: int,
    candidate_stats: dict[str, int],
) -> tuple[InboundWebhookEvent, bool]:
    existing = _latest_inbound_event(
        db=db,
        client_id=client.id,
        endpoint=endpoint,
        fingerprint=fingerprint,
    )
    if existing is not None:
        return existing, False

    replay_bucket = int(time.time()) // _REPLAY_CACHE_SECONDS
    event = InboundWebhookEvent(
        client_id=client.id,
        endpoint=endpoint,
        source=source,
        event_key=f"{endpoint}:{fingerprint}:{replay_bucket}",
        payload_sha256=fingerprint,
        payload_json=normalized_payload,
        status="pending",
    )
    db.add(event)
    try:
        db.flush()
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=None,
                event_type=(
                    "website_form_webhook_received"
                    if endpoint == "form"
                    else "zapier_webhook_received"
                ),
                decision={
                    "status": "accepted",
                    "event_id": event.id,
                    "queued_source": source,
                    "authentication": authentication,
                    "payload_sha256": fingerprint,
                    "payload_bytes": payload_bytes,
                    **candidate_stats,
                },
            )
        )
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = _latest_inbound_event(
            db=db,
            client_id=client.id,
            endpoint=endpoint,
            fingerprint=fingerprint,
        )
        if existing is None:
            raise
        return existing, False
    db.refresh(event)
    return event, True


def _dispatch_inbound_event(
    *,
    db: Session,
    event: InboundWebhookEvent,
    created: bool,
    client_id: int,
    endpoint: str,
    fingerprint: str,
) -> None:
    _ = created
    db.refresh(event)
    if event.status == "completed":
        return
    if event.status == "processing":
        updated_at = event.updated_at
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        if updated_at > datetime.now(timezone.utc) - timedelta(minutes=5):
            return
    if event.status not in {"queued", "dispatching"}:
        event.status = "queued"
        event.error_detail = ""
        event.updated_at = datetime.now(timezone.utc)
        db.commit()
    try:
        enqueue_process_webhook_event(event.id)
    except Exception as exc:
        db.rollback()
        stored = db.get(InboundWebhookEvent, event.id)
        if stored is not None:
            stored.status = "pending"
            stored.error_detail = f"Queue handoff failed: {type(exc).__name__}"[:500]
            stored.updated_at = datetime.now(timezone.utc)
            db.commit()
        _release_webhook_replay(
            client_id=client_id,
            endpoint=endpoint,
            fingerprint=fingerprint,
        )
        raise


@router.post("/form/{client_key}", status_code=status.HTTP_202_ACCEPTED)
async def website_form_webhook(
    client_key: str,
    request: Request,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
) -> dict[str, Any]:
    client = _load_client(db, client_key)
    effective_runtime = get_effective_runtime_map_for_client(
        settings=settings,
        overrides=load_runtime_overrides(db),
        client=client,
    )
    payload, raw_body, authentication = await _authenticated_webhook_payload(
        request=request,
        client=client,
        endpoint="form",
        effective_runtime=effective_runtime,
        settings=settings,
    )
    source, normalized_payload = _coerce_website_form_payload(payload)
    candidate_stats = _validated_candidate_stats(source, normalized_payload)
    fingerprint, duplicate = _reserve_webhook_replay(
        client_id=client.id,
        endpoint="form",
        raw_body=raw_body,
    )
    try:
        event, created = _persist_inbound_event(
            db=db,
            client=client,
            endpoint="form",
            source=source,
            normalized_payload=normalized_payload,
            fingerprint=fingerprint,
            authentication=authentication,
            payload_bytes=len(raw_body),
            candidate_stats=candidate_stats,
        )
        _dispatch_inbound_event(
            db=db,
            event=event,
            created=created,
            client_id=client.id,
            endpoint="form",
            fingerprint=fingerprint,
        )
    except Exception:
        db.rollback()
        _release_webhook_replay(client_id=client.id, endpoint="form", fingerprint=fingerprint)
        raise
    if duplicate or not created:
        return {
            "status": "accepted",
            "source": source,
            "client_key": client_key,
            "event_id": event.id,
            "duplicate": True,
        }
    incr("leads_received_total")
    return {
        "status": "accepted",
        "source": source,
        "client_key": client_key,
        "event_id": event.id,
    }


def _retired_webhook(integration: str, client_key: str) -> None:
    _ = client_key
    raise HTTPException(
        status_code=status.HTTP_410_GONE,
        detail=(
            f"{integration} webhook integration has been retired. "
            "Use the website form or Zapier webhook endpoint instead."
        ),
    )


@router.get("/meta/{client_key}")
def retired_meta_webhook_verification(client_key: str) -> None:
    _retired_webhook("Meta", client_key)


@router.post("/meta/{client_key}")
def retired_meta_webhook(client_key: str) -> None:
    _retired_webhook("Meta", client_key)


@router.post("/zapier/{client_key}", status_code=status.HTTP_202_ACCEPTED)
async def zapier_webhook(
    client_key: str,
    request: Request,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
) -> dict[str, Any]:
    client = _load_client(db, client_key)
    effective_runtime = get_effective_runtime_map_for_client(
        settings=settings,
        overrides=load_runtime_overrides(db),
        client=client,
    )
    payload, raw_body, authentication = await _authenticated_webhook_payload(
        request=request,
        client=client,
        endpoint="zapier",
        effective_runtime=effective_runtime,
        settings=settings,
    )
    queued_source = _zapier_lead_source(payload)
    normalized_payload = (
        payload
        if queued_source == "linkedin" and isinstance(payload.get("elements"), list)
        else _coerce_zapier_payload(payload)
    )
    candidate_stats = _validated_candidate_stats(queued_source, normalized_payload)
    fingerprint, duplicate = _reserve_webhook_replay(
        client_id=client.id,
        endpoint="zapier",
        raw_body=raw_body,
    )
    try:
        event, created = _persist_inbound_event(
            db=db,
            client=client,
            endpoint="zapier",
            source=queued_source,
            normalized_payload=normalized_payload,
            fingerprint=fingerprint,
            authentication=authentication,
            payload_bytes=len(raw_body),
            candidate_stats=candidate_stats,
        )
        _dispatch_inbound_event(
            db=db,
            event=event,
            created=created,
            client_id=client.id,
            endpoint="zapier",
            fingerprint=fingerprint,
        )
    except Exception:
        db.rollback()
        _release_webhook_replay(client_id=client.id, endpoint="zapier", fingerprint=fingerprint)
        raise
    if duplicate or not created:
        return {
            "status": "accepted",
            "source": "zapier",
            "queued_source": queued_source,
            "client_key": client_key,
            "event_id": event.id,
            "duplicate": True,
        }
    incr("leads_received_total")
    return {
        "status": "accepted",
        "source": "zapier",
        "queued_source": queued_source,
        "client_key": client_key,
        "event_id": event.id,
    }


@router.post("/linkedin/{client_key}")
def retired_linkedin_webhook(client_key: str) -> None:
    _retired_webhook("LinkedIn", client_key)
