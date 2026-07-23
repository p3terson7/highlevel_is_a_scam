from __future__ import annotations

import re
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from time import monotonic
from zoneinfo import ZoneInfo

from redis import Redis
from redis.exceptions import RedisError

from app.db.models import Client

STOP_KEYWORDS = {
    "stop",
    "unsubscribe",
    "cancel",
    "quit",
    "end",
}
HELP_KEYWORDS = {
    "help",
    "info",
}
START_KEYWORDS = {
    "start",
    "unstop",
}

_LOCAL_RATE_LIMIT_MAX_KEYS = 10_000
_local_rate_limit_lock = Lock()
_local_rate_limit_state: OrderedDict[str, tuple[int, float]] = OrderedDict()


@dataclass
class ComplianceDecision:
    is_stop: bool = False
    is_help: bool = False
    is_start: bool = False


def _normalized_keyword(text: str) -> str:
    return re.sub(r"\s+", "", text.strip().lower())


def evaluate_text(text: str) -> ComplianceDecision:
    normalized = _normalized_keyword(text)
    return ComplianceDecision(
        is_stop=normalized in STOP_KEYWORDS,
        is_help=normalized in HELP_KEYWORDS,
        is_start=normalized in START_KEYWORDS,
    )


def within_operating_hours(client: Client, now_utc: datetime | None = None) -> bool:
    hours = client.operating_hours or {}
    tz_name = client.timezone or "UTC"

    try:
        tzinfo = ZoneInfo(tz_name)
    except Exception:
        tzinfo = timezone.utc

    now = now_utc or datetime.now(timezone.utc)
    local_now = now.astimezone(tzinfo)

    allowed_days = hours.get("days", [0, 1, 2, 3, 4])
    if local_now.weekday() not in allowed_days:
        return False

    start = str(hours.get("start", "09:00"))
    end = str(hours.get("end", "18:00"))

    try:
        start_hour, start_minute = [int(x) for x in start.split(":", maxsplit=1)]
        end_hour, end_minute = [int(x) for x in end.split(":", maxsplit=1)]
    except Exception:
        start_hour, start_minute, end_hour, end_minute = 9, 0, 18, 0

    current_minutes = local_now.hour * 60 + local_now.minute
    start_minutes = start_hour * 60 + start_minute
    end_minutes = end_hour * 60 + end_minute

    return start_minutes <= current_minutes <= end_minutes


def is_rate_limited(
    redis_client: Redis | None,
    lead_id: int,
    max_messages: int,
    window_minutes: int,
    *,
    scope: str = "inbound",
) -> bool:
    normalized_scope = re.sub(r"[^a-z0-9_-]+", "-", str(scope or "inbound").strip().lower()).strip("-")
    normalized_scope = normalized_scope or "inbound"
    key = f"rate_limit:lead:{lead_id}"
    if normalized_scope != "inbound":
        key = f"{key}:{normalized_scope}"

    if redis_client is None:
        return _is_locally_rate_limited(
            key=key,
            max_messages=max_messages,
            window_minutes=window_minutes,
        )
    try:
        count = redis_client.incr(key)
        if count == 1:
            redis_client.expire(key, max(window_minutes, 1) * 60)
        return int(count) > max_messages
    except RedisError:
        # A process-local fixed window keeps provider callbacks bounded during a
        # Redis outage. It is intentionally capped to prevent attacker-controlled
        # lead IDs from growing process memory without limit.
        return _is_locally_rate_limited(
            key=key,
            max_messages=max_messages,
            window_minutes=window_minutes,
        )


def _is_locally_rate_limited(*, key: str, max_messages: int, window_minutes: int) -> bool:
    now = monotonic()
    window_seconds = max(int(window_minutes), 1) * 60
    limit = max(int(max_messages), 0)
    with _local_rate_limit_lock:
        count, expires_at = _local_rate_limit_state.get(key, (0, now + window_seconds))
        if expires_at <= now:
            count = 0
            expires_at = now + window_seconds
        count += 1
        _local_rate_limit_state[key] = (count, expires_at)
        _local_rate_limit_state.move_to_end(key)
        while len(_local_rate_limit_state) > _LOCAL_RATE_LIMIT_MAX_KEYS:
            _local_rate_limit_state.popitem(last=False)
    return count > limit


def clear_local_rate_limit_state() -> None:
    """Clear process-local admission state for dependency/test cache resets."""

    with _local_rate_limit_lock:
        _local_rate_limit_state.clear()
