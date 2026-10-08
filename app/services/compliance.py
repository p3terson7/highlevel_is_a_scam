from __future__ import annotations

import re
from collections import OrderedDict
from dataclasses import dataclass
from threading import Lock
from time import monotonic

from redis import Redis
from redis.exceptions import RedisError


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
