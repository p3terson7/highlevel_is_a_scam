from __future__ import annotations

import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from functools import lru_cache
from threading import Lock
from uuid import uuid4

from redis import Redis
from redis.exceptions import RedisError

from app.core.config import Settings


_LOCAL_ENVIRONMENTS = {"dev", "development", "local", "test", "testing"}
_MAX_LOCAL_SCOPES = 10_000
_LOCAL_LOCK = Lock()


@dataclass
class _LocalWindow:
    window_seconds: int
    reservations: deque[tuple[float, str]]


_LOCAL_WINDOWS: OrderedDict[str, _LocalWindow] = OrderedDict()

_ATOMIC_SLIDING_WINDOW_LUA = """
local redis_time = redis.call('TIME')
local now_us = (tonumber(redis_time[1]) * 1000000) + tonumber(redis_time[2])
local window_us = tonumber(ARGV[2]) * 1000000
local cutoff_us = now_us - window_us

redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', cutoff_us)

if redis.call('ZSCORE', KEYS[1], ARGV[3]) then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
    return 1
end

if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[1]) then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
    return 0
end

redis.call('ZADD', KEYS[1], now_us, ARGV[3])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
"""


@dataclass(frozen=True)
class SandboxAdmission:
    admitted: bool
    reason: str
    backend: str
    reservation_id: str


def admit_sandbox_action(
    *,
    settings: Settings,
    client_id: int,
    action: str,
    limit: int,
    window_seconds: int,
    reservation_id: str | None = None,
) -> SandboxAdmission:
    """Atomically reserve one tenant-scoped sandbox action.

    Redis is authoritative outside explicitly local/test environments. Reusing
    a reservation ID is idempotent so one logical admission is never counted
    twice if the caller has to repeat the reservation step.
    """

    bounded_limit = max(1, int(limit))
    bounded_window = max(1, int(window_seconds))
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"start", "message"}:
        raise ValueError("Unsupported sandbox admission action")

    member = str(reservation_id or uuid4().hex).strip()
    if not member:
        raise ValueError("Sandbox admission reservation ID is required")
    scope = f"sandbox-admission:{int(client_id)}:{normalized_action}"

    redis_client = _redis_client(settings.redis_url)
    if redis_client is not None:
        try:
            result = int(
                redis_client.eval(
                    _ATOMIC_SLIDING_WINDOW_LUA,
                    1,
                    scope,
                    bounded_limit,
                    bounded_window,
                    member,
                )
            )
            if result == 1:
                return SandboxAdmission(True, "admitted", "redis", member)
            if result == 0:
                return SandboxAdmission(False, "limit_exceeded", "redis", member)
        except (RedisError, TypeError, ValueError):
            # Do not log Redis exception strings: connection URLs may contain
            # credentials. Deployed callers fail closed below.
            pass

    if settings.env.strip().lower() not in _LOCAL_ENVIRONMENTS:
        return SandboxAdmission(
            False,
            "coordination_unavailable",
            "unavailable",
            member,
        )

    local_reason = _reserve_locally(
        scope=scope,
        limit=bounded_limit,
        window_seconds=bounded_window,
        reservation_id=member,
    )
    return SandboxAdmission(
        local_reason == "admitted",
        local_reason,
        "local" if local_reason != "coordination_unavailable" else "unavailable",
        member,
    )


@lru_cache(maxsize=4)
def _redis_client(redis_url: str) -> Redis | None:
    try:
        return Redis.from_url(
            redis_url,
            socket_connect_timeout=0.15,
            socket_timeout=0.15,
            retry_on_timeout=False,
        )
    except Exception:
        return None


def _reserve_locally(
    *,
    scope: str,
    limit: int,
    window_seconds: int,
    reservation_id: str,
) -> str:
    now = time.monotonic()
    with _LOCAL_LOCK:
        bucket = _LOCAL_WINDOWS.get(scope)
        if bucket is None:
            _prune_empty_local_windows(now=now)
            if len(_LOCAL_WINDOWS) >= _MAX_LOCAL_SCOPES:
                return "coordination_unavailable"
            bucket = _LocalWindow(
                window_seconds=window_seconds,
                reservations=deque(),
            )
            _LOCAL_WINDOWS[scope] = bucket
        else:
            _LOCAL_WINDOWS.move_to_end(scope)
            bucket.window_seconds = window_seconds

        cutoff = now - bucket.window_seconds
        while bucket.reservations and bucket.reservations[0][0] <= cutoff:
            bucket.reservations.popleft()

        if any(member == reservation_id for _, member in bucket.reservations):
            return "admitted"
        if len(bucket.reservations) >= limit:
            return "limit_exceeded"

        bucket.reservations.append((now, reservation_id))
        return "admitted"


def _prune_empty_local_windows(*, now: float) -> None:
    for scope, bucket in list(_LOCAL_WINDOWS.items()):
        cutoff = now - bucket.window_seconds
        while bucket.reservations and bucket.reservations[0][0] <= cutoff:
            bucket.reservations.popleft()
        if not bucket.reservations:
            _LOCAL_WINDOWS.pop(scope, None)


def reset_local_sandbox_admission_state() -> None:
    """Clear process-local admission state for isolated local tests."""

    with _LOCAL_LOCK:
        _LOCAL_WINDOWS.clear()
