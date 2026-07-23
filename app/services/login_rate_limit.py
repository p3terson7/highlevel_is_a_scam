from __future__ import annotations

import hashlib
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from functools import lru_cache
from uuid import uuid4

from redis import Redis
from redis.exceptions import RedisError

from app.core.config import Settings


_WINDOW_SECONDS = 15 * 60
_EMAIL_IP_LIMIT = 5
_EMAIL_LIMIT = 10
_IP_LIMIT = 100
_LOCAL_MAX_BUCKETS = 10_000
_LOCAL_LOCK = threading.Lock()
_LOCAL_FAILURES: dict[str, deque[tuple[float, str]]] = defaultdict(deque)

_REDIS_ADMIT_SCRIPT = """
local clock = redis.call('TIME')
local now = tonumber(clock[1]) + (tonumber(clock[2]) / 1000000)
local ttl = tonumber(ARGV[1])
local cutoff = now - ttl
local member = ARGV[2]

for index = 1, #KEYS do
    redis.call('ZREMRANGEBYSCORE', KEYS[index], 0, cutoff)
    if redis.call('ZCARD', KEYS[index]) >= tonumber(ARGV[2 + index]) then
        return 0
    end
end

for index = 1, #KEYS do
    redis.call('ZADD', KEYS[index], now, member)
    redis.call('EXPIRE', KEYS[index], ttl)
end
return 1
"""


@dataclass(frozen=True)
class PortalLoginAdmission:
    member: str
    backend: str


def admit_portal_login_attempt(
    *,
    settings: Settings,
    email: str,
    remote_ip: str,
) -> PortalLoginAdmission | None:
    """Atomically reserve rate-limit capacity before password hashing."""

    keys = _bucket_keys(email=email, remote_ip=remote_ip)
    member = f"{time.time_ns()}:{uuid4().hex}"
    redis_client = _redis_client(settings.redis_url)
    if redis_client is not None:
        arguments: list[object] = [
            *(key for key, _ in keys),
            _WINDOW_SECONDS,
            member,
            *(limit for _, limit in keys),
        ]
        try:
            admitted = redis_client.eval(_REDIS_ADMIT_SCRIPT, len(keys), *arguments)
            if int(admitted) == 1:
                return PortalLoginAdmission(member=member, backend="redis")
            return None
        except (RedisError, TypeError, ValueError):
            pass

    if _local_admit(keys, member=member):
        return PortalLoginAdmission(member=member, backend="local")
    return None


def clear_portal_login_failures(
    *,
    settings: Settings,
    email: str,
    remote_ip: str,
    admission: PortalLoginAdmission | None = None,
) -> None:
    keys = _bucket_keys(email=email, remote_ip=remote_ip)
    # Clear account-specific buckets on success. Retaining the aggregate IP
    # bucket prevents credential stuffing across many accounts.
    account_keys = [keys[0][0], keys[1][0]]
    redis_client = _redis_client(settings.redis_url)
    if redis_client is not None:
        try:
            pipeline = redis_client.pipeline(transaction=True)
            pipeline.delete(*account_keys)
            if admission is not None and admission.backend == "redis":
                pipeline.zrem(keys[2][0], admission.member)
            pipeline.execute()
        except RedisError:
            pass
    with _LOCAL_LOCK:
        for key in account_keys:
            _LOCAL_FAILURES.pop(key, None)
        if admission is not None and admission.backend == "local":
            ip_key = keys[2][0]
            bucket = _LOCAL_FAILURES.get(ip_key)
            if bucket is not None:
                _LOCAL_FAILURES[ip_key] = deque(
                    item for item in bucket if item[1] != admission.member
                )


def reset_local_portal_login_limits() -> None:
    with _LOCAL_LOCK:
        _LOCAL_FAILURES.clear()


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


def _bucket_keys(*, email: str, remote_ip: str) -> list[tuple[str, int]]:
    normalized_email = str(email or "").strip().lower()
    normalized_ip = str(remote_ip or "unknown").strip().lower() or "unknown"
    email_digest = hashlib.sha256(normalized_email.encode("utf-8")).hexdigest()[:24]
    ip_digest = hashlib.sha256(normalized_ip.encode("utf-8")).hexdigest()[:24]
    return [
        (f"portal-login:email-ip:{email_digest}:{ip_digest}", _EMAIL_IP_LIMIT),
        (f"portal-login:email:{email_digest}", _EMAIL_LIMIT),
        (f"portal-login:ip:{ip_digest}", _IP_LIMIT),
    ]


def _local_admit(keys: list[tuple[str, int]], *, member: str) -> bool:
    now = time.monotonic()
    cutoff = now - _WINDOW_SECONDS
    with _LOCAL_LOCK:
        if len(_LOCAL_FAILURES) >= _LOCAL_MAX_BUCKETS:
            for existing_key, bucket in list(_LOCAL_FAILURES.items()):
                while bucket and bucket[0][0] <= cutoff:
                    bucket.popleft()
                if not bucket:
                    _LOCAL_FAILURES.pop(existing_key, None)
        new_key_count = sum(1 for key, _ in keys if key not in _LOCAL_FAILURES)
        if len(_LOCAL_FAILURES) + new_key_count > _LOCAL_MAX_BUCKETS:
            return False
        for key, _ in keys:
            bucket = _LOCAL_FAILURES[key]
            while bucket and bucket[0][0] <= cutoff:
                bucket.popleft()
        if any(len(_LOCAL_FAILURES[key]) >= limit for key, limit in keys):
            return False
        for key, _ in keys:
            _LOCAL_FAILURES[key].append((now, member))
    return True
