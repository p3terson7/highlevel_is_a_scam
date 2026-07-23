from __future__ import annotations

import hashlib
import time
from collections import OrderedDict
from dataclasses import dataclass
from threading import Lock
from typing import Any

from app.core.config import Settings


_LOCAL_ENVIRONMENTS = {"dev", "development", "local", "test", "testing"}
_MAX_LOCAL_SCOPES = 10_000
_LOCAL_LOCK = Lock()
_LOCAL_WINDOWS: OrderedDict[str, OrderedDict[str, float]] = OrderedDict()

_ATOMIC_SLIDING_WINDOW_LUA = """
local redis_time = redis.call('TIME')
local now_us = (tonumber(redis_time[1]) * 1000000) + tonumber(redis_time[2])
local window_us = tonumber(ARGV[3]) * 1000000
local cutoff_us = now_us - window_us
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', cutoff_us)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', cutoff_us)
if redis.call('ZSCORE', KEYS[1], ARGV[4]) or redis.call('ZSCORE', KEYS[2], ARGV[4]) then
    return {1, 3}
end
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[1]) then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
    redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]))
    return {0, 1}
end
if redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[2]) then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
    redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]))
    return {0, 2}
end
redis.call('ZADD', KEYS[1], now_us, ARGV[4])
redis.call('ZADD', KEYS[2], now_us, ARGV[4])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]))
return {1, 0}
"""


@dataclass(frozen=True)
class TwilioInboundAdmission:
    admitted: bool
    reason: str
    backend: str
    limiting_scope: str = ""
    retry_after_seconds: int = 0
    sid_fingerprint: str = ""


def _scope_keys(*, client_id: int, account_sid: str) -> tuple[str, str]:
    account_fingerprint = hashlib.sha256(
        str(account_sid or "unsigned-local").strip().encode("utf-8")
    ).hexdigest()[:24]
    return (
        f"twilio-inbound-admission:tenant:{int(client_id)}",
        f"twilio-inbound-admission:account:{account_fingerprint}",
    )


def _prune_local_window(window: OrderedDict[str, float], *, cutoff: float) -> None:
    while window:
        oldest_member, oldest_at = next(iter(window.items()))
        if oldest_at > cutoff:
            break
        window.pop(oldest_member, None)


def _local_window(scope: str) -> OrderedDict[str, float]:
    window = _LOCAL_WINDOWS.get(scope)
    if window is None:
        window = OrderedDict()
        _LOCAL_WINDOWS[scope] = window
    else:
        _LOCAL_WINDOWS.move_to_end(scope)
    return window


def _admit_locally(
    *,
    tenant_scope: str,
    account_scope: str,
    member: str,
    tenant_limit: int,
    account_limit: int,
    window_seconds: int,
) -> tuple[bool, str, str]:
    now = time.monotonic()
    cutoff = now - window_seconds
    with _LOCAL_LOCK:
        tenant_window = _local_window(tenant_scope)
        account_window = _local_window(account_scope)
        _prune_local_window(tenant_window, cutoff=cutoff)
        _prune_local_window(account_window, cutoff=cutoff)
        if member in tenant_window or member in account_window:
            return True, "duplicate", ""
        if len(tenant_window) >= tenant_limit:
            return False, "limit_exceeded", "tenant"
        if len(account_window) >= account_limit:
            return False, "limit_exceeded", "account"
        tenant_window[member] = now
        account_window[member] = now
        while len(_LOCAL_WINDOWS) > _MAX_LOCAL_SCOPES:
            _LOCAL_WINDOWS.popitem(last=False)
        return True, "admitted", ""


def reset_local_twilio_admission_state() -> None:
    """Clear the process-local fallback. Intended for isolated local tests."""

    with _LOCAL_LOCK:
        _LOCAL_WINDOWS.clear()


def admit_twilio_inbound(
    *,
    redis_client: Any,
    settings: Settings,
    client_id: int,
    account_sid: str,
    message_sid: str,
) -> TwilioInboundAdmission:
    """Admit one already-authenticated Twilio callback for a tenant/account.

    Redis is authoritative outside explicitly local/test environments. A
    deployed process never falls back to an uncoordinated in-memory counter.
    """

    tenant_limit = max(1, int(settings.twilio_inbound_tenant_limit))
    account_limit = max(1, int(settings.twilio_inbound_account_limit))
    window_seconds = max(1, int(settings.twilio_inbound_window_seconds))
    tenant_scope, account_scope = _scope_keys(
        client_id=client_id,
        account_sid=account_sid,
    )
    sid_fingerprint = hashlib.sha256(str(message_sid).encode("utf-8")).hexdigest()[:24]
    member = f"sid:{sid_fingerprint}"

    if redis_client is not None:
        try:
            result = redis_client.eval(
                _ATOMIC_SLIDING_WINDOW_LUA,
                2,
                tenant_scope,
                account_scope,
                tenant_limit,
                account_limit,
                window_seconds,
                member,
            )
            admitted = int(result[0])
            limiting_scope_code = int(result[1])
            if admitted == 1:
                reason = "duplicate" if limiting_scope_code == 3 else "admitted"
                return TwilioInboundAdmission(
                    True,
                    reason,
                    "redis",
                    sid_fingerprint=sid_fingerprint,
                )
            if admitted == 0 and limiting_scope_code in {1, 2}:
                return TwilioInboundAdmission(
                    False,
                    "limit_exceeded",
                    "redis",
                    limiting_scope="tenant" if limiting_scope_code == 1 else "account",
                    retry_after_seconds=window_seconds,
                    sid_fingerprint=sid_fingerprint,
                )
            raise RuntimeError("unexpected Redis admission result")
        except Exception:
            # The route emits a deliberately PII-free audit record. Do not log
            # Redis exception strings here because proxy URLs may carry secrets.
            pass

    environment = settings.env.strip().lower()
    if environment not in _LOCAL_ENVIRONMENTS:
        return TwilioInboundAdmission(
            False,
            "coordination_unavailable",
            "unavailable",
            limiting_scope="coordination",
            retry_after_seconds=window_seconds,
            sid_fingerprint=sid_fingerprint,
        )

    admitted, reason, limiting_scope = _admit_locally(
        tenant_scope=tenant_scope,
        account_scope=account_scope,
        member=member,
        tenant_limit=tenant_limit,
        account_limit=account_limit,
        window_seconds=window_seconds,
    )
    return TwilioInboundAdmission(
        admitted,
        reason,
        "local",
        limiting_scope=limiting_scope,
        retry_after_seconds=0 if admitted else window_seconds,
        sid_fingerprint=sid_fingerprint,
    )
