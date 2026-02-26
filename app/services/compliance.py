from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
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


@dataclass
class ComplianceDecision:
    is_stop: bool = False
    is_help: bool = False


def _normalized_keyword(text: str) -> str:
    return re.sub(r"\s+", "", text.strip().lower())


def evaluate_text(text: str) -> ComplianceDecision:
    normalized = _normalized_keyword(text)
    return ComplianceDecision(
        is_stop=normalized in STOP_KEYWORDS,
        is_help=normalized in HELP_KEYWORDS,
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
) -> bool:
    if redis_client is None:
        return False

    key = f"rate_limit:lead:{lead_id}"
    try:
        count = redis_client.incr(key)
        if count == 1:
            redis_client.expire(key, max(window_minutes, 1) * 60)
        return int(count) > max_messages
    except RedisError:
        # Fail open when Redis is unavailable to avoid breaking inbound handling.
        return False
