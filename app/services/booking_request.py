from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

_DAY_NAMES = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
_DAY_TO_INDEX = {day: idx for idx, day in enumerate(_DAY_NAMES)}
_FRENCH_DAY_REPLACEMENTS = {
    "lundi": "monday",
    "mardi": "tuesday",
    "mercredi": "wednesday",
    "jeudi": "thursday",
    "vendredi": "friday",
    "samedi": "saturday",
    "dimanche": "sunday",
    "aujourd'hui": "today",
    "aujourd hui": "today",
    "demain": "tomorrow",
}
_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_PERIOD_WINDOWS = {
    "morning": (8 * 60, 12 * 60),
    "afternoon": (12 * 60, 17 * 60),
    "evening": (17 * 60, 20 * 60),
}


@dataclass(frozen=True)
class BookingTimeRequest:
    raw_text: str = ""
    source: str = "agent"
    scope: str = "broad"
    timezone_name: str = "UTC"
    requested_dates: tuple[str, ...] = ()
    date_range_start: str | None = None
    date_range_end: str | None = None
    requested_weekdays: tuple[str, ...] = ()
    avoid_weekdays: tuple[str, ...] = ()
    periods: tuple[str, ...] = ()
    exact_time: str | None = None
    exact_time_minutes: int | None = None
    range_start: str | None = None
    range_start_minutes: int | None = None
    range_end: str | None = None
    range_end_minutes: int | None = None
    all_day: bool = False
    change_of_mind: bool = False
    confidence: str = "low"
    reasons: tuple[str, ...] = field(default_factory=tuple)

    @property
    def has_time_constraint(self) -> bool:
        return (
            self.exact_time_minutes is not None
            or self.range_start_minutes is not None
            or self.range_end_minutes is not None
            or bool(self.periods)
        )

    @property
    def preferred_day(self) -> str | None:
        if self.requested_dates:
            try:
                parsed = date.fromisoformat(self.requested_dates[0])
            except ValueError:
                return None
            return _DAY_NAMES[parsed.weekday()]
        return self.requested_weekdays[0] if self.requested_weekdays else None

    def to_payload(self) -> dict[str, Any]:
        return {
            "raw_text": self.raw_text,
            "source": self.source,
            "scope": self.scope,
            "timezone": self.timezone_name,
            "requested_dates": list(self.requested_dates),
            "date_range_start": self.date_range_start,
            "date_range_end": self.date_range_end,
            "requested_weekdays": list(self.requested_weekdays),
            "avoid_weekdays": list(self.avoid_weekdays),
            "periods": list(self.periods),
            "exact_time": self.exact_time,
            "exact_time_minutes": self.exact_time_minutes,
            "range_start": self.range_start,
            "range_start_minutes": self.range_start_minutes,
            "range_end": self.range_end,
            "range_end_minutes": self.range_end_minutes,
            "all_day": self.all_day,
            "change_of_mind": self.change_of_mind,
            "confidence": self.confidence,
            "reasons": list(self.reasons),
        }


def build_booking_time_request(
    *,
    text: str | None = None,
    timezone_name: str = "UTC",
    now_utc: datetime | None = None,
    source: str = "agent",
    preferred_day: str | None = None,
    avoid_day: str | None = None,
    preferred_period: str | None = None,
    exact_time: str | None = None,
    range_start: str | None = None,
    range_end: str | None = None,
) -> BookingTimeRequest:
    tz = _tzinfo(timezone_name)
    now = (now_utc or datetime.now(timezone.utc)).astimezone(tz)
    raw_text = " ".join(str(text or "").split())
    normalized = _normalize(raw_text)
    reasons: list[str] = []

    requested_dates: list[str] = []
    requested_weekdays: list[str] = []
    avoid_weekdays: list[str] = []
    periods: list[str] = []
    date_range_start: str | None = None
    date_range_end: str | None = None

    for day in _extract_avoided_weekdays(normalized):
        avoid_weekdays.append(day)
        reasons.append(f"avoid_weekday:{day}")

    for parsed_date, reason in _extract_explicit_dates(normalized, now.date()):
        requested_dates.append(parsed_date.isoformat())
        reasons.append(reason)

    if re.search(r"\bnext\s+week\b", normalized):
        start = _next_week_start(now.date())
        date_range_start = start.isoformat()
        date_range_end = (start + timedelta(days=6)).isoformat()
        reasons.append("date_range:next_week")

    weekday_hits = _extract_weekday_requests(normalized, now.date())
    for hit in weekday_hits:
        if hit["kind"] == "date":
            requested_dates.append(hit["date"])
            reasons.append(str(hit["reason"]))
        elif hit["kind"] == "weekday":
            requested_weekdays.append(str(hit["weekday"]))
            reasons.append(str(hit["reason"]))

    if preferred_day:
        preferred_text = _normalize(preferred_day)
        preferred_date = _date_from_iso(preferred_text)
        if preferred_date is not None:
            requested_dates.append(preferred_date.isoformat())
            reasons.append("tool_preferred_date")
        else:
            day = _weekday_from_text(preferred_text)
            if day:
                requested_weekdays.append(day)
                reasons.append(f"tool_preferred_weekday:{day}")

    if avoid_day:
        day = _weekday_from_text(_normalize(avoid_day))
        if day:
            avoid_weekdays.append(day)
            reasons.append(f"tool_avoid_weekday:{day}")

    for period in _PERIOD_WINDOWS:
        if re.search(rf"\b{period}\b", normalized):
            periods.append(period)
            reasons.append(f"period:{period}")
    if preferred_period and preferred_period.strip().lower() in _PERIOD_WINDOWS:
        period = preferred_period.strip().lower()
        periods.append(period)
        reasons.append(f"tool_period:{period}")

    parsed_range = _extract_time_range(normalized)
    if parsed_range is None and (range_start or range_end):
        parsed_range = _normalize_time_range(range_start, range_end)
    exact = _extract_exact_time(normalized)
    if exact_time:
        exact = _parse_time_text(exact_time) or exact

    exact_label: str | None = None
    exact_minutes: int | None = None
    start_label: str | None = None
    start_minutes: int | None = None
    end_label: str | None = None
    end_minutes: int | None = None
    if parsed_range is not None:
        start_minutes, end_minutes = parsed_range
        start_label = _format_minutes(start_minutes)
        end_label = _format_minutes(end_minutes)
        reasons.append("time_range")
    elif exact is not None:
        exact_minutes = exact
        exact_label = _format_minutes(exact)
        reasons.append("exact_time")

    all_day = bool(re.search(r"\b(all\s+day|any\s+time|anytime|open\s+all\s+day|available\s+all\s+day)\b", normalized))
    if all_day:
        reasons.append("all_day")

    change_of_mind = bool(
        re.search(
            r"\b(those|that|these).{0,24}(?:don'?t|do not|doesn'?t|does not|won'?t|will not|can't|cannot).{0,24}(?:work|fit)\b",
            normalized,
        )
        or re.search(r"\b(instead|actually|if possible|would take|i'?d take|can you do|could you do)\b", normalized)
    )
    if change_of_mind:
        reasons.append("change_of_mind")

    requested_dates = _unique_ordered(requested_dates)
    requested_weekdays = [day for day in _unique_ordered(requested_weekdays) if day not in avoid_weekdays]
    avoid_weekdays = _unique_ordered(avoid_weekdays)
    if avoid_weekdays:
        requested_dates = [raw_date for raw_date in requested_dates if _weekday_for_iso_date(raw_date) not in avoid_weekdays]
    periods = _unique_ordered(periods)

    if requested_dates:
        scope = "specific_dates" if len(requested_dates) > 1 else "specific_date"
        confidence = "high"
    elif date_range_start and date_range_end:
        scope = "date_range"
        confidence = "medium"
    elif requested_weekdays:
        scope = "weekday_recurring"
        confidence = "medium"
    elif exact_minutes is not None or start_minutes is not None or periods:
        scope = "time_only"
        confidence = "medium"
    else:
        scope = "broad"
        confidence = "low"

    return BookingTimeRequest(
        raw_text=raw_text,
        source=source,
        scope=scope,
        timezone_name=timezone_name or "UTC",
        requested_dates=tuple(requested_dates),
        date_range_start=date_range_start,
        date_range_end=date_range_end,
        requested_weekdays=tuple(requested_weekdays),
        avoid_weekdays=tuple(avoid_weekdays),
        periods=tuple(periods),
        exact_time=exact_label,
        exact_time_minutes=exact_minutes,
        range_start=start_label,
        range_start_minutes=start_minutes,
        range_end=end_label,
        range_end_minutes=end_minutes,
        all_day=all_day,
        change_of_mind=change_of_mind,
        confidence=confidence,
        reasons=tuple(_unique_ordered(reasons)),
    )


def _extract_explicit_dates(text: str, today: date) -> list[tuple[date, str]]:
    results: list[tuple[date, str]] = []
    day_after_tomorrow = bool(re.search(r"\bday\s+after\s+tomorrow\b", text))
    if re.search(r"\btoday\b", text):
        results.append((today, "relative_date:today"))
    if re.search(r"\btomorrow\b", text) and not day_after_tomorrow:
        results.append((today + timedelta(days=1), "relative_date:tomorrow"))
    if day_after_tomorrow:
        results.append((today + timedelta(days=2), "relative_date:day_after_tomorrow"))

    for match in re.finditer(r"\b(20\d{2})-(\d{2})-(\d{2})\b", text):
        try:
            results.append((date(int(match.group(1)), int(match.group(2)), int(match.group(3))), "iso_date"))
        except ValueError:
            continue

    month_pattern = "|".join(sorted(_MONTHS, key=len, reverse=True))
    for match in re.finditer(rf"\b({month_pattern})\s+(\d{{1,2}})(?:st|nd|rd|th)?(?:,?\s*(20\d{{2}}))?\b", text):
        month = _MONTHS[match.group(1)]
        day = int(match.group(2))
        year = int(match.group(3) or today.year)
        try:
            parsed = date(year, month, day)
        except ValueError:
            continue
        if match.group(3) is None and parsed < today:
            parsed = date(year + 1, month, day)
        results.append((parsed, "month_date"))
    return results


def _extract_weekday_requests(text: str, today: date) -> list[dict[str, str]]:
    hits: list[dict[str, str]] = []
    for day in _DAY_NAMES:
        plural_pattern = rf"\b{day}s\b"
        if re.search(plural_pattern, text):
            hits.append({"kind": "weekday", "weekday": day, "reason": f"recurring_weekday:{day}"})
            continue

        next_pattern = rf"\bnext\s+{day}\b"
        french_next_pattern = rf"\b{day}\s+prochain\b"
        this_pattern = rf"\bthis\s+{day}\b"
        bare_pattern = rf"(?<!next\s)(?<!this\s)\b(?:on\s+)?{day}\b(?!\s+prochain)"
        if re.search(next_pattern, text) or re.search(french_next_pattern, text):
            hits.append({"kind": "date", "date": _next_weekday(today, _DAY_TO_INDEX[day], strict=True).isoformat(), "reason": f"specific_next_weekday:{day}"})
        elif re.search(this_pattern, text):
            hits.append({"kind": "date", "date": _next_weekday(today, _DAY_TO_INDEX[day], strict=False).isoformat(), "reason": f"specific_this_weekday:{day}"})
        elif re.search(bare_pattern, text):
            hits.append({"kind": "date", "date": _next_weekday(today, _DAY_TO_INDEX[day], strict=False).isoformat(), "reason": f"specific_bare_weekday:{day}"})
    return hits


def _extract_avoided_weekdays(text: str) -> list[str]:
    avoided: list[str] = []
    for day in _DAY_NAMES:
        pattern = rf"\b(?:not|no|avoid|skip|can'?t|cannot|don'?t|do not|won'?t|will not)\s+(?:do\s+|make\s+|meet\s+)?(?:on\s+)?{day}\b|\b{day}\s+(?:doesn'?t|does not|won'?t|will not)\s+work\b"
        if re.search(pattern, text):
            avoided.append(day)
    return avoided


def _extract_time_range(text: str) -> tuple[int, int] | None:
    time_token = r"\d{1,2}(?::\d{2}|\s*h\s*\d{0,2})?\s*(?:am|pm)?"
    match = re.search(
        rf"\b(?:between|from|entre|de)\s+({time_token})\s+(?:and|to|-|et|à|a)\s+({time_token})\b",
        text,
    )
    if match:
        return _normalize_time_range(match.group(1), match.group(2))
    after_match = re.search(rf"\b(?:after|from|après|apres|de)\s+({time_token})\b", text)
    if after_match:
        start = _parse_time_text(after_match.group(1))
        if start is not None:
            return (start, 24 * 60 - 1)
    before_match = re.search(rf"\b(?:before|avant)\s+({time_token}|noon|midi)\b", text)
    if before_match:
        end = 12 * 60 if before_match.group(1) in {"noon", "midi"} else _parse_time_text(before_match.group(1))
        if end is not None:
            return (0, end)
    return None


def _extract_exact_time(text: str) -> int | None:
    if re.search(r"\b(?:between|from|after|before)\b", text):
        return None
    h_match = re.search(r"\b\d{1,2}\s*h\s*\d{0,2}\b", text)
    if h_match:
        return _parse_time_text(h_match.group(0))
    match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", text)
    if not match:
        return None
    return _parse_time_parts(match.group(1), match.group(2), match.group(3))


def _parse_time_text(raw: str | None) -> int | None:
    text = _normalize(raw or "")
    if text in {"noon", "midi"}:
        return 12 * 60
    h_match = re.search(r"\b(\d{1,2})\s*h\s*(\d{1,2})?\b", text)
    if h_match:
        hour = int(h_match.group(1))
        minute = int(h_match.group(2) or "0")
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            return None
        return hour * 60 + minute
    match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", text)
    if not match:
        return None
    return _parse_time_parts(match.group(1), match.group(2), match.group(3))


def _parse_time_parts(hour_text: str, minute_text: str | None, meridiem: str) -> int | None:
    hour = int(hour_text)
    minute = int(minute_text or "0")
    if hour < 1 or hour > 12 or minute < 0 or minute > 59:
        return None
    if meridiem == "pm" and hour != 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    return hour * 60 + minute


def _normalize_time_range(start_raw: str | None, end_raw: str | None) -> tuple[int, int] | None:
    start_text = _normalize(start_raw or "")
    end_text = _normalize(end_raw or "")
    if not start_text or not end_text:
        return None
    start_meridiem = _last_meridiem(start_text)
    end_meridiem = _last_meridiem(end_text)
    if start_meridiem is None and end_meridiem is not None:
        start_text = f"{start_text} {end_meridiem}"
    if end_meridiem is None and start_meridiem is not None:
        end_text = f"{end_text} {start_meridiem}"
    start = _parse_time_text(start_text)
    end = _parse_time_text(end_text)
    if start is None or end is None:
        return None
    if end < start:
        end += 12 * 60
    return start, end


def _last_meridiem(text: str) -> str | None:
    match = re.search(r"\b(am|pm)\b", text)
    return match.group(1) if match else None


def _format_minutes(minutes: int) -> str:
    minutes = minutes % (24 * 60)
    hour24, minute = divmod(minutes, 60)
    meridiem = "AM" if hour24 < 12 else "PM"
    hour12 = hour24 % 12 or 12
    if minute == 0:
        return f"{hour12} {meridiem}"
    return f"{hour12}:{minute:02d} {meridiem}"


def _next_weekday(today: date, weekday: int, *, strict: bool) -> date:
    days = (weekday - today.weekday()) % 7
    if strict and days == 0:
        days = 7
    return today + timedelta(days=days)


def _next_week_start(today: date) -> date:
    days_until_monday = (0 - today.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    return today + timedelta(days=days_until_monday)


def _date_from_iso(text: str) -> date | None:
    match = re.fullmatch(r"20\d{2}-\d{2}-\d{2}", text)
    if not match:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _weekday_for_iso_date(raw: str) -> str | None:
    try:
        parsed = date.fromisoformat(str(raw))
    except ValueError:
        return None
    return _DAY_NAMES[parsed.weekday()]


def _weekday_from_text(text: str) -> str | None:
    for day in _DAY_NAMES:
        if re.search(rf"\b{day}\b", text):
            return day
    return None


def _normalize(text: str) -> str:
    value = str(text or "").lower().replace("’", "'")
    value = value.replace("a.m.", "am").replace("p.m.", "pm")
    value = value.replace("a.m", "am").replace("p.m", "pm")
    for source, target in _FRENCH_DAY_REPLACEMENTS.items():
        value = value.replace(source, target)
    return re.sub(r"\s+", " ", value).strip()


def _unique_ordered(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _tzinfo(tz_name: str):
    try:
        return ZoneInfo(tz_name or "UTC")
    except Exception:
        return timezone.utc
