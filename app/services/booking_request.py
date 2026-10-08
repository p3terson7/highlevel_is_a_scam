from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

_DAY_NAMES = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
_DAY_TO_INDEX = {day: idx for idx, day in enumerate(_DAY_NAMES)}
_FRENCH_DAY_REPLACEMENTS = {
    "semaine prochaine": "next week",
    "après-demain": "day after tomorrow",
    "apres-demain": "day after tomorrow",
    "après demain": "day after tomorrow",
    "apres demain": "day after tomorrow",
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
_PERIOD_ALIASES = {
    "morning": (
        "morning",
        "mornings",
        "matin",
        "matins",
        "matinee",
        "matinée",
        "avant-midi",
        "avant midi",
    ),
    "afternoon": (
        "afternoon",
        "afternoons",
        "apres-midi",
        "après-midi",
        "apres midi",
        "après midi",
    ),
    "evening": (
        "evening",
        "evenings",
        "tonight",
        "soir",
        "soirs",
        "soiree",
        "soirée",
        "soirees",
        "soirées",
    ),
}
_MONTHS = {
    "jan": 1,
    "january": 1,
    "janvier": 1,
    "feb": 2,
    "february": 2,
    "fev": 2,
    "fév": 2,
    "fevrier": 2,
    "février": 2,
    "mar": 3,
    "march": 3,
    "mars": 3,
    "apr": 4,
    "april": 4,
    "avr": 4,
    "avril": 4,
    "may": 5,
    "mai": 5,
    "jun": 6,
    "june": 6,
    "juin": 6,
    "jul": 7,
    "july": 7,
    "juil": 7,
    "juillet": 7,
    "aug": 8,
    "august": 8,
    "aout": 8,
    "août": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "septembre": 9,
    "oct": 10,
    "october": 10,
    "octobre": 10,
    "nov": 11,
    "november": 11,
    "novembre": 11,
    "dec": 12,
    "december": 12,
    "decembre": 12,
    "décembre": 12,
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

    def to_canonical_payload(self) -> dict[str, Any]:
        """Return only stable scheduling constraints for persistence/comparison."""
        return {
            "scope": self.scope,
            "timezone": self.timezone_name or "UTC",
            "requested_dates": list(self.requested_dates),
            "date_range_start": self.date_range_start,
            "date_range_end": self.date_range_end,
            "requested_weekdays": list(self.requested_weekdays),
            "avoid_weekdays": list(self.avoid_weekdays),
            "periods": list(self.periods),
            "exact_time_minutes": self.exact_time_minutes,
            "range_start_minutes": self.range_start_minutes,
            "range_end_minutes": self.range_end_minutes,
            "all_day": self.all_day,
        }

    def canonical_payload(self) -> dict[str, Any]:
        """Compatibility alias for downstream request-state persistence."""
        return self.to_canonical_payload()

    @property
    def fingerprint(self) -> str:
        """Stable identifier for semantically equivalent booking requests."""
        return booking_request_fingerprint(self)


def booking_request_fingerprint(request: BookingTimeRequest) -> str:
    canonical_json = json.dumps(
        request.to_canonical_payload(),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


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
    if _has_ambiguous_numeric_date(normalized):
        reasons.append("ambiguous_numeric_date")

    if re.search(r"\bnext\s+week\b", normalized):
        start = _next_week_start(now.date())
        date_range_start = start.isoformat()
        date_range_end = (start + timedelta(days=6)).isoformat()
        reasons.append("date_range:next_week")

    explicit_requested_dates = tuple(requested_dates)
    weekday_hits = _extract_weekday_requests(normalized, now.date())
    resolved_weekday_in_range = False
    for hit in weekday_hits:
        weekday = str(hit.get("reason") or "").rsplit(":", 1)[-1]
        if weekday not in _DAY_TO_INDEX:
            weekday = str(hit.get("weekday") or "")
        if weekday in avoid_weekdays:
            continue
        if explicit_requested_dates and weekday in _DAY_TO_INDEX:
            requested_date_weekdays = {
                _weekday_for_iso_date(raw_date)
                for raw_date in explicit_requested_dates
            }
            if weekday in requested_date_weekdays:
                reasons.append(f"weekday_date_label:{weekday}")
            else:
                reasons.append("weekday_date_mismatch")
            continue
        if (
            date_range_start
            and date_range_end
            and weekday in _DAY_TO_INDEX
        ):
            range_start_date = date.fromisoformat(date_range_start)
            target = range_start_date + timedelta(
                days=(
                    _DAY_TO_INDEX[weekday] - range_start_date.weekday()
                )
                % 7
            )
            if target <= date.fromisoformat(date_range_end):
                requested_dates.append(target.isoformat())
                reasons.append(f"weekday_in_date_range:{weekday}")
                resolved_weekday_in_range = True
            continue
        if hit["kind"] == "date":
            requested_dates.append(hit["date"])
            reasons.append(str(hit["reason"]))
        elif hit["kind"] == "weekday":
            requested_weekdays.append(str(hit["weekday"]))
            reasons.append(str(hit["reason"]))
    if resolved_weekday_in_range:
        date_range_start = None
        date_range_end = None

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

    for period in _extract_periods(normalized):
        periods.append(period)
        reasons.append(f"period:{period}")
    if preferred_period:
        tool_periods = _extract_periods(_normalize(preferred_period))
        if tool_periods:
            period = tool_periods[0]
            periods.append(period)
            reasons.append(f"tool_period:{period}")

    temporal_text = _mask_explicit_date_tokens(normalized)
    if _has_multiple_temporal_clauses(normalized):
        reasons.append("multiple_temporal_clauses")
    if _has_negated_time_constraint(temporal_text):
        reasons.append("negated_time_constraint")
    if _has_ambiguous_time_range(temporal_text):
        reasons.append("ambiguous_time")
    parsed_range = _extract_time_range(temporal_text)
    if parsed_range is None and (range_start or range_end):
        parsed_range = _normalize_time_range(range_start, range_end)
    exact = _extract_exact_time(temporal_text)
    if exact is None and _has_ambiguous_time_reference(temporal_text):
        reasons.append("ambiguous_time")
    if exact_time:
        parsed_tool_time = _parse_time_text(exact_time)
        if parsed_tool_time is not None:
            exact = parsed_tool_time

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

    all_day = bool(
        re.search(
            r"\b(all\s+day|any\s+time|anytime|open\s+all\s+day|available\s+all\s+day|"
            r"toute\s+la\s+journ[ée]e|n'importe\s+quelle\s+heure|a\s+tout\s+moment|"
            r"à\s+tout\s+moment)\b",
            normalized,
        )
    )
    if all_day:
        reasons.append("all_day")

    change_of_mind = bool(
        re.search(
            r"\b(those|that|these).{0,24}(?:don'?t|do not|doesn'?t|does not|won'?t|will not|can't|cannot).{0,24}(?:work|fit)\b",
            normalized,
        )
        or re.search(
            r"\b(instead|actually|if possible|would take|i'?d take|can you do|could you do|"
            r"finalement|plut[oô]t|[àa]\s+la\s+place)\b",
            normalized,
        )
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

    month_pattern = _month_pattern()
    for match in re.finditer(
        rf"\b({month_pattern})\s+(\d{{1,2}})(?:st|nd|rd|th)?(?:,?\s*(20\d{{2}}))?\b",
        text,
    ):
        month = _MONTHS[match.group(1)]
        day = int(match.group(2))
        year = int(match.group(3) or today.year)
        parsed = _future_date(year=year, month=month, day=day, today=today, year_explicit=bool(match.group(3)))
        if parsed is not None:
            results.append((parsed, "month_date"))

    for match in re.finditer(
        rf"\b(\d{{1,2}})(?:st|nd|rd|th|er)?\s+({month_pattern})(?:,?\s*(20\d{{2}}))?\b",
        text,
    ):
        day = int(match.group(1))
        month = _MONTHS[match.group(2)]
        year = int(match.group(3) or today.year)
        parsed = _future_date(year=year, month=month, day=day, today=today, year_explicit=bool(match.group(3)))
        if parsed is not None:
            results.append((parsed, "day_month_date"))

    for match in re.finditer(r"(?<![\d/])(\d{1,2})/(\d{1,2})(?:/(20\d{2}|\d{2}))?(?![\d/])", text):
        first = int(match.group(1))
        second = int(match.group(2))
        if first <= 12 and second <= 12:
            continue
        if first > 12:
            day, month = first, second
        else:
            month, day = first, second
        raw_year = match.group(3)
        year = int(raw_year) if raw_year else today.year
        if raw_year and len(raw_year) == 2:
            year += 2000
        parsed = _future_date(
            year=year,
            month=month,
            day=day,
            today=today,
            year_explicit=bool(raw_year),
        )
        if parsed is not None:
            results.append((parsed, "numeric_date"))
    return results


def _month_pattern() -> str:
    return "|".join(re.escape(value) for value in sorted(_MONTHS, key=len, reverse=True))


def _future_date(
    *,
    year: int,
    month: int,
    day: int,
    today: date,
    year_explicit: bool,
) -> date | None:
    try:
        parsed = date(year, month, day)
    except ValueError:
        return None
    if not year_explicit and parsed < today:
        try:
            parsed = date(year + 1, month, day)
        except ValueError:
            return None
    return parsed


def _has_ambiguous_numeric_date(text: str) -> bool:
    for match in re.finditer(
        r"(?<![\d/])(\d{1,2})/(\d{1,2})(?:/(?:20\d{2}|\d{2}))?(?![\d/])",
        text,
    ):
        first = int(match.group(1))
        second = int(match.group(2))
        if 1 <= first <= 12 and 1 <= second <= 12:
            return True
    return False


def _mask_explicit_date_tokens(text: str) -> str:
    """Keep calendar dates out of the time parser's numeric grammar."""

    month_pattern = _month_pattern()
    patterns = (
        r"\b20\d{2}-\d{2}-\d{2}\b",
        r"(?<![\d/])\d{1,2}/\d{1,2}(?:/(?:20\d{2}|\d{2}))?(?![\d/])",
        rf"\b(?:{month_pattern})\s+\d{{1,2}}(?:st|nd|rd|th)?(?:,?\s*20\d{{2}})?\b",
        rf"\b\d{{1,2}}(?:st|nd|rd|th|er)?\s+(?:{month_pattern})(?:,?\s*20\d{{2}})?\b",
    )
    masked = text
    for pattern in patterns:
        masked = re.sub(pattern, " ", masked)
    return re.sub(r"\s+", " ", masked).strip()


def _has_multiple_temporal_clauses(text: str) -> bool:
    """Detect alternatives that the flat request model cannot combine safely."""

    clauses = [
        clause.strip(" ,.;")
        for clause in re.split(r"\b(?:or|ou)\b", text)
        if clause.strip(" ,.;")
    ]
    if len(clauses) < 2:
        return False

    signatures: list[tuple[Any, ...]] = []
    for clause in clauses:
        has_day = any(re.search(rf"\b{day}\b", clause) for day in _DAY_NAMES)
        has_date = bool(
            re.search(r"\b20\d{2}-\d{2}-\d{2}\b", clause)
            or re.search(r"(?<![\d/])\d{1,2}/\d{1,2}(?![\d/])", clause)
            or re.search(rf"\b(?:{_month_pattern()})\b", clause)
        )
        if not (has_day or has_date):
            continue
        temporal_clause = _mask_explicit_date_tokens(clause)
        parsed_range = _extract_time_range(temporal_clause)
        parsed_exact = _extract_exact_time(temporal_clause)
        periods = tuple(_extract_periods(temporal_clause))
        if parsed_range is None and parsed_exact is None and not periods:
            continue
        signatures.append((parsed_range, parsed_exact, periods))
    return len(signatures) > 1 and len(set(signatures)) > 1


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
        pattern = (
            rf"\b(?:not|no|avoid|skip|except|excluding|can'?t|cannot|don'?t|do not|won'?t|will not)\s+"
            rf"(?:do\s+|make\s+|meet\s+)?(?:on\s+)?{day}\b|"
            rf"\b(?:any\s+day|anything|anytime)\s+(?:but|except)\s+(?:on\s+)?{day}\b|"
            rf"\bbut\s+not\s+(?:on\s+)?{day}\b|"
            rf"\b(?:pas|sauf|[ée]viter?)\s+(?:le\s+)?{day}\b|"
            rf"\b{day}\s+(?:doesn'?t|does not|won'?t|will not)\s+(?:work|suit)\b|"
            rf"\b{day}\s+is\s+no\s+good\b|"
            rf"\b{day}\s+ne\s+(?:marche|convient)\s+pas\b"
        )
        if re.search(pattern, text):
            avoided.append(day)
    return avoided


def _extract_time_range(text: str) -> tuple[int, int] | None:
    time_token = r"(?:\d{1,2}(?::\d{2}|\s*h\s*\d{0,2})?\s*(?:am|pm)?|noon|midi|midnight|minuit)"
    match = re.search(
        rf"\b(?:between|from|entre|de)\s+({time_token})\s+(?:and|to|-|et|à|a)\s+({time_token})\b",
        text,
    )
    if match:
        return _normalize_time_range(match.group(1), match.group(2))
    direct_match = re.search(
        rf"\b({time_token})\s+(?:to|-|et|à|a)\s+({time_token})\b",
        text,
    )
    if direct_match:
        return _normalize_time_range(direct_match.group(1), direct_match.group(2))
    after_match = re.search(rf"\b(?:after|from|après|apres|de)\s+({time_token})\b", text)
    if after_match:
        start = _parse_time_text(after_match.group(1))
        if start is not None:
            return (start, 24 * 60 - 1)
    before_match = re.search(rf"\b(?:before|avant)\s+({time_token}|noon|midi)\b", text)
    if before_match:
        endpoint = before_match.group(1)
        if endpoint in {"noon", "midi"}:
            end = 12 * 60
        elif endpoint in {"midnight", "minuit"}:
            end = 24 * 60 - 1
        else:
            end = _parse_time_text(endpoint)
        if end is not None:
            return (0, end)
    return None


def _extract_exact_time(text: str) -> int | None:
    named_match = re.search(r"\b(?:noon|midi|midnight|minuit)\b", text)
    if named_match:
        return _parse_time_text(named_match.group(0))
    h_match = re.search(r"\b\d{1,2}\s*h\s*\d{0,2}\b", text)
    if h_match:
        return _parse_time_text(h_match.group(0))
    twenty_four_hour_match = re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", text)
    if twenty_four_hour_match:
        return int(twenty_four_hour_match.group(1)) * 60 + int(twenty_four_hour_match.group(2))
    match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", text)
    if match:
        return _parse_time_parts(match.group(1), match.group(2), match.group(3))
    contextual_24_hour = re.search(r"\b(?:at|à|a)\s+(0|1[3-9]|2[0-3])\b", text)
    if contextual_24_hour:
        return int(contextual_24_hour.group(1)) * 60
    return None


def _has_ambiguous_time_reference(text: str) -> bool:
    """Flag contextual clock hours that lack a reliable AM/PM interpretation."""

    return bool(
        re.search(
            r"\b(?:at|à|a)\s+(?:[1-9]|1[0-2])"
            r"(?!\s*(?::\d{2}|h\b|am\b|pm\b|\d))",
            text,
        )
    )


def _has_ambiguous_time_range(text: str) -> bool:
    match = re.search(
        r"\b(?:between|from|entre|de)?\s*"
        r"(\d{1,2})\s+(?:and|to|-|et|à|a)\s+(\d{1,2})\b",
        text,
    )
    if not match:
        return False
    return all(1 <= int(value) <= 12 for value in match.groups())


def _has_negated_time_constraint(text: str) -> bool:
    time_token = (
        r"(?:\d{1,2}(?::\d{2}|\s*h\s*\d{0,2})?\s*(?:am|pm)?|"
        r"noon|midi|midnight|minuit)"
    )
    return bool(
        re.search(
            rf"\b(?:not|no|except|excluding|but\s+not|pas|sauf)\b"
            rf".{{0,16}}\b{time_token}\b",
            text,
        )
        or re.search(
            rf"\b{time_token}\b.{{0,16}}"
            r"\b(?:doesn'?t work|does not work|won'?t work|ne marche pas|ne convient pas)\b",
            text,
        )
    )


def _parse_time_text(raw: str | None) -> int | None:
    text = _normalize(raw or "")
    if text in {"noon", "midi"}:
        return 12 * 60
    if text in {"midnight", "minuit"}:
        return 0
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
    if start_meridiem is None and end_meridiem is not None and _is_numeric_time_text(start_text):
        start_hour = _numeric_hour(start_text)
        end_hour = _numeric_hour(end_text)
        inferred = (
            _opposite_meridiem(end_meridiem)
            if start_hour is not None
            and end_hour is not None
            and start_hour > end_hour
            else end_meridiem
        )
        start_text = f"{start_text} {inferred}"
    if end_meridiem is None and start_meridiem is not None and _is_numeric_time_text(end_text):
        start_hour = _numeric_hour(start_text)
        end_hour = _numeric_hour(end_text)
        inferred = (
            _opposite_meridiem(start_meridiem)
            if start_hour is not None
            and end_hour is not None
            and end_hour < start_hour
            else start_meridiem
        )
        end_text = f"{end_text} {inferred}"
    start = _parse_time_text(start_text)
    end = _parse_time_text(end_text)
    if start is None:
        start = _parse_bare_range_time(start_text)
    if end is None:
        end = _parse_bare_range_time(end_text)
    if start is None or end is None:
        return None
    if end == 0 and end_text in {"midnight", "minuit"} and start > 0:
        end = 24 * 60
    if end < start:
        end += 24 * 60
    return start, end


def _last_meridiem(text: str) -> str | None:
    match = re.search(r"\b(am|pm)\b", text)
    return match.group(1) if match else None


def _is_numeric_time_text(text: str) -> bool:
    return bool(re.fullmatch(r"\s*\d{1,2}(?::\d{2}|\s*h\s*\d{0,2})?\s*", text))


def _numeric_hour(text: str) -> int | None:
    match = re.search(r"\b(\d{1,2})", text)
    return int(match.group(1)) if match else None


def _opposite_meridiem(value: str) -> str:
    return "pm" if value == "am" else "am"


def _parse_bare_range_time(text: str) -> int | None:
    match = re.fullmatch(r"\s*(\d{1,2})(?::(\d{2}))?\s*", text)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2) or "0")
    if hour > 23 or minute > 59:
        return None
    return hour * 60 + minute


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


def _extract_periods(text: str) -> list[str]:
    periods: list[str] = []
    for period, aliases in _PERIOD_ALIASES.items():
        if any(re.search(rf"(?<!\w){re.escape(alias)}(?!\w)", text) for alias in aliases):
            periods.append(period)
    return periods


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
