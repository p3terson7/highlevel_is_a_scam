from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any, Sequence

from app.services.booking_planner import BookingPlanResult
from app.services.booking_request import BookingTimeRequest
from app.services.i18n import format_datetime_for_language, normalize_language

_FR_WEEKDAYS = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]
_FR_MONTHS = [
    "janvier",
    "février",
    "mars",
    "avril",
    "mai",
    "juin",
    "juillet",
    "août",
    "septembre",
    "octobre",
    "novembre",
    "décembre",
]
_FR_PERIODS = {
    "morning": "le matin",
    "afternoon": "l'après-midi",
    "evening": "en soirée",
}


def render_booking_slot_reply(
    *,
    slots: Sequence[Any],
    request: BookingTimeRequest,
    plan: BookingPlanResult,
    timezone_label: str,
    language: str = "en",
    coverage_summary: str = "",
    timezone_name: str = "UTC",
) -> str:
    language = normalize_language(language)
    if not slots:
        return _no_slots_reply(request=request, language=language)

    lines = [_intro(request=request, plan=plan, coverage_summary=coverage_summary, language=language)]
    lines.extend(
        f"{_slot_index(slot)}) {_slot_display_time(slot, language=language, timezone_name=timezone_name)}"
        for slot in slots
    )
    lines.append(_selection_prompt(slots=slots, timezone_label=timezone_label, language=language))
    return "\n".join(line for line in lines if line)


def _intro(*, request: BookingTimeRequest, plan: BookingPlanResult, coverage_summary: str, language: str) -> str:
    date_label = _request_date_label(request, language=language)
    time_label = _request_time_label(request, language=language)

    if language == "fr":
        if plan.match_mode == "exact_time" and date_label and time_label:
            return f"J'ai trouvé ce créneau pour un appel de consultation {date_label} à {time_label}:"
        if plan.match_mode == "same_day_alternative" and date_label:
            return f"Je ne vois pas ce créneau exact pour l'appel, mais voici les options les plus proches {date_label}:"
        if plan.match_mode in {"same_day", "period", "time_range"} and date_label:
            suffix = f" {time_label}" if time_label else ""
            return f"J'ai trouvé quelques créneaux d'appel {date_label}{suffix}:"
        if plan.match_mode in {"weekday", "same_weekday_alternative"} and request.requested_weekdays:
            return f"J'ai trouvé quelques créneaux d'appel le {_weekday_label(request.requested_weekdays[0], language=language)}:"
        if coverage_summary:
            return f"Je peux réserver un appel directement. Ce sera un appel de consultation. J'ai des disponibilités notamment {coverage_summary}. Voici quelques créneaux répartis:"
        return "Je peux réserver un appel directement. Ce sera un appel de consultation. Voici quelques créneaux disponibles:"

    if plan.match_mode == "exact_time" and date_label and time_label:
        return f"I found that consultation call time {date_label} at {time_label}:"
    if plan.match_mode == "same_day_alternative" and date_label:
        return f"I do not see that exact call time, but I found the closest options {date_label}:"
    if plan.match_mode in {"same_day", "period", "time_range"} and date_label:
        suffix = f" {time_label}" if time_label else ""
        return f"I found a few consultation call times {date_label}{suffix}:"
    if plan.match_mode in {"weekday", "same_weekday_alternative"} and request.requested_weekdays:
        return f"I found a few consultation call times on {request.requested_weekdays[0].title()}:"
    if plan.match_mode == "closest_alternative" and date_label:
        return f"I do not see openings {date_label}, but here are the closest consultation call times I found:"
    if coverage_summary:
        return f"I can book a consultation call directly. I have call openings including {coverage_summary}. Here are a few spread-out times:"
    return "I can book a consultation call directly. Here are a few available call times:"


def _no_slots_reply(*, request: BookingTimeRequest, language: str) -> str:
    if language == "fr":
        if request.scope != "broad":
            return "Je ne vois pas de disponibilités qui correspondent à cette demande. Envoyez-moi un autre jour ou une plage horaire, et je vérifierai."
        return "Je ne vois pas de disponibilités pour le moment. Envoyez-moi une journée et une plage horaire, et je peux vérifier d'autres options."
    if request.scope != "broad":
        return "I am not seeing call openings that match that request. Send me another day or time window and I can check again."
    return "I am not seeing open call times right now. Share a day and time window and I can check alternatives."


def _selection_prompt(*, slots: Sequence[Any], timezone_label: str, language: str) -> str:
    indexes = [_slot_index(slot) for slot in slots]
    choice_part = _choice_part(indexes, language=language)
    if language == "fr":
        return f"Répondez {choice_part} pour réserver l'appel, ou envoyez l'heure exacte souhaitée. Si aucune option ne fonctionne, envoyez-moi simplement un moment qui vous convient mieux. Heures affichées en {timezone_label}."
    return f"Reply with {choice_part} to book the call, or send the exact time you want. If none of those work, just send me a time that's better for you. Times shown in {timezone_label}."


def _choice_part(indexes: list[int], *, language: str) -> str:
    labels = [str(index) for index in indexes if index > 0]
    if not labels:
        return "une heure" if language == "fr" else "a time"
    if len(labels) == 1:
        return labels[0]
    if len(labels) == 2:
        return f"{labels[0]} ou {labels[1]}" if language == "fr" else f"{labels[0]} or {labels[1]}"
    return f"{', '.join(labels[:-1])}, ou {labels[-1]}" if language == "fr" else f"{', '.join(labels[:-1])}, or {labels[-1]}"


def _request_date_label(request: BookingTimeRequest, *, language: str) -> str:
    if request.requested_dates:
        labels = [_format_date(raw, language=language) for raw in request.requested_dates]
        if len(labels) == 1:
            return labels[0]
        joiner = "ou" if language == "fr" else "or"
        return ", ".join(labels[:-1]) + f", {joiner} {labels[-1]}"
    if request.date_range_start and request.date_range_end:
        start = _format_date(request.date_range_start, language=language)
        end = _format_date(request.date_range_end, language=language)
        return f"entre {start} et {end}" if language == "fr" else f"between {start} and {end}"
    return ""


def _request_time_label(request: BookingTimeRequest, *, language: str) -> str:
    if request.exact_time:
        return _format_time_label(request.exact_time, language=language)
    if request.range_start and request.range_end:
        start = _format_time_label(request.range_start, language=language)
        end = _format_time_label(request.range_end, language=language)
        return f"entre {start} et {end}" if language == "fr" else f"between {start} and {end}"
    if request.periods:
        if language == "fr":
            return "/".join(_FR_PERIODS.get(period, period) for period in request.periods)
        return "/".join(request.periods)
    return ""


def _format_date(raw: str, *, language: str) -> str:
    try:
        parsed = date.fromisoformat(str(raw))
    except ValueError:
        return str(raw)
    if language == "fr":
        return f"{_FR_WEEKDAYS[parsed.weekday()]} {parsed.day} {_FR_MONTHS[parsed.month - 1]}"
    return parsed.strftime("%a %b %d").replace(" 0", " ")


def _format_time_label(raw: str, *, language: str) -> str:
    text = " ".join(str(raw or "").split()).strip()
    if language != "fr":
        return text
    h_match = re.search(r"\b(\d{1,2})\s*h\s*(\d{1,2})?\b", text, re.IGNORECASE)
    if h_match:
        hour = int(h_match.group(1))
        minute = int(h_match.group(2) or "0")
        return f"{hour} h {minute:02d}"
    match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(AM|PM)\b", text, re.IGNORECASE)
    if not match:
        return text
    hour = int(match.group(1))
    minute = int(match.group(2) or "0")
    meridiem = match.group(3).lower()
    if meridiem == "pm" and hour != 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    return f"{hour} h {minute:02d}"


def _slot_index(slot: Any) -> int:
    raw = slot.get("index") if isinstance(slot, dict) else getattr(slot, "index", 0)
    try:
        return int(raw)
    except Exception:
        return 0


def _slot_display_time(slot: Any, *, language: str, timezone_name: str) -> str:
    display = str(slot.get("display_time") if isinstance(slot, dict) else getattr(slot, "display_time", "")).strip()
    start_time = str(slot.get("start_time") if isinstance(slot, dict) else getattr(slot, "start_time", "")).strip()
    parsed = _parse_slot_datetime(start_time)
    if parsed is not None and (language == "fr" or not display):
        return format_datetime_for_language(parsed, timezone_name=timezone_name, language=language)
    return display


def _weekday_label(day: str, *, language: str) -> str:
    normalized = str(day or "").strip().lower()
    weekdays_en = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    if normalized in weekdays_en:
        index = weekdays_en.index(normalized)
        return _FR_WEEKDAYS[index] if language == "fr" else normalized.title()
    return normalized


def _parse_slot_datetime(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
