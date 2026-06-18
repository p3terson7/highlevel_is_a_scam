from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from app.services.booking_request import BookingTimeRequest

_PERIOD_ORDER = ("morning", "afternoon", "evening", "late")


@dataclass(frozen=True)
class SlotView:
    slot: Any
    start_utc: datetime
    start_local: datetime
    end_local: datetime | None
    date_iso: str
    weekday: str
    minutes: int
    period: str


@dataclass(frozen=True)
class BookingPlanResult:
    slots: list[Any]
    strategy: str
    match_mode: str
    candidate_count: int
    considered_count: int
    selected_count: int
    fallback_reason: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy,
            "match_mode": self.match_mode,
            "candidate_count": self.candidate_count,
            "considered_count": self.considered_count,
            "selected_count": self.selected_count,
            "fallback_reason": self.fallback_reason,
        }


def plan_booking_slots(
    *,
    slots: Sequence[Any],
    request: BookingTimeRequest,
    limit: int,
    timezone_name: str,
) -> BookingPlanResult:
    views = _slot_views(slots, timezone_name=timezone_name)
    considered = _without_avoided_weekdays(views, request)
    limit = max(1, min(limit, len(considered) or limit))
    if not considered:
        return BookingPlanResult(
            slots=[],
            strategy="no_available_slots",
            match_mode="none",
            candidate_count=0,
            considered_count=len(views),
            selected_count=0,
            fallback_reason="no_slots_after_avoidance",
        )

    if request.scope in {"specific_date", "specific_dates"} and request.requested_dates:
        return _plan_for_specific_dates(views=considered, request=request, limit=limit)

    if request.scope == "date_range" and request.date_range_start and request.date_range_end:
        return _plan_for_date_range(views=considered, request=request, limit=limit)

    if request.scope == "weekday_recurring" and request.requested_weekdays:
        return _plan_for_weekdays(views=considered, request=request, limit=limit)

    if request.scope == "time_only":
        return _plan_for_time_only(views=considered, request=request, limit=limit)

    return _plan_broad(views=considered, limit=limit)


def _plan_for_specific_dates(*, views: list[SlotView], request: BookingTimeRequest, limit: int) -> BookingPlanResult:
    requested = set(request.requested_dates)
    same_date = [view for view in views if view.date_iso in requested]
    if not same_date:
        selected = _select_closest_to_requested_dates(views, requested_dates=request.requested_dates, limit=limit)
        return _result(selected, "specific_date", "closest_alternative", len(same_date), len(views), "no_slots_on_requested_date")

    exact = _filter_exact_time(same_date, request)
    if exact:
        selected = _select_nearest_time(exact, request.exact_time_minutes, limit=limit)
        return _result(selected, "specific_date_exact_time", "exact_time", len(exact), len(views))

    windowed = _filter_time_window(same_date, request)
    if windowed:
        selected = _select_within_dates(windowed, limit=limit, prefer_spread=not request.has_time_constraint or request.all_day)
        return _result(selected, "specific_date_window", _time_match_mode(request), len(windowed), len(views))

    if request.has_time_constraint:
        selected = _select_same_day_alternatives(same_date, request=request, limit=limit)
        return _result(selected, "specific_date_alternative", "same_day_alternative", len(same_date), len(views), "requested_time_unavailable")

    selected = _select_within_dates(same_date, limit=limit, prefer_spread=True)
    return _result(selected, "specific_date", "same_day", len(same_date), len(views))


def _plan_for_date_range(*, views: list[SlotView], request: BookingTimeRequest, limit: int) -> BookingPlanResult:
    ranged = [view for view in views if str(request.date_range_start) <= view.date_iso <= str(request.date_range_end)]
    if not ranged:
        selected = _select_closest_to_requested_dates(views, requested_dates=[str(request.date_range_start)], limit=limit)
        return _result(selected, "date_range", "closest_alternative", 0, len(views), "no_slots_in_requested_range")
    windowed = _filter_time_window(ranged, request)
    if windowed:
        selected = _select_broad_spread(windowed, limit=limit)
        return _result(selected, "date_range_window", _time_match_mode(request), len(windowed), len(views))
    if request.has_time_constraint:
        selected = _select_broad_spread(ranged, limit=limit)
        return _result(selected, "date_range_alternative", "closest_in_range", len(ranged), len(views), "requested_time_unavailable")
    selected = _select_broad_spread(ranged, limit=limit)
    return _result(selected, "date_range", "date_range", len(ranged), len(views))


def _plan_for_weekdays(*, views: list[SlotView], request: BookingTimeRequest, limit: int) -> BookingPlanResult:
    weekdays = set(request.requested_weekdays)
    matching = [view for view in views if view.weekday in weekdays]
    if not matching:
        selected = _select_broad_spread(views, limit=limit)
        return _result(selected, "weekday_recurring", "closest_alternative", 0, len(views), "no_slots_on_requested_weekday")
    windowed = _filter_time_window(matching, request)
    if windowed:
        matching = windowed
        match_mode = _time_match_mode(request)
    elif request.has_time_constraint:
        match_mode = "same_weekday_alternative"
    else:
        match_mode = "weekday"
    selected = _select_earliest_day_then_fill(matching, limit=limit)
    return _result(selected, "weekday_recurring", match_mode, len(matching), len(views))


def _plan_for_time_only(*, views: list[SlotView], request: BookingTimeRequest, limit: int) -> BookingPlanResult:
    exact = _filter_exact_time(views, request)
    if exact:
        selected = _select_broad_spread(exact, limit=limit)
        return _result(selected, "time_only_exact", "exact_time", len(exact), len(views))
    windowed = _filter_time_window(views, request)
    if windowed:
        selected = _select_broad_spread(windowed, limit=limit)
        return _result(selected, "time_only_window", _time_match_mode(request), len(windowed), len(views))
    selected = _select_broad_spread(views, limit=limit)
    return _result(selected, "time_only_alternative", "closest_alternative", 0, len(views), "requested_time_unavailable")


def _plan_broad(*, views: list[SlotView], limit: int) -> BookingPlanResult:
    selected = _select_broad_spread(views, limit=limit)
    return _result(selected, "broad_coverage", "broad_coverage", len(views), len(views))


def _slot_views(slots: Sequence[Any], *, timezone_name: str) -> list[SlotView]:
    tz = _tzinfo(timezone_name)
    views: list[SlotView] = []
    for slot in slots:
        start_raw = _slot_value(slot, "start_time")
        start_utc = _parse_datetime(start_raw)
        if start_utc is None:
            continue
        end_utc = _parse_datetime(_slot_value(slot, "end_time"))
        start_local = start_utc.astimezone(tz)
        end_local = end_utc.astimezone(tz) if end_utc is not None else None
        views.append(
            SlotView(
                slot=slot,
                start_utc=start_utc,
                start_local=start_local,
                end_local=end_local,
                date_iso=start_local.date().isoformat(),
                weekday=start_local.strftime("%A").lower(),
                minutes=start_local.hour * 60 + start_local.minute,
                period=_period_for_time(start_local.time()),
            )
        )
    return sorted(views, key=lambda item: item.start_utc)


def _without_avoided_weekdays(views: list[SlotView], request: BookingTimeRequest) -> list[SlotView]:
    avoided = set(request.avoid_weekdays)
    if not avoided:
        return views
    return [view for view in views if view.weekday not in avoided]


def _filter_exact_time(views: list[SlotView], request: BookingTimeRequest) -> list[SlotView]:
    if request.exact_time_minutes is None:
        return []
    return [view for view in views if view.minutes == request.exact_time_minutes]


def _filter_time_window(views: list[SlotView], request: BookingTimeRequest) -> list[SlotView]:
    if request.exact_time_minutes is not None:
        return _filter_exact_time(views, request)
    filtered = list(views)
    if request.range_start_minutes is not None:
        filtered = [view for view in filtered if view.minutes >= request.range_start_minutes]
    if request.range_end_minutes is not None:
        end = request.range_end_minutes % (24 * 60)
        filtered = [view for view in filtered if view.minutes <= end]
    if request.periods:
        periods = set(request.periods)
        filtered = [view for view in filtered if view.period in periods]
    return filtered


def _select_within_dates(views: list[SlotView], *, limit: int, prefer_spread: bool) -> list[SlotView]:
    if not prefer_spread:
        return views[:limit]
    return _select_day_period_spread(views, limit=limit)


def _select_same_day_alternatives(views: list[SlotView], *, request: BookingTimeRequest, limit: int) -> list[SlotView]:
    if request.exact_time_minutes is not None:
        return _select_nearest_time(views, request.exact_time_minutes, limit=limit)
    return _select_day_period_spread(views, limit=limit)


def _select_nearest_time(views: list[SlotView], target_minutes: int | None, *, limit: int) -> list[SlotView]:
    if target_minutes is None:
        return views[:limit]
    return sorted(views, key=lambda view: (abs(view.minutes - target_minutes), view.start_utc))[:limit]


def _select_earliest_day_then_fill(views: list[SlotView], *, limit: int) -> list[SlotView]:
    if not views:
        return []
    first_date = views[0].date_iso
    first_day = [view for view in views if view.date_iso == first_date]
    selected = _select_day_period_spread(first_day, limit=limit)
    if len(selected) >= limit:
        return selected[:limit]
    selected_keys = {view.start_utc for view in selected}
    for view in views:
        if view.start_utc in selected_keys:
            continue
        selected.append(view)
        selected_keys.add(view.start_utc)
        if len(selected) >= limit:
            break
    return sorted(selected, key=lambda view: view.start_utc)[:limit]


def _select_day_period_spread(views: list[SlotView], *, limit: int) -> list[SlotView]:
    if len(views) <= limit:
        return views[:limit]
    selected: list[SlotView] = []
    selected_keys: set[datetime] = set()

    def add(view: SlotView) -> None:
        if len(selected) >= limit or view.start_utc in selected_keys:
            return
        selected.append(view)
        selected_keys.add(view.start_utc)

    for date_iso in _ordered_dates(views):
        day_views = [view for view in views if view.date_iso == date_iso]
        for period in _PERIOD_ORDER:
            period_views = [view for view in day_views if view.period == period]
            if period_views:
                add(period_views[0])
            if len(selected) >= limit:
                return sorted(selected, key=lambda view: view.start_utc)
        for view in day_views:
            if _is_spaced(view, selected, minutes=90):
                add(view)
            if len(selected) >= limit:
                return sorted(selected, key=lambda item: item.start_utc)
    for view in views:
        add(view)
        if len(selected) >= limit:
            break
    return sorted(selected, key=lambda view: view.start_utc)


def _select_broad_spread(views: list[SlotView], *, limit: int) -> list[SlotView]:
    if len(views) <= limit:
        return views[:limit]
    selected: list[SlotView] = []
    selected_keys: set[datetime] = set()

    def add(view: SlotView) -> None:
        if len(selected) >= limit or view.start_utc in selected_keys:
            return
        selected.append(view)
        selected_keys.add(view.start_utc)

    seen_dates: set[str] = set()
    for view in views:
        if view.date_iso in seen_dates:
            continue
        seen_dates.add(view.date_iso)
        add(view)
        if len(selected) >= limit:
            return sorted(selected, key=lambda item: item.start_utc)

    seen_periods = {(view.date_iso, view.period) for view in selected}
    for view in views:
        key = (view.date_iso, view.period)
        if key in seen_periods:
            continue
        seen_periods.add(key)
        add(view)
        if len(selected) >= limit:
            return sorted(selected, key=lambda item: item.start_utc)

    for view in views:
        if _is_spaced(view, selected, minutes=90):
            add(view)
        if len(selected) >= limit:
            return sorted(selected, key=lambda item: item.start_utc)

    for view in views:
        add(view)
        if len(selected) >= limit:
            break
    return sorted(selected, key=lambda item: item.start_utc)


def _select_closest_to_requested_dates(views: list[SlotView], *, requested_dates: Sequence[str], limit: int) -> list[SlotView]:
    parsed_dates: list[date] = []
    for raw in requested_dates:
        try:
            parsed_dates.append(date.fromisoformat(str(raw)))
        except ValueError:
            continue
    if not parsed_dates:
        return _select_broad_spread(views, limit=limit)

    def date_distance_key(view: SlotView) -> tuple[int, int, datetime]:
        deltas = [(view.start_local.date() - target).days for target in parsed_dates]
        future_or_same = [delta for delta in deltas if delta >= 0]
        if future_or_same:
            return (0, min(future_or_same), view.start_utc)
        return (1, min(abs(delta) for delta in deltas), view.start_utc)

    return sorted(views, key=date_distance_key)[:limit]


def _result(
    selected: list[SlotView],
    strategy: str,
    match_mode: str,
    candidate_count: int,
    considered_count: int,
    fallback_reason: str | None = None,
) -> BookingPlanResult:
    return BookingPlanResult(
        slots=[view.slot for view in selected],
        strategy=strategy,
        match_mode=match_mode,
        candidate_count=candidate_count,
        considered_count=considered_count,
        selected_count=len(selected),
        fallback_reason=fallback_reason,
    )


def _time_match_mode(request: BookingTimeRequest) -> str:
    if request.exact_time_minutes is not None:
        return "exact_time"
    if request.range_start_minutes is not None or request.range_end_minutes is not None:
        return "time_range"
    if request.periods:
        return "period"
    return "same_day"


def _is_spaced(view: SlotView, selected: list[SlotView], *, minutes: int) -> bool:
    return all(abs((view.start_local - item.start_local).total_seconds()) >= minutes * 60 for item in selected)


def _ordered_dates(views: list[SlotView]) -> list[str]:
    return list(dict.fromkeys(view.date_iso for view in views))


def _period_for_time(value: dt_time) -> str:
    minutes = value.hour * 60 + value.minute
    if 8 * 60 <= minutes < 12 * 60:
        return "morning"
    if 12 * 60 <= minutes < 17 * 60:
        return "afternoon"
    if 17 * 60 <= minutes < 21 * 60:
        return "evening"
    return "late"


def _slot_value(slot: Any, key: str) -> str:
    if isinstance(slot, dict):
        return str(slot.get(key) or "")
    return str(getattr(slot, key, "") or "")


def _parse_datetime(raw: str) -> datetime | None:
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


def _tzinfo(tz_name: str):
    try:
        return ZoneInfo(tz_name or "UTC")
    except Exception:
        return timezone.utc
