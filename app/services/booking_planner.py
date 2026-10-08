from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, time as dt_time, timedelta, timezone
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
    outcome: str = ""
    constraints_satisfied: bool = False
    matching_slots: list[Any] = field(default_factory=list)
    alternative_slots: list[Any] = field(default_factory=list)
    searched_coverage: BookingSearchCoverage | None = None

    def to_payload(self) -> dict[str, Any]:
        coverage = self.searched_coverage
        return {
            "strategy": self.strategy,
            "match_mode": self.match_mode,
            "candidate_count": self.candidate_count,
            "considered_count": self.considered_count,
            "selected_count": self.selected_count,
            "fallback_reason": self.fallback_reason,
            "outcome": self.outcome,
            "constraints_satisfied": self.constraints_satisfied,
            "matching_count": len(self.matching_slots),
            "alternative_count": len(self.alternative_slots),
            "coverage_start_date": coverage.start_date if coverage else None,
            "coverage_end_date": coverage.end_date if coverage else None,
            "coverage_complete": coverage.complete if coverage else True,
            "coverage_reason": coverage.reason if coverage else None,
            "searched_coverage": coverage.to_payload() if coverage else None,
        }


@dataclass(frozen=True)
class BookingSearchCoverage:
    """The provider window that was actually searched for this request."""

    start_date: str | None = None
    end_date: str | None = None
    complete: bool = True
    reason: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "complete": self.complete,
            "reason": self.reason,
        }


def plan_booking_slots(
    *,
    slots: Sequence[Any],
    request: BookingTimeRequest,
    limit: int,
    timezone_name: str,
    searched_coverage: BookingSearchCoverage | None = None,
    coverage_start_date: str | None = None,
    coverage_end_date: str | None = None,
    coverage_complete: bool = True,
) -> BookingPlanResult:
    if searched_coverage is None and (
        coverage_start_date is not None or coverage_end_date is not None or not coverage_complete
    ):
        searched_coverage = BookingSearchCoverage(
            start_date=coverage_start_date,
            end_date=coverage_end_date,
            complete=coverage_complete,
        )
    views = _slot_views(slots, timezone_name=timezone_name)
    if _request_needs_clarification(request):
        return BookingPlanResult(
            slots=[],
            strategy="needs_clarification",
            match_mode="none",
            candidate_count=0,
            considered_count=len(views),
            selected_count=0,
            fallback_reason="incomplete_time_request",
            outcome="needs_clarification",
            constraints_satisfied=False,
            searched_coverage=searched_coverage,
        )
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
            outcome=_unmatched_outcome(request, searched_coverage),
            constraints_satisfied=False,
            searched_coverage=searched_coverage,
        )

    if request.scope in {"specific_date", "specific_dates"} and request.requested_dates:
        result = _plan_for_specific_dates(views=considered, request=request, limit=limit)
        return _with_coverage(result, request=request, coverage=searched_coverage)

    if request.scope == "date_range" and request.date_range_start and request.date_range_end:
        result = _plan_for_date_range(views=considered, request=request, limit=limit)
        return _with_coverage(result, request=request, coverage=searched_coverage)

    if request.scope == "weekday_recurring" and request.requested_weekdays:
        result = _plan_for_weekdays(views=considered, request=request, limit=limit)
        return _with_coverage(result, request=request, coverage=searched_coverage)

    if request.scope == "time_only":
        result = _plan_for_time_only(views=considered, request=request, limit=limit)
        return _with_coverage(result, request=request, coverage=searched_coverage)

    return _with_coverage(
        _plan_broad(views=considered, limit=limit),
        request=request,
        coverage=searched_coverage,
    )


def _plan_for_specific_dates(*, views: list[SlotView], request: BookingTimeRequest, limit: int) -> BookingPlanResult:
    requested = set(request.requested_dates)
    date_candidates = _views_for_anchor_dates(
        views,
        anchor_dates=requested,
        request=request,
    )
    if not date_candidates:
        selected = _select_closest_to_requested_dates(views, requested_dates=request.requested_dates, limit=limit)
        return _result(
            selected,
            "specific_date",
            "closest_alternative",
            0,
            len(views),
            "no_slots_on_requested_date",
            outcome="unavailable_within_coverage",
            constraints_satisfied=False,
        )

    exact = _filter_exact_time(date_candidates, request)
    if exact:
        selected = _select_nearest_time(exact, request.exact_time_minutes, limit=limit)
        return _result(
            selected,
            "specific_date_exact_time",
            "exact_time",
            len(exact),
            len(views),
            outcome="exact_available",
            constraints_satisfied=True,
        )

    windowed = _filter_time_window(
        date_candidates,
        request,
        anchor_dates=requested,
    )
    if windowed:
        selected = _select_within_dates(windowed, limit=limit, prefer_spread=not request.has_time_constraint or request.all_day)
        return _result(
            selected,
            "specific_date_window",
            _time_match_mode(request),
            len(windowed),
            len(views),
            outcome="requested_window_available",
            constraints_satisfied=True,
        )

    if request.has_time_constraint:
        selected = _select_same_day_alternatives(date_candidates, request=request, limit=limit)
        return _result(
            selected,
            "specific_date_alternative",
            "same_day_alternative",
            0,
            len(views),
            "requested_time_unavailable",
            outcome="unavailable_within_coverage",
            constraints_satisfied=False,
        )

    selected = _select_within_dates(date_candidates, limit=limit, prefer_spread=True)
    return _result(
        selected,
        "specific_date",
        "same_day",
        len(date_candidates),
        len(views),
        outcome="requested_window_available",
        constraints_satisfied=True,
    )


def _plan_for_date_range(*, views: list[SlotView], request: BookingTimeRequest, limit: int) -> BookingPlanResult:
    anchor_dates = _date_range_values(
        request.date_range_start,
        request.date_range_end,
    )
    ranged = _views_for_anchor_dates(
        views,
        anchor_dates=anchor_dates,
        request=request,
    )
    if not ranged:
        selected = _select_closest_to_requested_dates(views, requested_dates=[str(request.date_range_start)], limit=limit)
        return _result(
            selected,
            "date_range",
            "closest_alternative",
            0,
            len(views),
            "no_slots_in_requested_range",
            outcome="unavailable_within_coverage",
            constraints_satisfied=False,
        )
    windowed = _filter_time_window(
        ranged,
        request,
        anchor_dates=anchor_dates,
    )
    if windowed:
        selected = _select_broad_spread(windowed, limit=limit)
        return _result(
            selected,
            "date_range_window",
            _time_match_mode(request),
            len(windowed),
            len(views),
            outcome="requested_window_available",
            constraints_satisfied=True,
        )
    if request.has_time_constraint:
        selected = _select_broad_spread(ranged, limit=limit)
        return _result(
            selected,
            "date_range_alternative",
            "closest_in_range",
            0,
            len(views),
            "requested_time_unavailable",
            outcome="unavailable_within_coverage",
            constraints_satisfied=False,
        )
    selected = _select_broad_spread(ranged, limit=limit)
    return _result(
        selected,
        "date_range",
        "date_range",
        len(ranged),
        len(views),
        outcome="requested_window_available",
        constraints_satisfied=True,
    )


def _plan_for_weekdays(*, views: list[SlotView], request: BookingTimeRequest, limit: int) -> BookingPlanResult:
    weekdays = set(request.requested_weekdays)
    anchor_dates = {
        view.date_iso
        for view in views
        if view.weekday in weekdays
    }
    if _is_overnight_range(request):
        for view in views:
            previous_date = view.start_local.date() - timedelta(days=1)
            if previous_date.strftime("%A").lower() in weekdays:
                anchor_dates.add(previous_date.isoformat())
    matching = _views_for_anchor_dates(
        views,
        anchor_dates=anchor_dates,
        request=request,
    )
    if not matching:
        selected = _select_broad_spread(views, limit=limit)
        return _result(
            selected,
            "weekday_recurring",
            "closest_alternative",
            0,
            len(views),
            "no_slots_on_requested_weekday",
            outcome="unavailable_within_coverage",
            constraints_satisfied=False,
        )
    windowed = _filter_time_window(
        matching,
        request,
        anchor_dates=anchor_dates,
    )
    if windowed:
        selected = _select_earliest_day_then_fill(windowed, limit=limit)
        return _result(
            selected,
            "weekday_recurring",
            _time_match_mode(request),
            len(windowed),
            len(views),
            outcome="requested_window_available",
            constraints_satisfied=True,
        )
    elif request.has_time_constraint:
        selected = _select_earliest_day_then_fill(matching, limit=limit)
        return _result(
            selected,
            "weekday_recurring",
            "same_weekday_alternative",
            0,
            len(views),
            "requested_time_unavailable",
            outcome="unavailable_within_coverage",
            constraints_satisfied=False,
        )
    selected = _select_earliest_day_then_fill(matching, limit=limit)
    return _result(
        selected,
        "weekday_recurring",
        "weekday",
        len(matching),
        len(views),
        outcome="requested_window_available",
        constraints_satisfied=True,
    )


def _plan_for_time_only(*, views: list[SlotView], request: BookingTimeRequest, limit: int) -> BookingPlanResult:
    exact = _filter_exact_time(views, request)
    if exact:
        selected = _select_broad_spread(exact, limit=limit)
        return _result(
            selected,
            "time_only_exact",
            "exact_time",
            len(exact),
            len(views),
            outcome="exact_available",
            constraints_satisfied=True,
        )
    windowed = _filter_time_window(views, request)
    if windowed:
        selected = _select_broad_spread(windowed, limit=limit)
        return _result(
            selected,
            "time_only_window",
            _time_match_mode(request),
            len(windowed),
            len(views),
            outcome="requested_window_available",
            constraints_satisfied=True,
        )
    selected = _select_broad_spread(views, limit=limit)
    return _result(
        selected,
        "time_only_alternative",
        "closest_alternative",
        0,
        len(views),
        "requested_time_unavailable",
        outcome="unavailable_within_coverage",
        constraints_satisfied=False,
    )


def _plan_broad(*, views: list[SlotView], limit: int) -> BookingPlanResult:
    selected = _select_broad_spread(views, limit=limit)
    return _result(
        selected,
        "broad_coverage",
        "broad_coverage",
        len(views),
        len(views),
        outcome="broad_availability",
        constraints_satisfied=True,
    )


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


def _filter_time_window(
    views: list[SlotView],
    request: BookingTimeRequest,
    *,
    anchor_dates: set[str] | None = None,
) -> list[SlotView]:
    if request.exact_time_minutes is not None:
        return _filter_exact_time(views, request)
    filtered = list(views)
    start = request.range_start_minutes
    end = request.range_end_minutes
    if start is not None and end is not None and end > 24 * 60:
        overflow_end = end - 24 * 60
        if anchor_dates:
            filtered = [
                view
                for view in filtered
                if (
                    view.date_iso in anchor_dates
                    and view.minutes >= start
                )
                or (
                    (
                        view.start_local.date() - timedelta(days=1)
                    ).isoformat()
                    in anchor_dates
                    and view.minutes <= overflow_end
                )
            ]
        else:
            filtered = [
                view
                for view in filtered
                if view.minutes >= start or view.minutes <= overflow_end
            ]
    else:
        if start is not None:
            filtered = [view for view in filtered if view.minutes >= start]
        if end is not None:
            filtered = [
                view
                for view in filtered
                if view.minutes <= min(end, 24 * 60 - 1)
            ]
    if request.periods:
        periods = set(request.periods)
        filtered = [view for view in filtered if view.period in periods]
    return filtered


def _is_overnight_range(request: BookingTimeRequest) -> bool:
    return bool(
        request.range_start_minutes is not None
        and request.range_end_minutes is not None
        and request.range_end_minutes > 24 * 60
    )


def _views_for_anchor_dates(
    views: list[SlotView],
    *,
    anchor_dates: set[str],
    request: BookingTimeRequest,
) -> list[SlotView]:
    if not anchor_dates:
        return []
    if not _is_overnight_range(request):
        return [view for view in views if view.date_iso in anchor_dates]
    spill_dates = {
        (parsed + timedelta(days=1)).isoformat()
        for raw_date in anchor_dates
        if (parsed := _parse_date(raw_date)) is not None
    }
    return [
        view
        for view in views
        if view.date_iso in anchor_dates or view.date_iso in spill_dates
    ]


def _date_range_values(
    start_raw: str | None,
    end_raw: str | None,
) -> set[str]:
    start = _parse_date(start_raw)
    end = _parse_date(end_raw)
    if start is None or end is None or end < start:
        return set()
    return {
        (start + timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    }


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
    *,
    outcome: str,
    constraints_satisfied: bool,
) -> BookingPlanResult:
    selected_slots = [view.slot for view in selected]
    return BookingPlanResult(
        slots=selected_slots,
        strategy=strategy,
        match_mode=match_mode,
        candidate_count=candidate_count,
        considered_count=considered_count,
        selected_count=len(selected),
        fallback_reason=fallback_reason,
        outcome=outcome,
        constraints_satisfied=constraints_satisfied,
        matching_slots=selected_slots if constraints_satisfied else [],
        alternative_slots=[] if constraints_satisfied else selected_slots,
    )


def _with_coverage(
    result: BookingPlanResult,
    *,
    request: BookingTimeRequest,
    coverage: BookingSearchCoverage | None,
) -> BookingPlanResult:
    outcome = result.outcome
    if not result.constraints_satisfied and outcome == "unavailable_within_coverage":
        outcome = _unmatched_outcome_for_coverage(request, coverage)
    return replace(result, outcome=outcome, searched_coverage=coverage)


def _unmatched_outcome(
    request: BookingTimeRequest,
    coverage: BookingSearchCoverage | None,
) -> str:
    if request.scope == "broad" and not request.avoid_weekdays:
        return "no_availability"
    return _unmatched_outcome_for_coverage(request, coverage)


def _unmatched_outcome_for_coverage(
    request: BookingTimeRequest,
    coverage: BookingSearchCoverage | None,
) -> str:
    if coverage is not None and (not coverage.complete or not _coverage_contains_request(coverage, request)):
        return "outside_search_coverage"
    return "unavailable_within_coverage"


def _coverage_contains_request(
    coverage: BookingSearchCoverage,
    request: BookingTimeRequest,
) -> bool:
    start = _parse_date(coverage.start_date)
    end = _parse_date(coverage.end_date)
    if start is None or end is None:
        return True

    if request.requested_dates:
        targets = [_parse_date(raw) for raw in request.requested_dates]
        spill_days = 1 if _is_overnight_range(request) else 0
        return all(
            target is not None
            and start <= target
            and target + timedelta(days=spill_days) <= end
            for target in targets
        )
    if request.date_range_start or request.date_range_end:
        range_start = _parse_date(request.date_range_start)
        range_end = _parse_date(request.date_range_end)
        if range_end is not None and _is_overnight_range(request):
            range_end += timedelta(days=1)
        return (
            range_start is not None
            and range_end is not None
            and start <= range_start
            and range_end <= end
        )
    if request.requested_weekdays:
        requested_indexes = {_weekday_index(day) for day in request.requested_weekdays}
        requested_indexes.discard(None)
        latest_anchor = end - timedelta(days=1) if _is_overnight_range(request) else end
        covered_indexes: set[int] = set()
        current = start
        while current <= latest_anchor:
            if current.weekday() in requested_indexes:
                covered_indexes.add(current.weekday())
            current = date.fromordinal(current.toordinal() + 1)
        return covered_indexes == requested_indexes
    return True


def _parse_date(raw: str | None) -> date | None:
    if raw is None:
        return None
    try:
        return date.fromisoformat(str(raw))
    except ValueError:
        return None


def _weekday_index(day: str) -> int | None:
    weekdays = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
    normalized = str(day or "").strip().lower()
    try:
        return weekdays.index(normalized)
    except ValueError:
        return None


def _request_needs_clarification(request: BookingTimeRequest) -> bool:
    if {
        "ambiguous_time",
        "ambiguous_numeric_date",
        "multiple_temporal_clauses",
        "weekday_date_mismatch",
        "negated_time_constraint",
    } & set(request.reasons):
        return True
    if request.scope in {"specific_date", "specific_dates"}:
        return not request.requested_dates
    if request.scope == "date_range":
        return not (request.date_range_start and request.date_range_end)
    if request.scope == "weekday_recurring":
        return not request.requested_weekdays
    return False


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
