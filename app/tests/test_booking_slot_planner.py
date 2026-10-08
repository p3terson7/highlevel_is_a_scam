from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app.services.booking import BookingSlot
from app.services.booking_planner import BookingSearchCoverage, plan_booking_slots
from app.services.booking_request import BookingTimeRequest, build_booking_time_request

TZ = "America/Toronto"


def _now() -> datetime:
    return datetime(2026, 6, 17, 18, 53, tzinfo=timezone.utc)


def _slot(index: int, year: int, month: int, day: int, hour: int, minute: int = 0) -> BookingSlot:
    local = datetime(year, month, day, hour, minute, tzinfo=ZoneInfo(TZ))
    end = local.replace(minute=minute + 30) if minute <= 29 else local.replace(hour=hour + 1, minute=(minute + 30) % 60)
    start_utc = local.astimezone(timezone.utc).replace(microsecond=0)
    end_utc = end.astimezone(timezone.utc).replace(microsecond=0)
    display = local.strftime("%a %b %d at %-I:%M %p").replace(" 0", " ")
    hint = local.strftime("%A %-I:%M %p")
    return BookingSlot(
        index=index,
        start_time=start_utc.isoformat().replace("+00:00", "Z"),
        end_time=end_utc.isoformat().replace("+00:00", "Z"),
        display_time=display,
        display_hint=hint,
        search_blob=hint.lower(),
    )


def _selected_dates(plan) -> set[str]:
    return {slot.start_time[:10] for slot in plan.slots}


def test_specific_next_monday_does_not_offer_future_mondays():
    slots = [
        _slot(1, 2026, 6, 22, 9, 30),
        _slot(2, 2026, 6, 22, 11, 0),
        _slot(3, 2026, 6, 22, 14, 0),
        _slot(4, 2026, 6, 29, 9, 30),
        _slot(5, 2026, 7, 6, 9, 30),
    ]
    request = build_booking_time_request(
        text="I am available for a call next Monday all day",
        timezone_name=TZ,
        now_utc=_now(),
    )

    plan = plan_booking_slots(slots=slots, request=request, limit=3, timezone_name=TZ)

    assert plan.match_mode == "same_day"
    assert [slot.display_time for slot in plan.slots] == [
        "Mon Jun 22 at 9:30 AM",
        "Mon Jun 22 at 11:00 AM",
        "Mon Jun 22 at 2:00 PM",
    ]
    assert _selected_dates(plan) == {"2026-06-22"}


def test_exact_time_request_finds_requested_time_outside_previous_options():
    slots = [
        _slot(1, 2026, 6, 22, 9, 30),
        _slot(2, 2026, 6, 22, 10, 0),
        _slot(3, 2026, 6, 22, 11, 0),
    ]
    request = build_booking_time_request(text="Can you do Monday 11 AM?", timezone_name=TZ, now_utc=_now())

    plan = plan_booking_slots(slots=slots, request=request, limit=3, timezone_name=TZ)

    assert plan.match_mode == "exact_time"
    assert plan.outcome == "exact_available"
    assert plan.constraints_satisfied is True
    assert plan.matching_slots == plan.slots
    assert plan.alternative_slots == []
    assert len(plan.slots) == 1
    assert plan.slots[0].display_time == "Mon Jun 22 at 11:00 AM"


def test_exact_time_unavailable_stays_on_same_day_before_jumping_elsewhere():
    slots = [
        _slot(1, 2026, 6, 22, 9, 30),
        _slot(2, 2026, 6, 22, 10, 0),
        _slot(3, 2026, 6, 22, 14, 0),
        _slot(4, 2026, 6, 23, 11, 0),
    ]
    request = build_booking_time_request(text="Can you do Monday 11 AM?", timezone_name=TZ, now_utc=_now())

    plan = plan_booking_slots(slots=slots, request=request, limit=3, timezone_name=TZ)

    assert plan.match_mode == "same_day_alternative"
    assert plan.fallback_reason == "requested_time_unavailable"
    assert plan.outcome == "unavailable_within_coverage"
    assert plan.constraints_satisfied is False
    assert plan.matching_slots == []
    assert plan.alternative_slots == plan.slots
    assert _selected_dates(plan) == {"2026-06-22"}


def test_broad_request_still_spreads_across_days():
    slots = [
        _slot(1, 2026, 6, 18, 9, 0),
        _slot(2, 2026, 6, 18, 9, 30),
        _slot(3, 2026, 6, 18, 10, 0),
        _slot(4, 2026, 6, 19, 13, 0),
        _slot(5, 2026, 6, 22, 16, 0),
    ]
    request = build_booking_time_request(text="", timezone_name=TZ, now_utc=_now(), source="initial_offer")

    plan = plan_booking_slots(slots=slots, request=request, limit=3, timezone_name=TZ)

    assert plan.strategy == "broad_coverage"
    assert len(_selected_dates(plan)) == 3


def test_missing_requested_date_prefers_later_alternatives_before_earlier_ones():
    slots = [
        _slot(1, 2026, 6, 20, 9, 0),
        _slot(2, 2026, 6, 23, 9, 0),
        _slot(3, 2026, 6, 24, 9, 0),
    ]
    request = build_booking_time_request(
        text="I am available for a call next Monday all day",
        timezone_name=TZ,
        now_utc=_now(),
    )

    plan = plan_booking_slots(slots=slots, request=request, limit=2, timezone_name=TZ)

    assert plan.match_mode == "closest_alternative"
    assert [slot.display_time for slot in plan.slots] == [
        "Tue Jun 23 at 9:00 AM",
        "Wed Jun 24 at 9:00 AM",
    ]


def test_requested_weekday_time_mismatch_is_explicitly_alternatives():
    slots = [
        _slot(1, 2026, 6, 19, 9, 0),
        _slot(2, 2026, 6, 19, 10, 0),
        _slot(3, 2026, 6, 22, 12, 0),
    ]
    request = BookingTimeRequest(
        scope="weekday_recurring",
        requested_weekdays=("friday",),
        exact_time="12 PM",
        exact_time_minutes=12 * 60,
    )

    plan = plan_booking_slots(slots=slots, request=request, limit=2, timezone_name=TZ)

    assert plan.match_mode == "same_weekday_alternative"
    assert plan.outcome == "unavailable_within_coverage"
    assert plan.constraints_satisfied is False
    assert plan.matching_slots == []
    assert plan.alternative_slots == plan.slots
    assert all("Fri Jun 19" in slot.display_time for slot in plan.alternative_slots)


def test_unmatched_request_outside_provider_window_does_not_claim_unavailable():
    slots = [
        _slot(1, 2026, 6, 22, 9, 0),
        _slot(2, 2026, 6, 23, 9, 0),
    ]
    request = BookingTimeRequest(
        scope="specific_date",
        requested_dates=("2026-07-10",),
        exact_time="12 PM",
        exact_time_minutes=12 * 60,
    )

    plan = plan_booking_slots(
        slots=slots,
        request=request,
        limit=2,
        timezone_name=TZ,
        coverage_start_date="2026-06-18",
        coverage_end_date="2026-06-25",
        coverage_complete=True,
    )

    assert plan.outcome == "outside_search_coverage"
    assert plan.constraints_satisfied is False
    assert plan.alternative_slots == plan.slots
    assert plan.to_payload()["coverage_end_date"] == "2026-06-25"
    assert plan.to_payload()["coverage_complete"] is True


def test_matching_slots_always_satisfy_all_hard_date_and_time_constraints():
    slots = [
        _slot(1, 2026, 6, 22, 9, 0),
        _slot(2, 2026, 6, 22, 12, 0),
        _slot(3, 2026, 6, 23, 12, 0),
    ]
    request = BookingTimeRequest(
        scope="specific_date",
        requested_dates=("2026-06-22",),
        exact_time="12 PM",
        exact_time_minutes=12 * 60,
    )

    plan = plan_booking_slots(slots=slots, request=request, limit=3, timezone_name=TZ)

    assert plan.constraints_satisfied is True
    assert len(plan.matching_slots) == 1
    for slot in plan.matching_slots:
        local = datetime.fromisoformat(slot.start_time.replace("Z", "+00:00")).astimezone(ZoneInfo(TZ))
        assert local.date().isoformat() in request.requested_dates
        assert local.hour * 60 + local.minute == request.exact_time_minutes


def test_range_ending_at_midnight_keeps_late_evening_slots():
    request = build_booking_time_request(
        text="Can we meet Friday from 10 PM to midnight?",
        timezone_name=TZ,
        now_utc=_now(),
    )
    slots = [
        _slot(1, 2026, 6, 19, 21, 0),
        _slot(2, 2026, 6, 19, 22, 0),
        _slot(3, 2026, 6, 19, 23, 0),
        _slot(4, 2026, 6, 20, 0, 0),
    ]

    plan = plan_booking_slots(
        slots=slots,
        request=request,
        limit=3,
        timezone_name=TZ,
    )

    assert plan.outcome == "requested_window_available"
    assert plan.constraints_satisfied is True
    assert [
        datetime.fromisoformat(slot.start_time.replace("Z", "+00:00"))
        .astimezone(ZoneInfo(TZ))
        .hour
        for slot in plan.matching_slots
    ] == [22, 23]


def test_true_overnight_range_uses_the_requested_day_and_next_day_spill():
    request = build_booking_time_request(
        text="Friday from 11 PM to 1 AM",
        timezone_name=TZ,
        now_utc=_now(),
    )
    slots = [
        _slot(1, 2026, 6, 19, 22, 30),
        _slot(2, 2026, 6, 19, 23, 0),
        _slot(3, 2026, 6, 20, 0, 30),
        _slot(4, 2026, 6, 20, 2, 0),
    ]

    plan = plan_booking_slots(
        slots=slots,
        request=request,
        limit=4,
        timezone_name=TZ,
    )

    assert plan.outcome == "requested_window_available"
    selected_local = [
        datetime.fromisoformat(slot.start_time.replace("Z", "+00:00")).astimezone(
            ZoneInfo(TZ)
        )
        for slot in plan.matching_slots
    ]
    assert [(item.weekday(), item.hour, item.minute) for item in selected_local] == [
        (4, 23, 0),
        (5, 0, 30),
    ]


def test_ambiguous_clock_hour_requires_clarification_before_slot_selection():
    request = build_booking_time_request(
        text="Friday at 12",
        timezone_name=TZ,
        now_utc=_now(),
    )

    plan = plan_booking_slots(
        slots=[_slot(1, 2026, 6, 19, 9, 0), _slot(2, 2026, 6, 19, 12, 0)],
        request=request,
        limit=2,
        timezone_name=TZ,
    )

    assert plan.outcome == "needs_clarification"
    assert plan.constraints_satisfied is False
    assert plan.slots == []


def test_incomplete_constrained_request_needs_clarification_instead_of_offering_slots():
    request = BookingTimeRequest(scope="specific_date")

    plan = plan_booking_slots(
        slots=[_slot(1, 2026, 6, 22, 9, 0)],
        request=request,
        limit=1,
        timezone_name=TZ,
    )

    assert plan.outcome == "needs_clarification"
    assert plan.constraints_satisfied is False
    assert plan.slots == []


def test_structured_search_coverage_remains_supported_for_internal_callers():
    request = BookingTimeRequest(
        scope="specific_date",
        requested_dates=("2026-06-30",),
    )
    coverage = BookingSearchCoverage(
        start_date="2026-06-18",
        end_date="2026-06-25",
        complete=False,
        reason="provider_window_limit",
    )

    plan = plan_booking_slots(
        slots=[_slot(1, 2026, 6, 22, 9, 0)],
        request=request,
        limit=1,
        timezone_name=TZ,
        searched_coverage=coverage,
    )

    assert plan.outcome == "outside_search_coverage"
    assert plan.searched_coverage == coverage
    assert plan.to_payload()["coverage_reason"] == "provider_window_limit"
