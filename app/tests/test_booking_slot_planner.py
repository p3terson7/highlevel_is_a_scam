from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app.services.booking import BookingSlot
from app.services.booking_planner import plan_booking_slots
from app.services.booking_request import build_booking_time_request

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
