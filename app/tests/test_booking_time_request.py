from __future__ import annotations

from datetime import datetime, timezone

from app.services.booking_request import build_booking_time_request


def _now() -> datetime:
    return datetime(2026, 6, 17, 18, 53, tzinfo=timezone.utc)


def test_booking_request_preserves_next_weekday_as_specific_date():
    request = build_booking_time_request(
        text="I am available for a call next Monday all day",
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert request.scope == "specific_date"
    assert request.requested_dates == ("2026-06-22",)
    assert request.preferred_day == "monday"
    assert request.all_day is True
    assert "specific_next_weekday:monday" in request.reasons


def test_booking_request_distinguishes_recurring_weekday_from_next_occurrence():
    request = build_booking_time_request(
        text="Mondays usually work for me",
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert request.scope == "weekday_recurring"
    assert request.requested_weekdays == ("monday",)
    assert request.requested_dates == ()


def test_booking_request_parses_exact_time_and_change_of_mind():
    request = build_booking_time_request(
        text="Those don't work, can you do Monday 11 AM instead?",
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert request.scope == "specific_date"
    assert request.requested_dates == ("2026-06-22",)
    assert request.exact_time == "11 AM"
    assert request.exact_time_minutes == 11 * 60
    assert request.change_of_mind is True


def test_booking_request_parses_french_day_and_24h_time():
    request = build_booking_time_request(
        text="Mercredi à 10h00",
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert request.scope == "specific_date"
    assert request.preferred_day == "wednesday"
    assert request.exact_time == "10 AM"
    assert request.exact_time_minutes == 10 * 60
    assert "exact_time" in request.reasons


def test_booking_request_parses_french_next_weekday_as_specific_next_date():
    request = build_booking_time_request(
        text="Quelles sont les disponibilités pour mercredi prochain?",
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert request.scope == "specific_date"
    assert request.requested_dates == ("2026-06-24",)
    assert request.preferred_day == "wednesday"
    assert "specific_next_weekday:wednesday" in request.reasons


def test_booking_request_lets_avoidance_override_bare_weekday():
    request = build_booking_time_request(
        text="Not Monday, but any other morning is fine",
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert request.requested_dates == ()
    assert request.avoid_weekdays == ("monday",)
    assert request.periods == ("morning",)
