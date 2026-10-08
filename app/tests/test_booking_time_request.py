from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.services.booking_request import booking_request_fingerprint, build_booking_time_request


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


@pytest.mark.parametrize(
    ("text", "expected_minutes"),
    [
        ("Are you free Friday at NOON?!", 12 * 60),
        ("Vendredi prochain MIDI, ça marche?", 12 * 60),
        ("Est-ce possible de faire vendredi prochain à midi?", 12 * 60),
        ("Could we talk at midnight?", 0),
        ("Samedi à MINUIT.", 0),
    ],
)
def test_booking_request_parses_bilingual_named_instants(text, expected_minutes):
    request = build_booking_time_request(
        text=text,
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert request.exact_time_minutes == expected_minutes
    assert "exact_time" in request.reasons


def test_reported_french_next_friday_noon_request_is_one_canonical_request():
    request = build_booking_time_request(
        text="Oui, êtes-vous disponibles vendredi prochain midi ?",
        timezone_name="America/Toronto",
        now_utc=datetime(2026, 7, 23, 21, 24, tzinfo=timezone.utc),
    )

    assert request.scope == "specific_date"
    assert request.requested_dates == ("2026-07-24",)
    assert request.exact_time == "12 PM"
    assert request.exact_time_minutes == 12 * 60
    assert request.preferred_day == "friday"


def test_booking_request_parses_french_relative_ranges_and_all_day():
    request = build_booking_time_request(
        text="Je suis libre toute la journée après-demain ou la semaine prochaine.",
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert request.requested_dates == ("2026-06-19",)
    assert request.date_range_start == "2026-06-22"
    assert request.date_range_end == "2026-06-28"
    assert request.all_day is True


def test_booking_request_parses_french_weekday_avoidance():
    request = build_booking_time_request(
        text="Le matin fonctionne, mais pas le vendredi.",
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert request.requested_dates == ()
    assert request.avoid_weekdays == ("friday",)
    assert request.periods == ("morning",)


@pytest.mark.parametrize(
    ("text", "expected_period"),
    [
        ("Friday morning works.", "morning"),
        ("Vendredi en MATINÉE!", "morning"),
        ("L’après-midi serait préférable.", "afternoon"),
        ("Can we do this in the afternoon?", "afternoon"),
        ("Je suis libre en soirée.", "evening"),
        ("Tonight works for me.", "evening"),
    ],
)
def test_booking_request_parses_bilingual_day_period_categories(text, expected_period):
    request = build_booking_time_request(
        text=text,
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert request.periods == (expected_period,)


@pytest.mark.parametrize(
    ("text", "expected_start", "expected_end"),
    [
        ("Friday between noon and 2 PM.", 12 * 60, 14 * 60),
        ("Friday from 10 AM to noon.", 10 * 60, 12 * 60),
        ("Vendredi, entre 9h30 et 11h!", 9 * 60 + 30, 11 * 60),
        ("Vendredi de midi à 14h.", 12 * 60, 14 * 60),
        ("Vendredi 9h à 11h.", 9 * 60, 11 * 60),
        ("Friday from 10 PM to midnight.", 22 * 60, 24 * 60),
        ("Any time before midnight.", 0, 24 * 60 - 1),
    ],
)
def test_booking_request_parses_bilingual_explicit_time_ranges(
    text,
    expected_start,
    expected_end,
):
    request = build_booking_time_request(
        text=text,
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert request.range_start_minutes == expected_start
    assert request.range_end_minutes == expected_end
    assert request.exact_time_minutes is None


@pytest.mark.parametrize(
    ("text", "expected_date", "expected_minutes"),
    [
        ("Le 2 août à 15h", "2026-08-02", 15 * 60),
        ("2 August at 3 PM", "2026-08-02", 15 * 60),
        ("24/07 à midi", "2026-07-24", 12 * 60),
        ("Friday at 15:00", "2026-06-19", 15 * 60),
        ("Vendredi à 15", "2026-06-19", 15 * 60),
    ],
)
def test_booking_request_parses_unambiguous_bilingual_dates_and_24h_times(
    text,
    expected_date,
    expected_minutes,
):
    request = build_booking_time_request(
        text=text,
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert request.requested_dates == (expected_date,)
    assert request.exact_time_minutes == expected_minutes
    assert request.range_start_minutes is None


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        ("Friday at 12", "ambiguous_time"),
        ("Vendredi prochain à 8", "ambiguous_time"),
        ("Friday from 3 to 4", "ambiguous_time"),
        ("Friday between 11 and 1", "ambiguous_time"),
        ("Can we meet 07/08 at noon?", "ambiguous_numeric_date"),
    ],
)
def test_booking_request_marks_ambiguous_temporal_input_for_clarification(
    text,
    reason,
):
    request = build_booking_time_request(
        text=text,
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert reason in request.reasons


@pytest.mark.parametrize(
    "text",
    [
        "Friday next week at noon",
        "Next week Friday at noon",
        "Vendredi semaine prochaine à midi",
    ],
)
def test_weekday_qualified_by_next_week_resolves_inside_that_week(text):
    request = build_booking_time_request(
        text=text,
        timezone_name="America/Toronto",
        now_utc=datetime(2026, 7, 23, 20, 0, tzinfo=timezone.utc),
    )

    assert request.scope == "specific_date"
    assert request.requested_dates == ("2026-07-31",)
    assert request.date_range_start is None
    assert request.date_range_end is None
    assert request.exact_time_minutes == 12 * 60


@pytest.mark.parametrize(
    "text",
    [
        "Friday July 31 at noon",
        "Vendredi 31 juillet à midi",
    ],
)
def test_weekday_label_does_not_create_a_second_date(text):
    request = build_booking_time_request(
        text=text,
        timezone_name="America/Toronto",
        now_utc=datetime(2026, 7, 23, 20, 0, tzinfo=timezone.utc),
    )

    assert request.requested_dates == ("2026-07-31",)
    assert "weekday_date_mismatch" not in request.reasons


@pytest.mark.parametrize(
    ("text", "expected_dates"),
    [
        ("Friday or Saturday", ("2026-06-19", "2026-06-20")),
        ("Monday and Friday afternoon", ("2026-06-22", "2026-06-19")),
        (
            "Friday or Saturday next week",
            ("2026-06-26", "2026-06-27"),
        ),
    ],
)
def test_multiple_positive_weekdays_remain_distinct_requested_dates(
    text,
    expected_dates,
):
    request = build_booking_time_request(
        text=text,
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert request.requested_dates == expected_dates
    assert "weekday_date_mismatch" not in request.reasons


def test_distinct_date_time_alternatives_are_not_flattened_into_a_cartesian_request():
    request = build_booking_time_request(
        text="Friday at noon or Monday at 3 PM",
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert "multiple_temporal_clauses" in request.reasons


@pytest.mark.parametrize(
    "text",
    [
        "Next week except Friday",
        "Next week, not Friday",
        "Any day but Friday next week",
        "La semaine prochaine, sauf vendredi",
    ],
)
def test_avoided_weekday_remains_an_exclusion_inside_date_range(text):
    request = build_booking_time_request(
        text=text,
        timezone_name="America/Toronto",
        now_utc=datetime(2026, 7, 23, 20, 0, tzinfo=timezone.utc),
    )

    assert request.scope == "date_range"
    assert request.date_range_start == "2026-07-27"
    assert request.date_range_end == "2026-08-02"
    assert request.requested_dates == ()
    assert request.avoid_weekdays == ("friday",)


@pytest.mark.parametrize(
    "text",
    [
        "Friday, but not noon",
        "Friday anything except 12 PM",
        "Friday morning, not 9 AM",
        "Vendredi, mais pas midi",
    ],
)
def test_negated_time_is_not_treated_as_a_positive_booking_constraint(text):
    request = build_booking_time_request(
        text=text,
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert "negated_time_constraint" in request.reasons


@pytest.mark.parametrize(
    ("text", "expected_start", "expected_end"),
    [
        ("Friday from 11 PM to 1 AM", 23 * 60, 25 * 60),
        ("Vendredi de 23h à 1h", 23 * 60, 25 * 60),
    ],
)
def test_booking_request_preserves_overnight_ranges(
    text,
    expected_start,
    expected_end,
):
    request = build_booking_time_request(
        text=text,
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert request.range_start_minutes == expected_start
    assert request.range_end_minutes == expected_end


def test_tool_supplied_midnight_overrides_other_text_instead_of_being_dropped():
    request = build_booking_time_request(
        text="Monday at 11 AM",
        exact_time="midnight",
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert request.exact_time == "12 AM"
    assert request.exact_time_minutes == 0


def test_canonical_payload_and_fingerprint_ignore_raw_language_and_provenance():
    english = build_booking_time_request(
        text="Friday at noon",
        timezone_name="America/Toronto",
        now_utc=_now(),
        source="inbound_sms",
    )
    french = build_booking_time_request(
        text="VENDREDI À MIDI!",
        timezone_name="America/Toronto",
        now_utc=_now(),
        source="agent_tool",
    )

    assert english.to_canonical_payload() == french.to_canonical_payload()
    assert english.canonical_payload() == english.to_canonical_payload()
    assert english.fingerprint == french.fingerprint
    assert english.fingerprint == booking_request_fingerprint(english)
    assert "raw_text" not in english.to_canonical_payload()
    assert "source" not in english.to_canonical_payload()
    assert "confidence" not in english.to_canonical_payload()
    assert "reasons" not in english.to_canonical_payload()


def test_canonical_fingerprint_changes_when_a_scheduling_constraint_changes():
    noon = build_booking_time_request(
        text="Friday at noon",
        timezone_name="America/Toronto",
        now_utc=_now(),
    )
    afternoon = build_booking_time_request(
        text="Friday afternoon",
        timezone_name="America/Toronto",
        now_utc=_now(),
    )

    assert noon.fingerprint != afternoon.fingerprint
