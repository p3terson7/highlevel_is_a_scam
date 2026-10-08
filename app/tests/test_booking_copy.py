from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.services.agent_v3_helpers import _ensure_slot_fallback_line
from app.services.booking_copy import render_booking_slot_reply, render_slot_clarification
from app.services.booking_planner import BookingPlanResult
from app.services.booking_request import BookingTimeRequest


def test_clarification_does_not_repeat_legacy_english_labels_without_valid_dates():
    reply = render_slot_clarification(
        slots=[{"index": 2, "start_time": "invalid", "display_time": "Friday at 9 AM"}],
        language="fr", timezone_name="America/Toronto",
    )
    assert reply == "Quel créneau voulez-vous réserver?\n2) Option 2"


@pytest.mark.parametrize(
    ("language", "display_time", "expected_time", "reply_instruction", "forbidden_copy"),
    [
        (
            "fr",
            "jeudi 23 juillet à 15 h 00",
            "jeudi 23 juillet à 15 h 00",
            "Voulez-vous que je le réserve?",
            "heure exacte souhaitée",
        ),
        (
            "en",
            "Thu Jul 23 at 3:00 PM",
            "Thu Jul 23 at 3:00 PM",
            "Would you like me to reserve it?",
            "exact time you want",
        ),
    ],
)
def test_single_exact_slot_reply_mentions_requested_time_once(
    language: str,
    display_time: str,
    expected_time: str,
    reply_instruction: str,
    forbidden_copy: str,
):
    slot = SimpleNamespace(
        index=1,
        start_time="",
        end_time="",
        display_time=display_time,
    )
    request = BookingTimeRequest(
        scope="specific_date",
        requested_dates=("2026-07-23",),
        exact_time="3 PM",
        exact_time_minutes=15 * 60,
    )
    plan = BookingPlanResult(
        slots=[slot],
        strategy="specific_date_exact_time",
        match_mode="exact_time",
        candidate_count=1,
        considered_count=1,
        selected_count=1,
    )

    reply = render_booking_slot_reply(
        slots=[slot],
        request=request,
        plan=plan,
        timezone_label="EDT",
        language=language,
        timezone_name="America/Toronto",
    )
    delivered = _ensure_slot_fallback_line(reply, language=language)

    assert delivered == reply
    assert reply.count(expected_time) == 1
    assert reply_instruction in reply
    assert forbidden_copy not in reply


@pytest.mark.parametrize(
    ("language", "outcome", "constraints_satisfied", "has_slots", "scope", "expected"),
    [
        ("en", "exact_available", True, True, "specific_date", "Would you like me to reserve it?"),
        ("fr", "exact_available", True, True, "specific_date", "Voulez-vous que je le réserve?"),
        ("en", "requested_window_available", True, True, "specific_date", "I found a few"),
        ("fr", "requested_window_available", True, True, "specific_date", "J'ai trouvé quelques"),
        ("en", "unavailable_within_coverage", False, True, "specific_date", "do not exactly match"),
        ("fr", "unavailable_within_coverage", False, True, "specific_date", "ne correspondent pas exactement"),
        ("en", "outside_search_coverage", False, True, "specific_date", "outside the period"),
        ("fr", "outside_search_coverage", False, True, "specific_date", "en dehors de la période"),
        ("en", "needs_clarification", False, False, "specific_date", "more specific day or time window"),
        ("fr", "needs_clarification", False, False, "specific_date", "jour ou d'une plage horaire plus précise"),
        ("en", "no_availability", False, False, "broad", "open call times right now"),
        ("fr", "no_availability", False, False, "broad", "disponibilités pour le moment"),
        ("en", "broad_availability", True, True, "broad", "book a consultation call directly"),
        ("fr", "broad_availability", True, True, "broad", "réserver un appel directement"),
    ],
)
def test_every_planner_outcome_has_explicit_bilingual_copy(
    language: str,
    outcome: str,
    constraints_satisfied: bool,
    has_slots: bool,
    scope: str,
    expected: str,
):
    slot = SimpleNamespace(
        index=1,
        start_time="2026-07-27T13:00:00Z",
        end_time="2026-07-27T13:30:00Z",
        display_time="Mon Jul 27 at 9:00 AM",
    )
    request = BookingTimeRequest(
        scope=scope,
        requested_dates=("2026-07-24",) if scope != "broad" else (),
        exact_time="12 PM" if scope != "broad" else None,
        exact_time_minutes=12 * 60 if scope != "broad" else None,
    )
    slots = [slot] if has_slots else []
    plan = BookingPlanResult(
        slots=slots,
        strategy="test",
        match_mode="exact_time" if outcome == "exact_available" else "closest_alternative",
        candidate_count=1 if constraints_satisfied else 0,
        considered_count=1,
        selected_count=len(slots),
        fallback_reason=None if constraints_satisfied else "test_fallback",
        outcome=outcome,
        constraints_satisfied=constraints_satisfied,
        matching_slots=slots if constraints_satisfied else [],
        alternative_slots=[] if constraints_satisfied else slots,
    )

    reply = render_booking_slot_reply(
        slots=slots,
        request=request,
        plan=plan,
        timezone_label="EDT",
        language=language,
        timezone_name="America/Toronto",
    )

    assert expected in reply


@pytest.mark.parametrize(
    ("language", "forbidden_generic"),
    [
        ("en", "Here are a few available call times"),
        ("fr", "Voici quelques créneaux disponibles"),
    ],
)
def test_requested_friday_noon_never_labels_monday_alternatives_as_matches(
    language: str,
    forbidden_generic: str,
):
    slot = SimpleNamespace(
        index=1,
        start_time="2026-07-27T13:00:00Z",
        end_time="2026-07-27T13:30:00Z",
        display_time="Mon Jul 27 at 9:00 AM",
    )
    request = BookingTimeRequest(
        scope="specific_date",
        requested_dates=("2026-07-24",),
        exact_time="midi" if language == "fr" else "noon",
        exact_time_minutes=12 * 60,
    )
    plan = BookingPlanResult(
        slots=[slot],
        strategy="specific_date",
        match_mode="closest_alternative",
        candidate_count=0,
        considered_count=1,
        selected_count=1,
        fallback_reason="no_slots_on_requested_date",
        outcome="unavailable_within_coverage",
        constraints_satisfied=False,
        alternative_slots=[slot],
    )

    reply = render_booking_slot_reply(
        slots=[slot],
        request=request,
        plan=plan,
        timezone_label="EDT",
        language=language,
        timezone_name="America/Toronto",
    )

    assert forbidden_generic not in reply
    assert ("alternative" in reply.lower()) or ("alternatives" in reply.lower())
    assert "Mon Jul 27 at 9:00 AM" in reply or "lundi 27 juillet à 9 h 00" in reply


@pytest.mark.parametrize(
    ("language", "forbidden_fragments"),
    [
        ("en", ("on between", "at between")),
        ("fr", ("pour le entre", "à entre")),
    ],
)
def test_date_and_time_range_copy_uses_grammatical_prepositions(
    language,
    forbidden_fragments,
):
    slot = SimpleNamespace(
        index=1,
        start_time="2026-07-27T13:00:00Z",
        end_time="2026-07-27T13:30:00Z",
        display_time="Mon Jul 27 at 9:00 AM",
    )
    request = BookingTimeRequest(
        scope="date_range",
        date_range_start="2026-07-27",
        date_range_end="2026-07-31",
        range_start="9 AM",
        range_start_minutes=9 * 60,
        range_end="11 AM",
        range_end_minutes=11 * 60,
    )
    plan = BookingPlanResult(
        slots=[slot],
        strategy="date_range_alternative",
        match_mode="closest_in_range",
        candidate_count=0,
        considered_count=1,
        selected_count=1,
        fallback_reason="requested_time_unavailable",
        outcome="unavailable_within_coverage",
        constraints_satisfied=False,
        alternative_slots=[slot],
    )

    reply = render_booking_slot_reply(
        slots=[slot],
        request=request,
        plan=plan,
        timezone_label="EDT",
        language=language,
        timezone_name="America/Toronto",
    )

    assert all(fragment not in reply.lower() for fragment in forbidden_fragments)
