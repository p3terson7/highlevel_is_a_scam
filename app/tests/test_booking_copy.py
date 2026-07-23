from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.services.agent_v3_helpers import _ensure_slot_fallback_line
from app.services.booking_copy import render_booking_slot_reply
from app.services.booking_planner import BookingPlanResult
from app.services.booking_request import BookingTimeRequest


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
