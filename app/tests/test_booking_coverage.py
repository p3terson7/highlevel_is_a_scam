from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from app.db.models import (
    Client,
    ConversationStateEnum,
    Lead,
    LeadSource,
    Message,
    MessageDirection,
)
from app.services.booking import (
    AvailabilityCoverage,
    AvailabilitySearchResult,
    BookingProviderError,
    BookingService,
    BookingSlot,
    _calendly_search_window,
    _internal_search_coverage,
    calendar_booking_confirmed,
)
from app.services.booking_request import BookingTimeRequest
from app.services.inbound_sms import (
    _active_booking_offer,
    _should_try_deterministic_slot_selection,
)

TZ = "America/Toronto"
NOW_UTC = datetime(2026, 7, 23, 20, 36, tzinfo=timezone.utc)


def _client(*, language: str = "fr", horizon_days: int = 7) -> Client:
    return Client(
        id=100,
        client_key="coverage-test",
        business_name="Coverage Test",
        booking_mode="internal",
        timezone=TZ,
        provider_config={"language": language},
        booking_config={
            "internal_calendar": {
                "slot_minutes": 30,
                "notice_minutes": 0,
                "horizon_days": horizon_days,
                "availability": [
                    {
                        "day": day,
                        "enabled": True,
                        "start": "09:00",
                        "end": "17:00",
                    }
                    for day in range(7)
                ],
            }
        },
    )


def _slot(index: int, *, day: int, hour: int, minute: int = 0) -> BookingSlot:
    local_start = datetime(2026, 7, day, hour, minute, tzinfo=ZoneInfo(TZ))
    local_end = local_start + timedelta(minutes=30)
    return BookingSlot(
        index=index,
        start_time=local_start.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        end_time=local_end.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        display_time=local_start.strftime("%a %b %d at %-I:%M %p"),
        display_hint=local_start.strftime("%A %-I:%M %p"),
        search_blob=local_start.strftime("%A %-I:%M %p").lower(),
    )


class _ControlledCoverageBookingService(BookingService):
    def __init__(self, slots: list[BookingSlot]) -> None:
        super().__init__()
        self._controlled_slots = slots

    def _search_internal_slots(self, **kwargs) -> AvailabilitySearchResult:
        _ = kwargs
        return AvailabilitySearchResult(
            slots=list(self._controlled_slots),
            coverage=AvailabilityCoverage(
                provider="internal",
                start_date="2026-07-23",
                end_date="2026-07-30",
                complete=True,
            ),
        )


def test_internal_specific_date_search_expands_beyond_default_horizon():
    client = _client(horizon_days=7)
    request = BookingTimeRequest(
        scope="specific_date",
        timezone_name=TZ,
        requested_dates=("2026-08-20",),
    )

    coverage, horizon_days = _internal_search_coverage(
        client=client,
        request=request,
        now_utc=NOW_UTC,
    )

    assert horizon_days == 28
    assert coverage.end_date == "2026-08-20"
    assert coverage.complete is True
    assert coverage.reason is None


def test_internal_recurring_weekday_search_includes_the_next_occurrence():
    client = _client(horizon_days=1)
    request = BookingTimeRequest(
        scope="weekday_recurring",
        timezone_name=TZ,
        requested_weekdays=("wednesday",),
    )

    coverage, horizon_days = _internal_search_coverage(
        client=client,
        request=request,
        now_utc=NOW_UTC,
    )

    assert horizon_days == 6
    assert coverage.end_date == "2026-07-29"
    assert coverage.complete is True


def test_internal_overnight_search_includes_the_next_day_spill():
    client = _client(horizon_days=1)
    request = BookingTimeRequest(
        scope="specific_date",
        timezone_name=TZ,
        requested_dates=("2026-07-24",),
        range_start="11 PM",
        range_start_minutes=23 * 60,
        range_end="1 AM",
        range_end_minutes=25 * 60,
    )

    coverage, horizon_days = _internal_search_coverage(
        client=client,
        request=request,
        now_utc=NOW_UTC,
    )

    assert horizon_days == 2
    assert coverage.end_date == "2026-07-25"
    assert coverage.complete is True


def test_calendly_specific_date_search_moves_window_to_requested_date():
    client = SimpleNamespace(timezone=TZ)
    request = BookingTimeRequest(
        scope="specific_date",
        timezone_name=TZ,
        requested_dates=("2026-08-20",),
        exact_time="12 PM",
        exact_time_minutes=12 * 60,
    )

    start, end, coverage = _calendly_search_window(
        client=client,
        request=request,
        now_utc=NOW_UTC,
    )

    assert start.astimezone(ZoneInfo(TZ)).date().isoformat() == "2026-08-20"
    assert end > start
    assert end - start <= timedelta(days=7)
    assert coverage.start_date == "2026-08-20"
    assert coverage.end_date == "2026-08-20"
    assert coverage.complete is True


def test_calendly_last_partial_day_is_requeried_as_a_complete_requested_day():
    client = SimpleNamespace(timezone=TZ)
    request = BookingTimeRequest(
        scope="specific_date",
        timezone_name=TZ,
        requested_dates=("2026-07-30",),
        exact_time="6 PM",
        exact_time_minutes=18 * 60,
    )

    start, _, coverage = _calendly_search_window(
        client=client,
        request=request,
        now_utc=NOW_UTC,
    )

    assert start.astimezone(ZoneInfo(TZ)).date().isoformat() == "2026-07-30"
    assert coverage.start_date == "2026-07-30"
    assert coverage.end_date == "2026-07-30"
    assert coverage.complete is True


def test_calendly_overnight_window_covers_the_next_day_endpoint():
    client = SimpleNamespace(timezone=TZ)
    request = BookingTimeRequest(
        scope="specific_date",
        timezone_name=TZ,
        requested_dates=("2026-07-24",),
        range_start="11 PM",
        range_start_minutes=23 * 60,
        range_end="1 AM",
        range_end_minutes=25 * 60,
    )

    start, end, coverage = _calendly_search_window(
        client=client,
        request=request,
        now_utc=NOW_UTC,
    )

    assert start.astimezone(ZoneInfo(TZ)).date().isoformat() <= "2026-07-24"
    assert end.astimezone(ZoneInfo(TZ)).isoformat().startswith(
        "2026-07-25T01:00:00"
    )
    assert coverage.end_date == "2026-07-25"
    assert coverage.complete is True


def test_calendly_fall_dst_range_does_not_claim_complete_coverage():
    client = SimpleNamespace(timezone=TZ)
    request = BookingTimeRequest(
        scope="date_range",
        timezone_name=TZ,
        date_range_start="2026-10-26",
        date_range_end="2026-11-01",
    )

    start, end, coverage = _calendly_search_window(
        client=client,
        request=request,
        now_utc=datetime(2026, 10, 1, 16, 0, tzinfo=timezone.utc),
    )

    assert end - start == timedelta(days=7)
    assert coverage.complete is False
    assert coverage.reason == "requested_window_exceeds_provider_limit"


def test_calendly_result_cap_marks_coverage_incomplete():
    class CappedCalendlyService(BookingService):
        def _request(self, **kwargs):
            _ = kwargs
            first = datetime(2026, 7, 24, 13, 0, tzinfo=timezone.utc)
            return {
                "collection": [
                    {
                        "start_time": (
                            first + timedelta(minutes=30 * index)
                        ).isoformat().replace("+00:00", "Z"),
                        "end_time": (
                            first + timedelta(minutes=30 * (index + 1))
                        ).isoformat().replace("+00:00", "Z"),
                    }
                    for index in range(241)
                ]
            }

    client = Client(
        id=101,
        client_key="calendly-cap",
        business_name="Calendly Cap",
        booking_mode="calendly",
        timezone=TZ,
        provider_config={"language": "en"},
        booking_config={
            "calendly_personal_access_token": "token",
            "calendly_event_type_uri": "https://api.calendly.com/event_types/test",
        },
    )
    request = BookingTimeRequest(scope="broad", timezone_name=TZ)

    result = CappedCalendlyService()._search_calendly_slots(
        client=client,
        request=request,
        limit=240,
        now_utc=NOW_UTC,
    )

    assert len(result.slots) == 240
    assert result.coverage.complete is False
    assert result.coverage.reason == "result_limit_reached"


def test_timezone_label_comes_from_the_offered_slot_date():
    winter = BookingSlot(
        index=1,
        start_time="2026-11-06T17:00:00Z",
        end_time="2026-11-06T17:30:00Z",
        display_time="Fri Nov 6 at 12:00 PM",
        display_hint="Friday 12:00 PM",
        search_blob="friday 12 pm",
    )

    assert BookingService()._timezone_abbreviation(TZ, [winter]) == "EST"


@pytest.mark.parametrize(
    ("slots", "expected_outcome", "expected_constraints", "expected_copy"),
    [
        (
            [_slot(1, day=24, hour=12)],
            "exact_available",
            True,
            "Voulez-vous que je le réserve?",
        ),
        (
            [_slot(1, day=27, hour=9), _slot(2, day=28, hour=10)],
            "unavailable_within_coverage",
            False,
            "ne correspondent pas exactement",
        ),
    ],
)
def test_french_next_friday_noon_preserves_constraints_through_offer(
    monkeypatch,
    slots,
    expected_outcome,
    expected_constraints,
    expected_copy,
):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return NOW_UTC.replace(tzinfo=None)
            return NOW_UTC.astimezone(tz)

    monkeypatch.setattr("app.services.booking.datetime", FixedDateTime)
    service = _ControlledCoverageBookingService(slots)
    offer = service.find_slots(
        client=_client(),
        lead=None,
        request_text="Oui, êtes-vous disponibles vendredi prochain midi ?",
        limit=3,
        db=None,
    )
    payload = offer.raw_payload["booking_offer"]

    assert payload["request"]["requested_dates"] == ["2026-07-24"]
    assert payload["request"]["exact_time_minutes"] == 12 * 60
    assert payload["outcome"] == expected_outcome
    assert payload["constraints_satisfied"] is expected_constraints
    assert payload["matched_preference"] is expected_constraints
    assert payload["planner"]["matching_count"] == (1 if expected_constraints else 0)
    assert payload["planner"]["alternative_count"] == (0 if expected_constraints else 2)
    assert payload["coverage"]["complete"] is True
    assert len(payload["request_fingerprint"]) == 64
    assert len(payload["offer_fingerprint"]) == 64
    assert expected_copy in offer.reply_text


def test_canonical_noon_request_supersedes_old_offer_before_llm(monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return NOW_UTC.replace(tzinfo=None)
            return NOW_UTC.astimezone(tz)

    monkeypatch.setattr("app.services.booking.datetime", FixedDateTime)
    service = _ControlledCoverageBookingService([_slot(1, day=24, hour=12)])
    lead = Lead(
        client_id=100,
        source=LeadSource.META,
        full_name="Noon Lead",
        phone="+15555550100",
        email="noon@example.com",
        form_answers={},
        raw_payload={
            "pending_step": "slot_selection_pending",
            "active_booking_offer": {
                "provider": "internal",
                "slots": [
                    {
                        "index": 1,
                        "start_time": "2026-07-27T13:00:00Z",
                    }
                ],
            },
        },
        consented=True,
        opted_out=False,
        conversation_state=ConversationStateEnum.BOOKING_SENT,
    )

    result = service.handle_time_request(
        client=_client(),
        lead=lead,
        inbound_text="Vendredi prochain midi, ça marche?",
        db=None,
    )

    assert result is not None
    assert result.handled is True
    assert result.audit_event_type == "calendar_exact_time_checked"
    assert result.next_state == ConversationStateEnum.BOOKING_SENT
    assert result.raw_payload["booking_offer"]["outcome"] == "exact_available"
    assert result.raw_payload["booking_offer"]["slots"][0]["start_time"].startswith(
        "2026-07-24T16:00:00"
    )
    assert "Voulez-vous que je le réserve?" in result.reply_text


def test_unoffered_time_is_routed_as_new_request_not_old_slot_selection():
    service = _ControlledCoverageBookingService([_slot(1, day=24, hour=12)])
    offered_slot = _slot(1, day=27, hour=9).__dict__
    offered_slot["search_blob"] = "monday 9 am | monday 9:00 am"
    active_offer = {"provider": "internal", "slots": [offered_slot]}
    lead = Lead(
        client_id=100,
        source=LeadSource.META,
        full_name="Routing Lead",
        phone="+15555550101",
        email="routing@example.com",
        form_answers={},
        raw_payload={
            "pending_step": "slot_selection_pending",
            "active_booking_offer": active_offer,
        },
        consented=True,
        opted_out=False,
        conversation_state=ConversationStateEnum.BOOKING_SENT,
    )

    assert not _should_try_deterministic_slot_selection(
        lead=lead,
        inbound_text="Monday 9 AM",
        active_offer=active_offer,
        booking_service=service,
    )
    assert _should_try_deterministic_slot_selection(
        lead=lead,
        inbound_text="Monday 9 AM is good",
        active_offer=active_offer,
        booking_service=service,
    )
    assert not _should_try_deterministic_slot_selection(
        lead=lead,
        inbound_text="Friday noon",
        active_offer=active_offer,
        booking_service=service,
    )


@pytest.mark.parametrize(
    "inbound_text",
    [
        "Not Monday 9 AM",
        "Monday 9 AM?",
        "Maybe Monday 9 AM",
        "Lundi 9h ne marche pas",
        "Peut-être lundi 9h",
    ],
)
def test_nonaffirmative_time_mentions_never_enter_booking_mutation_route(
    inbound_text,
):
    service = _ControlledCoverageBookingService([_slot(1, day=27, hour=9)])
    offered_slot = _slot(1, day=27, hour=9).__dict__
    offered_slot["search_blob"] = (
        "monday 9 am | monday 9:00 am | lundi 9h | lundi 9 h"
    )
    active_offer = {"provider": "internal", "slots": [offered_slot]}
    lead = Lead(
        client_id=100,
        source=LeadSource.META,
        full_name="Polarity Lead",
        phone="+15555550103",
        email="polarity@example.com",
        form_answers={},
        raw_payload={
            "pending_step": "slot_selection_pending",
            "active_booking_offer": active_offer,
        },
        consented=True,
        opted_out=False,
        conversation_state=ConversationStateEnum.BOOKING_SENT,
    )

    assert not _should_try_deterministic_slot_selection(
        lead=lead,
        inbound_text=inbound_text,
        active_offer=active_offer,
        booking_service=service,
    )


@pytest.mark.parametrize(
    ("message", "expected_index"),
    [
        ("book vendredi 19h", 2),
        ("book vendredi 9h", 1),
        ("réservez vendredi 11h", 4),
        ("réservez vendredi 1h", 3),
    ],
)
def test_slot_matching_uses_clock_token_boundaries(message, expected_index):
    slots = [
        {"index": 1, "search_blob": "friday 9h | vendredi 9h"},
        {"index": 2, "search_blob": "friday 19h | vendredi 19h"},
        {"index": 3, "search_blob": "friday 1h | vendredi 1h"},
        {"index": 4, "search_blob": "friday 11h | vendredi 11h"},
    ]

    matched = BookingService()._match_slot(message, slots)

    assert matched is not None
    assert matched["index"] == expected_index


def test_versioned_expired_offer_is_refreshed_instead_of_booked(monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return NOW_UTC.replace(tzinfo=None)
            return NOW_UTC.astimezone(tz)

    monkeypatch.setattr("app.services.booking.datetime", FixedDateTime)
    service = _ControlledCoverageBookingService([_slot(1, day=24, hour=12)])
    stale_offer = {
        "schema_version": 2,
        "provider": "internal",
        "provider_config_fingerprint": "old-config",
        "generated_at": (NOW_UTC - timedelta(days=2)).isoformat(),
        "request": {"raw_text": "Friday at noon"},
        "slots": [_slot(1, day=24, hour=12).__dict__],
    }
    lead = Lead(
        client_id=100,
        source=LeadSource.META,
        full_name="Expired Offer Lead",
        phone="+15555550104",
        email="expired@example.com",
        form_answers={},
        raw_payload={"pending_step": "slot_selection_pending"},
        consented=True,
        opted_out=False,
        conversation_state=ConversationStateEnum.BOOKING_SENT,
    )

    result = service.handle_slot_selection(
        client=_client(),
        lead=lead,
        inbound_text="1",
        history=[],
        active_offer=stale_offer,
        db=None,
    )

    assert result is not None
    assert result.audit_event_type == "calendar_booking_offer_refreshed"
    assert result.audit_decision["reason"] == "offer_expired"
    assert "booking" not in result.raw_payload


def test_internal_booking_rejects_past_slot_before_database_mutation():
    client = _client()
    lead = Lead(
        client_id=100,
        source=LeadSource.META,
        full_name="Past Slot Lead",
        phone="+15555550105",
        email="past@example.com",
        form_answers={},
        raw_payload={},
        consented=True,
        opted_out=False,
    )

    with pytest.raises(BookingProviderError, match="current calendar settings"):
        BookingService()._book_internal_slot(
            client=client,
            lead=lead,
            slot={
                "start_time": "2025-07-24T16:00:00Z",
                "end_time": "2025-07-24T16:30:00Z",
            },
            db=None,
        )


def test_empty_new_offer_does_not_resurrect_superseded_history_menu():
    old_offer = {
        "provider": "internal",
        "slots": [_slot(1, day=27, hour=9).__dict__],
    }
    lead = Lead(
        client_id=100,
        source=LeadSource.META,
        full_name="No Availability Lead",
        phone="+15555550102",
        email="no-availability@example.com",
        form_answers={},
        raw_payload={
            "active_booking_offer": {
                "provider": "internal",
                "slots": [],
                "request_fingerprint": "new-request",
                "offer_fingerprint": "empty-new-offer",
                "outcome": "unavailable_within_coverage",
            }
        },
        consented=True,
        opted_out=False,
        conversation_state=ConversationStateEnum.BOOKING_SENT,
    )
    history = [
        Message(
            client_id=100,
            lead_id=200,
            direction=MessageDirection.OUTBOUND,
            body="Old options",
            raw_payload={"booking_offer": old_offer},
        )
    ]

    assert _active_booking_offer(lead, history) is None


@pytest.mark.parametrize(
    "text",
    [
        "I'm not booked",
        "Is it booked?",
        "I haven't booked yet",
        "I booked it but cancelled",
        "The pricing is booked up",
        "Je n'ai pas réservé",
        "I booked a hotel",
        "I booked another vendor",
        "I booked my flight",
        "I wonder if the booking is confirmed",
    ],
)
def test_booking_confirmation_rejects_questions_negation_and_corrections(text):
    assert calendar_booking_confirmed(text) is False


@pytest.mark.parametrize(
    "text",
    [
        "I'm booked",
        "I booked the appointment",
        "Booking is confirmed",
        "J'ai réservé le rendez-vous",
    ],
)
def test_booking_confirmation_accepts_explicit_first_party_confirmation(text):
    assert calendar_booking_confirmed(text) is True


def test_terminal_offer_tombstone_blocks_history_resurrection():
    old_offer = {
        "provider": "internal",
        "slots": [_slot(1, day=27, hour=9).__dict__],
    }
    lead = Lead(
        client_id=100,
        source=LeadSource.META,
        full_name="Booked Lead",
        phone="+15555550106",
        email="booked@example.com",
        form_answers={},
        raw_payload={
            "active_booking_offer": {
                "schema_version": 2,
                "slots": [],
                "status": "booked",
            }
        },
        consented=True,
        opted_out=False,
        conversation_state=ConversationStateEnum.BOOKED,
    )
    history = [
        Message(
            client_id=100,
            lead_id=200,
            direction=MessageDirection.OUTBOUND,
            body="Old options",
            raw_payload={"booking_offer": old_offer},
        )
    ]

    assert _active_booking_offer(lead, history) is None
