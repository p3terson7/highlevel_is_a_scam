from __future__ import annotations

from contextlib import nullcontext
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dt_time, timezone
from typing import Any, Sequence
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models import CalendarBooking, Client, ConversationStateEnum, Lead, Message
from app.db.session import get_session_factory

_CALENDLY_API_BASE = "https://api.calendly.com"
_EMAIL_RE = re.compile(r"(?P<email>[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", re.IGNORECASE)
_INTERNAL_PROVIDER = "internal"
_INTERNAL_MODE_ALIASES = {"internal", "calendar"}
_INTERNAL_DEFAULT_SLOT_MINUTES = 30
_INTERNAL_DEFAULT_NOTICE_MINUTES = 120
_INTERNAL_DEFAULT_HORIZON_DAYS = 14


class BookingProviderError(RuntimeError):
    pass


@dataclass(frozen=True)
class BookingSlot:
    index: int
    start_time: str
    end_time: str | None
    display_time: str
    display_hint: str
    search_blob: str


@dataclass(frozen=True)
class SlotOffer:
    reply_text: str
    slots: list[BookingSlot]
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class BookingSelectionResult:
    handled: bool
    reply_text: str
    next_state: ConversationStateEnum
    raw_payload: dict[str, Any]
    audit_event_type: str
    audit_decision: dict[str, Any]
    transition_reason: str


def ensure_booking_link(reply_text: str, client: Client) -> str:
    if not client.booking_url:
        return reply_text
    if client.booking_url in reply_text:
        return reply_text
    return f"{reply_text} Book here: {client.booking_url}".strip()


def handoff_suffix(client: Client) -> str:
    if not client.fallback_handoff_number:
        return ""
    return f" For immediate help, call {client.fallback_handoff_number}."


def extract_email(text: str) -> str | None:
    match = _EMAIL_RE.search(text or "")
    if not match:
        return None
    return match.group("email").strip().lower()


def automated_booking_enabled(client: Client) -> bool:
    mode = booking_mode_label(client)
    config = client.booking_config or {}
    if mode == "calendly":
        return (
            bool(str(config.get("calendly_personal_access_token", "")).strip())
            and bool(str(config.get("calendly_event_type_uri", "")).strip())
        )
    if mode in _INTERNAL_MODE_ALIASES:
        return _internal_has_availability(_internal_calendar_config(client))
    return False


def internal_calendar_enabled(client: Client) -> bool:
    return booking_mode_label(client) in _INTERNAL_MODE_ALIASES and _internal_has_availability(_internal_calendar_config(client))


def internal_calendar_preview_config(client: Client) -> dict[str, Any]:
    config = _internal_calendar_config(client)
    return {
        "slot_minutes": config["slot_minutes"],
        "notice_minutes": config["notice_minutes"],
        "horizon_days": config["horizon_days"],
        "availability": [dict(item) for item in config["availability"]],
        "enabled": _internal_has_availability(config),
    }


def calendar_booking_confirmed(inbound_text: str) -> bool:
    text = str(inbound_text or "").strip().lower()
    if not text:
        return False
    if "booked" in text and any(word in text for word in ("i", "we", "it is", "it's", "already", "just")):
        return True
    return any(phrase in text for phrase in ("appointment booked", "booking confirmed", "i booked", "i'm booked", "im booked"))


def looks_like_slot_selection_message(inbound_text: str) -> bool:
    raw = str(inbound_text or "").strip()
    if not raw:
        return False
    normalized = _normalize_slot_text(raw)
    if not normalized:
        return False
    if _booked_from_reply(raw):
        return False

    if re.fullmatch(r"(option\s*)?[1-3]", normalized):
        return True
    if len(normalized.split()) <= 4 and re.search(r"\b(1|2|3)\b", normalized):
        return True

    has_time_marker = bool(re.search(r"\b\d{1,2}(:\d{2})?\s?(am|pm)\b", normalized))
    has_day_marker = any(
        token in normalized
        for token in (
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
            "tomorrow",
            "today",
            "tonight",
            "next week",
            "this week",
        )
    )
    if not has_time_marker and not has_day_marker:
        return False

    if "?" in raw and any(token in normalized for token in ("do", "can", "could", "any", "availability", "available", "what", "which")):
        return False

    return True


def _internal_calendar_config(client: Client) -> dict[str, Any]:
    raw_config = client.booking_config if isinstance(client.booking_config, dict) else {}
    internal_raw = raw_config.get("internal_calendar") if isinstance(raw_config.get("internal_calendar"), dict) else raw_config
    if not isinstance(internal_raw, dict):
        internal_raw = {}

    availability_rows_raw = internal_raw.get("availability", [])
    availability_rows: list[dict[str, Any]] = []
    if isinstance(availability_rows_raw, list):
        for row in availability_rows_raw:
            if not isinstance(row, dict):
                continue
            day = _to_int(row.get("day"), default=-1)
            if day < 0 or day > 6:
                continue
            start = str(row.get("start", "")).strip()
            end = str(row.get("end", "")).strip()
            enabled = bool(row.get("enabled", False))
            if not enabled:
                continue
            if _parse_hhmm(start) is None or _parse_hhmm(end) is None:
                continue
            availability_rows.append(
                {
                    "day": day,
                    "start": start,
                    "end": end,
                    "enabled": True,
                }
            )

    return {
        "slot_minutes": max(15, min(180, _to_int(internal_raw.get("slot_minutes"), default=_INTERNAL_DEFAULT_SLOT_MINUTES))),
        "notice_minutes": max(0, min(24 * 60, _to_int(internal_raw.get("notice_minutes"), default=_INTERNAL_DEFAULT_NOTICE_MINUTES))),
        "horizon_days": max(1, min(60, _to_int(internal_raw.get("horizon_days"), default=_INTERNAL_DEFAULT_HORIZON_DAYS))),
        "availability": availability_rows,
    }


def _internal_has_availability(config: dict[str, Any]) -> bool:
    rows = config.get("availability", [])
    return isinstance(rows, list) and any(bool(item.get("enabled")) for item in rows if isinstance(item, dict))


def _to_int(raw: Any, *, default: int) -> int:
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def _parse_hhmm(raw: str) -> dt_time | None:
    text = str(raw or "").strip()
    if not re.fullmatch(r"\d{2}:\d{2}", text):
        return None
    try:
        parsed = datetime.strptime(text, "%H:%M")
    except ValueError:
        return None
    return parsed.time()


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso_utc(dt: datetime) -> str:
    return _as_utc(dt).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _overlaps(start_a: datetime, end_a: datetime, start_b: datetime, end_b: datetime) -> bool:
    return start_a < end_b and start_b < end_a


def _internal_slot_key(slot: dict[str, Any]) -> tuple[str, str]:
    return (str(slot.get("start_time", "")).strip(), str(slot.get("end_time", "")).strip())


def _format_internal_booking_time(start_at: datetime, *, timezone_name: str) -> str:
    local_dt = start_at.astimezone(_tzinfo(timezone_name))
    return local_dt.strftime("%a %b %d at %I:%M %p").replace(" 0", " ")


def _as_session_context(db: Session | None):
    if db is not None:
        return nullcontext(db)
    SessionLocal = get_session_factory()
    return SessionLocal()


def _booking_title(lead: Lead) -> str:
    name = (lead.full_name or "").strip()
    return f"Lead call - {name or lead.phone or f'Lead {lead.id}'}"


def _candidate_starts(
    *,
    local_date,
    start_time: dt_time,
    end_time: dt_time,
    slot_minutes: int,
    tz_name: str,
) -> list[tuple[datetime, datetime]]:
    tz = _tzinfo(tz_name)
    block_start = datetime.combine(local_date, start_time, tzinfo=tz)
    block_end = datetime.combine(local_date, end_time, tzinfo=tz)
    if block_end <= block_start:
        return []
    results: list[tuple[datetime, datetime]] = []
    slot_delta = timedelta(minutes=slot_minutes)
    pointer = block_start
    while pointer + slot_delta <= block_end:
        results.append((pointer, pointer + slot_delta))
        pointer += slot_delta
    return results


def _existing_internal_bookings(
    db: Session,
    *,
    client_id: int,
    start_at: datetime,
    end_at: datetime,
) -> list[CalendarBooking]:
    return db.scalars(
        select(CalendarBooking)
        .where(
            CalendarBooking.client_id == client_id,
            CalendarBooking.provider == _INTERNAL_PROVIDER,
            CalendarBooking.status == "scheduled",
            CalendarBooking.start_at < end_at,
            CalendarBooking.end_at > start_at,
        )
        .order_by(CalendarBooking.start_at.asc())
    ).all()


def _slot_occupied(*, start_at: datetime, end_at: datetime, existing: Sequence[CalendarBooking]) -> bool:
    start_ref = _as_utc(start_at)
    end_ref = _as_utc(end_at)
    return any(_overlaps(start_ref, end_ref, _as_utc(booking.start_at), _as_utc(booking.end_at)) for booking in existing)


def _internal_booking_info(booking: CalendarBooking) -> dict[str, Any]:
    return {
        "booking_id": booking.id,
        "provider": booking.provider,
        "status": booking.status,
        "start_time": _iso_utc(booking.start_at),
        "end_time": _iso_utc(booking.end_at),
        "display_time": _format_internal_booking_time(booking.start_at, timezone_name=booking.timezone),
    }


def _cancel_existing_internal_bookings_for_lead(
    db: Session,
    *,
    client_id: int,
    lead_id: int,
    keep_start_at: datetime,
    keep_end_at: datetime,
) -> list[int]:
    existing = db.scalars(
        select(CalendarBooking)
        .where(
            CalendarBooking.client_id == client_id,
            CalendarBooking.lead_id == lead_id,
            CalendarBooking.provider == _INTERNAL_PROVIDER,
            CalendarBooking.status == "scheduled",
        )
        .order_by(CalendarBooking.start_at.asc())
    ).all()
    cancelled: list[int] = []
    keep_start = _as_utc(keep_start_at)
    keep_end = _as_utc(keep_end_at)
    now = datetime.now(timezone.utc)
    for booking in existing:
        if _as_utc(booking.start_at) == keep_start and _as_utc(booking.end_at) == keep_end:
            continue
        booking.status = "cancelled"
        booking.updated_at = now
        cancelled.append(int(booking.id))
    return cancelled


def _booked_from_reply(inbound_text: str) -> bool:
    return calendar_booking_confirmed(inbound_text)


def _to_utc_datetime(raw_value: str) -> datetime | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    value = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def booking_mode_label(client: Client) -> str:
    return str(client.booking_mode or "link").strip().lower() or "link"


class BookingService:
    def __init__(self, timeout_seconds: int = 20) -> None:
        self._timeout_seconds = timeout_seconds

    def preview_slots(self, client: Client, *, limit: int = 3, db: Session | None = None) -> SlotOffer:
        return self.offer_slots(client=client, lead=None, limit=limit, db=db)

    def offer_slots(
        self,
        client: Client,
        lead: Lead | None,
        *,
        limit: int = 3,
        db: Session | None = None,
    ) -> SlotOffer:
        if not automated_booking_enabled(client):
            raise BookingProviderError("Automated booking is not configured for this client.")

        mode = booking_mode_label(client)
        provider = "calendly"
        expanded_limit = max(limit * 12, 96)
        if mode in _INTERNAL_MODE_ALIASES:
            provider = _INTERNAL_PROVIDER
            slots = self._list_internal_slots(client=client, limit=expanded_limit, db=db)
        else:
            slots = self._list_calendly_slots(client=client, limit=expanded_limit)
        all_available_slots = list(slots)
        if not slots:
            fallback = "I am not seeing open times right now. Share a day and time window and I can check alternatives."
            return SlotOffer(
                reply_text=fallback,
                slots=[],
                raw_payload={"booking_offer": {"provider": provider, "slots": []}},
            )

        slots = _spread_first_offer_slots(
            slots=slots,
            limit=max(1, min(limit, len(slots))),
            timezone_name=client.timezone or "UTC",
        )
        slots = _reindex_slots(slots)
        coverage_summary = _availability_coverage_summary(
            slots=all_available_slots,
            timezone_name=client.timezone or "UTC",
            day_limit=3,
        )
        timezone_label = self._timezone_abbreviation(client.timezone)
        lines = ["I can book a call directly."]
        if coverage_summary:
            lines.append(f"I have call openings including {coverage_summary}.")
        lines.extend(
            [
                "Here are a few spread-out call times to lock in now:",
                *[f"{slot.index}) {slot.display_time}" for slot in slots],
                f"{_slot_selection_prompt(slots)}. If none of those work, just send me a time that's better for you. Times shown in {timezone_label}.",
            ]
        )
        raw_payload = {
            "booking_offer": {
                "provider": provider,
                "event_type_uri": self._calendly_config(client)["calendly_event_type_uri"] if provider == "calendly" else "",
                "slots": [slot.__dict__ for slot in slots],
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
        }
        return SlotOffer(reply_text="\n".join(lines), slots=slots, raw_payload=raw_payload)

    def find_slots(
        self,
        *,
        client: Client,
        lead: Lead | None,
        preferred_day: str | None = None,
        avoid_day: str | None = None,
        preferred_period: str | None = None,
        exact_time: str | None = None,
        range_start: str | None = None,
        range_end: str | None = None,
        limit: int = 3,
        db: Session | None = None,
    ) -> SlotOffer:
        if not automated_booking_enabled(client):
            raise BookingProviderError("Automated booking is not configured for this client.")

        mode = booking_mode_label(client)
        provider = _INTERNAL_PROVIDER if mode in _INTERNAL_MODE_ALIASES else "calendly"
        specific_request = bool(preferred_day or avoid_day or preferred_period or exact_time or range_start or range_end)
        expanded_limit = max(limit * 12, 96)
        if specific_request:
            expanded_limit = max(expanded_limit, 96)
        if provider == _INTERNAL_PROVIDER:
            slots = self._list_internal_slots(client=client, limit=expanded_limit, db=db)
        else:
            slots = self._list_calendly_slots(client=client, limit=expanded_limit)
        all_available_slots = list(slots)

        filtered = _filter_slots(
            slots=slots,
            preferred_day=preferred_day,
            avoid_day=avoid_day,
            preferred_period=preferred_period,
            exact_time=exact_time,
            range_start=range_start,
            range_end=range_end,
        )
        matched_preference = bool(filtered)
        match_mode = "exact" if filtered else "none"
        if filtered:
            slots = _spread_first_offer_slots(
                slots=filtered,
                limit=max(1, min(limit, len(filtered))),
                timezone_name=client.timezone or "UTC",
            )
        elif specific_request:
            relaxed = _filter_slots(
                slots=slots,
                preferred_day=preferred_day,
                avoid_day=avoid_day,
                preferred_period=None,
                exact_time=None,
                range_start=None,
                range_end=None,
            )
            if relaxed:
                slots = _spread_first_offer_slots(
                    slots=relaxed,
                    limit=max(1, min(limit, len(relaxed))),
                    timezone_name=client.timezone or "UTC",
                )
                match_mode = "same_day_alternative" if preferred_day else "closest_alternative"
            else:
                slots = _spread_first_offer_slots(
                    slots=slots,
                    limit=max(1, min(limit, len(slots))),
                    timezone_name=client.timezone or "UTC",
                )
                match_mode = "closest_alternative"
        else:
            slots = _spread_first_offer_slots(
                slots=slots,
                limit=max(1, min(limit, len(slots))),
                timezone_name=client.timezone or "UTC",
            )
            match_mode = "exact"

        if not slots:
            fallback = "I am not seeing open times right now. Share a day and time window and I can check alternatives."
            return SlotOffer(
                reply_text=fallback,
                slots=[],
                raw_payload={
                    "booking_offer": {
                        "provider": provider,
                        "slots": [],
                        "preferred_day": preferred_day,
                        "avoid_day": avoid_day,
                        "preferred_period": preferred_period,
                        "exact_time": exact_time,
                        "range_start": range_start,
                        "range_end": range_end,
                        "matched_preference": False,
                        "match_mode": "none",
                    }
                },
            )

        slots = _reindex_slots(slots)
        timezone_label = self._timezone_abbreviation(client.timezone)
        intro = "I found a few call times that should work:"
        if not specific_request:
            coverage_summary = _availability_coverage_summary(
                slots=all_available_slots,
                timezone_name=client.timezone or "UTC",
                day_limit=3,
            )
            if coverage_summary:
                intro = f"I have call openings including {coverage_summary}."
        elif preferred_day:
            intro = f"I found a few {preferred_day.strip().title()} call options:"
            if not matched_preference:
                if match_mode == "same_day_alternative":
                    intro = f"I found {preferred_day.strip().title()} call openings, but not in that exact window. Here are the closest {preferred_day.strip().title()} times:"
                else:
                    intro = f"I’m not seeing {preferred_day.strip().title()} call openings that match that request, but here are the next closest times:"
        elif avoid_day and not matched_preference:
            intro = f"I skipped {avoid_day.strip().title()} and found the next closest times:"
        elif (preferred_period or range_start or range_end) and not matched_preference:
            intro = f"I’m not seeing that exact window, but here are the closest times I have:"
        elif exact_time and not matched_preference:
            intro = f"I’m not seeing that exact time, but here are the closest options:"
        lines = [
            intro,
            *[f"{slot.index}) {slot.display_time}" for slot in slots],
            f"{_slot_selection_prompt(slots)}. If none of those work, just send me a time that's better for you. Times shown in {timezone_label}.",
        ]
        return SlotOffer(
            reply_text="\n".join(lines),
            slots=slots,
            raw_payload={
                "booking_offer": {
                    "provider": provider,
                    "slots": [slot.__dict__ for slot in slots],
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "preferred_day": preferred_day,
                    "avoid_day": avoid_day,
                    "preferred_period": preferred_period,
                    "exact_time": exact_time,
                    "range_start": range_start,
                    "range_end": range_end,
                    "matched_preference": matched_preference,
                    "match_mode": match_mode,
                }
            },
        )

    def book_requested_slot(
        self,
        *,
        client: Client,
        lead: Lead,
        latest_offer: dict[str, Any] | None,
        slot_index: int | None = None,
        slot_start_time: str | None = None,
        slot_text: str | None = None,
        db: Session | None = None,
    ) -> dict[str, Any]:
        slots = []
        if isinstance(latest_offer, dict) and isinstance(latest_offer.get("slots"), list):
            slots = [slot for slot in latest_offer.get("slots", []) if isinstance(slot, dict)]

        matched: dict[str, Any] | None = None
        if slot_start_time:
            for slot in slots:
                if str(slot.get("start_time", "")).strip() == str(slot_start_time).strip():
                    matched = slot
                    break
        if matched is None and slot_index:
            for slot in slots:
                try:
                    if int(slot.get("index")) == int(slot_index):
                        matched = slot
                        break
                except Exception:
                    continue
        if matched is None and slot_text:
            matched = self._match_slot(slot_text, slots)

        if matched is None:
            return {
                "reply_text": "I couldn’t match that to one of the current call options. I can check that time and send fresh call times.",
                "slots": slots,
                "runtime_payload": {
                    "booking_offer": latest_offer or {},
                    "pending_step": "slot_selection_pending" if slots else None,
                },
            }

        offer_provider = str((latest_offer or {}).get("provider", "")).strip().lower()
        if offer_provider == _INTERNAL_PROVIDER or booking_mode_label(client) in _INTERNAL_MODE_ALIASES:
            booking = self._book_internal_slot(client=client, lead=lead, slot=matched, db=db)
        else:
            booking = self._book_calendly_slot(client=client, lead=lead, slot=matched)

        was_rescheduled = bool(booking.get("rescheduled_from_booking_ids") or booking.get("rescheduled_from_event_uri"))
        reply_prefix = "Updated. Your call is now set" if was_rescheduled else "Booked. Your call is set"
        return {
            "reply_text": f"{reply_prefix} for {matched.get('display_time')}.",
            "booking": booking,
            "runtime_payload": {
                "calendar_booking": {
                    "provider": booking.get("provider", offer_provider or "calendly"),
                    "slot": matched,
                    "booking": booking,
                },
                "pending_step": None,
            },
        }

    def handle_slot_selection(
        self,
        *,
        client: Client,
        lead: Lead,
        inbound_text: str,
        history: Sequence[Message],
        db: Session | None = None,
    ) -> BookingSelectionResult | None:
        latest_offer = self._latest_offer(history)
        if latest_offer is None:
            return None
        if not looks_like_slot_selection_message(inbound_text):
            return None

        slots = latest_offer.get("slots", [])
        matched = self._match_slot(inbound_text, slots)
        if matched is None:
            timezone_label = self._timezone_abbreviation(client.timezone)
            indexed_slots = _reindex_dict_slots(slots)
            lines = [
                "I did not catch which slot you want.",
                *[f"{slot.get('index')}) {slot.get('display_time')}" for slot in indexed_slots],
                f"{_slot_selection_prompt_from_dict_slots(indexed_slots, allow_exact_time=False)} Times shown in {timezone_label}.",
            ]
            return BookingSelectionResult(
                handled=True,
                reply_text="\n".join(lines),
                next_state=ConversationStateEnum.BOOKING_SENT,
                raw_payload={"booking_offer": {**latest_offer, "slots": indexed_slots}},
                audit_event_type="calendar_booking_offer_repeated",
                audit_decision={"inbound": inbound_text, "slots": indexed_slots},
                transition_reason="calendar_booking_offer_repeated",
            )

        offer_provider = str(latest_offer.get("provider", "")).strip().lower()
        if offer_provider == _INTERNAL_PROVIDER or booking_mode_label(client) in _INTERNAL_MODE_ALIASES:
            try:
                booking = self._book_internal_slot(client=client, lead=lead, slot=matched, db=db)
            except BookingProviderError:
                refreshed = self.offer_slots(client=client, lead=lead, db=db)
                return BookingSelectionResult(
                    handled=True,
                    reply_text=refreshed.reply_text,
                    next_state=ConversationStateEnum.BOOKING_SENT,
                    raw_payload=refreshed.raw_payload,
                    audit_event_type="calendar_booking_offer_repeated",
                    audit_decision={"inbound": inbound_text, "reason": "slot_no_longer_available"},
                    transition_reason="calendar_booking_offer_repeated",
                )
        else:
            booking = self._book_calendly_slot(client=client, lead=lead, slot=matched)
        was_rescheduled = bool(booking.get("rescheduled_from_booking_ids") or booking.get("rescheduled_from_event_uri"))
        confirmation = [
            f"{'Updated. Your call is now set' if was_rescheduled else 'Booked. Your call is set'} for {matched.get('display_time')}.",
        ]
        if lead.email.strip():
            confirmation.append(f"Confirmation will be sent to {lead.email}.")
        if booking.get("reschedule_url"):
            confirmation.append(f"Reschedule: {booking['reschedule_url']}")
        if booking.get("booking_id"):
            confirmation.append("Saved on our calendar.")
        return BookingSelectionResult(
            handled=True,
            reply_text=" ".join(confirmation),
            next_state=ConversationStateEnum.BOOKED,
            raw_payload={
                "calendar_booking": {
                    "provider": booking.get("provider", offer_provider or "calendly"),
                    "slot": matched,
                    "booking": booking,
                }
            },
            audit_event_type="calendar_booking_created",
            audit_decision={
                "inbound": inbound_text,
                "slot": matched,
                "booking": booking,
            },
            transition_reason="calendar_booking_created",
        )

    def _calendly_config(self, client: Client) -> dict[str, str]:
        config = client.booking_config or {}
        return {
            "calendly_personal_access_token": str(config.get("calendly_personal_access_token", "")).strip(),
            "calendly_event_type_uri": str(config.get("calendly_event_type_uri", "")).strip(),
        }

    def _list_calendly_slots(self, *, client: Client, limit: int) -> list[BookingSlot]:
        config = self._calendly_config(client)
        if not config["calendly_personal_access_token"] or not config["calendly_event_type_uri"]:
            raise BookingProviderError("Calendly token and event type URI are required.")

        start = datetime.now(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=30)
        end = start + timedelta(days=7)
        response = self._request(
            token=config["calendly_personal_access_token"],
            method="GET",
            path="/event_type_available_times",
            params={
                "event_type": config["calendly_event_type_uri"],
                "start_time": start.isoformat().replace("+00:00", "Z"),
                "end_time": end.isoformat().replace("+00:00", "Z"),
            },
        )
        collection = response.get("collection", [])
        if not isinstance(collection, list):
            return []

        tz_name = client.timezone or "UTC"
        slots: list[BookingSlot] = []
        for item in collection:
            if not isinstance(item, dict):
                continue
            start_time = str(item.get("start_time", "")).strip()
            if not start_time:
                continue
            local_dt = _to_local_datetime(start_time, tz_name)
            display_time = local_dt.strftime("%a %b %d at %I:%M %p").replace(" 0", " ")
            search_blob = _slot_search_blob(local_dt)
            slots.append(
                BookingSlot(
                    index=len(slots) + 1,
                    start_time=start_time,
                    end_time=str(item.get("end_time", "")).strip() or None,
                    display_time=display_time,
                    display_hint=local_dt.strftime("%A %I:%M %p").replace(" 0", " "),
                    search_blob=search_blob,
                )
            )
            if len(slots) >= limit:
                break
        return slots

    def _book_calendly_slot(self, *, client: Client, lead: Lead, slot: dict[str, Any]) -> dict[str, Any]:
        config = self._calendly_config(client)
        if not lead.email.strip():
            raise BookingProviderError("Lead email is required before booking.")

        payload = {
            "event_type": config["calendly_event_type_uri"],
            "start_time": str(slot.get("start_time", "")).strip(),
            "invitee": {
                "email": lead.email.strip(),
                "name": lead.full_name.strip() or lead.phone or "Lead",
                "timezone": client.timezone or "UTC",
            },
        }
        response = self._request(
            token=config["calendly_personal_access_token"],
            method="POST",
            path="/invitees",
            json=payload,
        )
        resource = response.get("resource", response)
        return {
            "event_uri": str(resource.get("event", "")).strip(),
            "invitee_uri": str(resource.get("uri", "")).strip(),
            "reschedule_url": str(resource.get("reschedule_url", "")).strip(),
            "cancel_url": str(resource.get("cancel_url", "")).strip(),
        }

    def _list_internal_slots(
        self,
        *,
        client: Client,
        limit: int,
        db: Session | None,
    ) -> list[BookingSlot]:
        config = _internal_calendar_config(client)
        availability_rows = [row for row in config["availability"] if bool(row.get("enabled"))]
        if not availability_rows:
            raise BookingProviderError("Internal calendar availability is not configured.")

        clamped_limit = max(1, min(limit, 240))
        slot_minutes = config["slot_minutes"]
        notice_minutes = config["notice_minutes"]
        horizon_days = config["horizon_days"]

        tz_name = client.timezone or "UTC"
        tz = _tzinfo(tz_name)
        now_local = datetime.now(timezone.utc).astimezone(tz)
        earliest_local = now_local + timedelta(minutes=notice_minutes)

        window_start_utc = earliest_local.astimezone(timezone.utc)
        window_end_local = datetime.combine(
            now_local.date() + timedelta(days=horizon_days + 1),
            dt_time(0, 0),
            tzinfo=tz,
        )
        window_end_utc = window_end_local.astimezone(timezone.utc)

        slots: list[BookingSlot] = []
        with _as_session_context(db) as session:
            existing = _existing_internal_bookings(
                session,
                client_id=client.id,
                start_at=window_start_utc,
                end_at=window_end_utc,
            )
            for day_offset in range(horizon_days + 1):
                local_date = now_local.date() + timedelta(days=day_offset)
                day_rows = [row for row in availability_rows if _to_int(row.get("day"), default=-1) == local_date.weekday()]
                if not day_rows:
                    continue
                for row in day_rows:
                    start_time = _parse_hhmm(str(row.get("start", "")))
                    end_time = _parse_hhmm(str(row.get("end", "")))
                    if start_time is None or end_time is None:
                        continue
                    candidates = _candidate_starts(
                        local_date=local_date,
                        start_time=start_time,
                        end_time=end_time,
                        slot_minutes=slot_minutes,
                        tz_name=tz_name,
                    )
                    for start_local, end_local in candidates:
                        if start_local < earliest_local:
                            continue
                        start_at = start_local.astimezone(timezone.utc)
                        end_at = end_local.astimezone(timezone.utc)
                        if _slot_occupied(start_at=start_at, end_at=end_at, existing=existing):
                            continue

                        display_time = _format_internal_booking_time(start_at, timezone_name=tz_name)
                        local_dt = start_at.astimezone(tz)
                        slots.append(
                            BookingSlot(
                                index=len(slots) + 1,
                                start_time=_iso_utc(start_at),
                                end_time=_iso_utc(end_at),
                                display_time=display_time,
                                display_hint=local_dt.strftime("%A %I:%M %p").replace(" 0", " "),
                                search_blob=_slot_search_blob(local_dt),
                            )
                        )
                        if len(slots) >= clamped_limit:
                            return slots
        return slots

    def _book_internal_slot(
        self,
        *,
        client: Client,
        lead: Lead,
        slot: dict[str, Any],
        db: Session | None,
    ) -> dict[str, Any]:
        start_at = _to_utc_datetime(str(slot.get("start_time", "")).strip())
        end_at = _to_utc_datetime(str(slot.get("end_time", "")).strip())
        if start_at is None:
            raise BookingProviderError("Selected slot is missing a start time.")
        if end_at is None:
            slot_minutes = _internal_calendar_config(client)["slot_minutes"]
            end_at = start_at + timedelta(minutes=slot_minutes)
        if end_at <= start_at:
            raise BookingProviderError("Selected slot has an invalid time range.")

        with _as_session_context(db) as session:
            duplicate = session.scalar(
                select(CalendarBooking)
                .where(
                    CalendarBooking.client_id == client.id,
                    CalendarBooking.lead_id == lead.id,
                    CalendarBooking.provider == _INTERNAL_PROVIDER,
                    CalendarBooking.status == "scheduled",
                    CalendarBooking.start_at == start_at,
                    CalendarBooking.end_at == end_at,
                )
                .limit(1)
            )
            if duplicate is not None:
                return _internal_booking_info(duplicate)

            existing = _existing_internal_bookings(
                session,
                client_id=client.id,
                start_at=start_at,
                end_at=end_at,
            )
            if _slot_occupied(start_at=start_at, end_at=end_at, existing=existing):
                raise BookingProviderError("That time is no longer available.")

            try:
                with session.begin_nested():
                    cancelled_booking_ids = _cancel_existing_internal_bookings_for_lead(
                        session,
                        client_id=client.id,
                        lead_id=lead.id,
                        keep_start_at=start_at,
                        keep_end_at=end_at,
                    )
                    booking = CalendarBooking(
                        client_id=client.id,
                        lead_id=lead.id,
                        provider=_INTERNAL_PROVIDER,
                        source="sms_ai",
                        status="scheduled",
                        start_at=start_at,
                        end_at=end_at,
                        timezone=client.timezone or "UTC",
                        title=_booking_title(lead),
                        notes="Booked by AI SMS agent",
                    )
                    session.add(booking)
                    session.flush()
            except IntegrityError as exc:
                duplicate = session.scalar(
                    select(CalendarBooking)
                    .where(
                        CalendarBooking.client_id == client.id,
                        CalendarBooking.provider == _INTERNAL_PROVIDER,
                        CalendarBooking.status == "scheduled",
                        CalendarBooking.start_at == start_at,
                        CalendarBooking.end_at == end_at,
                    )
                    .limit(1)
                )
                if duplicate is not None and duplicate.lead_id == lead.id:
                    return _internal_booking_info(duplicate)
                raise BookingProviderError("That time is no longer available.") from exc
            if db is None:
                session.commit()
            info = _internal_booking_info(booking)
            if cancelled_booking_ids:
                info["rescheduled_from_booking_ids"] = cancelled_booking_ids
            return info

    def _request(
        self,
        *,
        token: str,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with httpx.Client(base_url=_CALENDLY_API_BASE, timeout=self._timeout_seconds) as client:
            response = client.request(
                method,
                path,
                params=params,
                json=json,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
            )
        if response.is_error:
            detail = _provider_error_detail(response)
            raise BookingProviderError(detail)
        payload = response.json()
        if not isinstance(payload, dict):
            raise BookingProviderError("Unexpected booking provider response")
        return payload

    def _latest_offer(self, history: Sequence[Message]) -> dict[str, Any] | None:
        for message in reversed(history):
            offer = (message.raw_payload or {}).get("booking_offer")
            if isinstance(offer, dict) and isinstance(offer.get("slots"), list) and offer.get("slots"):
                return offer
        return None

    def _match_slot(self, inbound_text: str, slots: list[dict[str, Any]]) -> dict[str, Any] | None:
        normalized = _normalize_slot_text(inbound_text)
        if not normalized:
            return None
        for slot in slots:
            index = str(slot.get("index", "")).strip()
            if index and re.search(rf"(^|\\D){re.escape(index)}($|\\D)", normalized):
                return slot
        for slot in slots:
            variants = [item.strip() for item in str(slot.get("search_blob", "")).split("|") if item.strip()]
            if any(variant in normalized for variant in variants):
                return slot
            if slot.get("display_hint"):
                hint = _normalize_slot_text(str(slot["display_hint"]))
                if hint and hint in normalized:
                    return slot
        return None

    def _timezone_abbreviation(self, tz_name: str) -> str:
        local_now = datetime.now(timezone.utc).astimezone(_tzinfo(tz_name))
        return local_now.tzname() or tz_name


def build_booking_service(timeout_seconds: int = 20) -> BookingService:
    return BookingService(timeout_seconds=timeout_seconds)


def _provider_error_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return f"Booking provider error ({response.status_code})"

    if isinstance(payload, dict):
        details = payload.get("details")
        if isinstance(details, list) and details:
            detail_parts: list[str] = []
            for item in details[:3]:
                if not isinstance(item, dict):
                    continue
                parameter = str(item.get("parameter", "")).strip()
                message = str(item.get("message", "")).strip()
                if parameter and message:
                    detail_parts.append(f"{parameter} {message}")
            if detail_parts:
                return f"{payload.get('message', 'Booking provider error')} ({response.status_code}): {', '.join(detail_parts)}"
        for key in ("message", "title", "detail"):
            if payload.get(key):
                return f"{payload[key]} ({response.status_code})"
    return f"Booking provider error ({response.status_code})"


def _tzinfo(tz_name: str):
    try:
        return ZoneInfo(tz_name or "UTC")
    except Exception:
        return timezone.utc


def _to_local_datetime(iso_value: str, tz_name: str) -> datetime:
    value = iso_value.replace("Z", "+00:00")
    return datetime.fromisoformat(value).astimezone(_tzinfo(tz_name))


def _normalize_slot_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(text or "").strip().lower()).strip()


def _slot_search_blob(local_dt: datetime) -> str:
    hour = str(int(local_dt.strftime("%I")))
    minute = local_dt.strftime("%M")
    meridiem = local_dt.strftime("%p")
    parts = [
        local_dt.strftime("%A %I %p"),
        local_dt.strftime("%A %I:%M %p"),
        local_dt.strftime("%a %I %p"),
        local_dt.strftime("%a %I:%M %p"),
        local_dt.strftime("%B %d %I:%M %p"),
        local_dt.strftime("%m/%d %I:%M %p"),
        f"{local_dt.strftime('%A')} {hour} {meridiem}",
        f"{local_dt.strftime('%A')} {hour}:{minute} {meridiem}",
        f"{local_dt.strftime('%a')} {hour} {meridiem}",
        f"{local_dt.strftime('%a')} {hour}:{minute} {meridiem}",
    ]
    variants = set()
    for part in parts:
        normalized = _normalize_slot_text(part)
        if normalized:
            variants.add(normalized)
            variants.add(normalized.replace(" am", "am").replace(" pm", "pm"))
    return " | ".join(sorted(variants))


def _filter_slots(
    *,
    slots: Sequence[BookingSlot],
    preferred_day: str | None,
    avoid_day: str | None,
    preferred_period: str | None,
    exact_time: str | None,
    range_start: str | None,
    range_end: str | None,
) -> list[BookingSlot]:
    day_value = _normalize_slot_text(preferred_day or "")
    avoid_day_value = _normalize_slot_text(avoid_day or "")
    period_value = _normalize_slot_text(preferred_period or "")
    time_value = _normalize_slot_text(exact_time or "")
    range_start_minutes = _time_text_to_minutes(range_start)
    range_end_minutes = _time_text_to_minutes(range_end)
    filtered: list[BookingSlot] = []
    for slot in slots:
        haystack = _normalize_slot_text(
            " ".join(
                [
                    slot.display_time,
                    slot.display_hint,
                    slot.search_blob,
                ]
            )
        )
        slot_minutes = _slot_minutes(slot)
        if day_value and day_value not in haystack:
            continue
        if avoid_day_value and avoid_day_value in haystack:
            continue
        if period_value:
            if period_value == "morning" and not re.search(r"\b(8|9|10|11)(:00|:30)?\s?am\b", haystack):
                continue
            if period_value == "afternoon" and not re.search(r"\b(12|1|2|3|4)(:00|:30)?\s?pm\b", haystack):
                continue
            if period_value == "evening" and not re.search(r"\b(5|6|7|8)(:00|:30)?\s?pm\b", haystack):
                continue
        if time_value and time_value not in haystack:
            continue
        if range_start_minutes is not None and slot_minutes is not None and slot_minutes < range_start_minutes:
            continue
        if range_end_minutes is not None and slot_minutes is not None and slot_minutes > range_end_minutes:
            continue
        filtered.append(slot)
    return filtered


def _slot_minutes(slot: BookingSlot) -> int | None:
    return _time_text_to_minutes(f"{slot.display_hint} {slot.display_time}")


def _time_text_to_minutes(raw: str | None) -> int | None:
    text = str(raw or "").strip().lower()
    if not text:
        return None
    match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", text)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2) or "0")
    meridiem = match.group(3)
    if meridiem == "pm" and hour != 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    return hour * 60 + minute


def _spread_first_offer_slots(
    *,
    slots: Sequence[BookingSlot],
    limit: int,
    timezone_name: str,
) -> list[BookingSlot]:
    if not slots:
        return []
    ordered = sorted(list(slots), key=_slot_sort_key)
    target = max(1, min(limit, len(ordered)))
    if len(ordered) <= target:
        return ordered[:target]

    selected: list[BookingSlot] = []
    selected_keys: set[str] = set()

    def slot_key(slot: BookingSlot) -> str:
        return f"{slot.start_time}|{slot.end_time or ''}"

    def add_slot(slot: BookingSlot) -> None:
        key = slot_key(slot)
        if key in selected_keys or len(selected) >= target:
            return
        selected.append(slot)
        selected_keys.add(key)

    seen_days: set[str] = set()
    for slot in ordered:
        local_dt = _slot_local_start(slot, timezone_name)
        day_key = local_dt.date().isoformat() if local_dt is not None else str(slot.start_time)[:10]
        if day_key in seen_days:
            continue
        seen_days.add(day_key)
        add_slot(slot)
        if len(selected) >= target:
            return sorted(selected, key=_slot_sort_key)[:target]

    seen_day_periods: set[tuple[str, str]] = set()
    for slot in selected:
        local_dt = _slot_local_start(slot, timezone_name)
        if local_dt is None:
            continue
        seen_day_periods.add((local_dt.date().isoformat(), _time_period(local_dt)))

    for slot in ordered:
        local_dt = _slot_local_start(slot, timezone_name)
        if local_dt is None:
            continue
        key = (local_dt.date().isoformat(), _time_period(local_dt))
        if key in seen_day_periods:
            continue
        seen_day_periods.add(key)
        add_slot(slot)
        if len(selected) >= target:
            return sorted(selected, key=_slot_sort_key)[:target]

    for slot in ordered:
        if _slot_is_spaced_from_selection(slot=slot, selected=selected, timezone_name=timezone_name, minimum_minutes=90):
            add_slot(slot)
        if len(selected) >= target:
            return sorted(selected, key=_slot_sort_key)[:target]

    for slot in ordered:
        add_slot(slot)
        if len(selected) >= target:
            break
    return sorted(selected, key=_slot_sort_key)[:target]


def _reindex_slots(slots: Sequence[BookingSlot]) -> list[BookingSlot]:
    normalized: list[BookingSlot] = []
    for idx, slot in enumerate(slots, start=1):
        normalized.append(
            BookingSlot(
                index=idx,
                start_time=slot.start_time,
                end_time=slot.end_time,
                display_time=slot.display_time,
                display_hint=slot.display_hint,
                search_blob=slot.search_blob,
            )
        )
    return normalized


def _reindex_dict_slots(slots: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for idx, slot in enumerate(slots, start=1):
        item = dict(slot or {})
        item["index"] = idx
        normalized.append(item)
    return normalized


def _availability_coverage_summary(
    *,
    slots: Sequence[BookingSlot],
    timezone_name: str,
    day_limit: int = 3,
) -> str:
    if not slots:
        return ""
    ordered = sorted(list(slots), key=_slot_sort_key)
    day_periods: dict[str, dict[str, Any]] = {}
    for slot in ordered:
        local_dt = _slot_local_start(slot, timezone_name)
        if local_dt is None:
            continue
        day_key = local_dt.date().isoformat()
        period = _time_period(local_dt)
        payload = day_periods.setdefault(
            day_key,
            {
                "date": local_dt.date(),
                "day_name": local_dt.strftime("%A"),
                "periods": [],
            },
        )
        periods = payload["periods"]
        if period not in periods:
            periods.append(period)

    if not day_periods:
        return ""

    limit = max(1, day_limit)
    day_summaries: list[str] = []
    for payload in sorted(day_periods.values(), key=lambda item: item["date"])[:limit]:
        periods = [str(item) for item in payload.get("periods", [])]
        period_phrase = _period_coverage_phrase(periods)
        if period_phrase:
            day_summaries.append(f"{payload['day_name']} {period_phrase}")
        else:
            day_summaries.append(str(payload["day_name"]))
    return _join_with_and(day_summaries)


def _slot_selection_prompt(slots: Sequence[BookingSlot]) -> str:
    return _slot_selection_prompt_from_indices([slot.index for slot in slots], allow_exact_time=True)


def _slot_selection_prompt_from_dict_slots(
    slots: Sequence[dict[str, Any]],
    *,
    allow_exact_time: bool,
) -> str:
    indexes: list[int] = []
    for slot in slots:
        try:
            indexes.append(int(slot.get("index")))
        except Exception:
            continue
    return _slot_selection_prompt_from_indices(indexes, allow_exact_time=allow_exact_time)


def _slot_selection_prompt_from_indices(indices: Sequence[int], *, allow_exact_time: bool) -> str:
    cleaned_set: set[int] = set()
    for value in indices:
        try:
            parsed = int(value)
        except Exception:
            continue
        if parsed > 0:
            cleaned_set.add(parsed)
    cleaned = sorted(cleaned_set)
    if not cleaned:
        return "Share a preferred day and time"

    labels = [str(item) for item in cleaned]
    if len(labels) == 1:
        choice_part = labels[0]
    elif len(labels) == 2:
        choice_part = f"{labels[0]} or {labels[1]}"
    else:
        choice_part = f"{', '.join(labels[:-1])}, or {labels[-1]}"

    if allow_exact_time:
        return f"Reply with {choice_part} to book the call, or send the exact time you want"
    return f"Reply with {choice_part} to book the call"


def _slot_sort_key(slot: BookingSlot) -> tuple[datetime, int]:
    parsed = _to_utc_datetime(slot.start_time)
    if parsed is None:
        parsed = datetime.max.replace(tzinfo=timezone.utc)
    return parsed, int(slot.index)


def _slot_local_start(slot: BookingSlot, timezone_name: str) -> datetime | None:
    start_at = _to_utc_datetime(slot.start_time)
    if start_at is None:
        return None
    return start_at.astimezone(_tzinfo(timezone_name))


def _slot_is_spaced_from_selection(
    *,
    slot: BookingSlot,
    selected: Sequence[BookingSlot],
    timezone_name: str,
    minimum_minutes: int,
) -> bool:
    candidate = _slot_local_start(slot, timezone_name)
    if candidate is None:
        return True
    for existing in selected:
        existing_start = _slot_local_start(existing, timezone_name)
        if existing_start is None:
            continue
        if existing_start.date() != candidate.date():
            continue
        if abs((candidate - existing_start).total_seconds()) < minimum_minutes * 60:
            return False
    return True


def _evenly_spaced_select(items: Sequence[BookingSlot], target: int) -> list[BookingSlot]:
    if not items or target <= 0:
        return []
    if target >= len(items):
        return list(items)
    if target == 1:
        return [items[0]]

    last_index = len(items) - 1
    step = last_index / (target - 1)
    chosen_indexes: list[int] = []
    used: set[int] = set()
    for i in range(target):
        candidate = int(round(i * step))
        candidate = max(0, min(last_index, candidate))
        while candidate in used and candidate < last_index:
            candidate += 1
        if candidate in used:
            candidate = next((idx for idx in range(last_index + 1) if idx not in used), candidate)
        if candidate not in used:
            used.add(candidate)
            chosen_indexes.append(candidate)

    if len(chosen_indexes) < target:
        for idx in range(last_index + 1):
            if idx in used:
                continue
            chosen_indexes.append(idx)
            if len(chosen_indexes) >= target:
                break
    chosen_indexes.sort()
    return [items[idx] for idx in chosen_indexes[:target]]


def _time_period(local_dt: datetime) -> str:
    hour = local_dt.hour
    if hour < 12:
        return "morning"
    if hour < 17:
        return "afternoon"
    return "evening"


def _period_coverage_phrase(periods: Sequence[str]) -> str:
    order = {"morning": 0, "afternoon": 1, "evening": 2}
    unique = [item for item in periods if item in order]
    if not unique:
        return ""
    normalized = sorted(set(unique), key=lambda item: order[item])
    if len(normalized) == 1:
        return normalized[0]
    if len(normalized) == 3:
        return "morning through evening"
    if len(normalized) == 2:
        first_idx = order[normalized[0]]
        second_idx = order[normalized[1]]
        if second_idx == first_idx + 1:
            return f"{normalized[0]} through {normalized[1]}"
        return f"{normalized[0]} and {normalized[1]}"
    return _join_with_and(normalized)


def _join_with_and(items: Sequence[str]) -> str:
    cleaned = [str(item).strip() for item in items if str(item).strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    return f"{', '.join(cleaned[:-1])}, and {cleaned[-1]}"
