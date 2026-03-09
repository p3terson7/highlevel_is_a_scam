from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence
from zoneinfo import ZoneInfo

import httpx

from app.db.models import Client, ConversationStateEnum, Lead, Message

_CALENDLY_API_BASE = "https://api.calendly.com"
_EMAIL_RE = re.compile(r"(?P<email>[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", re.IGNORECASE)


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


def calendar_booking_confirmed(_: str) -> bool:
    return False


def extract_email(text: str) -> str | None:
    match = _EMAIL_RE.search(text or "")
    if not match:
        return None
    return match.group("email").strip().lower()


def automated_booking_enabled(client: Client) -> bool:
    config = client.booking_config or {}
    return (
        str(client.booking_mode or "link").strip().lower() == "calendly"
        and bool(str(config.get("calendly_personal_access_token", "")).strip())
        and bool(str(config.get("calendly_event_type_uri", "")).strip())
    )


def booking_mode_label(client: Client) -> str:
    return str(client.booking_mode or "link").strip().lower() or "link"


class BookingService:
    def __init__(self, timeout_seconds: int = 20) -> None:
        self._timeout_seconds = timeout_seconds

    def preview_slots(self, client: Client, *, limit: int = 3) -> SlotOffer:
        return self.offer_slots(client=client, lead=None, limit=limit)

    def offer_slots(self, client: Client, lead: Lead | None, *, limit: int = 3) -> SlotOffer:
        if not automated_booking_enabled(client):
            raise BookingProviderError("Automated booking is not configured for this client.")

        slots = self._list_calendly_slots(client=client, limit=limit)
        if not slots:
            fallback = "I am not seeing open times right now."
            if client.booking_url:
                fallback = ensure_booking_link(
                    "I am not seeing open times right now, but you can still book here.",
                    client,
                )
            return SlotOffer(
                reply_text=fallback,
                slots=[],
                raw_payload={"booking_offer": {"provider": "calendly", "slots": []}},
            )

        timezone_label = self._timezone_abbreviation(client.timezone)
        lines = [
            "I can book this directly. Here are the next available times:",
            *[f"{slot.index}) {slot.display_time}" for slot in slots],
            f"Reply with 1, 2, or 3, or send the exact time you want. Times shown in {timezone_label}.",
        ]
        raw_payload = {
            "booking_offer": {
                "provider": "calendly",
                "event_type_uri": self._calendly_config(client)["calendly_event_type_uri"],
                "slots": [slot.__dict__ for slot in slots],
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
        }
        return SlotOffer(reply_text="\n".join(lines), slots=slots, raw_payload=raw_payload)

    def handle_slot_selection(
        self,
        *,
        client: Client,
        lead: Lead,
        inbound_text: str,
        history: Sequence[Message],
    ) -> BookingSelectionResult | None:
        latest_offer = self._latest_offer(history)
        if latest_offer is None:
            return None

        slots = latest_offer.get("slots", [])
        matched = self._match_slot(inbound_text, slots)
        if matched is None:
            timezone_label = self._timezone_abbreviation(client.timezone)
            lines = [
                "I did not catch which slot you want.",
                *[f"{slot.get('index')}) {slot.get('display_time')}" for slot in slots],
                f"Reply with 1, 2, or 3. Times shown in {timezone_label}.",
            ]
            return BookingSelectionResult(
                handled=True,
                reply_text="\n".join(lines),
                next_state=ConversationStateEnum.BOOKING_SENT,
                raw_payload={"booking_offer": latest_offer},
                audit_event_type="calendar_booking_offer_repeated",
                audit_decision={"inbound": inbound_text, "slots": slots},
                transition_reason="calendar_booking_offer_repeated",
            )

        booking = self._book_calendly_slot(client=client, lead=lead, slot=matched)
        confirmation = [
            f"Booked. You are set for {matched.get('display_time')}.",
            f"Confirmation will be sent to {lead.email}.",
        ]
        if booking.get("reschedule_url"):
            confirmation.append(f"Reschedule: {booking['reschedule_url']}")
        return BookingSelectionResult(
            handled=True,
            reply_text=" ".join(confirmation),
            next_state=ConversationStateEnum.BOOKED,
            raw_payload={
                "calendar_booking": {
                    "provider": "calendly",
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
    parts = [
        local_dt.strftime("%A %I %p"),
        local_dt.strftime("%A %I:%M %p"),
        local_dt.strftime("%a %I %p"),
        local_dt.strftime("%a %I:%M %p"),
        local_dt.strftime("%B %d %I:%M %p"),
        local_dt.strftime("%m/%d %I:%M %p"),
    ]
    variants = set()
    for part in parts:
        normalized = _normalize_slot_text(part)
        if normalized:
            variants.add(normalized)
            variants.add(normalized.replace(" am", "am").replace(" pm", "pm"))
    return " | ".join(sorted(variants))
