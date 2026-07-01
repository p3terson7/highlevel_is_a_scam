from __future__ import annotations

import re
from datetime import datetime, timezone
from html import escape
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.db.models import AuditLog, CalendarBooking, Client, Lead
from app.services.i18n import client_language, normalize_language
from app.services.lead_summary import format_answer_value

ZAPIER_BOOKING_WEBHOOK_CONFIG_KEY = "zapier_booking_webhook_url"

_SENT_EVENT = "zapier_booking_webhook_sent"
_FAILED_EVENT = "zapier_booking_webhook_failed"
_DEFAULT_TIMEOUT_SECONDS = 8
_SCHEMA_VERSION = "2026-06-17"

logger = get_logger(__name__)


def notify_zapier_booking_webhook(
    *,
    db: Session,
    client: Client,
    lead: Lead,
    booking: CalendarBooking | None = None,
    calendar_booking: dict[str, Any] | None = None,
    trigger: str,
    timeout_seconds: int = _DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    url = _zapier_booking_webhook_url(client)
    if not url:
        return {"status": "skipped", "reason": "not_configured"}
    if not _valid_webhook_url(url):
        _record_webhook_audit(
            db=db,
            client=client,
            lead=lead,
            event_type=_FAILED_EVENT,
            decision={"reason": "invalid_url", "trigger": trigger},
        )
        return {"status": "failed", "reason": "invalid_url"}

    payload = build_zapier_booking_payload(
        client=client,
        lead=lead,
        booking=booking,
        calendar_booking=calendar_booking,
        trigger=trigger,
    )
    dedupe_key = str(payload.get("meeting", {}).get("dedupe_key") or "").strip()
    if dedupe_key and _already_sent(db=db, client_id=client.id, lead_id=lead.id, dedupe_key=dedupe_key):
        return {"status": "skipped", "reason": "already_sent", "dedupe_key": dedupe_key}

    try:
        response = _post_json(url=url, payload=payload, timeout_seconds=timeout_seconds)
        if response.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"Zapier webhook returned HTTP {response.status_code}",
                request=response.request,
                response=response,
            )
    except Exception as exc:
        logger.warning(
            "zapier_booking_webhook_failed",
            extra={"client_id": client.id, "lead_id": lead.id, "trigger": trigger, "error": str(exc)},
        )
        _record_webhook_audit(
            db=db,
            client=client,
            lead=lead,
            event_type=_FAILED_EVENT,
            decision={
                "trigger": trigger,
                "dedupe_key": dedupe_key,
                "error": str(exc),
                "endpoint_host": _webhook_host(url),
                "payload": payload,
            },
        )
        return {"status": "failed", "reason": str(exc), "dedupe_key": dedupe_key}

    _record_webhook_audit(
        db=db,
        client=client,
        lead=lead,
        event_type=_SENT_EVENT,
        decision={
            "trigger": trigger,
            "dedupe_key": dedupe_key,
            "status_code": response.status_code,
            "endpoint_host": _webhook_host(url),
            "payload": payload,
        },
    )
    return {"status": "sent", "status_code": response.status_code, "dedupe_key": dedupe_key}


def build_zapier_booking_payload(
    *,
    client: Client,
    lead: Lead,
    booking: CalendarBooking | None = None,
    calendar_booking: dict[str, Any] | None = None,
    trigger: str,
) -> dict[str, Any]:
    form = _form_payload(lead)
    meeting = _meeting_payload(
        client=client,
        lead=lead,
        booking=booking,
        calendar_booking=calendar_booking,
    )
    calendar_event = _calendar_event_payload(client=client, lead=lead, meeting=meeting, form=form)
    email_confirmation = _email_confirmation_payload(
        client=client,
        lead=lead,
        meeting=meeting,
        calendar_event=calendar_event,
        form=form,
    )
    return {
        "event_type": "calendar_booking.created",
        "schema_version": _SCHEMA_VERSION,
        "trigger": trigger,
        "sent_at": _iso(datetime.now(timezone.utc)),
        "client": _client_payload(client),
        "lead": _lead_payload(lead),
        "form": form,
        "meeting": meeting,
        "calendar_event": calendar_event,
        "email_confirmation": email_confirmation,
        "zapier_mapping_hints": _zapier_mapping_hints(),
        # Backward-compatible aliases for existing Zaps.
        "form_answers": form["answers_map"],
        "form_answers_list": form["answers"],
    }


def _zapier_booking_webhook_url(client: Client) -> str:
    provider_config = client.provider_config if isinstance(client.provider_config, dict) else {}
    return str(provider_config.get(ZAPIER_BOOKING_WEBHOOK_CONFIG_KEY) or "").strip()


def _client_payload(client: Client) -> dict[str, Any]:
    return {
        "id": client.id,
        "client_key": client.client_key,
        "business_name": client.business_name,
        "portal_display_name": client.portal_display_name,
        "contact_email": client.portal_email,
        "timezone": client.timezone,
    }


def _lead_payload(lead: Lead) -> dict[str, Any]:
    source = getattr(lead.source, "value", lead.source)
    state = getattr(lead.conversation_state, "value", lead.conversation_state)
    return {
        "id": lead.id,
        "external_lead_id": lead.external_lead_id,
        "source": source,
        "full_name": lead.full_name,
        "phone": lead.phone,
        "email": lead.email,
        "city": lead.city,
        "crm_stage": lead.crm_stage,
        "conversation_state": state,
        "owner_name": lead.owner_name,
        "created_at": _iso(lead.created_at),
        "updated_at": _iso(lead.updated_at),
    }


def _meeting_payload(
    *,
    client: Client,
    lead: Lead,
    booking: CalendarBooking | None,
    calendar_booking: dict[str, Any] | None,
) -> dict[str, Any]:
    language = client_language(client, lead=lead)
    title = _meeting_title(client=client, lead=lead)
    if booking is not None:
        timezone_name = booking.timezone or client.timezone or "UTC"
        payload = {
            "id": booking.id,
            "dedupe_key": f"calendar_booking:{booking.id}",
            "provider": booking.provider,
            "source": booking.source,
            "status": booking.status,
            "title": title,
            "summary": title,
            "internal_title": booking.title,
            "notes": booking.notes,
            "timezone": timezone_name,
            "start_at": _iso(booking.start_at),
            "start_at_utc": _iso(_as_utc(booking.start_at)),
            "end_at": _iso(booking.end_at),
            "end_at_utc": _iso(_as_utc(booking.end_at)),
        }
        payload.update(
            _local_time_fields(
                start_at=booking.start_at,
                end_at=booking.end_at,
                timezone_name=timezone_name,
                language=language,
            )
        )
        return payload

    raw = calendar_booking if isinstance(calendar_booking, dict) else {}
    raw_booking = _json_object(raw.get("booking"))
    raw_slot = _json_object(raw.get("slot"))
    provider = str(raw.get("provider") or raw_booking.get("provider") or "").strip()
    booking_id = raw_booking.get("booking_id")
    event_uri = str(raw_booking.get("event_uri") or "").strip()
    start_at = _parse_datetime(str(raw_booking.get("start_time") or raw_slot.get("start_time") or ""))
    end_at = _parse_datetime(str(raw_booking.get("end_time") or raw_slot.get("end_time") or ""))
    timezone_name = str(raw_booking.get("timezone") or client.timezone or "UTC").strip() or "UTC"
    dedupe_value = booking_id or event_uri or str(raw_slot.get("start_time") or "")
    payload = {
        "id": booking_id,
        "dedupe_key": f"calendar_booking:{dedupe_value or lead.id}",
        "provider": provider,
        "source": "sms_ai",
        "status": str(raw_booking.get("status") or "scheduled"),
        "title": title,
        "summary": title,
        "internal_title": str(
            raw_booking.get("title")
            or f"Lead call - {lead.full_name or lead.phone or lead.id}"
        ),
        "notes": str(raw_booking.get("notes") or ""),
        "timezone": timezone_name,
        "start_at": _iso(start_at),
        "start_at_utc": _iso(start_at),
        "end_at": _iso(end_at),
        "end_at_utc": _iso(end_at),
        "display_time": str(raw_booking.get("display_time") or raw_slot.get("display_time") or ""),
        "provider_event_uri": event_uri,
        "provider_invitee_uri": str(raw_booking.get("invitee_uri") or ""),
        "reschedule_url": str(raw_booking.get("reschedule_url") or ""),
        "cancel_url": str(raw_booking.get("cancel_url") or ""),
    }
    payload.update(
        _local_time_fields(
            start_at=start_at,
            end_at=end_at,
            timezone_name=timezone_name,
            language=language,
        )
    )
    return payload


def _local_time_fields(
    *,
    start_at: datetime | None,
    end_at: datetime | None,
    timezone_name: str,
    language: str = "en",
) -> dict[str, Any]:
    language = normalize_language(language)
    start_local = _as_local(start_at, timezone_name)
    end_local = _as_local(end_at, timezone_name)
    duration_minutes = None
    if start_at is not None and end_at is not None:
        duration_minutes = int((end_at - start_at).total_seconds() / 60)
    date_label = _local_date_label(start_local, language=language)
    time_range_label = _local_time_range_label(
        start_local=start_local,
        end_local=end_local,
        timezone_name=timezone_name,
        language=language,
    )
    return {
        "local_start_at": _iso(start_local),
        "local_end_at": _iso(end_local),
        "start_at_local": _iso(start_local),
        "end_at_local": _iso(end_local),
        "zapier_start_datetime": _iso(start_local),
        "zapier_end_datetime": _iso(end_local),
        "google_calendar_start": _iso(start_local),
        "google_calendar_end": _iso(end_local),
        "date": start_local.date().isoformat() if start_local else None,
        "date_label": date_label,
        "start_time": start_local.strftime("%H:%M") if start_local else None,
        "end_time": end_local.strftime("%H:%M") if end_local else None,
        "start_time_24h": start_local.strftime("%H:%M") if start_local else None,
        "end_time_24h": end_local.strftime("%H:%M") if end_local else None,
        "start_time_12h": _clock_label(start_local),
        "end_time_12h": _clock_label(end_local),
        "start_time_display": _clock_label(start_local, language=language),
        "end_time_display": _clock_label(end_local, language=language),
        "time_range_label": time_range_label,
        "duration_minutes": duration_minutes,
    }


def _form_payload(lead: Lead) -> dict[str, Any]:
    normalized_answers = _json_object(lead.form_answers)
    rows: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    for raw_row in _extract_raw_form_answer_rows(_json_object(lead.raw_payload)):
        _append_form_row(
            rows=rows,
            seen_keys=seen_keys,
            raw_question=raw_row[0],
            raw_answer=raw_row[1],
        )

    for key, value in normalized_answers.items():
        _append_form_row(
            rows=rows,
            seen_keys=seen_keys,
            raw_question=_question_label(key),
            raw_answer=value,
        )

    answers_map: dict[str, Any] = dict(normalized_answers)
    question_answer_map: dict[str, str] = {}
    for row in rows:
        answers_map.setdefault(row["key"], row["value"])
        question_answer_map[row["question"]] = row["answer"]

    answers_text = "\n".join(f"{row['question']}: {row['answer']}" for row in rows)
    return {
        "answers": rows,
        "answers_map": answers_map,
        "question_answer_map": question_answer_map,
        "answers_text": answers_text,
    }


def _extract_raw_form_answer_rows(raw_payload: dict[str, Any]) -> list[tuple[Any, Any]]:
    rows: list[tuple[Any, Any]] = []
    containers = [raw_payload]
    nested_lead = raw_payload.get("lead")
    if isinstance(nested_lead, dict):
        containers.append(nested_lead)

    for container in containers:
        submitted = container.get("submitted_form_answers")
        if isinstance(submitted, list):
            for item in submitted:
                if not isinstance(item, dict):
                    continue
                question = item.get("question") or item.get("label") or item.get("name") or item.get("key")
                answer = item.get("answer") if "answer" in item else item.get("value")
                rows.append((question, answer))

        field_data = container.get("field_data")
        if isinstance(field_data, list):
            for item in field_data:
                if not isinstance(item, dict):
                    continue
                question = item.get("question") or item.get("label") or item.get("name")
                answer = item.get("answer") if "answer" in item else item.get("value", item.get("values"))
                rows.append((question, answer))

        for key in ("form_answers", "fields"):
            values = container.get(key)
            if isinstance(values, dict):
                rows.extend(values.items())

    return rows


def _append_form_row(
    *,
    rows: list[dict[str, Any]],
    seen_keys: set[str],
    raw_question: Any,
    raw_answer: Any,
) -> None:
    question = str(raw_question or "").strip()
    value = _clean_answer_value(raw_answer)
    answer = format_answer_value(value).strip()
    if not question or answer == "":
        return
    key = _form_key(question)
    if not key or key in seen_keys:
        return
    rows.append(
        {
            "question": question,
            "key": key,
            "answer": answer,
            "value": value,
        }
    )
    seen_keys.add(key)


def _clean_answer_value(value: Any) -> Any:
    if isinstance(value, list):
        cleaned = [_clean_answer_value(item) for item in value]
        cleaned = [item for item in cleaned if item not in ("", None)]
        if len(cleaned) == 1:
            return cleaned[0]
        return cleaned
    if value is None:
        return ""
    if isinstance(value, str):
        return " ".join(value.replace("\n", " ").split()).strip()
    return value


def _form_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _question_label(key: str) -> str:
    return str(key or "").replace("_", " ").strip().title()


def _calendar_event_payload(
    *,
    client: Client,
    lead: Lead,
    meeting: dict[str, Any],
    form: dict[str, Any],
) -> dict[str, Any]:
    language = client_language(client, lead=lead)
    summary = str(
        meeting.get("summary")
        or meeting.get("title")
        or _meeting_title(client=client, lead=lead)
    )
    description = _calendar_description(client=client, lead=lead, meeting=meeting, form=form)
    description_html = _calendar_description_html(client=client, lead=lead, meeting=meeting, form=form)
    attendee_emails = [lead.email] if lead.email else []
    return {
        "summary": summary,
        "title": summary,
        "description": description,
        "description_text": description,
        "description_html": description_html,
        "start_datetime": meeting.get("start_at_local"),
        "end_datetime": meeting.get("end_at_local"),
        "timezone": meeting.get("timezone") or client.timezone,
        "attendee_emails": attendee_emails,
        "attendee_email": lead.email,
        "location": "Appel de consultation" if language == "fr" else "Conference call",
    }


def _email_confirmation_payload(
    *,
    client: Client,
    lead: Lead,
    meeting: dict[str, Any],
    calendar_event: dict[str, Any],
    form: dict[str, Any],
) -> dict[str, Any]:
    language = client_language(client, lead=lead)
    business_name = _business_name(client)
    lead_name = _lead_display_name(lead)
    first_name = lead.full_name.split()[0] if lead.full_name else ("bonjour" if language == "fr" else "there")
    time_label = str(meeting.get("time_range_label") or ("l'heure prévue" if language == "fr" else "the scheduled time"))
    subject = f"{business_name}: rencontre confirmée avec {lead_name}" if language == "fr" else f"{business_name}: meeting confirmed with {lead_name}"
    details = _email_details_lines(lead=lead, meeting=meeting, form=form, language=language)
    if language == "fr":
        body_text = "\n".join(
            [
                f"Bonjour {first_name},",
                "",
                f"Votre rencontre avec {business_name} est réservée.",
                "",
                f"Quand: {time_label}",
                f"Sujet: {calendar_event.get('summary') or meeting.get('title')}",
                "",
                "Détails partagés:",
                *details,
                "",
                "Au plaisir de vous parler.",
            ]
        )
        intro = f"Votre rencontre avec {business_name} est réservée."
    else:
        body_text = "\n".join(
            [
                f"Hi {first_name},",
                "",
                f"Your meeting with {business_name} is booked.",
                "",
                f"When: {time_label}",
                f"Topic: {calendar_event.get('summary') or meeting.get('title')}",
                "",
                "Details shared:",
                *details,
                "",
                "Looking forward to meeting.",
            ]
        )
        intro = f"Your meeting with {business_name} is booked."
    body_html = _email_confirmation_html(
        client=client,
        lead=lead,
        meeting=meeting,
        calendar_event=calendar_event,
        form=form,
        intro=intro,
    )
    return {
        "to": lead.email,
        "from_name": business_name,
        "reply_to": client.portal_email,
        "subject": subject,
        "body_text": body_text,
        "body_html": body_html,
        "structured_description": {
            "intro": intro,
            "meeting": {
                "title": calendar_event.get("summary"),
                "time": time_label,
                "start_datetime": calendar_event.get("start_datetime"),
                "end_datetime": calendar_event.get("end_datetime"),
                "timezone": calendar_event.get("timezone"),
            },
            "lead": {
                "name": lead.full_name,
                "email": lead.email,
                "phone": lead.phone,
                "city": lead.city,
            },
            "form_answers": form.get("answers", []),
        },
    }


def _calendar_description(
    *,
    client: Client,
    lead: Lead,
    meeting: dict[str, Any],
    form: dict[str, Any],
) -> str:
    language = client_language(client, lead=lead)
    if language == "fr":
        lines = [
            "RENCONTRE",
            f"Titre: {meeting.get('title') or _meeting_title(client=client, lead=lead)}",
            f"Entreprise: {_business_name(client)}",
            f"Quand: {meeting.get('time_range_label') or ''}",
        ]
        lines.extend(["", "CONTACT"])
        lines.append(f"Lead: {_lead_display_name(lead)}")
    else:
        lines = [
            "MEETING",
            f"Title: {meeting.get('title') or _meeting_title(client=client, lead=lead)}",
            f"Business: {_business_name(client)}",
            f"When: {meeting.get('time_range_label') or ''}",
        ]
        lines.extend(["", "CONTACT"])
        lines.append(f"Lead: {_lead_display_name(lead)}")
    if lead.email:
        lines.append(f"Courriel: {lead.email}" if language == "fr" else f"Email: {lead.email}")
    if lead.phone:
        lines.append(f"Téléphone: {lead.phone}" if language == "fr" else f"Phone: {lead.phone}")
    if lead.city:
        lines.append(f"Ville / emplacement: {lead.city}" if language == "fr" else f"Location / City: {lead.city}")
    if meeting.get("notes"):
        lines.extend(["", f"Notes: {meeting['notes']}"])
    form_rows = _form_answer_rows(form)
    if form_rows:
        lines.extend(["", "RÉPONSES DU FORMULAIRE" if language == "fr" else "FORM ANSWERS"])
        lines.extend(f"- {label}: {value}" for label, value in form_rows)
    return "\n".join(line for line in lines if line is not None)


def _calendar_description_html(
    *,
    client: Client,
    lead: Lead,
    meeting: dict[str, Any],
    form: dict[str, Any],
) -> str:
    language = client_language(client, lead=lead)
    business_name = _business_name(client)
    title = str(meeting.get("title") or _meeting_title(client=client, lead=lead))
    time_label = str(meeting.get("time_range_label") or "")
    contact_rows = _contact_rows(lead, language=language)
    form_rows = _form_answer_rows(form)
    notes = str(meeting.get("notes") or "").strip()
    return _simple_calendar_html(
        sections=[
            (
                "Rencontre" if language == "fr" else "Meeting",
                [
                    ("Titre" if language == "fr" else "Title", title),
                    ("Entreprise" if language == "fr" else "Business", business_name),
                    ("Quand" if language == "fr" else "When", time_label),
                ],
            ),
            ("Contact", contact_rows),
            ("Réponses du formulaire" if language == "fr" else "Form answers", form_rows),
            ("Notes", [("Notes internes" if language == "fr" else "Internal notes", notes)] if notes else []),
        ],
    )


def _email_details_lines(*, lead: Lead, meeting: dict[str, Any], form: dict[str, Any], language: str = "en") -> list[str]:
    lines: list[str] = []
    if lead.email:
        lines.append(f"- {'Courriel' if language == 'fr' else 'Email'}: {lead.email}")
    if lead.phone:
        lines.append(f"- {'Téléphone' if language == 'fr' else 'Phone'}: {lead.phone}")
    if lead.city:
        lines.append(f"- {'Ville / emplacement' if language == 'fr' else 'Location / City'}: {lead.city}")
    if meeting.get("notes"):
        lines.append(f"- Notes: {meeting['notes']}")
    for row in form.get("answers", [])[:10]:
        if isinstance(row, dict):
            lines.append(f"- {row.get('question')}: {row.get('answer')}")
    return lines or ["- Aucun détail de formulaire supplémentaire n'a été fourni." if language == "fr" else "- No additional form details were provided."]


def _email_confirmation_html(
    *,
    client: Client,
    lead: Lead,
    meeting: dict[str, Any],
    calendar_event: dict[str, Any],
    form: dict[str, Any],
    intro: str,
) -> str:
    language = client_language(client, lead=lead)
    business_name = _business_name(client)
    lead_name = _lead_display_name(lead)
    time_label = str(meeting.get("time_range_label") or ("l'heure prévue" if language == "fr" else "the scheduled time"))
    title = str(calendar_event.get("summary") or meeting.get("title") or _meeting_title(client=client, lead=lead))
    return _styled_card_html(
        title="Rencontre confirmée" if language == "fr" else "Meeting confirmed",
        eyebrow=business_name,
        intro=(
            f"Bonjour {lead.full_name.split()[0] if lead.full_name else ''}, {intro}".strip()
            if language == "fr"
            else f"Hi {lead.full_name.split()[0] if lead.full_name else 'there'}, {intro}"
        ),
        sections=[
            (
                "Rencontre" if language == "fr" else "Meeting",
                [
                    ("Titre" if language == "fr" else "Title", title),
                    ("Quand" if language == "fr" else "When", time_label),
                    ("Lead", lead_name),
                ],
            ),
            ("Contact", _contact_rows(lead, language=language)),
            ("Réponses du formulaire" if language == "fr" else "Form answers", _form_answer_rows(form)),
        ],
        compact=False,
        footer=(
            f"Si quelque chose change, répondez à ce courriel et {escape(business_name)} vous aidera à mettre la rencontre à jour."
            if language == "fr"
            else f"If anything changes, reply to this email and {escape(business_name)} will help update the meeting."
        ),
    )


def _styled_card_html(
    *,
    title: str,
    eyebrow: str,
    intro: str,
    sections: list[tuple[str, list[tuple[str, str]]]],
    compact: bool,
    footer: str = "",
) -> str:
    _ = compact
    max_width = "640px"
    body = [
        '<div style="margin:0;padding:0;background:#f5f5f7;">',
        (
            f'<div style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,'
            f'Helvetica,Arial,sans-serif;color:#1d1d1f;line-height:1.45;'
            f'max-width:{max_width};margin:0 auto;padding:28px 16px;">'
        ),
        (
            f'<div style="background:#ffffff;border:1px solid #e5e5ea;border-radius:18px;'
            f'overflow:hidden;">'
        ),
        '<div style="padding:28px 28px 10px;">',
        (
            '<div style="width:10px;height:10px;border-radius:999px;background:#007aff;'
            'margin-bottom:18px;"></div>'
        ),
        (
            f'<div style="font-size:12px;letter-spacing:.08em;text-transform:uppercase;'
            f'color:#6e6e73;font-weight:700;margin-bottom:6px;">{escape(eyebrow)}</div>'
            f'<div style="font-size:28px;line-height:1.12;font-weight:700;letter-spacing:-.02em;'
            f'margin:0 0 12px;color:#1d1d1f;">{escape(title)}</div>'
            f'<div style="font-size:16px;color:#424245;margin:0 0 22px;">{escape(intro)}</div>'
        ),
        (
            '<div style="height:1px;background:#e5e5ea;margin:0 0 4px;"></div>'
        ),
    ]
    for heading, rows in sections:
        if not rows:
            continue
        body.append(_section_html(heading=heading, rows=rows))
    if footer:
        body.append(
            f'<div style="margin-top:22px;color:#6e6e73;font-size:13px;">{footer}</div>'
        )
    body.extend(["</div>", "</div>", "</div>", "</div>"])
    return "".join(body)


def _section_html(*, heading: str, rows: list[tuple[str, str]]) -> str:
    row_html = []
    for label, value in rows:
        if not value:
            continue
        row_html.append(
            "<tr>"
            f'<td style="padding:7px 12px 7px 0;color:#6e6e73;font-size:13px;'
            f'font-weight:700;vertical-align:top;width:190px;">{escape(label)}</td>'
            f'<td style="padding:7px 0;color:#1d1d1f;font-size:14px;vertical-align:top;">'
            f"{escape(value)}</td>"
            "</tr>"
        )
    if not row_html:
        return ""
    return (
        '<div style="margin:0;">'
        f'<div style="font-size:15px;font-weight:700;color:#1d1d1f;'
        f'padding:18px 0 8px;">'
        f"{escape(heading)}</div>"
        '<table role="presentation" cellpadding="0" cellspacing="0" '
        'style="border-collapse:collapse;width:100%;">'
        + "".join(row_html)
        + "</table></div>"
    )


def _simple_calendar_html(*, sections: list[tuple[str, list[tuple[str, str]]]]) -> str:
    html: list[str] = []
    for heading, rows in sections:
        filtered = [(label, value) for label, value in rows if value]
        if not filtered:
            continue
        html.append(f"<strong>{escape(heading)}</strong><ul>")
        for label, value in filtered:
            html.append(f"<li><strong>{escape(label)}:</strong> {escape(value)}</li>")
        html.append("</ul>")
    return "".join(html)


def _contact_rows(lead: Lead, *, language: str = "en") -> list[tuple[str, str]]:
    rows = [("Lead", _lead_display_name(lead))]
    if lead.email:
        rows.append(("Courriel" if language == "fr" else "Email", lead.email))
    if lead.phone:
        rows.append(("Téléphone" if language == "fr" else "Phone", lead.phone))
    if lead.city:
        rows.append(("Ville / emplacement" if language == "fr" else "Location / City", lead.city))
    return rows


def _form_answer_rows(form: dict[str, Any]) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for row in form.get("answers", []):
        if not isinstance(row, dict):
            continue
        question = str(row.get("question") or "").strip()
        answer = str(row.get("answer") or "").strip()
        if question and answer:
            rows.append((question, answer))
    return rows


def _meeting_title(*, client: Client, lead: Lead) -> str:
    noun = "rencontre" if client_language(client, lead=lead) == "fr" else "meeting"
    return f"{_business_name(client)} {noun} - {_lead_display_name(lead)}"


def _lead_display_name(lead: Lead) -> str:
    return str(lead.full_name or lead.email or lead.phone or f"Lead {lead.id}").strip()


def _business_name(client: Client) -> str:
    return str(client.business_name or client.portal_display_name or "Business").strip()


def _zapier_mapping_hints() -> dict[str, Any]:
    return {
        "google_calendar": {
            "summary": "calendar_event.summary",
            "description": "calendar_event.description",
            "html_description_if_supported": "calendar_event.description_html",
            "start_date_time": "calendar_event.start_datetime",
            "end_date_time": "calendar_event.end_datetime",
            "attendee_emails": "calendar_event.attendee_emails",
        },
        "gmail_confirmation": {
            "to": "email_confirmation.to",
            "from_name": "email_confirmation.from_name",
            "reply_to": "email_confirmation.reply_to",
            "subject": "email_confirmation.subject",
            "plain_body": "email_confirmation.body_text",
            "html_body": "email_confirmation.body_html",
        },
        "important": (
            "For calendar start/end fields, use calendar_event.start_datetime and "
            "calendar_event.end_datetime. Do not map meeting.start_time or "
            "meeting.end_time into Google Calendar date fields because those are "
            "display-only times."
        ),
    }


_FR_WEEKDAYS = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]
_FR_MONTHS = [
    "janvier",
    "février",
    "mars",
    "avril",
    "mai",
    "juin",
    "juillet",
    "août",
    "septembre",
    "octobre",
    "novembre",
    "décembre",
]


def _local_date_label(value: datetime | None, *, language: str = "en") -> str | None:
    if value is None:
        return None
    if normalize_language(language) == "fr":
        return f"{_FR_WEEKDAYS[value.weekday()]} {value.day} {_FR_MONTHS[value.month - 1]} {value.year}"
    return f"{value.strftime('%A, %B')} {value.day}, {value.year}"


def _local_time_range_label(
    *,
    start_local: datetime | None,
    end_local: datetime | None,
    timezone_name: str,
    language: str = "en",
) -> str | None:
    if start_local is None:
        return None
    language = normalize_language(language)
    date_label = _local_date_label(start_local, language=language)
    start_label = _clock_label(start_local, language=language)
    end_label = _clock_label(end_local, language=language)
    tz_label = start_local.tzname() or timezone_name
    if end_label:
        if language == "fr":
            return f"{date_label} de {start_label} à {end_label} {tz_label}"
        return f"{date_label} from {start_label} to {end_label} {tz_label}"
    if language == "fr":
        return f"{date_label} à {start_label} {tz_label}"
    return f"{date_label} at {start_label} {tz_label}"


def _clock_label(value: datetime | None, *, language: str = "en") -> str | None:
    if value is None:
        return None
    if normalize_language(language) == "fr":
        return f"{value.hour} h {value.minute:02d}"
    return value.strftime("%I:%M %p").lstrip("0")


def _record_webhook_audit(
    *,
    db: Session,
    client: Client,
    lead: Lead,
    event_type: str,
    decision: dict[str, Any],
) -> None:
    db.add(AuditLog(client_id=client.id, lead_id=lead.id, event_type=event_type, decision=decision))
    db.commit()


def _already_sent(*, db: Session, client_id: int, lead_id: int, dedupe_key: str) -> bool:
    recent = db.scalars(
        select(AuditLog)
        .where(
            AuditLog.client_id == client_id,
            AuditLog.lead_id == lead_id,
            AuditLog.event_type == _SENT_EVENT,
        )
        .order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
        .limit(25)
    ).all()
    return any(str((row.decision or {}).get("dedupe_key") or "") == dedupe_key for row in recent)


def _post_json(*, url: str, payload: dict[str, Any], timeout_seconds: int) -> httpx.Response:
    with httpx.Client(timeout=timeout_seconds) as client:
        return client.post(url, json=payload)


def _json_object(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


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


def _as_local(value: datetime | None, timezone_name: str) -> datetime | None:
    if value is None:
        return None
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        tz = timezone.utc
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(tz)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.replace(microsecond=0).isoformat()


def _valid_webhook_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _webhook_host(url: str) -> str:
    return urlparse(url).netloc
