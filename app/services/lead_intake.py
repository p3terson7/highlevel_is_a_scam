from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import Client, Lead, LeadSource


@dataclass
class NormalizedLead:
    external_lead_id: str | None
    full_name: str
    phone: str
    email: str
    city: str
    form_answers: dict[str, Any]
    raw_payload: dict[str, Any]
    consented: bool = True


def normalize_phone(raw_phone: str | None) -> str:
    if not raw_phone:
        return ""
    cleaned = raw_phone.strip()
    if cleaned.startswith("+"):
        digits = re.sub(r"\D", "", cleaned)
        return f"+{digits}" if digits else ""

    digits = re.sub(r"\D", "", cleaned)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return f"+{digits}" if digits else ""


def _field_lookup(answers: dict[str, Any], keys: list[str]) -> str:
    lowered = {str(key).lower(): value for key, value in answers.items()}
    for key in keys:
        if key in lowered and lowered[key]:
            return str(lowered[key]).strip()
    return ""


def _extract_common_fields(form_answers: dict[str, Any]) -> tuple[str, str, str, str]:
    full_name = _field_lookup(
        form_answers,
        [
            "full_name",
            "name",
            "contact_name",
            "first_name",
        ],
    )
    email = _field_lookup(form_answers, ["email", "email_address"])
    phone = normalize_phone(
        _field_lookup(
            form_answers,
            ["phone", "phone_number", "mobile_phone", "cell", "mobile"],
        )
    )
    city = _field_lookup(form_answers, ["city", "location_city", "location"])
    return full_name, phone, email, city


def _meta_field_data_to_dict(field_data: Any) -> dict[str, Any]:
    if not isinstance(field_data, list):
        return {}

    output: dict[str, Any] = {}
    for item in field_data:
        if not isinstance(item, dict):
            continue
        key = str(item.get("name", "")).strip()
        values = item.get("values", [])
        if not key:
            continue
        if isinstance(values, list):
            output[key] = values[0] if len(values) == 1 else values
        else:
            output[key] = values
    return output


def fetch_meta_lead_details(leadgen_id: str, client: Client) -> dict[str, Any]:
    """
    Placeholder for a real Meta Graph API call.
    In production, use the client's access token to call:
    GET /{leadgen_id}?fields=field_data,created_time
    """
    _ = client
    _ = leadgen_id
    return {}


def fetch_linkedin_lead_details(linkedin_lead_id: str, client: Client) -> dict[str, Any]:
    """
    Placeholder for a real LinkedIn Lead Gen API call.
    In production, fetch full lead details when notifications only include IDs.
    """
    _ = client
    _ = linkedin_lead_id
    return {}


def normalize_meta_payload(payload: dict[str, Any], client: Client) -> list[NormalizedLead]:
    candidates: list[NormalizedLead] = []

    if isinstance(payload.get("lead"), dict):
        lead_payload = payload["lead"]
        form_answers = lead_payload.get("form_answers", lead_payload)
        if not isinstance(form_answers, dict):
            form_answers = {}
        name, phone, email, city = _extract_common_fields(form_answers)
        candidates.append(
            NormalizedLead(
                external_lead_id=str(lead_payload.get("id") or payload.get("lead_id") or "") or None,
                full_name=name,
                phone=phone,
                email=email,
                city=city,
                form_answers=form_answers,
                raw_payload=lead_payload,
                consented=True,
            )
        )

    for entry in payload.get("entry", []) if isinstance(payload.get("entry"), list) else []:
        for change in entry.get("changes", []) if isinstance(entry.get("changes"), list) else []:
            value = change.get("value") if isinstance(change, dict) else {}
            if not isinstance(value, dict):
                continue

            leadgen_id = str(value.get("leadgen_id") or value.get("lead_id") or "").strip()
            form_answers = _meta_field_data_to_dict(value.get("field_data"))

            # If webhook only includes lead ID, this is where a real API fetch is plugged in.
            if not form_answers and leadgen_id:
                fetched = fetch_meta_lead_details(leadgen_id, client)
                form_answers = _meta_field_data_to_dict(fetched.get("field_data"))
                value = {**value, **fetched}

            name, phone, email, city = _extract_common_fields(form_answers)
            candidates.append(
                NormalizedLead(
                    external_lead_id=leadgen_id or None,
                    full_name=name,
                    phone=phone,
                    email=email,
                    city=city,
                    form_answers=form_answers,
                    raw_payload=value,
                    consented=True,
                )
            )

    if isinstance(payload.get("leads"), list):
        for item in payload["leads"]:
            if not isinstance(item, dict):
                continue
            form_answers = item.get("form_answers", item)
            if not isinstance(form_answers, dict):
                form_answers = {}
            name, phone, email, city = _extract_common_fields(form_answers)
            candidates.append(
                NormalizedLead(
                    external_lead_id=str(item.get("id") or item.get("lead_id") or "") or None,
                    full_name=name,
                    phone=phone,
                    email=email,
                    city=city,
                    form_answers=form_answers,
                    raw_payload=item,
                    consented=True,
                )
            )

    return candidates


def normalize_linkedin_payload(payload: dict[str, Any], client: Client) -> list[NormalizedLead]:
    candidates: list[NormalizedLead] = []

    elements = payload.get("elements", []) if isinstance(payload.get("elements"), list) else []
    for element in elements:
        if not isinstance(element, dict):
            continue
        lead_id = str(
            element.get("leadId")
            or element.get("leadGenFormResponse")
            or element.get("id")
            or ""
        ).strip()

        lead_data = element.get("lead")
        if not isinstance(lead_data, dict):
            lead_data = element.get("formResponse") if isinstance(element.get("formResponse"), dict) else {}

        if not lead_data and lead_id:
            # Plug LinkedIn lead fetch here when notification contains ID only.
            lead_data = fetch_linkedin_lead_details(lead_id, client)

        answers = lead_data.get("form_answers", lead_data)
        if not isinstance(answers, dict):
            answers = {}

        name, phone, email, city = _extract_common_fields(answers)
        candidates.append(
            NormalizedLead(
                external_lead_id=lead_id or None,
                full_name=name,
                phone=phone,
                email=email,
                city=city,
                form_answers=answers,
                raw_payload=element,
                consented=True,
            )
        )

    if isinstance(payload.get("lead"), dict):
        lead_data = payload["lead"]
        answers = lead_data.get("form_answers", lead_data)
        if not isinstance(answers, dict):
            answers = {}
        name, phone, email, city = _extract_common_fields(answers)
        candidates.append(
            NormalizedLead(
                external_lead_id=str(lead_data.get("id") or payload.get("lead_id") or "") or None,
                full_name=name,
                phone=phone,
                email=email,
                city=city,
                form_answers=answers,
                raw_payload=lead_data,
                consented=True,
            )
        )

    return candidates


def normalize_webhook_payload(source: str, payload: dict[str, Any], client: Client) -> list[NormalizedLead]:
    if source == LeadSource.META.value:
        return normalize_meta_payload(payload, client)
    if source == LeadSource.LINKEDIN.value:
        return normalize_linkedin_payload(payload, client)
    return []


def _source_enum(source: str) -> LeadSource:
    for item in LeadSource:
        if item.value == source:
            return item
    return LeadSource.MANUAL


def upsert_lead(
    db: Session,
    client: Client,
    source: str,
    normalized: NormalizedLead,
) -> tuple[Lead, bool, bool]:
    lead: Lead | None = None
    created = False

    if normalized.external_lead_id:
        lead = db.scalar(
            select(Lead).where(
                Lead.client_id == client.id,
                Lead.external_lead_id == normalized.external_lead_id,
            )
        )

    if lead is None and normalized.phone:
        lead = db.scalar(
            select(Lead)
            .where(Lead.client_id == client.id, Lead.phone == normalized.phone)
            .order_by(Lead.created_at.desc())
            .limit(1)
        )

    if lead is None:
        lead = Lead(
            client_id=client.id,
            external_lead_id=normalized.external_lead_id,
            source=_source_enum(source),
            full_name=normalized.full_name,
            phone=normalized.phone,
            email=normalized.email,
            city=normalized.city,
            form_answers=normalized.form_answers,
            raw_payload=normalized.raw_payload,
            consented=normalized.consented,
        )
        db.add(lead)
        created = True
    else:
        if normalized.external_lead_id and not lead.external_lead_id:
            lead.external_lead_id = normalized.external_lead_id
        if normalized.full_name and not lead.full_name:
            lead.full_name = normalized.full_name
        if normalized.phone and not lead.phone:
            lead.phone = normalized.phone
        if normalized.email and not lead.email:
            lead.email = normalized.email
        if normalized.city and not lead.city:
            lead.city = normalized.city

        lead.form_answers = {**(lead.form_answers or {}), **(normalized.form_answers or {})}
        lead.raw_payload = normalized.raw_payload or lead.raw_payload

    db.flush()

    should_send_initial_sms = bool(
        lead.phone
        and lead.consented
        and not lead.opted_out
        and lead.initial_sms_sent_at is None
    )
    return lead, created, should_send_initial_sms
