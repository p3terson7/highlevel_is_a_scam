from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import Client, Lead, LeadSource
from app.services.lead_summary import filter_question_form_answers, normalize_form_answers

_IDENTITY_CONTEXT_KEYS = (
    "id",
    "lead_id",
    "external_lead_id",
    "leadgen_id",
    "lead_gen_id",
    "linkedin_lead_id",
    "lead_gen_form_response_id",
    "full_name",
    "name",
    "contact_name",
    "first_name",
    "last_name",
    "phone",
    "phone_number",
    "mobile_phone",
    "cell",
    "mobile",
    "email",
    "email_address",
    "city",
    "location_city",
    "location",
)

_EXTERNAL_ID_KEYS = (
    "id",
    "lead_id",
    "external_lead_id",
    "leadgen_id",
    "lead_gen_id",
    "linkedin_lead_id",
    "lead_gen_form_response_id",
    "lead_gen_form_response",
)

_CONSENT_FIELD_KEYS = (
    "sms_consent",
    "consent_sms",
    "consent_to_sms",
    "sms_opt_in",
    "contact_by_sms",
)
_CONSENT_TRUE_VALUES = {"1", "true", "yes", "on", "accepted", "checked", "opted_in"}
_CONSENT_FALSE_VALUES = {"0", "false", "no", "off", "declined", "unchecked", "not_granted"}
MAX_WEBHOOK_LEADS_PER_REQUEST = 10
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@dataclass
class NormalizedLead:
    external_lead_id: str | None
    full_name: str
    phone: str
    email: str
    city: str
    form_answers: dict[str, Any]
    raw_payload: dict[str, Any]
    consented: bool = False


def _consent_value(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    normalized = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if normalized in _CONSENT_TRUE_VALUES:
        return True
    if normalized in _CONSENT_FALSE_VALUES:
        return False
    return None


def _consent_evidence(*sources: Any) -> tuple[bool, dict[str, Any]]:
    signals: list[tuple[str, bool]] = []
    metadata: dict[str, str] = {}

    for source_index, source in enumerate(sources):
        if not isinstance(source, dict):
            continue

        nested = source.get("consent")
        if isinstance(nested, dict):
            for key in ("sms", *_CONSENT_FIELD_KEYS):
                if key not in nested:
                    continue
                signal = _consent_value(nested.get(key))
                if signal is not None:
                    signals.append((f"source_{source_index}.consent.{key}", signal))
            for key in ("captured_at", "text", "method", "form"):
                value = str(nested.get(key) or "").strip()
                if value and key not in metadata:
                    metadata[key] = value[:512]

        for key in _CONSENT_FIELD_KEYS:
            if key not in source:
                continue
            signal = _consent_value(source.get(key))
            if signal is not None:
                signals.append((f"source_{source_index}.{key}", signal))

    if not signals:
        return False, {"granted": False, "status": "not_provided", "source_fields": []}

    granted = all(signal for _, signal in signals)
    evidence: dict[str, Any] = {
        "granted": granted,
        "status": "granted" if granted else "declined_or_conflicting",
        "source_fields": [key for key, _ in signals][:12],
    }
    evidence.update(metadata)
    return granted, evidence


def _raw_payload_with_consent(payload: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
    return {**payload, "consent_evidence": evidence}


def _question_form_answers(raw_answers: dict[str, Any]) -> dict[str, Any]:
    answers = filter_question_form_answers(raw_answers)
    for key in _CONSENT_FIELD_KEYS:
        answers.pop(key, None)
    answers.pop("consent", None)
    return answers


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
    lowered: dict[str, Any] = {}
    for key, value in answers.items():
        raw_key = str(key).strip().lower()
        if not raw_key:
            continue
        lowered.setdefault(raw_key, value)
        canonical = re.sub(r"[^a-z0-9]+", "_", raw_key).strip("_")
        if canonical:
            lowered.setdefault(canonical, value)
    for key in keys:
        if key in lowered and lowered[key]:
            return str(lowered[key]).strip()
    return ""


def _identity_context(form_answers: dict[str, Any], *payloads: Any) -> dict[str, Any]:
    context = dict(form_answers)
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        for key in _IDENTITY_CONTEXT_KEYS:
            value = payload.get(key)
            if value not in (None, ""):
                context.setdefault(key, value)
    return context


def _extract_external_id(*payloads: Any) -> str | None:
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        for key in _EXTERNAL_ID_KEYS:
            value = payload.get(key)
            if value not in (None, ""):
                return str(value).strip() or None
    return None


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
    last_name = _field_lookup(form_answers, ["last_name"])
    if full_name and last_name and last_name.lower() not in full_name.lower():
        full_name = f"{full_name} {last_name}".strip()
    elif not full_name and last_name:
        full_name = last_name
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


def normalize_meta_payload(
    payload: dict[str, Any],
) -> list[NormalizedLead]:
    candidates: list[NormalizedLead] = []

    if isinstance(payload.get("lead"), dict):
        lead_payload = payload["lead"]
        form_answers = lead_payload.get("form_answers", lead_payload)
        if not isinstance(form_answers, dict):
            form_answers = {}
        form_answers = normalize_form_answers(form_answers)
        identity = _identity_context(form_answers, lead_payload, payload)
        name, phone, email, city = _extract_common_fields(identity)
        consented, consent_evidence = _consent_evidence(lead_payload, payload, form_answers)
        candidates.append(
            NormalizedLead(
                external_lead_id=_extract_external_id(lead_payload, payload, form_answers),
                full_name=name,
                phone=phone,
                email=email,
                city=city,
                form_answers=_question_form_answers(form_answers),
                raw_payload=_raw_payload_with_consent(lead_payload, consent_evidence),
                consented=consented,
            )
        )

    for entry in payload.get("entry", []) if isinstance(payload.get("entry"), list) else []:
        for change in entry.get("changes", []) if isinstance(entry.get("changes"), list) else []:
            value = change.get("value") if isinstance(change, dict) else {}
            if not isinstance(value, dict):
                continue

            leadgen_id = str(value.get("leadgen_id") or value.get("lead_id") or "").strip()
            form_answers = _meta_field_data_to_dict(value.get("field_data"))

            form_answers = normalize_form_answers(form_answers)

            identity = _identity_context(form_answers, value)
            name, phone, email, city = _extract_common_fields(identity)
            consented, consent_evidence = _consent_evidence(value, payload, form_answers)
            candidates.append(
                NormalizedLead(
                    external_lead_id=leadgen_id or None,
                    full_name=name,
                    phone=phone,
                    email=email,
                    city=city,
                    form_answers=_question_form_answers(form_answers),
                    raw_payload=_raw_payload_with_consent(value, consent_evidence),
                    consented=consented,
                )
            )

    if isinstance(payload.get("leads"), list):
        for item in payload["leads"]:
            if not isinstance(item, dict):
                continue
            form_answers = item.get("form_answers", item)
            if not isinstance(form_answers, dict):
                form_answers = {}
            form_answers = normalize_form_answers(form_answers)
            identity = _identity_context(form_answers, item)
            name, phone, email, city = _extract_common_fields(identity)
            consented, consent_evidence = _consent_evidence(item, payload, form_answers)
            candidates.append(
                NormalizedLead(
                    external_lead_id=_extract_external_id(item, form_answers),
                    full_name=name,
                    phone=phone,
                    email=email,
                    city=city,
                    form_answers=_question_form_answers(form_answers),
                    raw_payload=_raw_payload_with_consent(item, consent_evidence),
                    consented=consented,
                )
            )

    return candidates


def normalize_linkedin_payload(payload: dict[str, Any]) -> list[NormalizedLead]:
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

        answers = lead_data.get("form_answers", lead_data)
        if not isinstance(answers, dict):
            answers = {}
        answers = normalize_form_answers(answers)

        identity = _identity_context(answers, lead_data, element)
        name, phone, email, city = _extract_common_fields(identity)
        consented, consent_evidence = _consent_evidence(lead_data, element, payload, answers)
        candidates.append(
            NormalizedLead(
                external_lead_id=lead_id or _extract_external_id(lead_data, answers),
                full_name=name,
                phone=phone,
                email=email,
                city=city,
                form_answers=_question_form_answers(answers),
                raw_payload=_raw_payload_with_consent(element, consent_evidence),
                consented=consented,
            )
        )

    if isinstance(payload.get("lead"), dict):
        lead_data = payload["lead"]
        answers = lead_data.get("form_answers", lead_data)
        if not isinstance(answers, dict):
            answers = {}
        answers = normalize_form_answers(answers)
        identity = _identity_context(answers, lead_data, payload)
        name, phone, email, city = _extract_common_fields(identity)
        consented, consent_evidence = _consent_evidence(lead_data, payload, answers)
        candidates.append(
            NormalizedLead(
                external_lead_id=_extract_external_id(lead_data, payload, answers),
                full_name=name,
                phone=phone,
                email=email,
                city=city,
                form_answers=_question_form_answers(answers),
                raw_payload=_raw_payload_with_consent(lead_data, consent_evidence),
                consented=consented,
            )
        )

    return candidates


def normalize_simple_payload(payload: dict[str, Any]) -> list[NormalizedLead]:
    lead_data = payload.get("lead") if isinstance(payload.get("lead"), dict) else payload
    if not isinstance(lead_data, dict):
        return []
    answers = lead_data.get("form_answers", lead_data)
    if not isinstance(answers, dict):
        answers = {}
    answers = normalize_form_answers(answers)
    identity = _identity_context(answers, lead_data, payload)
    name, phone, email, city = _extract_common_fields(identity)
    consented, consent_evidence = _consent_evidence(lead_data, payload, answers)
    return [
        NormalizedLead(
            external_lead_id=_extract_external_id(lead_data, payload, answers),
            full_name=name,
            phone=phone,
            email=email,
            city=city,
            form_answers=_question_form_answers(answers),
            raw_payload=_raw_payload_with_consent(lead_data, consent_evidence),
            consented=consented,
        )
    ]


def normalize_webhook_payload(
    source: str,
    payload: dict[str, Any],
) -> list[NormalizedLead]:
    if source == LeadSource.META.value:
        return normalize_meta_payload(payload)
    if source == LeadSource.LINKEDIN.value:
        return normalize_linkedin_payload(payload)
    if source == LeadSource.MANUAL.value:
        return normalize_simple_payload(payload)
    return []


def validate_webhook_candidates(candidates: list[NormalizedLead]) -> dict[str, int]:
    if not candidates:
        raise ValueError("Payload does not contain a lead")
    if len(candidates) > MAX_WEBHOOK_LEADS_PER_REQUEST:
        raise ValueError(f"Payload exceeds the {MAX_WEBHOOK_LEADS_PER_REQUEST}-lead batch limit")

    limits = {
        "external_lead_id": 255,
        "full_name": 255,
        "phone": 32,
        "email": 255,
        "city": 128,
    }
    for index, candidate in enumerate(candidates):
        for field, limit in limits.items():
            value = getattr(candidate, field)
            if value is not None and len(str(value)) > limit:
                raise ValueError(f"Lead {index + 1} field '{field}' exceeds {limit} characters")

        phone_digits = re.sub(r"\D", "", candidate.phone)
        phone_valid = not candidate.phone or 8 <= len(phone_digits) <= 15
        email_valid = not candidate.email or bool(_EMAIL_RE.fullmatch(candidate.email))
        if not phone_valid:
            raise ValueError(f"Lead {index + 1} has an invalid phone number")
        if not email_valid:
            raise ValueError(f"Lead {index + 1} has an invalid email address")
        if not candidate.phone and not candidate.email:
            raise ValueError(f"Lead {index + 1} must include a phone number or email address")

    return {
        "candidate_count": len(candidates),
        "consented_count": sum(1 for candidate in candidates if candidate.consented),
    }


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

    if lead is None and normalized.email:
        lead = db.scalar(
            select(Lead)
            .where(
                Lead.client_id == client.id,
                Lead.email == normalized.email,
            )
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
        existing_raw_payload = dict(lead.raw_payload or {})
        incoming_raw_payload = dict(normalized.raw_payload or {})
        incoming_consent = incoming_raw_payload.get("consent_evidence")
        incoming_consent_status = (
            str(incoming_consent.get("status") or "")
            if isinstance(incoming_consent, dict)
            else ""
        )
        if lead.consented and incoming_consent_status == "not_provided":
            # A source that simply omits the field must not erase previously
            # captured permission. An explicit decline/conflict below does.
            incoming_raw_payload.pop("consent_evidence", None)
        lead.raw_payload = {**existing_raw_payload, **incoming_raw_payload}
        if incoming_consent_status != "not_provided":
            lead.consented = bool(normalized.consented)

    db.flush()

    should_send_initial_sms = bool(
        lead.phone
        and lead.consented
        and not lead.opted_out
        and lead.initial_sms_sent_at is None
    )
    return lead, created, should_send_initial_sms
