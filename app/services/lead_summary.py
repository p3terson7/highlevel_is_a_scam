from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

_KEY_ALIASES = {
    "name": "full_name",
    "contact_name": "full_name",
    "first_name": "full_name",
    "last_name": "last_name",
    "phone": "phone_number",
    "mobile": "phone_number",
    "mobile_phone": "phone_number",
    "cell": "phone_number",
    "email_address": "email",
    "business_type": "business_type",
    "industry": "business_type",
    "service_type": "business_type",
    "running_ads": "running_ads",
    "running_ads_": "running_ads",
    "ads_status": "running_ads",
    "when_to_start": "when_to_start",
    "timeline": "when_to_start",
    "start_timeline": "when_to_start",
    "biggest_marketing_challenge": "biggest_marketing_challenge",
    "main_challenge": "biggest_marketing_challenge",
    "challenge": "biggest_marketing_challenge",
}

_SUMMARY_LABELS = {
    "business_type": "Business type",
    "biggest_marketing_challenge": "Main challenge",
    "running_ads": "Running ads",
    "when_to_start": "Timeline",
    "city": "City",
    "email": "Email",
    "phone_number": "Phone",
}

_PRIORITY_KEYS = (
    "business_type",
    "biggest_marketing_challenge",
    "running_ads",
    "when_to_start",
    "city",
    "email",
    "phone_number",
)


def _canonical_key(raw_key: Any) -> str:
    base = re.sub(r"[^a-z0-9]+", "_", str(raw_key or "").strip().lower()).strip("_")
    if not base:
        return ""
    return _KEY_ALIASES.get(base, base)


def _normalize_scalar(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, str):
        text = " ".join(value.replace("\n", " ").split()).strip().strip('"')
        return text
    return value


def _normalize_value(value: Any) -> Any:
    if isinstance(value, list):
        normalized_list = []
        for item in value:
            normalized_item = _normalize_scalar(item)
            if normalized_item not in ("", None):
                normalized_list.append(normalized_item)
        if not normalized_list:
            return ""
        return normalized_list[0] if len(normalized_list) == 1 else normalized_list
    return _normalize_scalar(value)


def normalize_form_answers(raw_answers: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(raw_answers, Mapping):
        return {}

    output: dict[str, Any] = {}
    for raw_key, raw_value in raw_answers.items():
        key = _canonical_key(raw_key)
        if not key:
            continue
        value = _normalize_value(raw_value)
        if value in ("", None):
            continue
        output[key] = value
    return output


def format_answer_value(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return str(value)


def build_lead_summary_lines(raw_answers: Mapping[str, Any] | None, limit: int = 4) -> list[dict[str, str]]:
    answers = normalize_form_answers(raw_answers)
    if not answers:
        return []

    rows: list[dict[str, str]] = []
    seen: set[str] = set()

    for key in _PRIORITY_KEYS:
        if key not in answers:
            continue
        rows.append(
            {
                "key": key,
                "label": _SUMMARY_LABELS.get(key, key.replace("_", " ").title()),
                "value": format_answer_value(answers[key]),
            }
        )
        seen.add(key)
        if len(rows) >= limit:
            return rows

    for key, value in answers.items():
        if key in seen:
            continue
        rows.append(
            {
                "key": key,
                "label": _SUMMARY_LABELS.get(key, key.replace("_", " ").title()),
                "value": format_answer_value(value),
            }
        )
        if len(rows) >= limit:
            break
    return rows


def build_lead_summary_text(raw_answers: Mapping[str, Any] | None, limit: int = 4) -> str:
    lines = build_lead_summary_lines(raw_answers, limit=limit)
    if not lines:
        return "No qualification details captured yet."
    return " | ".join(f"{line['label']}: {line['value']}" for line in lines)
