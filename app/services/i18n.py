from __future__ import annotations

import re
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from app.db.models import Client, Lead

DEFAULT_LANGUAGE = "en"
SUPPORTED_LANGUAGES = {"en", "fr"}
LANGUAGE_LOCALES = {
    "en": "en-US",
    "fr": "fr-CA",
}

_FRENCH_HINT_RE = re.compile(
    r"\b(bonjour|salut|merci|rappel|appel|soumission|pi[eè]ce|d[ée]lai|"
    r"urgent|besoin|plan|fichier|entreprise|disponible|demain|aujourd'hui)\b|[àâçéèêëîïôûùüÿñæœ]",
    re.IGNORECASE,
)

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


def normalize_language(value: Any) -> str:
    text = str(value or "").strip().lower().replace("_", "-")
    if text.startswith("fr"):
        return "fr"
    if text.startswith("en"):
        return "en"
    return DEFAULT_LANGUAGE


def detect_language(text: str | None, *, fallback: str = DEFAULT_LANGUAGE) -> str:
    if _FRENCH_HINT_RE.search(text or ""):
        return "fr"
    return normalize_language(fallback)


def client_language(client: Client, *, lead: Lead | None = None, inbound_text: str | None = None) -> str:
    provider_config = client.provider_config if isinstance(client.provider_config, dict) else {}
    workspace_language = normalize_language(provider_config.get("language"))
    if workspace_language != DEFAULT_LANGUAGE:
        return workspace_language

    lead_payload = lead.raw_payload if lead and isinstance(lead.raw_payload, dict) else {}
    for key in ("language", "locale", "lead_language"):
        if lead_payload.get(key):
            return normalize_language(lead_payload.get(key))

    return detect_language(inbound_text, fallback=workspace_language)


def language_locale(language: str) -> str:
    return LANGUAGE_LOCALES.get(normalize_language(language), LANGUAGE_LOCALES[DEFAULT_LANGUAGE])


def language_instruction(language: str) -> str:
    if normalize_language(language) == "fr":
        return (
            "response_language is fr. Write in natural Quebec-friendly French, using polite but simple wording. "
            "Avoid literal English translations and avoid France-only phrasing. Keep SMS replies short."
        )
    return "response_language is en. Write in natural, concise English."


def format_datetime_for_language(value: datetime, *, timezone_name: str, language: str) -> str:
    try:
        tz = ZoneInfo(timezone_name or "UTC")
    except Exception:
        tz = ZoneInfo("UTC")
    local_dt = value.astimezone(tz)
    if normalize_language(language) == "fr":
        weekday = _FR_WEEKDAYS[local_dt.weekday()]
        month = _FR_MONTHS[local_dt.month - 1]
        minute = f"{local_dt.minute:02d}"
        return f"{weekday} {local_dt.day} {month} à {local_dt.hour} h {minute}"
    return local_dt.strftime("%a %b %d at %I:%M %p").replace(" 0", " ")
