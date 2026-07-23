from __future__ import annotations

import re
import unicodedata
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from app.db.models import Client, ConversationStateEnum, Lead, Message, MessageDirection
from app.services.agent_v3_types import *
from app.services.i18n import normalize_language
from app.services.lead_summary import build_lead_summary_text

# Match the existing outbound-message API ceiling. A 320-character hard cut
# removed structured slot choices and their selection instructions before the
# message reached persistence or the UI.
_MAX_AGENT_REPLY_CHARS = 1_600


def _serialize_message(message: Message) -> dict[str, Any]:
    raw_payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
    agent_payload = raw_payload.get("agent") if isinstance(raw_payload.get("agent"), dict) else {}
    booking_offer = raw_payload.get("booking_offer") if isinstance(raw_payload.get("booking_offer"), dict) else None
    calendar_booking = raw_payload.get("calendar_booking") if isinstance(raw_payload.get("calendar_booking"), dict) else None
    return {
        "direction": message.direction.value if isinstance(message.direction, MessageDirection) else str(message.direction),
        "body": " ".join(str(message.body or "").split()),
        "question_key": agent_payload.get("next_question_key"),
        "agent_action": agent_payload.get("action"),
        "booking_offer": booking_offer,
        "calendar_booking": calendar_booking,
    }


def _agent_identity_context(client: Client) -> dict[str, Any]:
    return {
        "name": _ASSISTANT_NAME,
        "role": "AI assistant",
        "business_name": client.business_name,
        "is_human": False,
        "is_founder_or_owner": False,
        "job": "Answer lead questions, collect useful context, and help book a convenient meeting when appropriate.",
    }


def _is_identity_question(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    if normalized in {"founder", "owner", "human", "person", "bot", "assistant"}:
        return True
    if "?" in str(text or "") and re.search(r"\b(founder|owner|human|person|bot|assistant)\b", normalized):
        if not re.search(r"\bi (?:am|m) (?:the )?(?:owner|founder)\b", normalized):
            return True
    return bool(_IDENTITY_QUESTION_PATTERN.search(normalized))


def _identity_agent_response(context: dict[str, Any]) -> AgentResponse:
    current_state = str(context.get("current_state") or "").upper()
    next_state = ConversationStateEnum.BOOKED if current_state == ConversationStateEnum.BOOKED.value else ConversationStateEnum.QUALIFYING
    reply_text = _identity_reply(context)
    offer_due = _qualified_scoping_call_offer_due(context)
    if offer_due:
        reply_text = _answer_then_explicit_expert_meeting_offer(reply_text, context)
    return _finalize_response_with_context(
        AgentResponse(
            reply_text=reply_text,
            next_state=next_state,
            collected_fields=QualificationMemory.model_validate(context.get("qualification_memory") or {}),
            next_question_key=None,
            action="offer_booking" if offer_due else "none",
            conversation_act="answer_then_soft_cta" if offer_due else "answer_question",
            tool_call=ToolCall(),
            runtime_payload={
                "flow_state": "IDENTITY_ANSWERED",
                **({"scoping_call_offer_forced": True} if offer_due else {}),
            },
        ),
        context,
    )


def _identity_reply(context: dict[str, Any]) -> str:
    identity = context.get("agent_identity") if isinstance(context.get("agent_identity"), dict) else {}
    assistant_name = str(identity.get("name") or _ASSISTANT_NAME).strip()
    business_name = str(identity.get("business_name") or context.get("business_name") or "the business").strip()
    language = str(context.get("response_language") or "en")
    inbound = str(context.get("latest_inbound_message") or "")
    asks_if_assistant_owns = bool(re.search(r"\b(are you|you(?:'re| are))\b.*\b(founder|owner|co-?founder)\b", inbound, re.IGNORECASE))
    asks_business_identity = bool(re.search(r"\b(who|founder|owner|behind|runs?|owns?|started|founded)\b", inbound, re.IGNORECASE))

    if language == "fr":
        prefix = (
            f"Non - ici {assistant_name}, l'assistante de {business_name}."
            if asks_if_assistant_owns
            else f"Ici {assistant_name}, l'assistante de {business_name}."
        )
        source_fact = _extract_business_identity_fact(context) if asks_business_identity else ""
        if source_fact:
            return f"{prefix} Selon les infos de l'entreprise que j'ai: {source_fact} Je peux répondre aux questions ici."
        if asks_business_identity:
            return f"{prefix} Je n'ai pas de détails confirmés sur le fondateur ou propriétaire dans mon contexte, mais je peux répondre aux questions ici."
        return f"{prefix} Je peux répondre aux questions ici."

    prefix = f"No - I'm {assistant_name}, the assistant for {business_name}." if asks_if_assistant_owns else f"I'm {assistant_name}, the assistant for {business_name}."
    source_fact = _extract_business_identity_fact(context) if asks_business_identity else ""
    if source_fact:
        return f"{prefix} From the business info I have: {source_fact} I can answer questions here."
    if asks_business_identity:
        return f"{prefix} I do not have confirmed founder or owner details in my context, but I can answer questions here."
    return f"{prefix} I can answer questions here."


def _extract_business_identity_fact(context: dict[str, Any]) -> str:
    source_text = "\n".join(
        str(context.get(key) or "")
        for key in ("knowledge_context", "business_profile_context", "faq_context", "ai_context")
        if str(context.get(key) or "").strip()
    )
    if not source_text:
        return ""
    lines = [line.strip() for line in source_text.splitlines() if line.strip() and not line.lower().startswith("source:")]
    candidate_text = " ".join(lines)
    sentences = re.split(r"(?<=[.!?])\s+", candidate_text)
    identity_terms = ("founder", "co-founder", "cofounder", "founded by", "owned by", "run by", "runs", "led by", "started by", "behind")
    assistant_norm = _normalize_text(_ASSISTANT_NAME)
    for sentence in sentences:
        clean = " ".join(sentence.split()).strip(" -")
        normalized = _normalize_text(clean)
        if not clean or assistant_norm in normalized:
            continue
        if any(term in normalized for term in identity_terms):
            return _trim_sms_text(clean)
    return ""


def _extract_answered_missing_field_keys(history: Sequence[Message]) -> list[str]:
    answered: list[str] = []
    pending_key: str | None = None
    for message in history:
        body = str(message.body or "")
        if message.direction == MessageDirection.OUTBOUND:
            inferred = _infer_missing_field_key_from_text(body)
            if inferred:
                pending_key = inferred
            continue
        if message.direction == MessageDirection.INBOUND and pending_key:
            if _is_substantive_missing_field_answer(body, pending_key):
                if pending_key not in answered:
                    answered.append(pending_key)
                pending_key = None
    return answered


def _infer_missing_field_key_from_text(text: str) -> str | None:
    normalized = _normalize_text(text)
    if not normalized:
        return None
    for spec in _GENERIC_MISSING_FIELD_SPECS:
        question = _normalize_text(str(spec.get("question") or ""))
        if question and question in normalized:
            return str(spec["key"])
        localized_question = _normalize_text(_GENERIC_MISSING_QUESTION_FR.get(str(spec["key"]), ""))
        if localized_question and localized_question in normalized:
            return str(spec["key"])
        for token in (*tuple(spec.get("question_tokens") or ()), *tuple(spec.get("question_tokens_fr") or ())):
            if _normalize_text(str(token)) in normalized:
                return str(spec["key"])
    return None


def _is_substantive_missing_field_answer(text: str, field_key: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    short_ack = {
        "yes",
        "yeah",
        "yep",
        "ok",
        "okay",
        "sure",
        "no",
        "nope",
        "thanks",
        "thank you",
        "oui",
        "non",
        "merci",
        "d accord",
    }
    if normalized in short_ack and field_key not in {"decision_process", "follow_up_contact"}:
        return False
    return len(normalized) >= 2


def _extract_acknowledged_form_fact_keys(*, known_form_facts: Sequence[dict[str, str]], history: Sequence[Message]) -> list[str]:
    outbound_text = _normalize_text(" ".join(str(message.body or "") for message in history if message.direction == MessageDirection.OUTBOUND))
    if not outbound_text:
        return []
    acknowledged: list[str] = []
    for fact in known_form_facts:
        key = str(fact.get("key") or "")
        if key and _fact_is_mentioned(fact, outbound_text):
            acknowledged.append(key)
    return acknowledged


def _fact_is_mentioned(fact: dict[str, str], normalized_outbound_text: str) -> bool:
    return any(term in normalized_outbound_text for term in _fact_terms(fact))


def _fact_terms(fact: dict[str, str]) -> list[str]:
    key = _normalize_text(str(fact.get("key") or ""))
    label = _normalize_text(str(fact.get("label") or ""))
    value = _normalize_text(str(fact.get("value") or ""))
    terms: list[str] = []
    if len(value) >= 3:
        terms.append(value[:160])
        terms.extend(_salient_value_terms(value))
    if any(token in key or token in label for token in ("decision", "role", "owner", "approver")):
        terms.extend(["decision maker", "decision-maker", "owner"])
    if any(token in key or token in label for token in ("city", "location", "region", "market")) and len(value) >= 3:
        terms.append(value)
    if any(token in key or token in label for token in ("timeline", "deadline", "start", "date")) and len(value) >= 3:
        terms.append(value)
    return list(dict.fromkeys(term for term in terms if term))


_FACT_TERM_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "avec",
    "de",
    "des",
    "du",
    "en",
    "est",
    "et",
    "for",
    "la",
    "le",
    "les",
    "of",
    "ou",
    "the",
    "to",
    "un",
    "une",
    "with",
}


def _salient_value_terms(value: str) -> list[str]:
    """Return compact phrases that survive natural paraphrasing of form answers."""

    normalized = _normalize_text(value)
    if not normalized:
        return []
    terms: list[str] = []
    for chunk in re.split(r"[,;|/]+|\s+-\s+", normalized):
        chunk = chunk.strip(" .:-")
        if len(chunk) >= 7:
            terms.append(chunk[:160])
        words = [word for word in re.findall(r"[a-z0-9]+", chunk) if word not in _FACT_TERM_STOPWORDS]
        for width in (3, 2):
            for index in range(max(0, len(words) - width + 1)):
                phrase = " ".join(words[index : index + width])
                if len(phrase) >= 7:
                    terms.append(phrase)
    return list(dict.fromkeys(terms))[:16]


def _slot_offer_tool_result(*, offer: Any, availability_query: dict[str, Any]) -> dict[str, Any]:
    pending_step = "slot_selection_pending" if offer.slots else None
    return {
        "kind": "slots" if offer.slots else "no_slots",
        "slots": [slot.__dict__ for slot in offer.slots],
        "reply_hint": offer.reply_text,
        "availability_query": availability_query,
        "runtime_payload": {
            "booking_offer": offer.raw_payload.get("booking_offer", {}),
            "pending_step": pending_step,
        },
        "fallback_reply": offer.reply_text,
    }


def _ensure_slot_fallback_line(text: str, *, language: str = "en") -> str:
    language = normalize_language(language)
    clean = " ".join(str(text or "").split()).strip()
    fallback = (
        "Si aucune option ne fonctionne, envoyez-moi simplement un moment qui vous convient mieux."
        if language == "fr"
        else "If none of those work, just send me a time that's better for you."
    )
    if not clean:
        return fallback
    if language == "fr":
        clean = re.sub(
            r"\s*If none of those work,[^.?!]*(?:[.?!]|$)",
            "",
            clean,
            flags=re.IGNORECASE,
        ).strip()
    normalized = _normalize_text(clean)
    if any(
        phrase in normalized
        for phrase in (
            "if none of those work",
            "if that time doesn t work",
            "si aucune option ne fonctionne",
            "si ce creneau ne fonctionne pas",
        )
    ):
        return clean
    return f"{clean} {fallback}"


_FR_MONTH_ABBR = {
    "jan": "janvier",
    "feb": "février",
    "mar": "mars",
    "apr": "avril",
    "may": "mai",
    "jun": "juin",
    "jul": "juillet",
    "aug": "août",
    "sep": "septembre",
    "oct": "octobre",
    "nov": "novembre",
    "dec": "décembre",
}
_FR_WEEKDAY_ABBR = {
    "mon": "lundi",
    "tue": "mardi",
    "wed": "mercredi",
    "thu": "jeudi",
    "fri": "vendredi",
    "sat": "samedi",
    "sun": "dimanche",
}
_QUESTION_TEXT_FR = {
    "decision_makers": "Êtes-vous la personne qui prend la décision, et est-ce que quelqu'un d'autre devrait participer à l'appel?",
    "urgency_driver": "Y a-t-il une échéance ou une date importante derrière cette demande?",
}
_GENERIC_MISSING_QUESTION_FR = {
    "desired_outcome": "À quoi ressemblerait un bon résultat pour vous?",
    "request_type": "Quel type d'aide recherchez-vous?",
    "timeline": "Idéalement, quand aimeriez-vous commencer ou régler ça?",
    "decision_process": "Êtes-vous la bonne personne pour coordonner la suite, ou faut-il inclure quelqu'un d'autre?",
    "follow_up_contact": "Quel est le meilleur courriel ou moyen de contact si l'équipe doit envoyer des détails?",
}
_LOCALIZED_AGENT_REPLIES = {
    "pick_slot_first": {
        "en": "I can lock that in once you pick one of the offered times.",
        "fr": "Je peux le réserver dès que vous choisissez un des créneaux proposés.",
    },
    "booked": {
        "en": "Perfect. You're booked.",
        "fr": "Parfait. Votre appel est réservé.",
    },
    "booked_closing": {
        "en": "Perfect. See you then.",
        "fr": "Parfait. À bientôt pour l'appel.",
    },
    "booked_followup": {
        "en": "You're all set. Text me here anytime if something changes before the meeting.",
        "fr": "C'est tout bon. Répondez ici si quelque chose change avant l'appel.",
    },
    "share_times": {
        "en": "I'll check the available times for a meeting with an expert.",
        "fr": "Je vérifie les disponibilités pour un rendez-vous avec un expert.",
    },
    "understood": {
        "en": "Understood.",
        "fr": "Compris.",
    },
    "handoff": {
        "en": "Understood. I'll have someone reach out.",
        "fr": "Compris. Je vais demander à quelqu'un de vous contacter.",
    },
    "availability_handoff": {
        "en": "Your scoping-call request is noted, but I can't access the calendar right now. I'm handing this to the team to coordinate the meeting.",
        "fr": "Votre demande d'appel de cadrage est bien notée, mais je n'arrive pas à consulter le calendrier pour le moment. Je la transmets à l'équipe pour coordonner le rendez-vous.",
    },
}


def _question_text_for_language(key: str | None, language: str) -> str:
    key = str(key or "")
    language = normalize_language(language)
    if language == "fr" and key in _QUESTION_TEXT_FR:
        return _QUESTION_TEXT_FR[key]
    spec = _QUESTION_SPEC_BY_KEY.get(key)
    return spec.question if spec else ""


def _missing_field_question_for_language(field: dict[str, Any], language: str) -> str:
    key = str(field.get("key") or "")
    language = normalize_language(language)
    if language == "fr" and key in _GENERIC_MISSING_QUESTION_FR:
        return _GENERIC_MISSING_QUESTION_FR[key]
    return str(field.get("question") or "")


def _localized_agent_reply(key: str, context_or_language: dict[str, Any] | str | None = None) -> str:
    if isinstance(context_or_language, dict):
        language = str(context_or_language.get("response_language") or "en")
    else:
        language = str(context_or_language or "en")
    language = normalize_language(language)
    options = _LOCALIZED_AGENT_REPLIES.get(key, _LOCALIZED_AGENT_REPLIES["understood"])
    return options.get(language) or options["en"]


def _should_check_fresh_slots(preferences: dict[str, str]) -> bool:
    return any(
        bool(preferences.get(key))
        for key in ("preferred_day", "preferred_period", "exact_time", "range_start", "range_end", "avoid_day")
    )


def _booking_preferences_with_offer_context(
    preferences: dict[str, str],
    *,
    latest_offer: dict[str, Any] | None,
) -> dict[str, str]:
    enriched = dict(preferences or {})
    if enriched.get("exact_time") and not enriched.get("preferred_day"):
        inferred_day = _single_day_from_offer(latest_offer)
        if inferred_day:
            enriched["preferred_day"] = inferred_day
    return enriched


def _single_day_from_offer(latest_offer: dict[str, Any] | None) -> str | None:
    if not isinstance(latest_offer, dict):
        return None
    slots = latest_offer.get("slots")
    if not isinstance(slots, list) or not slots:
        return None
    days: set[str] = set()
    for slot in slots:
        if not isinstance(slot, dict):
            continue
        haystack = _normalize_text(
            " ".join(
                [
                    str(slot.get("display_time", "")),
                    str(slot.get("display_hint", "")),
                    str(slot.get("search_blob", "")),
                ]
            )
        )
        matched_day = next((day for day in _DAY_NAMES if day in haystack), None)
        if matched_day:
            days.add(matched_day)
    if len(days) == 1:
        return next(iter(days))
    return None


def _latest_booking_offer(history: Sequence[Message]) -> dict[str, Any] | None:
    for message in reversed(history):
        raw_payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
        offer = raw_payload.get("booking_offer")
        if isinstance(offer, dict) and isinstance(offer.get("slots"), list) and offer.get("slots"):
            return offer
    return None


def _build_known_form_facts(answers: dict[str, Any], *, lead: Lead) -> list[dict[str, str]]:
    facts: list[dict[str, str]] = []
    if lead.full_name:
        facts.append({"key": "lead_name", "label": "Lead name", "value": lead.full_name})
    if lead.city:
        facts.append({"key": "lead_city", "label": "Lead city", "value": lead.city})
    for key, value in answers.items():
        text = _stringify_answer(value)
        if not text:
            continue
        facts.append({"key": key, "label": _humanize_key(key), "value": text})
    return facts[:14]


def _important_missing_fields(
    *,
    answers: dict[str, Any],
    memory: QualificationMemory,
    lead: Lead,
    answered_field_keys: Sequence[str] = (),
) -> list[dict[str, str]]:
    missing: list[dict[str, str]] = []
    answered = {str(key) for key in answered_field_keys}
    for spec in _GENERIC_MISSING_FIELD_SPECS:
        key = str(spec["key"])
        if key in answered:
            continue
        if key == "request_type" and memory.service_needed:
            continue
        if key == "timeline" and memory.timeline:
            continue
        if key == "decision_process" and memory.decision_makers:
            continue
        if key == "follow_up_contact" and lead.email:
            continue
        if _answers_have_signal(answers, key_tokens=tuple(spec.get("key_tokens") or ())):
            continue
        missing.append({field: str(spec[field]) for field in ("key", "label", "question", "why")})
    return missing[:5]


def _classify_lead_intent(
    *,
    answers: dict[str, Any],
    memory: QualificationMemory,
    inbound_text: str,
    history: Sequence[Message],
    explicit_booking_intent: bool,
    inbound_preferences: dict[str, str],
) -> dict[str, Any]:
    text = str(inbound_text or "")
    answer_blob = _answers_blob(answers)
    score = 0
    reasons: list[str] = []

    low_signal = bool(_LOW_INTENT_PATTERN.search(text))
    pricing_question = bool(_PRICING_PATTERN.search(text))
    buying_signal = bool(_BUYING_SIGNAL_PATTERN.search(text))
    scheduling_intent = _has_scheduling_intent(text)

    if memory.service_needed:
        score += 2
        reasons.append("clear_service_or_request_need")
    if _answers_have_signal(answers, key_tokens=("service", "offering", "product", "scope", "request", "need", "goal", "problem", "challenge", "interest")):
        score += 2
        reasons.append("specific_form_scope")
    elif answers:
        score += 1
        reasons.append("some_form_context")

    if memory.timeline:
        score += 1
        reasons.append("timeline_provided")
        if _timeline_is_urgent(memory.timeline) or _timeline_is_urgent(answer_blob):
            score += 2
            reasons.append("urgent_timeline")

    if memory.decision_makers:
        score += 1
        reasons.append("decision_path_known")
        if _DECISION_MAKER_PATTERN.search(memory.decision_makers):
            score += 1
            reasons.append("decision_maker_signal")

    if memory.locations or _answers_have_signal(answers, key_tokens=("location", "address", "city", "market", "region")):
        score += 1
        reasons.append("location_context")

    if _SCOPE_QUANTITY_PATTERN.search(answer_blob) or _answers_have_signal(answers, key_tokens=("size", "quantity", "count", "volume", "scope", "scale", "locations", "units", "users", "seats", "rooms")):
        score += 1
        reasons.append("meaningful_scale_or_scope")

    if pricing_question:
        score += 2
        reasons.append("pricing_question")
    if explicit_booking_intent or (inbound_preferences and scheduling_intent):
        score += 3
        reasons.append("scheduling_intent")
    if buying_signal:
        score += 2
        reasons.append("buying_signal")
    if low_signal:
        score -= 3
        reasons.append("low_intent_language")
    if not answers and not any(memory.known_fields().values()) and _lead_asked_question(text):
        score += 1
        reasons.append("generic_question")

    if low_signal and not explicit_booking_intent and not buying_signal:
        level: IntentLevel = "LOW_INTENT"
    elif score >= 6:
        level = "HIGH_INTENT"
    elif score >= 3:
        level = "MEDIUM_INTENT"
    else:
        level = "LOW_INTENT"

    # If prior conversation already sent slots, keep the lead at least medium unless they refused the call.
    if level == "LOW_INTENT" and not low_signal and any(_message_suggests_meeting(message.body or "") for message in history):
        level = "MEDIUM_INTENT"

    return {"level": level, "score": score, "reasons": reasons[:8]}


def _build_cta_state(
    *,
    history: Sequence[Message],
    raw_payload: dict[str, Any],
    inbound_text: str,
    explicit_booking_intent: bool,
    inbound_preferences: dict[str, str],
    latest_offer: dict[str, Any] | None,
) -> dict[str, Any]:
    previous = raw_payload.get("cta_state") if isinstance(raw_payload.get("cta_state"), dict) else {}
    previous_count = _to_int(previous.get("meeting_suggested_count"), default=0) if previous else 0
    history_count = _count_meeting_suggestions(history)
    meeting_suggested_count = max(previous_count, history_count)
    last_outbound = _latest_outbound_from_history(history)
    recent_meeting_cta = _latest_outbound_invites_meeting(history)
    accepted = bool(
        explicit_booking_intent
        or _extract_slot_choice(inbound_text, latest_offer)
        or (inbound_preferences and _has_scheduling_intent(inbound_text))
    )
    rejected = bool(previous.get("meeting_rejected")) or bool(_CALL_REFUSAL_PATTERN.search(inbound_text or ""))
    ignored = bool(recent_meeting_cta and not accepted and not rejected and str(inbound_text or "").strip())
    renewed_buying_intent = bool(accepted or _BUYING_SIGNAL_PATTERN.search(inbound_text or ""))
    suppress = bool(rejected or (ignored and not renewed_buying_intent) or (meeting_suggested_count >= 2 and not renewed_buying_intent))

    return {
        "meeting_suggested_count": meeting_suggested_count,
        "meeting_accepted": bool(previous.get("meeting_accepted")) or accepted,
        "meeting_rejected": rejected,
        "meeting_ignored": ignored,
        "recent_meeting_cta": recent_meeting_cta,
        "suppress_meeting_cta": suppress,
        "last_cta": previous.get("last_cta") or (_cta_label_for_text(last_outbound.get("body", "")) if last_outbound else None),
        "renewed_buying_intent": renewed_buying_intent,
    }


def _build_internal_lead_summary(
    *,
    lead: Lead,
    normalized_answers: dict[str, Any],
    memory: QualificationMemory,
    intent_profile: dict[str, Any],
    important_missing_fields: list[dict[str, str]],
    cta_state: dict[str, Any],
    meeting_status: str,
) -> dict[str, Any]:
    unanswered = [field["label"] for field in important_missing_fields[:4]]
    intent_level = str(intent_profile.get("level") or "LOW_INTENT")
    if meeting_status == "booked":
        qualification_level = "qualified_booked"
    elif intent_level == "HIGH_INTENT" and len(unanswered) <= 2:
        qualification_level = "qualified"
    elif intent_level == "LOW_INTENT":
        qualification_level = "nurture"
    else:
        qualification_level = "qualifying"

    summary = {
        "lead_name": lead.full_name or "",
        "source_platform": lead.source.value if getattr(lead, "source", None) else "",
        "form_answers_summary": build_lead_summary_text(normalized_answers, limit=8),
        "service_interest": memory.service_needed or _first_answer_by_tokens(normalized_answers, ("service", "offering", "product", "scope", "request", "need", "goal", "problem", "challenge", "interest")),
        "pain_point": _first_answer_by_tokens(normalized_answers, ("pain", "problem", "challenge", "goal", "reason")),
        "desired_outcome": _first_answer_by_tokens(normalized_answers, ("purpose", "goal", "reason", "use_case", "driver", "outcome")),
        "timeline": memory.timeline or _first_answer_by_tokens(normalized_answers, ("timeline", "deadline", "start", "date")),
        "scale_or_scope": _first_answer_by_tokens(normalized_answers, ("size", "quantity", "count", "volume", "scope", "scale", "locations", "units", "users", "seats", "rooms")),
        "location": memory.locations or lead.city or _first_answer_by_tokens(normalized_answers, ("location", "address", "city", "region", "market")),
        "decision_maker_role": memory.decision_makers or _first_answer_by_tokens(normalized_answers, ("decision", "role", "approver", "stakeholder")),
        "intent_level": intent_level,
        "intent_reasons": intent_profile.get("reasons") or [],
        "qualification_level": qualification_level,
        "unanswered_questions": unanswered,
        "meeting_status": meeting_status,
        "meeting_suggested_count": cta_state.get("meeting_suggested_count", 0),
        "recommended_follow_up": _recommended_follow_up(
            intent_level=intent_level,
            qualification_level=qualification_level,
            meeting_status=meeting_status,
            missing_fields=important_missing_fields,
            cta_state=cta_state,
        ),
    }
    return {key: value for key, value in summary.items() if value not in (None, "", [])}


def _meeting_status(*, lead: Lead, cta_state: dict[str, Any], latest_offer: dict[str, Any] | None) -> str:
    if lead.conversation_state == ConversationStateEnum.BOOKED:
        return "booked"
    if cta_state.get("meeting_rejected"):
        return "call_refused"
    if latest_offer:
        return "slots_sent"
    if cta_state.get("meeting_accepted"):
        return "accepted_pending_slots"
    if int(cta_state.get("meeting_suggested_count") or 0) > 0:
        return "suggested"
    return "not_suggested"


def _recommended_follow_up(
    *,
    intent_level: str,
    qualification_level: str,
    meeting_status: str,
    missing_fields: list[dict[str, str]],
    cta_state: dict[str, Any],
) -> str:
    if meeting_status == "booked":
        return "Prepare a concise handoff with form answers, discovered lead details, and unanswered questions."
    if cta_state.get("meeting_rejected"):
        return "Respect the no-call preference and continue helping by SMS/email unless the lead asks to schedule."
    if intent_level == "HIGH_INTENT":
        if missing_fields:
            return f"Confirm {missing_fields[0]['label'].lower()} and then offer the most useful next step."
        return "Offer to coordinate the next step with varied, low-pressure wording."
    if qualification_level == "nurture":
        return "Answer basic questions, clarify what they are trying to understand, and avoid pushing a meeting."
    return "Clarify the need one question at a time before suggesting a meeting."


def _recommended_response_strategy(
    *,
    intent_level: str,
    cta_state: dict[str, Any],
    pricing_question: bool,
    pricing_context_available: bool,
    lead_question_detected: bool,
) -> str:
    if cta_state.get("meeting_rejected"):
        return "Answer helpfully and do not suggest a call unless the lead reverses course."
    if pricing_question:
        if pricing_context_available:
            return "Answer the pricing question only from ai_context, then guide to fit; do not present live times unless the lead explicitly asks to schedule."
        return "Do not discuss pricing or budget. Say confirmed package details are not available here, then help with fit or process. Do not present live times unless the lead explicitly asks to schedule."
    if cta_state.get("suppress_meeting_cta"):
        return "Continue helping or ask one useful missing question; do not repeat the meeting CTA this turn."
    if lead_question_detected:
        return "Answer the question from the most specific available context before any qualification or meeting suggestion."
    if intent_level == "HIGH_INTENT":
        return "Use known form details, ask one important missing question, and only offer booking as a soft next step if natural."
    if intent_level == "MEDIUM_INTENT":
        return "Clarify the need or answer the question before suggesting any meeting."
    return "Educate and keep the lead warm with one simple question."


def _attach_behavior_runtime(response: AgentResponse, context: dict[str, Any]) -> None:
    cta_state = dict(context.get("cta_state") or {})
    reply_suggests_meeting = _message_invites_meeting(response.reply_text)
    if response.tool_call.name == "find_slots" or response.runtime_payload.get("booking_offer"):
        reply_suggests_meeting = True
        cta_label = "live_availability"
    else:
        cta_label = _cta_label_for_text(response.reply_text) if reply_suggests_meeting else cta_state.get("last_cta")

    if reply_suggests_meeting:
        cta_state["meeting_suggested_count"] = int(cta_state.get("meeting_suggested_count") or 0) + 1
        cta_state["last_cta"] = cta_label
        cta_state["recent_meeting_cta"] = True

    if response.next_state == ConversationStateEnum.BOOKED or response.action == "mark_booked" or response.runtime_payload.get("calendar_booking"):
        cta_state["meeting_accepted"] = True
        cta_state["meeting_status"] = "booked"
    elif context.get("call_refusal"):
        cta_state["meeting_rejected"] = True
        cta_state["meeting_status"] = "call_refused"
    elif cta_state.get("meeting_accepted"):
        cta_state["meeting_status"] = "accepted_pending_slots"
    elif response.runtime_payload.get("booking_offer"):
        cta_state["meeting_status"] = "slots_sent"
    elif int(cta_state.get("meeting_suggested_count") or 0) > 0:
        cta_state["meeting_status"] = "suggested"
    else:
        cta_state["meeting_status"] = "not_suggested"

    response.runtime_payload["cta_state"] = cta_state
    response.runtime_payload["intent_level"] = context.get("intent_level", "LOW_INTENT")
    response.runtime_payload["intent_score"] = context.get("intent_score", 0)
    response.runtime_payload["intent_reasons"] = context.get("intent_reasons", [])
    response.runtime_payload["conversation_act"] = response.conversation_act
    response.runtime_payload["lead_intent"] = response.lead_intent
    response.runtime_payload["planner_confidence"] = response.confidence
    response.runtime_payload["planner_reasoning_summary"] = response.reasoning_summary
    response.runtime_payload["uses_knowledge_context"] = response.uses_knowledge_context
    response.runtime_payload["knowledge_retrieval"] = context.get(
        "knowledge_retrieval",
        {"context_available": False, "selected_sources": []},
    )
    response.runtime_payload["important_missing_fields"] = context.get("important_missing_fields", [])
    lead_summary = dict(context.get("internal_lead_summary") or {})
    if lead_summary:
        lead_summary["meeting_status"] = cta_state.get("meeting_status", lead_summary.get("meeting_status", "not_suggested"))
        lead_summary["meeting_suggested_count"] = cta_state.get(
            "meeting_suggested_count",
            lead_summary.get("meeting_suggested_count", 0),
        )
        if lead_summary["meeting_status"] == "booked":
            lead_summary["qualification_level"] = "qualified_booked"
        lead_summary["recommended_follow_up"] = _recommended_follow_up(
            intent_level=str(context.get("intent_level") or lead_summary.get("intent_level") or "LOW_INTENT"),
            qualification_level=str(lead_summary.get("qualification_level") or "qualifying"),
            meeting_status=str(lead_summary.get("meeting_status") or "not_suggested"),
            missing_fields=context.get("important_missing_fields", []),
            cta_state=cta_state,
        )
    response.runtime_payload["lead_summary"] = lead_summary
    recommended_follow_up = lead_summary.get("recommended_follow_up")
    if recommended_follow_up:
        response.runtime_payload["recommended_follow_up"] = recommended_follow_up


def _finalize_response_with_context(response: AgentResponse, context: dict[str, Any]) -> AgentResponse:
    guarded_text, guardrail_events, reply_replaced = (
        _apply_response_guardrails_with_events(
            response.reply_text,
            context,
        )
    )
    response.reply_text = guarded_text
    if bool(response.runtime_payload.get("scoping_call_offer_forced")):
        # Identity/pricing guardrails run after planner policy and may replace
        # the entire draft. Reassert the required visible consent question as
        # a final postcondition, with suffix-preserving length control.
        response.reply_text = (
            _answer_then_explicit_expert_meeting_offer(response.reply_text, context)
            if bool(context.get("lead_question_detected"))
            else _explicit_expert_meeting_offer(context)
        )
    existing_events = response.runtime_payload.get("guardrail_events")
    merged_events = [
        str(event)[:80]
        for event in (existing_events if isinstance(existing_events, list) else [])
        if str(event).strip()
    ]
    legacy_reason = str(
        response.runtime_payload.get("reply_guardrail_reason") or ""
    ).strip()
    if legacy_reason and legacy_reason not in merged_events:
        merged_events.append(legacy_reason[:80])
    for event in guardrail_events:
        if event not in merged_events:
            merged_events.append(event)
    if merged_events:
        response.runtime_payload["guardrail_events"] = merged_events[:8]
    if reply_replaced:
        # The final delivered copy is a deterministic policy bridge, not the
        # source-grounded answer proposed by the model.
        response.uses_knowledge_context = False
    # Runtime/CTA diagnostics must describe the delivered reply, after policy
    # replacements and language normalization, rather than the model draft.
    _attach_behavior_runtime(response, context)
    return _finalize_response(response)


def _answers_blob(answers: dict[str, Any]) -> str:
    return " ".join(f"{key} {_stringify_answer(value)}" for key, value in answers.items()).strip()


def _has_explicit_pricing_context(ai_context: str) -> bool:
    text = " ".join(str(ai_context or "").split()).strip()
    if not text:
        return False
    normalized = _normalize_text(text)
    if not normalized:
        return False
    if re.search(r"\b(no|do not|don't|never)\s+(?:discuss|mention|share|talk about)?\s*(?:pricing|prices?|costs?|rates?|fees?|budget)\b", normalized):
        return False
    if _PRICE_AMOUNT_PATTERN.search(text):
        return True
    return bool(re.search(r"\b(pricing|prices?|rates?|fees?|packages?|plans?)\s*[:=-]\s*\S", text, re.IGNORECASE))


def _answers_have_signal(
    answers: dict[str, Any],
    *,
    key_tokens: Sequence[str] = (),
    value_tokens: Sequence[str] = (),
) -> bool:
    for key, value in answers.items():
        key_norm = _normalize_text(key)
        value_norm = _normalize_text(_stringify_answer(value))
        if any(_normalize_text(token) in key_norm for token in key_tokens):
            return True
        if value_tokens and any(_normalize_text(token) in value_norm for token in value_tokens):
            return True
    return False


def _first_answer_by_tokens(answers: dict[str, Any], tokens: Sequence[str]) -> str | None:
    for key, value in answers.items():
        key_norm = _normalize_text(key)
        if any(_normalize_text(token) in key_norm for token in tokens):
            text = _stringify_answer(value)
            if text:
                return text
    return None


def _timeline_is_urgent(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    if any(phrase in normalized for phrase in ("no rush", "not urgent", "flexible", "no hurry", "whenever")):
        return False
    return bool(_TIMELINE_PATTERN.search(normalized)) or any(
        phrase in normalized
        for phrase in (
            "as soon as possible",
            "asap",
            "urgent",
            "right away",
            "soon",
            "rush",
        )
    )


def _lead_asked_question(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    if "?" in str(text or "") or _PRICING_PATTERN.search(normalized):
        return True
    if re.match(r"^(?:parlez|expliquez|dites)[ -]moi\b", normalized):
        return True
    if re.match(
        r"^(?:"
        r"avez[ -]vous|pouvez[ -]vous|savez[ -]vous|faites[ -]vous|offrez[ -]vous|"
        r"est[ -]ce|est[ -]il|est[ -]elle|peut[ -]on|"
        r"comment|combien|quand|pourquoi|qui|que|quoi|quel|quelle|quels|quelles|ou"
        r")\b",
        normalized,
    ):
        return True
    return normalized.startswith(
        (
            "what ",
            "how ",
            "when ",
            "where ",
            "why ",
            "who ",
            "do ",
            "does ",
            "did ",
            "can ",
            "could ",
            "is ",
            "are ",
            "will ",
            "would ",
        )
    )


def _message_suggests_meeting(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    return bool(_MEETING_CTA_PATTERN.search(normalized))


def _message_invites_meeting(text: str) -> bool:
    """Return true only when visible copy actually asks the lead to schedule.

    Meeting vocabulary alone is intentionally insufficient: explaining what a
    scoping call is must not make a later bare "yes" count as booking consent.
    """

    normalized = _normalize_text(text)
    if not normalized or not _message_suggests_meeting(normalized):
        return False
    return bool(
        re.search(
            r"\b(?:"
            r"would you (?:like|want)|do you want|shall i|may i|can i|"
            r"i can (?:also )?(?:help (?:you )?)?(?:book|schedule|find|share|send|line up|set up)|"
            r"we can (?:book|schedule|set up|line up)|"
            r"reply with|which (?:time|option|slot)[^?]{0,40}(?:works|suits|fits)|"
            r"voulez-vous|souhaitez-vous|aimeriez-vous|puis-je|"
            r"je peux (?:aussi )?vous (?:aider|proposer|montrer|envoyer)|"
            r"repondez|quel creneau|quelle (?:heure|option|disponibilite)"
            r")\b",
            normalized,
        )
    )


def _latest_outbound_invites_meeting(history: Sequence[Message]) -> bool:
    """Recognize a pending invitation the lead could actually see."""

    last_outbound = _latest_outbound_from_history(history)
    if not last_outbound:
        return False
    return _message_invites_meeting(str(last_outbound.get("body") or ""))


def _count_meeting_suggestions(history: Sequence[Message]) -> int:
    count = 0
    for message in history:
        if message.direction == MessageDirection.OUTBOUND and _message_invites_meeting(message.body or ""):
            count += 1
    return count


def _latest_outbound_from_history(history: Sequence[Message]) -> dict[str, Any] | None:
    for message in reversed(history):
        if message.direction != MessageDirection.OUTBOUND:
            continue
        return {
            "body": " ".join(str(message.body or "").split()).strip(),
            "raw_payload": message.raw_payload if isinstance(message.raw_payload, dict) else {},
        }
    return None


def _cta_label_for_text(text: str) -> str | None:
    normalized = _normalize_text(text)
    if not normalized:
        return None
    if any(token in normalized for token in ("availability", "times", "calendar", "disponibilite", "creneau", "calendrier")):
        return "availability_offer"
    if "call" in normalized or "appel" in normalized:
        return "call_suggestion"
    if any(token in normalized for token in ("meeting", "appointment", "rendez-vous", "rencontre")):
        return "meeting_suggestion"
    if "connect" in normalized or "coordinate" in normalized:
        return "connect_with_team"
    return "next_step_suggestion"


def _strip_meeting_cta(text: str, *, fallback: str) -> str:
    clean = " ".join(str(text or "").split()).strip()
    if not clean:
        return fallback
    clean = re.sub(
        r",?\s+or\s+would\s+you\s+rather\s+i\s+help\s+(?:line up|set up|schedule|book)[^.?!]*(?:[.?!]|$)",
        "?",
        clean,
        flags=re.IGNORECASE,
    )
    clean = re.sub(
        r"\s*(?:if you want|if you'd like|if helpful),?\s+i can help\s+(?:line up|set up|schedule|book)[^.?!]*(?:[.?!]|$)",
        " ",
        clean,
        flags=re.IGNORECASE,
    )
    clean = re.sub(
        r"\s*i can help\s+(?:line up|set up|schedule|book)[^.?!]*(?:[.?!]|$)",
        " ",
        clean,
        flags=re.IGNORECASE,
    )
    # Preserve a factual answer when the model appends a French or English
    # meeting invitation in the same sentence. The broad sentence filter below
    # remains the safety net for unsupported availability/booking-only copy.
    clean = re.sub(
        r"[,;:]?\s*(?:"
        r"would you (?:like|want)|do you want|shall i|"
        r"can i (?:help|show|find|book|schedule)|"
        r"voulez-vous|souhaitez-vous|aimeriez-vous|"
        r"est-ce que vous (?:voulez|souhaitez)|"
        r"(?:je peux|puis-je) (?:aussi )?vous (?:proposer|montrer|aider)"
        r")[^.?!]*(?:meeting|appointment|call|availability|available times|book|schedule|"
        r"rendez[- ]vous|rencontre|appel|disponibilit[ée]s?|cr[ée]neaux?|r[ée]server|planifier)"
        r"[^.?!]*(?:[.?!]|$)",
        " ",
        clean,
        flags=re.IGNORECASE,
    )
    clean = re.sub(r"\s+([,.!?])", r"\1", clean)
    clean = re.sub(r"\s{2,}", " ", clean).strip()
    parts = re.split(r"(?<=[.!?])\s+", clean)
    kept = [part.strip() for part in parts if part.strip() and not _message_invites_meeting(part)]
    stripped = " ".join(kept).strip()
    return stripped or fallback


def _strip_booking_completion_claim(text: str, *, fallback: str) -> str:
    """Remove an unauthorized booking claim while retaining separate factual copy."""

    clean = " ".join(str(text or "").split()).strip()
    if not clean:
        return fallback
    completion_pattern = re.compile(
        r"\b(booked|booking confirmed|confirmed your|locked (?:it|that) in|scheduled|"
        r"r[ée]serv[ée]|confirm[ée])\b",
        re.IGNORECASE,
    )
    parts = re.split(r"(?<=[.!?])\s+", clean)
    kept = [part.strip() for part in parts if part.strip() and not completion_pattern.search(part)]
    stripped = " ".join(kept).strip()
    return stripped or fallback


def _qualified_scoping_call_offer_due(context: dict[str, Any]) -> bool:
    """Require the first consent-first expert-call offer on outbound turn two.

    A known need plus high intent is enough. Optional qualification details may
    improve the expert handoff, but they must not turn the SMS exchange into an
    interrogation or postpone the useful next step.
    """

    cta_state = context.get("cta_state") if isinstance(context.get("cta_state"), dict) else {}
    memory = (
        context.get("qualification_memory")
        if isinstance(context.get("qualification_memory"), dict)
        else {}
    )
    internal_summary = (
        context.get("internal_lead_summary")
        if isinstance(context.get("internal_lead_summary"), dict)
        else {}
    )
    core_need_known = bool(
        str(memory.get("service_needed") or "").strip()
        or str(internal_summary.get("service_interest") or "").strip()
    )
    return bool(
        int(context.get("outbound_turn_count") or 0) >= 1
        and str(context.get("intent_level") or "") == "HIGH_INTENT"
        and core_need_known
        and not bool(context.get("already_booked"))
        and str(context.get("current_state") or "").upper()
        != ConversationStateEnum.BOOKED.value
        and not bool(context.get("call_refusal"))
        and not bool(context.get("handoff_intent"))
        and not bool(context.get("explicit_booking_intent"))
        and not bool(context.get("scheduling_intent_detected"))
        and not bool(context.get("booked_confirmation_intent"))
        and not bool(cta_state.get("meeting_rejected"))
        and not bool(cta_state.get("suppress_meeting_cta"))
        and int(cta_state.get("meeting_suggested_count") or 0) == 0
    )


def _meeting_cta_allowed_for_turn(context: dict[str, Any]) -> bool:
    cta_state = context.get("cta_state") if isinstance(context.get("cta_state"), dict) else {}
    if context.get("call_refusal") or cta_state.get("meeting_rejected"):
        return False
    if (
        context.get("explicit_booking_intent")
        or context.get("scheduling_intent_detected")
        or context.get("booked_confirmation_intent")
    ):
        return True
    if context.get("pricing_question") and not context.get("pricing_context_available"):
        return _soft_call_cta_allowed(context)
    if _qualified_scoping_call_offer_due(context):
        return True
    return False


def _explicit_expert_meeting_question(context: dict[str, Any]) -> str:
    business_name = str(context.get("business_name") or "").strip()
    language = normalize_language(str(context.get("response_language") or "en"))
    if language == "fr":
        expert = f"un expert chez {business_name}" if business_name else "un expert de l'équipe"
        return f"Voulez-vous que je vous aide à réserver un appel de cadrage avec {expert}?"
    expert = f"an expert at {business_name}" if business_name else "an expert on the team"
    return f"Would you like me to help book a scoping call with {expert}?"


def _explicit_expert_meeting_offer(context: dict[str, Any]) -> str:
    language = normalize_language(str(context.get("response_language") or "en"))
    prefix = "C'est noté." if language == "fr" else "Got it."
    return f"{prefix} {_explicit_expert_meeting_question(context)}"


def _answer_then_explicit_expert_meeting_offer(
    text: str,
    context: dict[str, Any],
) -> str:
    answer = _strip_meeting_cta(text, fallback="").strip()
    # Once the lead is qualified, the single useful question is consent for
    # the expert call—not another optional qualification question.
    answer = answer.replace("?", ".")
    answer = re.sub(r"\s+([,.!])", r"\1", answer)
    answer = re.sub(r"\.{2,}", ".", answer).strip(" .")
    question = _explicit_expert_meeting_question(context)
    if not answer:
        return _explicit_expert_meeting_offer(context)
    max_answer_chars = max(0, _MAX_AGENT_REPLY_CHARS - len(question) - 2)
    if len(answer) > max_answer_chars:
        clipped = answer[: max(0, max_answer_chars - 1)].rstrip()
        if " " in clipped:
            clipped = clipped.rsplit(" ", 1)[0] or clipped
        answer = f"{clipped.rstrip(' .')}…"
    separator = " " if answer.endswith(("!", "…")) else ". "
    return f"{answer}{separator}{question}"


def _non_booking_bridge_reply(context: dict[str, Any]) -> str:
    language = str(context.get("response_language") or "en")
    if language == "fr":
        if context.get("call_refusal") or (context.get("cta_state") or {}).get("meeting_rejected"):
            return "Pas de problème. Je peux continuer à vous aider ici. Qu'aimeriez-vous clarifier ensuite?"
        if context.get("pricing_question"):
            if context.get("pricing_context_available"):
                return "Je peux répondre à partir des détails fournis par l'entreprise. Quelle partie voulez-vous clarifier?"
            base = (
                "Je n'ai pas de détails de prix ou de forfait confirmés ici. "
                "En général, ça dépend de l'étendue, du délai, de la zone desservie, du niveau de service et des besoins particuliers."
            )
            if _qualified_scoping_call_offer_due(context):
                return f"{base} {_explicit_expert_meeting_question(context)}"
            if _soft_call_cta_allowed(context):
                return f"{base} L'équipe peut clarifier ça pendant un appel de consultation; voulez-vous que je vous aide à organiser ça?"
            return f"{base} Je peux aussi vous aider à clarifier l'adéquation ou le processus ici."
        if str(context.get("intent_level") or "") == "LOW_INTENT":
            return "Pas de problème. Je peux vous aider à vous faire une idée générale d'abord. Cherchez-vous surtout à comprendre le processus, les délais ou l'adéquation?"
        missing = context.get("recommended_missing_field")
        if isinstance(missing, dict) and missing.get("question"):
            return _missing_field_question_for_language(missing, language)
        return "Ça fait du sens. Qu'est-ce qui serait le plus utile à clarifier en premier?"
    if context.get("call_refusal") or (context.get("cta_state") or {}).get("meeting_rejected"):
        return "No problem. I can keep helping here instead. What would you like to understand next?"
    if context.get("pricing_question"):
        if context.get("pricing_context_available"):
            return "I can answer from the package details the business provided. What part would you like me to clarify?"
        base = (
            "I do not have confirmed package or pricing details here. "
            "It usually depends on scope, timeline, service area, package level, and any special requirements."
        )
        if _qualified_scoping_call_offer_due(context):
            return f"{base} {_explicit_expert_meeting_question(context)}"
        if _soft_call_cta_allowed(context):
            return f"{base} The team can review that on a consultation call; would you like me to help set that up?"
        return f"{base} I can also help clarify fit or process here."
    if str(context.get("intent_level") or "") == "LOW_INTENT":
        return "No problem. I can help you get a general idea first. Are you mostly trying to understand process, timeline, or fit?"
    missing = context.get("recommended_missing_field")
    if isinstance(missing, dict) and missing.get("question"):
        return _missing_field_question_for_language(missing, language)
    return "That makes sense. What would be most helpful to clarify first?"


def _apply_response_guardrails(
    text: str,
    context: dict[str, Any],
) -> str:
    """Backward-compatible text-only response guardrail API."""

    guarded_text, _, _ = _apply_response_guardrails_with_events(text, context)
    return guarded_text


def _apply_response_guardrails_with_events(
    text: str,
    context: dict[str, Any],
) -> tuple[str, list[str], bool]:
    clean = " ".join(str(text or "").split()).strip()
    if not clean:
        return clean, [], False
    if _reply_has_identity_violation(clean):
        return _identity_reply(context), ["identity_violation_replaced"], True
    clean = _remove_redundant_acknowledged_fact_clauses(clean, context)
    pricing_replaced = bool(
        _reply_has_budget_language(clean)
        or (
            not context.get("pricing_context_available")
            and _reply_has_pricing_language(clean)
        )
    )
    clean = _remove_disallowed_pricing_language(clean, context)
    clean = _ensure_initial_intro(clean, context)
    clean = _enforce_response_language(clean, context)
    events = ["disallowed_pricing_replaced"] if pricing_replaced else []
    return clean, events, pricing_replaced


def _enforce_response_language(text: str, context: dict[str, Any]) -> str:
    language = normalize_language(str(context.get("response_language") or "en"))
    if language != "fr":
        return text
    clean = _replace_common_english_booking_copy(text)
    clean = _localize_english_slot_dates(clean)
    clean = _localize_english_clock_times(clean)
    clean = re.sub(r"\bconsultation call\b", "appel de consultation", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\bstrategy call\b", "appel de consultation", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\bthe team\b", "l'équipe", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\s+([,.!?])", r"\1", clean)
    clean = re.sub(r"\s{2,}", " ", clean).strip()
    if _reply_is_clearly_english(clean, context):
        return _ensure_initial_intro(_non_booking_bridge_reply(context), context)
    return clean


_LANGUAGE_GUARD_IGNORED_RE = re.compile(
    r"(?:https?://|www\.)\S+|\b[\w.+-]+@[\w.-]+\.[a-z]{2,}\b",
    re.IGNORECASE,
)
_ENGLISH_LANGUAGE_MARKERS = frozenset(
    "are can clear could for great hello help hoping how is looking need outcome "
    "please reaching should sure thank thanks that the this understand want what "
    "when where which why with would you your".split()
)
_FRENCH_LANGUAGE_MARKERS = frozenset(
    "aide aider appel avec besoin bonjour clarifier comment confirme dans des est "
    "ici la le les merci notre nous pouvez pour pourquoi projet quel quelle quels "
    "quelles souhaitez sur une votre vous".split()
)


def _reply_is_clearly_english(text: str, context: dict[str, Any]) -> bool:
    """Detect a clear English draft without treating brands or URLs as language."""

    candidate = str(text or "").strip()
    if context.get("initial_outreach"):
        prefix = _initial_intro_prefix(context)
        if candidate.casefold().startswith(prefix.casefold()):
            candidate = candidate[len(prefix) :].lstrip()
    candidate = _LANGUAGE_GUARD_IGNORED_RE.sub(" ", candidate)
    tokens = re.findall(r"[a-z]+", _normalize_text(candidate))
    if len(tokens) < 2:
        return False

    english_score = sum(token in _ENGLISH_LANGUAGE_MARKERS for token in tokens)
    french_score = sum(token in _FRENCH_LANGUAGE_MARKERS for token in tokens)
    return english_score >= 3 and english_score >= french_score + 2


def _replace_common_english_booking_copy(text: str) -> str:
    clean = str(text or "")
    clean = re.sub(
        r"\bIf none of those work, just send me a time that's better for you\.?",
        "Si aucune option ne fonctionne, envoyez-moi simplement un moment qui vous convient mieux.",
        clean,
        flags=re.IGNORECASE,
    )
    clean = re.sub(
        r"\bTimes shown in ([A-Z]{2,5})\.?",
        r"Heures affichées en \1.",
        clean,
        flags=re.IGNORECASE,
    )
    clean = re.sub(
        r"\bReply with ([^.;]+?) to book the call, or send the exact time you want\.?",
        r"Répondez \1 pour réserver l'appel, ou envoyez l'heure exacte souhaitée.",
        clean,
        flags=re.IGNORECASE,
    )
    clean = re.sub(
        r"\bReply with ([^.;]+?) and I(?:'|’)ll lock it in\.?",
        r"Répondez \1 et je le réserve.",
        clean,
        flags=re.IGNORECASE,
    )
    clean = re.sub(
        r"\bI can lock that in once you pick one of the offered times\.?",
        _LOCALIZED_AGENT_REPLIES["pick_slot_first"]["fr"],
        clean,
        flags=re.IGNORECASE,
    )
    clean = re.sub(
        r"\bPerfect\. You(?:'|’)re booked\.?",
        _LOCALIZED_AGENT_REPLIES["booked"]["fr"],
        clean,
        flags=re.IGNORECASE,
    )
    return clean


def _localize_english_slot_dates(text: str) -> str:
    pattern = re.compile(
        r"\b(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+"
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+"
        r"(\d{1,2})\s+at\s+(\d{1,2})(?::(\d{2}))?\s*(AM|PM)\b",
        re.IGNORECASE,
    )

    def repl(match: re.Match[str]) -> str:
        day = _FR_WEEKDAY_ABBR.get(match.group(1).lower()[:3], match.group(1))
        month = _FR_MONTH_ABBR.get(match.group(2).lower()[:3], match.group(2))
        day_number = int(match.group(3))
        hour = int(match.group(4))
        minute = int(match.group(5) or "0")
        meridiem = match.group(6).lower()
        if meridiem == "pm" and hour != 12:
            hour += 12
        if meridiem == "am" and hour == 12:
            hour = 0
        return f"{day} {day_number} {month} à {hour} h {minute:02d}"

    return pattern.sub(repl, text)


def _localize_english_clock_times(text: str) -> str:
    pattern = re.compile(r"\b(\d{1,2})(?::(\d{2}))\s*(AM|PM)\b", re.IGNORECASE)

    def repl(match: re.Match[str]) -> str:
        hour = int(match.group(1))
        minute = int(match.group(2) or "0")
        meridiem = match.group(3).lower()
        if meridiem == "pm" and hour != 12:
            hour += 12
        if meridiem == "am" and hour == 12:
            hour = 0
        return f"{hour} h {minute:02d}"

    return pattern.sub(repl, text)


def _remove_disallowed_pricing_language(text: str, context: dict[str, Any]) -> str:
    if _reply_has_budget_language(text):
        return _non_booking_bridge_reply(
            {**context, "pricing_question": bool(context.get("pricing_question"))}
        )
    if context.get("pricing_context_available"):
        return text
    if not _reply_has_pricing_language(text):
        return text
    return _non_booking_bridge_reply(
        {**context, "pricing_question": bool(context.get("pricing_question"))}
    )


def _soft_call_cta_allowed(context: dict[str, Any]) -> bool:
    cta_state = context.get("cta_state") if isinstance(context.get("cta_state"), dict) else {}
    if context.get("call_refusal") or cta_state.get("meeting_rejected"):
        return False
    if cta_state.get("suppress_meeting_cta") or cta_state.get("meeting_ignored"):
        return False
    if str(context.get("intent_level") or "") == "LOW_INTENT":
        return False
    return _to_int(cta_state.get("meeting_suggested_count"), default=0) == 0


def _reply_has_budget_language(text: str) -> bool:
    return bool(re.search(r"\bbudget\b", text or "", re.IGNORECASE) or _BUDGET_TALK_PATTERN.search(text or ""))


def _reply_has_pricing_language(text: str) -> bool:
    return bool(
        _PRICING_PATTERN.search(text or "")
        or _PRICE_AMOUNT_PATTERN.search(text or "")
        or _BUDGET_TALK_PATTERN.search(text or "")
    )


def _ensure_initial_intro(text: str, context: dict[str, Any]) -> str:
    if not context.get("initial_outreach"):
        return text
    prefix = _initial_intro_prefix(context)
    normalized = _normalize_text(text)
    identity = context.get("agent_identity") if isinstance(context.get("agent_identity"), dict) else {}
    assistant_name = _normalize_text(identity.get("name") or _ASSISTANT_NAME)
    assistant_intro_present = bool(
        assistant_name
        and assistant_name in normalized
        and (
            "assistant for" in normalized
            or re.search(r"\bassistant(?:e)?\s+(?:de|chez|pour)\b", normalized)
            or re.search(rf"\b(?:ici|je suis|i m|i am|this is)\s+{re.escape(assistant_name)}\b", normalized)
        )
    )
    if assistant_intro_present:
        return text
    if normalized.startswith(_normalize_text(prefix)):
        return text
    return f"{prefix} {text}".strip()


def _initial_intro_prefix(context: dict[str, Any]) -> str:
    lead_name = str(context.get("lead_name") or "").strip()
    language = str(context.get("response_language") or "en")
    first_name = lead_name.split()[0] if lead_name else ("bonjour" if language == "fr" else "there")
    business_name = str(context.get("business_name") or ("l'entreprise" if language == "fr" else "the business")).strip() or ("l'entreprise" if language == "fr" else "the business")
    if language == "fr":
        return f"Bonjour {first_name}, ici {_ASSISTANT_NAME}, l'assistante de {business_name}."
    return f"Hi {first_name}, I'm {_ASSISTANT_NAME}, the assistant for {business_name}."


def _reply_has_identity_violation(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    if _ASSISTANT_OWNERSHIP_CLAIM_PATTERN.search(normalized):
        return True
    assistant = _normalize_text(_ASSISTANT_NAME)
    if assistant and assistant in normalized:
        ownership_terms = ("run by", "runs", "owned by", "owner", "founder", "founded by", "started by", "behind")
        if any(term in normalized for term in ownership_terms) and "assistant" not in normalized:
            return True
    return False


def _remove_redundant_acknowledged_fact_clauses(text: str, context: dict[str, Any]) -> str:
    fact_terms = _acknowledged_fact_terms(context)
    if not fact_terms:
        return text
    term_pattern = "|".join(re.escape(term) for term in fact_terms if len(term) >= 3)
    if not term_pattern:
        return text
    clean = text
    contextual_preamble = re.compile(
        rf"\b(?:since|given|as|because)\s+[^,.!?]{{0,180}}(?:{term_pattern})[^,.!?]{{0,180}},\s*",
        re.IGNORECASE,
    )
    direct_preamble = re.compile(
        rf"\b(?:you(?:'re| are)\s+[^,.!?]{{0,160}}(?:{term_pattern})[^,.!?]{{0,160}},\s*)",
        re.IGNORECASE,
    )
    clean = contextual_preamble.sub("", clean)
    clean = direct_preamble.sub("", clean)
    clean = re.sub(r"\s+([,.!?])", r"\1", clean)
    clean = re.sub(r"\s{2,}", " ", clean).strip(" ,")
    clean = _compact_repetitive_question_preamble(clean, context=context, fact_terms=fact_terms)
    if clean and clean[0].islower():
        clean = clean[0].upper() + clean[1:]
    return clean or text


_RECAP_CUES = (
    "as you mentioned",
    "c est bien note",
    "c est note",
    "given",
    "got it",
    "j ai bien note",
    "j ai note",
    "parfait",
    "since",
    "thanks",
    "avec",
    "bien note",
    "comme",
    "merci",
    "vu",
)

_QUESTION_START_PATTERN = re.compile(
    r"\b(?:"
    r"are you|can you|could you|do you|does|how|is there|what|when|where|which|who|would you|"
    r"avez-vous|etes-vous|est-ce|faut-il|quand|quel(?:le)?s?|qui|souhaitez-vous|voulez-vous|y a-t-il"
    r")\b",
    re.IGNORECASE,
)


def _compact_repetitive_question_preamble(
    text: str,
    *,
    context: dict[str, Any],
    fact_terms: Sequence[str],
) -> str:
    """Keep the next question while dropping an already-known project recap."""

    if "?" not in text:
        return text
    normalized = _normalize_text(text)
    if not any(cue in normalized for cue in _RECAP_CUES):
        return text
    mentioned_terms = {
        term
        for term in fact_terms
        if len(term) >= 7 and _normalize_text(term) in normalized
    }
    if len(mentioned_terms) < 2:
        return text

    sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", text) if part.strip()]
    question_index = next((index for index in range(len(sentences) - 1, -1, -1) if "?" in sentences[index]), None)
    if question_index is None:
        return text
    question = sentences[question_index]
    if ":" in question:
        trailing = question.rsplit(":", 1)[1].strip()
        if "?" in trailing:
            question = trailing
    else:
        normalized_question = _normalize_text(question)
        match = _QUESTION_START_PATTERN.search(normalized_question)
        if match and match.start() > 0:
            # Accent folding preserves character positions for the supported
            # French/English copy closely enough for question-starter slicing.
            question = question[match.start() :].strip(" ,:;-")

    if not question or "?" not in question:
        return text
    if question[0].islower():
        question = question[0].upper() + question[1:]

    acknowledgment = ""
    for candidate in reversed(sentences[:question_index]):
        candidate_norm = _normalize_text(candidate)
        if len(candidate) <= 45 and not any(term in candidate_norm for term in mentioned_terms):
            acknowledgment = candidate
            break
    if not acknowledgment:
        acknowledgment = "C'est noté." if normalize_language(str(context.get("response_language") or "en")) == "fr" else "Got it."
    if acknowledgment[-1:] not in ".!?":
        acknowledgment += "."
    return f"{acknowledgment} {question}".strip()


def _acknowledged_fact_terms(context: dict[str, Any]) -> list[str]:
    acknowledged = {str(key) for key in context.get("acknowledged_form_fact_keys", [])}
    if not acknowledged:
        return []
    terms: list[str] = []
    for fact in context.get("known_form_facts", []):
        if not isinstance(fact, dict) or str(fact.get("key") or "") not in acknowledged:
            continue
        terms.extend(_fact_terms({key: str(value) for key, value in fact.items()}))
    return list(dict.fromkeys(_normalize_text(term)[:160] for term in terms if _normalize_text(term)))[:80]


def _stringify_answer(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value if str(item).strip())
    return " ".join(str(value or "").split()).strip()


def _humanize_key(key: str) -> str:
    return str(key or "").replace("_", " ").strip().title()


def _extract_from_form_answers(answers: dict[str, Any]) -> QualificationMemory:
    extracted: dict[str, Any] = {}
    for key, value in answers.items():
        key_norm = _normalize_text(key)
        text = " ".join(str(value or "").split()).strip()
        if not text:
            continue
        if _key_has_semantic_token(
            key_norm,
            (
                "service",
                "offering",
                "product",
                "problem",
                "challenge",
                "scope",
                "request",
                "need",
                "project",
                "goal",
                "interest",
                "besoin",
                "projet",
                "selectionner",
            ),
        ):
            extracted.setdefault("service_needed", text)
        timeline_key = _key_has_semantic_token(
            key_norm,
            ("timeline", "start", "deadline", "date", "delai", "echeance", "realisation"),
        )
        urgency_key = _key_has_semantic_token(key_norm, ("urgency", "urgent", "urgence"))
        if timeline_key or urgency_key:
            extracted.setdefault("timeline", text)
            if urgency_key:
                # A dedicated urgency answer is more informative than a generic
                # deadline and should replace it as the urgency driver.
                extracted["urgency_driver"] = text
            else:
                extracted.setdefault("urgency_driver", text)
        if _key_has_semantic_token(
            key_norm,
            ("location", "market", "region", "city", "site", "address", "lieu", "ville", "adresse"),
        ):
            extracted.setdefault("locations", text)
        if _key_has_semantic_token(
            key_norm,
            (
                "decision",
                "approv",
                "stakeholder",
                "attendee",
                "role",
                "decideur",
                "decisionnaire",
                "approb",
                "responsable",
                "participant",
            ),
        ):
            extracted.setdefault("decision_makers", text)
        partial = _extract_from_text(text)
        extracted.update({k: v for k, v in partial.model_dump(exclude_none=True).items() if v not in (None, "")})
    return QualificationMemory.model_validate(extracted)


def _key_has_semantic_token(key: str, tokens: Sequence[str]) -> bool:
    """Match form-key concepts as words, not accidental substrings.

    This notably prevents the French attachment verb ``joindre`` from being
    interpreted as the English qualification concept ``join``.
    """

    normalized_key = _normalize_text(key).replace("-", " ")
    return any(
        bool(re.search(rf"\b{re.escape(_normalize_text(token))}[a-z0-9]*\b", normalized_key))
        for token in tokens
        if _normalize_text(token)
    )


def _extract_from_messages(history: Sequence[Message]) -> QualificationMemory:
    extracted: dict[str, Any] = {}
    for message in history:
        if message.direction != MessageDirection.INBOUND:
            continue
        partial = _extract_from_text(message.body)
        extracted.update({k: v for k, v in partial.model_dump(exclude_none=True).items() if v is not None})
    return QualificationMemory.model_validate(extracted)


def _extract_from_text(text: str) -> QualificationMemory:
    raw = " ".join(str(text or "").split()).strip()
    normalized = _normalize_text(raw)
    if not normalized:
        return QualificationMemory()
    extracted: dict[str, Any] = {}
    timeline = _extract_timeline(raw)
    if timeline:
        extracted["timeline"] = timeline
        extracted["urgency_driver"] = timeline
    service_needed = _extract_service_needed(raw)
    if service_needed:
        extracted["service_needed"] = service_needed
    if _DECISION_MAKER_PATTERN.search(raw):
        extracted["decision_makers"] = raw
    if _has_booking_intent(raw):
        extracted["booking_intent_locked"] = True
    return QualificationMemory.model_validate(extracted)


def _extract_timeline(text: str) -> str | None:
    match = _TIMELINE_PATTERN.search(text)
    if not match:
        return None
    return " ".join(match.group(0).split())


def _extract_service_needed(text: str) -> str | None:
    normalized = _normalize_text(text)
    patterns = (
        r"\b(?:we need|i need|looking for|want help with|need help with)\s+(.+)$",
        r"\b(?:project is|scope is)\s+(.+)$",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized)
        if match:
            snippet = match.group(1).strip(" .,!;")
            if len(snippet) >= 4:
                return snippet[:160]
    return None


def _merge_memory(*memories: QualificationMemory) -> QualificationMemory:
    merged: dict[str, Any] = {}
    for memory in memories:
        for key, value in memory.model_dump(exclude_none=True).items():
            if value is None:
                continue
            merged[key] = value
    return QualificationMemory.model_validate(merged)


def _memory_has_value(memory: QualificationMemory, key: QuestionKey) -> bool:
    value = getattr(memory, key)
    return value is not None and value != ""


def _booking_threshold(*, memory: QualificationMemory) -> tuple[bool, list[str]]:
    qualifiers_ready = _memory_has_value(memory, "decision_makers") and _memory_has_value(memory, "urgency_driver")
    booking_ready = bool(memory.booking_intent_locked) or qualifiers_ready
    missing: list[str] = []
    if not _memory_has_value(memory, "decision_makers"):
        missing.append("decision_makers")
    if not _memory_has_value(memory, "urgency_driver"):
        missing.append("urgency_driver")
    return booking_ready, missing


def _extract_asked_question_keys(history: Sequence[Message]) -> list[QuestionKey]:
    keys: list[QuestionKey] = []
    for message in history:
        if message.direction != MessageDirection.OUTBOUND:
            continue
        raw_payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
        agent_payload = raw_payload.get("agent") if isinstance(raw_payload.get("agent"), dict) else {}
        stored_key = agent_payload.get("next_question_key")
        if stored_key in _QUESTION_SPEC_BY_KEY:
            keys.append(stored_key)
            continue
        guessed = _infer_question_key_from_text(message.body or "")
        if guessed:
            keys.append(guessed)
    return keys


def _latest_outbound_question_key(history: Sequence[Message]) -> QuestionKey | None:
    latest = _latest_outbound_from_history(history)
    if not latest:
        return None
    raw_payload = latest.get("raw_payload")
    raw_payload = raw_payload if isinstance(raw_payload, dict) else {}
    agent_payload = raw_payload.get("agent") if isinstance(raw_payload.get("agent"), dict) else {}
    stored_key = agent_payload.get("next_question_key")
    if stored_key in _QUESTION_SPEC_BY_KEY:
        return stored_key
    return _infer_question_key_from_text(str(latest.get("body") or ""))


def _is_unknown_qualification_answer(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return True
    return bool(
        re.search(
            r"\b(?:"
            r"je (?:ne )?sais pas|pas sur|aucune idee|a confirmer|je dois verifier|"
            r"on ne sait pas|i don'?t know|not sure|no idea|to be confirmed|"
            r"i need to (?:check|confirm)|need to (?:check|confirm)"
            r")\b",
            normalized,
        )
    )


def _infer_question_key_from_text(text: str) -> QuestionKey | None:
    normalized = _normalize_text(text)
    if any(
        phrase in normalized
        for phrase in (
            "decision-maker",
            "decision maker",
            "anyone else join",
            "personne qui prend la decision",
            "personne qui valide",
            "quelqu un d autre",
        )
    ):
        return "decision_makers"
    if any(
        phrase in normalized
        for phrase in (
            "deadline",
            "key date",
            "approval timeline",
            "driving this",
            "date importante",
            "date de livraison",
            "quelle echeance",
            "quel delai",
        )
    ):
        return "urgency_driver"
    return None


def _recommended_next_question_key(*, memory: QualificationMemory, asked_question_keys: Sequence[str]) -> QuestionKey | None:
    asked = {str(key) for key in asked_question_keys}
    for key in _QUESTION_ORDER:
        if key in asked:
            continue
        if _memory_has_value(memory, key):
            continue
        return key
    return None


def _normalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(text or "").strip())
    folded = "".join(character for character in normalized if not unicodedata.combining(character)).casefold()
    cleaned = re.sub(r"\s+", " ", folded)
    cleaned = re.sub(r"[^a-z0-9@+:/?.,\- ]", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _append_question(text: str, question: str) -> str:
    base = " ".join(str(text or "").split()).strip()
    question = question.strip()
    if not base:
        return question
    if base.endswith("?"):
        return base
    return f"{base} {question}".strip()


def _replace_question(text: str, question: str) -> str:
    base = " ".join(str(text or "").split()).strip()
    if "?" not in base:
        return _append_question(base, question)
    prefix = base.split("?", 1)[0]
    if "." in prefix:
        prefix = prefix.rsplit(".", 1)[0].strip()
    prefix = prefix.rstrip(" .")
    return f"{prefix}. {question}" if prefix else question


def _trim_sms_text(text: str) -> str:
    clean = " ".join(str(text or "").split()).strip()
    if len(clean) <= _MAX_AGENT_REPLY_CHARS:
        return clean
    return clean[: _MAX_AGENT_REPLY_CHARS - 3].rstrip() + "..."


def _ensure_single_question(text: str) -> str:
    clean = " ".join(str(text or "").split()).strip()
    if clean.count("?") <= 1:
        return clean
    first = clean.find("?")
    return f"{clean[: first + 1]} {clean[first + 1 :].replace('?', '.') }".strip()


def _finalize_response(response: AgentResponse) -> AgentResponse:
    response.reply_text = _trim_sms_text(_ensure_single_question(response.reply_text))
    if response.action != "ask_next_question":
        response.next_question_key = None
    return response


def _has_booking_intent(text: str, *, allow_generic_confirmation: bool = False) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    generic_affirmation = _is_generic_booking_affirmation(normalized)
    strong_booking_signal = bool(
        re.search(
            r"\b(book|booking|lock|schedule|scheduled|set it up|set a call|confirm|appointment|meeting|call|calendar|"
            r"r[ée]serv|bloque|planifier|confirmer|rendez-vous|appel)\b",
            normalized,
        )
    )
    if generic_affirmation:
        return bool(allow_generic_confirmation)
    if strong_booking_signal and _BOOKING_INTENT_PATTERN.search(normalized):
        return True
    return any(
        phrase in normalized
        for phrase in (
            "available next",
            "what are your availabilities",
            "what availability",
            "can we schedule",
            "book a meeting",
            "book a call",
            "set a call",
            "send me times",
            "avez-vous un creneau",
            "avez vous un creneau",
            "quelles sont vos disponibilites",
            "envoyez-moi des creneaux",
            "envoyez moi des creneaux",
            "peut-on planifier",
            "peut on planifier",
        )
    )


def _is_generic_booking_affirmation(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    # Natural confirmations often contain conversational punctuation
    # ("Yes, go ahead"), which must not prevent consent from being recognized.
    normalized = re.sub(r"[,.!?;:]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return bool(
        re.fullmatch(
            r"(?:"
            r"yes(?: please| go ahead)?|yeah(?: go ahead)?|yep|sure(?: go ahead)?|"
            r"ok|okay|sounds good|works|works for me|go ahead|"
            r"oui(?:\s+(?:svp|s il vous plait|allez[- ]y|volontiers|bien sur|je veux|ca me va))?|"
            r"allez[- ]y|allons[- ]y|certainement|volontiers|ca marche|d accord|parfait|super"
            r")",
            normalized,
        )
    )


def _has_scheduling_intent(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    if _has_booking_intent(text):
        return True
    if re.search(r"\b(can|could|would)\s+(?:you|we)\s+do\b", normalized):
        return True
    return bool(
        re.search(
            r"\b(availability|availabilities|available|free|openings?|slots?|times?|calendar|schedule|book|meeting|call|appointment|"
            r"disponibilit|creneau|creneaux|rendez-vous|appel|horaire)\b",
            normalized,
        )
    )


def _extract_slot_choice(inbound_text: str, latest_offer: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(latest_offer, dict):
        return None
    slots = latest_offer.get("slots")
    if not isinstance(slots, list) or not slots:
        return None
    normalized = _normalize_text(inbound_text)
    if not normalized:
        return None
    slot_indexes: set[int] = set()
    for slot in slots:
        try:
            slot_indexes.add(int(slot.get("index")))
        except Exception:
            continue
    for numeric_choice in re.finditer(r"\b(\d+)\b", normalized):
        index = int(numeric_choice.group(1))
        if index in slot_indexes:
            return {"slot_index": index}
    for slot in slots:
        blob = _normalize_text(str(slot.get("search_blob", "")))
        start_time = str(slot.get("start_time", "")).strip()
        if blob and any(part.strip() and part.strip() in normalized for part in blob.split("|")):
            return {"slot_start_time": start_time} if start_time else {}
    time_match = re.search(r"\b(\d{1,2}(?::\d{2})?\s?(?:am|pm)|\d{1,2}\s*h\s*\d{0,2})\b", normalized)
    if time_match:
        for slot in slots:
            haystack = _normalize_text(
                " ".join(
                    [
                        str(slot.get("display_time", "")),
                        str(slot.get("display_hint", "")),
                        str(slot.get("search_blob", "")),
                    ]
                )
            )
            if _normalize_text(time_match.group(1)) in haystack:
                start_time = str(slot.get("start_time", "")).strip()
                return {"slot_start_time": start_time} if start_time else {}
    return None


def _normalize_optional_string(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _extract_booking_preferences(text: str) -> dict[str, str]:
    normalized = _normalize_text(text)
    if not normalized:
        return {}

    preferences: dict[str, str] = {}
    day_name = next((day for day in _DAY_NAMES if day in normalized), None)
    unavailable = any(
        phrase in normalized
        for phrase in (
            "not available",
            "dont work",
            "doesnt work",
            "can't do",
            "cant do",
            "won't work",
            "wont work",
            "not free",
        )
    )
    if day_name:
        if unavailable:
            preferences["avoid_day"] = day_name
        else:
            preferences["preferred_day"] = day_name

    date_match = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", normalized)
    if date_match and "preferred_day" not in preferences and "avoid_day" not in preferences:
        inferred_day = _normalize_requested_day(date_match.group(1))
        if inferred_day:
            preferences["preferred_day"] = inferred_day

    if "morning" in normalized:
        preferences["preferred_period"] = "morning"
    elif "afternoon" in normalized:
        preferences["preferred_period"] = "afternoon"
    elif "evening" in normalized:
        preferences["preferred_period"] = "evening"

    range_match = re.search(
        r"\b(?:between|from|entre|de)\s+(\d{1,2}(?::\d{2}|\s*h\s*\d{0,2})?\s*(?:am|pm)?)\s+(?:and|to|et|à|a)\s+(\d{1,2}(?::\d{2}|\s*h\s*\d{0,2})?\s*(?:am|pm)?)\b",
        normalized,
    )
    if range_match:
        start_raw = range_match.group(1).strip()
        end_raw = range_match.group(2).strip()
        range_pair = _normalize_time_range(start_raw, end_raw)
        if range_pair:
            preferences["range_start"], preferences["range_end"] = range_pair

    time_match = re.search(r"\b(\d{1,2}(?::\d{2})?\s?(?:am|pm)|\d{1,2}\s*h\s*\d{0,2})\b", normalized)
    if time_match and "range_start" not in preferences:
        preferences["exact_time"] = time_match.group(1)

    return preferences


def _latest_outbound_text(recent_messages: Any) -> str | None:
    if not isinstance(recent_messages, list):
        return None
    for message in reversed(recent_messages):
        if not isinstance(message, dict):
            continue
        if str(message.get("direction", "")).strip().lower() != MessageDirection.OUTBOUND.value:
            continue
        text = " ".join(str(message.get("body", "")).split()).strip()
        if text:
            return text
    return None


def _normalize_requested_day(value: str | None) -> str | None:
    text = _normalize_text(value or "")
    if not text:
        return None
    for day in _DAY_NAMES:
        if day in text:
            return day
    if re.fullmatch(r"20\d{2}-\d{2}-\d{2}", text):
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d")
        except ValueError:
            return None
        return _DAY_NAMES[parsed.weekday()]
    return None


def _normalize_time_range(start_raw: str, end_raw: str) -> tuple[str, str] | None:
    start_match = re.search(r"^(\d{1,2})(?::(\d{2})|\s*h\s*(\d{1,2})?)?\s*(am|pm)?$", start_raw)
    end_match = re.search(r"^(\d{1,2})(?::(\d{2})|\s*h\s*(\d{1,2})?)?\s*(am|pm)?$", end_raw)
    if not start_match or not end_match:
        return None

    start_hour = int(start_match.group(1))
    start_minute = int(start_match.group(2) or start_match.group(3) or "0")
    start_meridiem = (start_match.group(4) or "").strip()

    end_hour = int(end_match.group(1))
    end_minute = int(end_match.group(2) or end_match.group(3) or "0")
    end_meridiem = (end_match.group(4) or "").strip()

    if not start_meridiem and end_meridiem:
        if end_meridiem == "pm" and start_hour > end_hour:
            start_meridiem = "am"
        else:
            start_meridiem = end_meridiem
    if not end_meridiem and start_meridiem:
        if start_meridiem == "am" and end_hour < start_hour:
            end_meridiem = "pm"
        else:
            end_meridiem = start_meridiem

    if not start_meridiem and not end_meridiem:
        if start_hour >= 8 and end_hour <= 6:
            start_meridiem = "am"
            end_meridiem = "pm"
        elif end_hour <= 11:
            start_meridiem = "am"
            end_meridiem = "am"
        else:
            start_meridiem = "pm"
            end_meridiem = "pm"

    start_time = f"{start_hour}:{start_minute:02d} {start_meridiem}"
    end_time = f"{end_hour}:{end_minute:02d} {end_meridiem}"
    return start_time, end_time


def _to_int(value: Any, *, default: int) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


__all__ = [name for name in globals() if not name.startswith("__")]
