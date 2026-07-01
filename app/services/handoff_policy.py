from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal, Sequence

from app.db.models import Client, Lead, Message
from app.services.i18n import client_language

HandoffLevel = Literal["none", "soft", "required"]

_LIMITS_KEY = "agent_limits"

_HUMAN_REQUEST_RE = re.compile(
    r"\b("
    r"human|real person|representative|manager|someone from (?:your|the) team|"
    r"talk to someone|speak to someone|call me|can someone call|have someone call|"
    r"humain|personne|quelqu'un|quelqu un|repr[ée]sentant|g[ée]rant|appelez-moi|"
    r"parler [àa] quelqu'un|parler a quelqu'un"
    r")\b",
    re.IGNORECASE,
)
_FRUSTRATION_RE = re.compile(
    r"\b("
    r"not helpful|you don'?t understand|do not understand|stop repeating|talking in circles|"
    r"this is frustrating|frustrated|useless|annoying|bad bot|"
    r"pas utile|vous ne comprenez pas|tu ne comprends pas|arr[êe]te de r[ée]p[ée]ter|"
    r"frustrant|inutile"
    r")\b",
    re.IGNORECASE,
)
_COMPLAINT_OR_ACCOUNT_RE = re.compile(
    r"\b("
    r"complaint|complain|refund|cancel my account|cancel my service|billing|invoice|charged|"
    r"chargeback|dispute|angry|upset|"
    r"plainte|remboursement|facture|facturation|annuler mon compte|contestation|f[âa]ch[ée]"
    r")\b",
    re.IGNORECASE,
)
_LEGAL_OR_BINDING_RE = re.compile(
    r"\b("
    r"legal|lawyer|attorney|lawsuit|liability|contract terms|contract language|"
    r"warranty|guarantee|guaranteed|can you guarantee|commit in writing|"
    r"juridique|avocat|poursuite|responsabilit[ée]|garantie|garantir|contrat"
    r")\b",
    re.IGNORECASE,
)
_CUSTOM_QUOTE_RE = re.compile(
    r"\b("
    r"firm quote|exact quote|custom quote|formal quote|official quote|written quote|proposal|bid|"
    r"devis|soumission officielle|soumission exacte|proposition officielle"
    r")\b",
    re.IGNORECASE,
)
_PRICING_RE = re.compile(
    r"\b(price|pricing|cost|quote|estimate|how much|rates?|fees?|prix|tarif|co[ûu]t|soumission|combien|estimation)\b",
    re.IGNORECASE,
)
_PRICE_AMOUNT_RE = re.compile(
    r"(\$\s?\d|\b\d[\d,]*(?:\.\d+)?\s?(?:cad|usd|dollars?)\b)",
    re.IGNORECASE,
)
_UNKNOWN_REPLY_RE = re.compile(
    r"\b("
    r"i don'?t know|i do not know|i'?m not sure|i am not sure|i don'?t have (?:that|this) information|"
    r"i cannot answer|i can'?t answer|not enough information|"
    r"je ne sais pas|je n'ai pas cette information|je ne peux pas r[ée]pondre|pas assez d'information"
    r")\b",
    re.IGNORECASE,
)
_BOOKING_FAILURE_REPLY_RE = re.compile(
    r"\b("
    r"couldn'?t match|could not match|did not catch which slot|current call options|"
    r"not seeing call openings|not seeing open call times|"
    r"pas pu associer|pas saisi quelle option|pas de disponibilit[ée]s"
    r")\b",
    re.IGNORECASE,
)
_RISKY_COMMITMENT_RE = re.compile(
    r"\b("
    r"we guarantee|i guarantee|guaranteed|definitely (?:can|will)|will definitely|"
    r"we will meet (?:that|the) deadline|we can promise|"
    r"nous garantissons|je garantis|garanti|c'est certain|nous respecterons le d[ée]lai"
    r")\b",
    re.IGNORECASE,
)
_NEGATED_GUARANTEE_RE = re.compile(
    r"\b(can'?t|cannot|do not|don'?t|not|no|ne peux pas|ne peut pas|pas)\b.{0,20}\b(guarantee|guaranteed|garantie|garantir)\b",
    re.IGNORECASE,
)
_MEDIA_REFERENCE_RE = re.compile(
    r"\b(see attached|attached|attachment|image|photo|picture|screenshot|schedule pic|photo ci-jointe|image jointe|capture|pi[èe]ce jointe)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class HandoffDecision:
    level: HandoffLevel = "none"
    reason: str = ""
    reply_text: str = ""
    summary: dict[str, Any] = field(default_factory=dict)
    state_updates: dict[str, Any] = field(default_factory=dict)

    @property
    def should_handoff(self) -> bool:
        return self.level in {"soft", "required"}


def handoff_policy_enabled(client: Client) -> bool:
    config = client.provider_config if isinstance(client.provider_config, dict) else {}
    raw = str(config.get("agent_handoff_policy_enabled", "true")).strip().lower()
    return raw not in {"0", "false", "off", "no"}


def evaluate_pre_llm_handoff(
    *,
    client: Client,
    lead: Lead,
    inbound_text: str,
    history: Sequence[Message],
    media_attachments: Sequence[dict[str, Any]] | None = None,
) -> HandoffDecision:
    if not handoff_policy_enabled(client):
        return HandoffDecision()

    text = " ".join(str(inbound_text or "").split()).strip()
    media = [item for item in media_attachments or [] if isinstance(item, dict)]

    if media and (not text or _MEDIA_REFERENCE_RE.search(text)):
        return _decision(
            client=client,
            lead=lead,
            inbound_text=text,
            history=history,
            level="required",
            reason="unsupported_media",
            reply_key="unsupported_media",
            media_attachments=media,
        )
    if _HUMAN_REQUEST_RE.search(text):
        return _decision(client=client, lead=lead, inbound_text=text, history=history, level="required", reason="explicit_human_request")
    if _FRUSTRATION_RE.search(text):
        return _decision(client=client, lead=lead, inbound_text=text, history=history, level="required", reason="frustration_or_confusion")
    if _COMPLAINT_OR_ACCOUNT_RE.search(text):
        return _decision(client=client, lead=lead, inbound_text=text, history=history, level="required", reason="complaint_or_account_issue")
    if _LEGAL_OR_BINDING_RE.search(text):
        return _decision(client=client, lead=lead, inbound_text=text, history=history, level="required", reason="legal_or_binding_request")
    if _CUSTOM_QUOTE_RE.search(text):
        return _decision(client=client, lead=lead, inbound_text=text, history=history, level="soft", reason="custom_quote_requested", reply_key="custom_quote")
    if _PRICING_RE.search(text) and not _pricing_context_available(client):
        limits = _limits(lead)
        if int(limits.get("pricing_without_context_count") or 0) >= 1:
            updates = _limit_update(lead, "pricing_without_context_count")
            return _decision(
                client=client,
                lead=lead,
                inbound_text=text,
                history=history,
                level="soft",
                reason="pricing_without_context_repeated",
                reply_key="pricing",
                extra_state_updates=updates,
            )
    return HandoffDecision()


def evaluate_post_llm_handoff(
    *,
    client: Client,
    lead: Lead,
    inbound_text: str,
    reply_text: str,
    history: Sequence[Message],
    runtime_payload: dict[str, Any] | None = None,
) -> HandoffDecision:
    _ = runtime_payload
    if not handoff_policy_enabled(client):
        return HandoffDecision()

    inbound = " ".join(str(inbound_text or "").split()).strip()
    reply = " ".join(str(reply_text or "").split()).strip()

    if _has_unsupported_commitment(reply):
        return _decision(
            client=client,
            lead=lead,
            inbound_text=inbound,
            history=history,
            level="required",
            reason="unsupported_commitment",
            reply_key="unsupported_commitment",
        )

    if _PRICE_AMOUNT_RE.search(reply) and not _pricing_context_available(client):
        updates = _limit_update(lead, "pricing_without_context_count")
        return _decision(
            client=client,
            lead=lead,
            inbound_text=inbound,
            history=history,
            level="soft",
            reason="pricing_without_context",
            reply_key="pricing",
            extra_state_updates=updates,
        )

    if _PRICING_RE.search(inbound) and not _pricing_context_available(client):
        updates = _limit_update(lead, "pricing_without_context_count")
        current_count = int((updates.get(_LIMITS_KEY) or {}).get("pricing_without_context_count") or 0)
        if current_count >= 2:
            return _decision(
                client=client,
                lead=lead,
                inbound_text=inbound,
                history=history,
                level="soft",
                reason="pricing_without_context_repeated",
                reply_key="pricing",
                extra_state_updates=updates,
            )
        return HandoffDecision(state_updates=updates)

    if _UNKNOWN_REPLY_RE.search(reply):
        updates = _limit_update(lead, "unknown_answer_count")
        current_count = int((updates.get(_LIMITS_KEY) or {}).get("unknown_answer_count") or 0)
        if current_count >= 2:
            return _decision(
                client=client,
                lead=lead,
                inbound_text=inbound,
                history=history,
                level="soft",
                reason="repeated_unknown_answer",
                reply_key="unknown",
                extra_state_updates=updates,
            )
        return HandoffDecision(state_updates=updates)

    if _BOOKING_FAILURE_REPLY_RE.search(reply):
        updates = _limit_update(lead, "booking_retry_count")
        current_count = int((updates.get(_LIMITS_KEY) or {}).get("booking_retry_count") or 0)
        if current_count >= 3:
            return _decision(
                client=client,
                lead=lead,
                inbound_text=inbound,
                history=history,
                level="soft",
                reason="booking_loop_limit",
                reply_key="booking_loop",
                extra_state_updates=updates,
            )
        return HandoffDecision(state_updates=updates)

    return HandoffDecision()


def build_handoff_state(decision: HandoffDecision, *, created_at: str) -> dict[str, Any]:
    if not decision.should_handoff:
        return {}
    return {
        "needed": True,
        "level": decision.level,
        "reason": decision.reason,
        "created_at": created_at,
        "summary": decision.summary,
    }


def _decision(
    *,
    client: Client,
    lead: Lead,
    inbound_text: str,
    history: Sequence[Message],
    level: HandoffLevel,
    reason: str,
    reply_key: str | None = None,
    media_attachments: Sequence[dict[str, Any]] | None = None,
    extra_state_updates: dict[str, Any] | None = None,
) -> HandoffDecision:
    language = client_language(client, lead=lead, inbound_text=inbound_text)
    summary = _summary(
        client=client,
        lead=lead,
        inbound_text=inbound_text,
        history=history,
        level=level,
        reason=reason,
        media_attachments=media_attachments,
    )
    return HandoffDecision(
        level=level,
        reason=reason,
        reply_text=_reply(language=language, key=reply_key or reason, level=level),
        summary=summary,
        state_updates=extra_state_updates or {},
    )


def _reply(*, language: str, key: str, level: HandoffLevel) -> str:
    if language == "fr":
        if key == "unsupported_media":
            return "J'ai bien reçu la pièce jointe, mais je ne peux pas encore analyser les images dans ce chat. Je vais la signaler à l'équipe pour qu'une personne puisse la vérifier."
        if key == "custom_quote":
            return "Pour un devis ferme, je ne veux pas deviner. Je vais transmettre les détails à l'équipe pour qu'une personne puisse vous revenir correctement."
        if key == "pricing":
            return "Je ne veux pas inventer un prix. Je vais transmettre votre demande à l'équipe pour qu'une personne puisse vous donner une réponse plus fiable."
        if key == "booking_loop":
            return "On semble tourner en rond côté disponibilités. Je vais faire suivre ça à l'équipe pour qu'une personne puisse coordonner le meilleur moment avec vous."
        if key == "unknown":
            return "Je ne veux pas vous donner une réponse approximative. Je vais faire suivre votre question à l'équipe."
        if key == "unsupported_commitment":
            return "Je ne peux pas confirmer cet engagement automatiquement. Je vais demander à l'équipe de vérifier et de vous répondre."
        if level == "soft":
            return "C'est probablement mieux qu'une personne de l'équipe vérifie ça directement. Je vais leur transmettre les détails."
        return "Je ne veux pas deviner là-dessus. Je vais transmettre les détails à l'équipe pour qu'une personne puisse vous répondre correctement."

    if key == "unsupported_media":
        return "I received the attachment, but I can't analyze media in this chat yet. I'll flag it for the team so someone can review it."
    if key == "custom_quote":
        return "For a firm quote, I don't want to guess. I'll pass the details to the team so someone can follow up properly."
    if key == "pricing":
        return "I don't want to invent pricing. I'll pass this to the team so someone can give you a more reliable answer."
    if key == "booking_loop":
        return "It looks like we're going in circles on availability. I'll flag this for the team so someone can help coordinate the best time."
    if key == "unknown":
        return "I don't want to give you a rough answer on that. I'll flag your question for the team."
    if key == "unsupported_commitment":
        return "I can't confirm that commitment automatically. I'll have the team review it and follow up."
    if level == "soft":
        return "That is probably best reviewed by someone on the team directly. I'll pass the details along."
    return "I don't want to guess on that. I'll flag this for the team so someone can follow up properly."


def _summary(
    *,
    client: Client,
    lead: Lead,
    inbound_text: str,
    history: Sequence[Message],
    level: HandoffLevel,
    reason: str,
    media_attachments: Sequence[dict[str, Any]] | None,
) -> dict[str, Any]:
    raw_payload = lead.raw_payload if isinstance(lead.raw_payload, dict) else {}
    return {
        "level": level,
        "reason": reason,
        "business_name": client.business_name,
        "lead": {
            "name": lead.full_name,
            "phone": lead.phone,
            "email": lead.email,
            "city": lead.city,
            "source": getattr(lead.source, "value", str(lead.source)),
        },
        "form_answers": lead.form_answers or {},
        "last_inbound": inbound_text,
        "conversation_state": lead.conversation_state.value if lead.conversation_state else "",
        "intent_level": raw_payload.get("intent_level"),
        "lead_summary": raw_payload.get("lead_summary"),
        "recent_messages": [
            {
                "direction": getattr(message.direction, "value", str(message.direction)),
                "body": " ".join(str(message.body or "").split()),
            }
            for message in history[-6:]
        ],
        "media_attachments": list(media_attachments or []),
        "recommended_follow_up": _recommended_follow_up(reason=reason),
    }


def _recommended_follow_up(*, reason: str) -> str:
    if reason == "unsupported_media":
        return "Review the attachment and reply with any schedule or project details the AI could not read."
    if reason in {"pricing_without_context", "pricing_without_context_repeated", "custom_quote_requested"}:
        return "Review scope and provide pricing guidance or ask for missing quote details."
    if reason in {"booking_loop_limit", "explicit_human_request"}:
        return "Contact the lead directly to coordinate next steps."
    if reason == "unsupported_commitment":
        return "Verify feasibility before making any commitment."
    return "Review the conversation and follow up with the lead."


def _limits(lead: Lead) -> dict[str, Any]:
    raw_payload = lead.raw_payload if isinstance(lead.raw_payload, dict) else {}
    limits = raw_payload.get(_LIMITS_KEY) if isinstance(raw_payload.get(_LIMITS_KEY), dict) else {}
    return dict(limits)


def _limit_update(lead: Lead, key: str) -> dict[str, Any]:
    limits = _limits(lead)
    limits[key] = int(limits.get(key) or 0) + 1
    return {_LIMITS_KEY: limits}


def _pricing_context_available(client: Client) -> bool:
    ai_context = " ".join(str(getattr(client, "ai_context", "") or "").split()).strip()
    if not ai_context:
        return False
    if re.search(r"\b(no|do not|don't|never)\s+(?:discuss|mention|share|talk about)?\s*(?:pricing|prices?|costs?|rates?|fees?|budget)\b", ai_context, re.IGNORECASE):
        return False
    if _PRICE_AMOUNT_RE.search(ai_context):
        return True
    return bool(re.search(r"\b(pricing|prices?|rates?|fees?|packages?|plans?)\s*[:=-]\s*\S", ai_context, re.IGNORECASE))


def _has_unsupported_commitment(reply_text: str) -> bool:
    if not _RISKY_COMMITMENT_RE.search(reply_text or ""):
        return False
    return not _NEGATED_GUARANTEE_RE.search(reply_text or "")
