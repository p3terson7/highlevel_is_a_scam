from __future__ import annotations

import pytest
from sqlalchemy import select

from app.db.models import Client, ConversationStateEnum, Lead, LeadSource, Message, MessageDirection
from app.db.session import get_session_factory
from app.services.agent_v3 import LLMAgentV3
from app.services.agent_v3_helpers import (
    _apply_response_guardrails,
    _answer_then_explicit_expert_meeting_offer,
    _count_meeting_suggestions,
    _ensure_slot_fallback_line,
    _extract_from_form_answers,
    _has_booking_intent,
    _lead_asked_question,
    _latest_outbound_invites_meeting,
    _message_invites_meeting,
    _message_suggests_meeting,
    _normalize_text,
    _strip_meeting_cta,
    _trim_sms_text,
)
from app.services.agent_v3_types import _HANDOFF_PATTERN, _PRICING_PATTERN
from app.services.booking import BookingService
from app.services.i18n import client_language, detect_language, format_datetime_for_language, remember_lead_language
from app.services.lead_summary import normalize_form_answers
from app.services.sms_service import SMSService, load_default_templates


class DummyProvider:
    def generate_json(self, *, system_prompt: str, user_prompt: str) -> dict:
        raise RuntimeError("not used")


class DummySmsProvider:
    def send_sms(self, to_number: str, body: str) -> str:
        return "SM-DUMMY"


@pytest.mark.parametrize(
    "text",
    [
        "comment",
        "combien d'expérience avez-vous",
        "avez vous des exemples de projet",
        "pouvez-vous préciser",
        "quand intervenez-vous",
    ],
)
def test_bare_french_questions_are_detected_without_question_mark(text: str):
    assert _lead_asked_question(text) is True


def test_french_experience_question_is_not_misclassified_as_pricing():
    assert _PRICING_PATTERN.search("combien d'expérience avez-vous") is None
    assert _PRICING_PATTERN.search("combien ça coûte") is not None


@pytest.mark.parametrize(
    "text",
    ["ça coûte combien", "c'est combien", "quel est le prix", "quel tarif"],
)
def test_colloquial_french_pricing_questions_are_detected(text: str):
    assert _PRICING_PATTERN.search(text) is not None
    assert _lead_asked_question(text) is True


@pytest.mark.parametrize(
    "text",
    ["Mon manager décidera.", "Non, aucune autre personne.", "Le gérant doit valider."],
)
def test_decision_path_answers_are_not_misclassified_as_handoff(text: str):
    assert _HANDOFF_PATTERN.search(text) is None


@pytest.mark.parametrize(
    "text",
    ["Puis-je parler à une personne?", "I need a human now.", "Can someone from your team call me?"],
)
def test_explicit_human_requests_are_detected(text: str):
    assert _HANDOFF_PATTERN.search(text) is not None


def _internal_always_open_config() -> dict:
    return {
        "internal_calendar": {
            "slot_minutes": 30,
            "notice_minutes": 0,
            "horizon_days": 7,
            "availability": [
                {"day": day, "enabled": True, "start": "00:00", "end": "23:59"}
                for day in range(7)
            ],
        }
    }


def test_client_language_uses_workspace_setting_then_detects_french(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.provider_config = {"language": "fr"}
        assert client_language(client, inbound_text="hello") == "fr"

        client.provider_config = {}
        assert detect_language("Bonjour, j'ai une pièce urgente à scanner") == "fr"
        assert client_language(client, inbound_text="Bonjour, merci") == "fr"
        assert detect_language("10h00") == "fr"


def test_client_language_sticks_to_detected_lead_language(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.provider_config = {}
        lead = Lead(
            client_id=client.id,
            source=LeadSource.META,
            full_name="Marc Tremblay",
            phone="+15145550109",
            email="marc-stick@example.com",
            city="Montreal",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.flush()

        assert remember_lead_language(client, lead, inbound_text="Bonjour, 10h00 fonctionne") == "fr"
        assert lead.raw_payload["lead_language"] == "fr"
        assert client_language(client, lead=lead, inbound_text="lock it in") == "fr"


def test_sms_templates_render_in_workspace_language(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.provider_config = {"language": "fr"}
        service = SMSService(provider=DummySmsProvider(), templates=load_default_templates())

        body = service.render_template(client, "initial_sms", context={"first_name": "Marc"})

        assert body.startswith("Bonjour Marc")
        assert "merci d’avoir contacté" in body


def test_internal_booking_offer_uses_french_copy_and_time_format(test_context):
    SessionLocal = get_session_factory()
    booking_service = BookingService()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.provider_config = {"language": "fr"}
        client.booking_mode = "internal"
        client.booking_config = _internal_always_open_config()

        lead = Lead(
            client_id=client.id,
            source=LeadSource.META,
            full_name="Marc Tremblay",
            phone="+15145550100",
            email="marc@example.com",
            city="Montréal",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
        )
        db.add(lead)
        db.flush()

        offer = booking_service.offer_slots(client=client, lead=lead, db=db)

        assert "Je peux réserver un appel directement" in offer.reply_text
        assert "Répondez" in offer.reply_text
        assert "If none of those work" not in offer.reply_text
        assert "Times shown" not in offer.reply_text
        assert any(" à " in slot.display_time and " h " in slot.display_time for slot in offer.slots)


def test_slot_fallback_line_respects_french_language():
    reply = _ensure_slot_fallback_line(
        "Je peux réserver un appel de consultation. J'ai ces créneaux : 1) mercredi 24 juin à 10 h 00.",
        language="fr",
    )

    assert "If none of those work" not in reply
    assert "Si aucune option ne fonctionne" in reply


def test_slot_fallback_line_replaces_existing_english_suffix_in_french():
    reply = _ensure_slot_fallback_line(
        "Je peux réserver un appel. If none of those work, just send me a time that's better for you.",
        language="fr",
    )

    assert "If none of those work" not in reply
    assert reply.endswith("Si aucune option ne fonctionne, envoyez-moi simplement un moment qui vous convient mieux.")


def test_agent_preserves_complete_concatenated_sms_copy_beyond_320_characters():
    selection_instruction = "Répondez 1, 2 ou 3 pour réserver l'appel, ou envoyez l'heure exacte souhaitée."
    body = (
        "Je peux réserver un appel de consultation directement. "
        + "Plusieurs disponibilités pertinentes ont été trouvées cette semaine. " * 4
        + selection_instruction
    )

    assert 320 < len(body) < 1_600
    assert _trim_sms_text(body).endswith(selection_instruction)


def test_agent_reply_safety_limit_matches_supported_outbound_message_size():
    body = "x" * 1_700

    trimmed = _trim_sms_text(body)

    assert len(trimmed) == 1_600
    assert trimmed.endswith("...")


def test_agent_guardrails_localize_common_english_booking_copy_in_french():
    reply = _apply_response_guardrails(
        "Je peux réserver un appel. 1) Wed Jun 24 at 10:00 AM. Reply with 1 and I'll lock it in. If none of those work, just send me a time that's better for you. Times shown in EDT.",
        {"response_language": "fr"},
    )

    assert "Wed Jun" not in reply
    assert "10:00 AM" not in reply
    assert "Reply with" not in reply
    assert "If none of those work" not in reply
    assert "Times shown" not in reply
    assert "mercredi 24 juin à 10 h 00" in reply
    assert "Répondez 1 et je le réserve" in reply
    assert "Heures affichées en EDT" in reply


def test_initial_french_intro_is_not_prepended_when_model_already_introduced_agent():
    context = {
        "initial_outreach": True,
        "response_language": "fr",
        "lead_name": "Big John / Fonderie Laurentide",
        "business_name": "3D PreciScan",
        "agent_identity": {"name": "Hermes"},
    }
    draft = (
        "Bonjour Big John, ici Hermes, l’assistante de 3D PreciScan. "
        "J’ai bien noté votre demande."
    )

    reply = _apply_response_guardrails(draft, context)

    assert reply == draft
    assert reply.count("Bonjour") == 1
    assert _normalize_text(reply).count("ici hermes") == 1


def test_initial_french_intro_is_still_added_when_model_omits_it():
    context = {
        "initial_outreach": True,
        "response_language": "fr",
        "lead_name": "Big John / Fonderie Laurentide",
        "business_name": "3D PreciScan",
        "agent_identity": {"name": "Hermes"},
    }

    reply = _apply_response_guardrails("J’ai bien noté votre demande.", context)

    assert reply.startswith("Bonjour Big, ici Hermes, l'assistante de 3D PreciScan.")
    assert _normalize_text(reply).count("ici hermes") == 1


def test_agent_context_and_prompt_include_response_language(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        client.provider_config = {"language": "fr"}
        lead = Lead(
            client_id=client.id,
            source=LeadSource.LINKEDIN,
            full_name="Julie Gagnon",
            phone="+15145550101",
            email="julie@example.com",
            city="Laval",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.flush()

        agent = LLMAgentV3(provider=DummyProvider())
        context = agent._build_context(client=client, lead=lead, inbound_text="Bonjour, êtes-vous disponible mardi?", history=[], knowledge_context="")
        prompt = agent._build_decision_prompt(client=client)

        assert context["response_language"] == "fr"
        assert "Quebec-friendly French" in prompt
        assert "Bonjour {first_name}" in prompt


def test_agent_normalization_folds_french_accents_without_splitting_words():
    normalized = _normalize_text("Créneau, délai, arrêt et sélectionné")

    assert "creneau" in normalized
    assert "delai" in normalized
    assert "arret" in normalized
    assert "selectionne" in normalized


@pytest.mark.parametrize(
    "copy",
    [
        "Voulez-vous que je vous propose des horaires pour un rendez-vous avec un expert de 3D PreciScan?",
        "Je peux aussi vous proposer un créneau pour cadrer rapidement la prise en charge.",
    ],
)
def test_agent_recognizes_french_meeting_ctas_including_legacy_copy(copy: str):
    assert _message_suggests_meeting(copy) is True


@pytest.mark.parametrize(
    "copy",
    [
        "Voulez-vous que je vous aide à réserver un appel de cadrage avec un expert?",
        "Would you like me to help book a scoping call with an expert?",
        "Je peux aussi vous proposer un créneau pour cadrer rapidement la prise en charge.",
    ],
)
def test_visible_scoping_call_invitations_are_detected(copy: str):
    assert _message_invites_meeting(copy) is True


def test_factual_call_explanation_does_not_create_booking_consent_context():
    history = [
        Message(
            direction=MessageDirection.OUTBOUND,
            body="Un appel de cadrage dure normalement 20 minutes et sert à valider la faisabilité.",
            raw_payload={"agent": {"action": "offer_booking"}},
        )
    ]

    assert _message_suggests_meeting(history[0].body) is True
    assert _message_invites_meeting(history[0].body) is False
    assert _latest_outbound_invites_meeting(history) is False
    assert _count_meeting_suggestions(history) == 0
    assert _has_booking_intent("oui", allow_generic_confirmation=False) is False


def test_meeting_cta_stripping_preserves_factual_call_explanation():
    fact = "L'appel de cadrage dure 20 minutes et sert à valider la faisabilité."
    mixed = f"{fact} Voulez-vous que je vous aide à le réserver?"

    assert _strip_meeting_cta(fact, fallback="fallback") == fact
    assert _strip_meeting_cta(mixed, fallback="fallback") == fact


def test_answer_then_scoping_call_keeps_canonical_cta_inside_sms_limit():
    context = {"response_language": "fr", "business_name": "3D PreciScan"}
    question = "Voulez-vous que je vous aide à réserver un appel de cadrage avec un expert chez 3D PreciScan?"
    reply = _answer_then_explicit_expert_meeting_offer(
        "La numérisation permet de documenter précisément la géométrie. " * 40,
        context,
    )

    assert len(reply) <= 1_600
    assert reply.endswith(question)
    assert reply.casefold().count("appel de cadrage") == 1


def test_french_decision_question_does_not_count_as_a_meeting_offer():
    question = "Êtes-vous la personne qui prend la décision, et est-ce que quelqu'un d'autre devrait participer à l'appel?"

    assert _message_suggests_meeting(question) is False


@pytest.mark.parametrize("message", ["Je voudrais prendre rendez-vous.", "Avez-vous un créneau?"])
def test_explicit_french_scheduling_request_is_booking_intent(message: str):
    assert _has_booking_intent(message) is True


@pytest.mark.parametrize("reply", ["oui allez-y", "oui allez y", "allez-y"])
def test_french_affirmation_accepts_booking_only_with_meeting_context(reply: str):
    assert _has_booking_intent(reply, allow_generic_confirmation=True) is True
    assert _has_booking_intent(reply, allow_generic_confirmation=False) is False


@pytest.mark.parametrize("reply", ["oui", "ok", "parfait"])
def test_unrelated_bare_french_affirmation_is_not_booking_intent(reply: str):
    assert _has_booking_intent(reply, allow_generic_confirmation=False) is False


def test_preciscan_french_form_maps_qualification_fields_without_treating_joindre_as_join():
    answers = normalize_form_answers(
        {
            "Joindre des fichiers (Images, STL...)": (
                "Deux photos de la pièce brisée et une ancienne fiche fournisseur sont disponibles."
            ),
            "Délai de réalisation souhaité?": "Dans les 5 jours ouvrables",
            "La demande est-elle urgente?": "Oui — arrêt partiel de production",
            "Sélectionner les services requis": "Scan 3D, Rétro-ingénierie (3D & 2D), Modélisation",
        }
    )

    assert "delai_de_realisation_souhaite" in answers
    assert "selectionner_les_services_requis" in answers

    memory = _extract_from_form_answers(answers)

    assert memory.decision_makers is None
    assert memory.service_needed == "Scan 3D, Rétro-ingénierie (3D & 2D), Modélisation"
    assert memory.timeline is not None
    assert "5 jours" in memory.timeline
    assert memory.urgency_driver is not None
    assert "arrêt partiel" in memory.urgency_driver
