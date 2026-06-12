from __future__ import annotations

from sqlalchemy import select

from app.db.models import Client, ConversationStateEnum, Lead, LeadSource
from app.db.session import get_session_factory
from app.services.agent_v3 import LLMAgentV3
from app.services.booking import BookingService
from app.services.i18n import client_language, detect_language, format_datetime_for_language
from app.services.sms_service import SMSService, load_default_templates


class DummyProvider:
    def generate_json(self, *, system_prompt: str, user_prompt: str) -> dict:
        raise RuntimeError("not used")


class DummySmsProvider:
    def send_sms(self, to_number: str, body: str) -> str:
        return "SM-DUMMY"


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
        assert any(" à " in slot.display_time and " h " in slot.display_time for slot in offer.slots)


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
