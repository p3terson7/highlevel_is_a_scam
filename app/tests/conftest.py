from __future__ import annotations

from dataclasses import dataclass

import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.core.deps import clear_dependency_caches, get_llm_agent, get_sms_service
from app.db.models import Base, Client, ConversationStateEnum
from app.db.session import get_engine, get_session_factory, reset_db_caches
from app.services.llm_agent import AgentAction, AgentResponse


class FakeSMSService:
    def __init__(self) -> None:
        self.sent: list[dict[str, str]] = []

    def render_template(self, client: Client, template_key: str, context: dict | None = None) -> str:
        templates = {
            "stop_confirmation": "You are unsubscribed.",
            "help_response": "Help message.",
            "after_hours": "We will reply tomorrow from {business_name}.",
            "initial_sms": "Hi from {business_name}.",
            "follow_up": "Follow up from {business_name}: {booking_url}",
        }
        template = templates.get(template_key, template_key)
        values = {
            "business_name": client.business_name,
            "booking_url": client.booking_url,
        }
        if context:
            values.update(context)
        return template.format(**values)

    def send_message(self, to_number: str, body: str) -> str:
        sid = f"SM{len(self.sent) + 1:06d}"
        self.sent.append({"to": to_number, "body": body, "sid": sid})
        return sid


class FakeLLMAgent:
    def __init__(self) -> None:
        self.calls = 0

    def next_reply(self, client: Client, lead, inbound_text: str, history):
        self.calls += 1
        _ = lead
        _ = history
        return AgentResponse(
            reply_text=f"Thanks for the message about '{inbound_text}'.",
            next_state=ConversationStateEnum.QUALIFYING,
            actions=[AgentAction(type="send_booking_link", payload={"url": client.booking_url})],
        )


@dataclass
class TestContext:
    client: TestClient
    fake_sms: FakeSMSService
    fake_llm: FakeLLMAgent
    client_key: str


@pytest.fixture
def test_context(tmp_path, monkeypatch) -> TestContext:
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+pysqlite:///{db_path.as_posix()}")
    monkeypatch.setenv("RQ_EAGER", "true")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "")
    monkeypatch.setenv("ADMIN_TOKEN", "test-admin-token")
    monkeypatch.setenv("AUTO_CREATE_TABLES", "false")

    get_settings.cache_clear()
    clear_dependency_caches()
    reset_db_caches()

    engine = get_engine()
    Base.metadata.create_all(engine)

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        seeded_client = Client(
            client_key="test-client-key",
            business_name="Acme Solar",
            tone="friendly",
            timezone="UTC",
            qualification_questions=[
                "What problem are you trying to solve?",
                "When would you like to start?",
            ],
            booking_url="https://example.com/book",
            fallback_handoff_number="+15550001111",
            consent_text="Reply STOP to opt out.",
            operating_hours={"days": [0, 1, 2, 3, 4, 5, 6], "start": "00:00", "end": "23:59"},
            faq_context="We install residential solar systems.",
            template_overrides={},
        )
        db.add(seeded_client)
        db.commit()

    from app.main import app

    fake_sms = FakeSMSService()
    fake_llm = FakeLLMAgent()
    app.dependency_overrides[get_sms_service] = lambda: fake_sms
    app.dependency_overrides[get_llm_agent] = lambda: fake_llm

    with TestClient(app) as client:
        yield TestContext(
            client=client,
            fake_sms=fake_sms,
            fake_llm=fake_llm,
            client_key="test-client-key",
        )

    app.dependency_overrides.clear()
