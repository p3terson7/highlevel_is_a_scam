from sqlalchemy import select

from app.db.models import Lead, LeadSource, Message
from app.db.session import get_session_factory


def test_sms_inbound_agent_reply_flow(test_context):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = Lead(
            client_id=1,
            external_lead_id="meta-lead-002",
            source=LeadSource.META,
            full_name="John Reply",
            phone="+15557778888",
            email="john@example.com",
            city="Denver",
            form_answers={"interest": "roof replacement"},
            raw_payload={"source": "seed"},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.commit()

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+1 (555) 777-8888",
            "Body": "Can I book this week?",
            "MessageSid": "SM-IN-001",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["state"] == "BOOKING_SENT"
    assert test_context.fake_llm.calls == 1

    assert len(test_context.fake_sms.sent) >= 1
    assert "https://example.com/book" in test_context.fake_sms.sent[-1]["body"]

    with SessionLocal() as db:
        lead = db.scalar(select(Lead).where(Lead.phone == "+15557778888"))
        assert lead is not None
        assert lead.conversation_state.value == "BOOKING_SENT"

        messages = db.scalars(select(Message).where(Message.lead_id == lead.id)).all()
        assert len(messages) >= 2
