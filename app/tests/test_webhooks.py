from sqlalchemy import select

from app.db.models import Lead, Message, MessageDirection
from app.db.session import get_session_factory


def test_meta_webhook_intake_and_initial_sms(test_context):
    payload = {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "leadgen_id": "meta-lead-001",
                            "field_data": [
                                {"name": "full_name", "values": ["Jane Prospect"]},
                                {"name": "phone_number", "values": ["+1 (555) 123-4567"]},
                                {"name": "email", "values": ["jane@example.com"]},
                                {"name": "city", "values": ["Austin"]},
                            ],
                        }
                    }
                ]
            }
        ]
    }

    response = test_context.client.post(f"/webhooks/meta/{test_context.client_key}", json=payload)

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = db.scalar(
            select(Lead).where(Lead.external_lead_id == "meta-lead-001", Lead.client_id == 1)
        )
        assert lead is not None
        assert lead.phone == "+15551234567"

        outbound_messages = db.scalars(
            select(Message).where(
                Message.lead_id == lead.id,
                Message.direction == MessageDirection.OUTBOUND,
            )
        ).all()
        assert len(outbound_messages) >= 1
