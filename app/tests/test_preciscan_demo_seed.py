from __future__ import annotations

from sqlalchemy import func, select

from app.db.models import CalendarBooking, Client, Lead, LeadTag, Message, MessageDirection
from app.db.session import get_session_factory
from app.services.crm import CRM_STAGE_LOST, CRM_STAGE_MEETING_BOOKED, CRM_STAGE_WON
from app.services.preciscan_demo_seed import (
    CLIENT_KEY,
    DEMO_PREFIX,
    FORM_QUESTIONS,
    PORTAL_EMAIL,
    PORTAL_PASSWORD,
    seed_preciscan_demo_data,
)


def _admin_headers() -> dict[str, str]:
    return {"X-Admin-Token": "test-admin-token-32-characters-long!"}


def test_preciscan_demo_seed_creates_french_industrial_leads(test_context):
    session_factory = get_session_factory()
    with session_factory() as db:
        result = seed_preciscan_demo_data(db, reset=True)
        db.commit()

    assert result["seeded"] is True
    assert result["client_key"] == CLIENT_KEY
    assert result["seeded_leads"] == 10
    assert result["recommended_showcase_lead"] == "Isabelle Fortin"

    with session_factory() as db:
        client = db.scalar(select(Client).where(Client.client_key == CLIENT_KEY))
        assert client is not None
        assert client.business_name == "3D PreciScan"
        assert client.portal_email == PORTAL_EMAIL
        assert client.portal_enabled is True
        assert client.qualification_questions == FORM_QUESTIONS
        assert len(FORM_QUESTIONS) == 4

        leads = db.scalars(
            select(Lead).where(
                Lead.client_id == client.id,
                Lead.external_lead_id.like(f"{DEMO_PREFIX}-%"),
            )
        ).all()
        assert len(leads) == 10
        assert all(set(FORM_QUESTIONS) == set(lead.form_answers) for lead in leads)

        statuses = {lead.raw_payload["status"] for lead in leads}
        assert {
            "Nouveau",
            "Contacté",
            "À qualifier",
            "Appel réservé",
            "Soumission envoyée",
            "Gagné",
            "Perdu",
            "No-show",
        }.issubset(statuses)
        campaigns = {lead.raw_payload["campaign_name"] for lead in leads}
        assert "LinkedIn - Pièce sans plan CAD" in campaigns
        assert "LinkedIn - Inspection 3D" in campaigns
        assert "Meta Retargeting - Rétro-ingénierie" in campaigns
        assert "Facebook - Fournisseur disparu" in campaigns

        urgent = next(lead for lead in leads if lead.full_name == "Isabelle Fortin")
        assert urgent.crm_stage == CRM_STAGE_MEETING_BOOKED
        assert urgent.raw_payload["lead_score"] == 97
        assert urgent.raw_payload["estimated_value"] == 32000
        assert "arrêt de production" in urgent.form_answers["Quelle est votre situation actuelle?"]
        urgent_messages = db.scalars(
            select(Message)
            .where(Message.lead_id == urgent.id)
            .order_by(Message.created_at.asc(), Message.id.asc())
        ).all()
        assert urgent_messages
        assert urgent_messages[0].direction == MessageDirection.OUTBOUND
        assert all("Formulaire reçu" not in message.body for message in urgent_messages)

        won = next(lead for lead in leads if lead.full_name == "Pierre-Luc Simard")
        no_show = next(lead for lead in leads if lead.full_name == "Amélie Pelletier")
        assert won.crm_stage == CRM_STAGE_WON
        assert no_show.crm_stage == CRM_STAGE_LOST

        urgent_tags = {tag.tag for tag in db.scalars(select(LeadTag).where(LeadTag.lead_id == urgent.id)).all()}
        assert {"urgence-production", "hot", "step"}.issubset(urgent_tags)

        bookings_total = db.scalar(
            select(func.count(CalendarBooking.id)).where(CalendarBooking.client_id == client.id)
        )
        no_show_booking = db.scalar(select(CalendarBooking).where(CalendarBooking.lead_id == no_show.id))
        assert bookings_total == 5
        assert no_show_booking is not None
        assert no_show_booking.status == "no_show"


def test_preciscan_seed_endpoint_and_idempotency(test_context):
    first = test_context.client.post(
        "/ui/api/seed-preciscan?reset=true",
        headers=_admin_headers(),
    )
    assert first.status_code == 200
    assert first.json()["seeded"] is True

    second = test_context.client.post(
        "/ui/api/seed-preciscan",
        headers=_admin_headers(),
    )
    assert second.status_code == 200
    assert second.json()["seeded"] is False
    assert second.json()["reason"] == "preciscan_demo_data_already_present"

    login = test_context.client.post(
        "/ui/api/login/client",
        json={"email": PORTAL_EMAIL, "password": PORTAL_PASSWORD},
    )
    assert login.status_code == 200
    assert login.json()["session"]["client_key"] == CLIENT_KEY


def test_regular_demo_seed_also_populates_preciscan(test_context):
    seed = test_context.client.post("/ui/api/seed-demo?reset=true", headers=_admin_headers())
    assert seed.status_code == 200
    payload = seed.json()
    assert CLIENT_KEY in payload["client_keys"]
    assert payload["preciscan"]["seeded"] is True
    assert payload["preciscan"]["seeded_leads"] == 10

    session_factory = get_session_factory()
    with session_factory() as db:
        client = db.scalar(select(Client).where(Client.client_key == CLIENT_KEY))
        assert client is not None
        leads_total = db.scalar(
            select(func.count(Lead.id)).where(
                Lead.client_id == client.id,
                Lead.external_lead_id.like(f"{DEMO_PREFIX}-%"),
            )
        )
    assert leads_total == 10
