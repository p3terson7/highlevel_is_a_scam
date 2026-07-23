from __future__ import annotations

from sqlalchemy import func, select

from app.db.models import CalendarBooking, Client, Lead, LeadTag
from app.db.session import get_session_factory
from app.services.crm import CRM_STAGE_CONTACTED, CRM_STAGE_MEETING_BOOKED, CRM_STAGE_WON
from app.services.stackleads_demo_seed import (
    CLIENT_KEY,
    DEMO_PREFIX,
    PORTAL_EMAIL,
    PORTAL_PASSWORD,
    ZAPIER_BOOKING_WEBHOOK_URL,
    seed_stackleads_demo_data,
)


def _admin_headers() -> dict[str, str]:
    return {"X-Admin-Token": "test-admin-token-32-characters-long!"}


def test_stackleads_demo_seed_creates_speed_to_lead_showcase(test_context):
    session_factory = get_session_factory()
    with session_factory() as db:
        result = seed_stackleads_demo_data(db, reset=True)
        db.commit()

    assert result["seeded"] is True
    assert result["client_key"] == CLIENT_KEY
    assert result["seeded_leads"] == 31
    assert result["recommended_showcase_lead"] == "Nina Alvarez"

    with session_factory() as db:
        client = db.scalar(select(Client).where(Client.client_key == CLIENT_KEY))
        assert client is not None
        assert client.business_name == "StackLeads"
        assert client.portal_enabled is True
        if ZAPIER_BOOKING_WEBHOOK_URL:
            assert client.provider_config["zapier_booking_webhook_url"] == ZAPIER_BOOKING_WEBHOOK_URL
        else:
            assert "zapier_booking_webhook_url" not in client.provider_config
        ad_report = client.provider_config["demo_ad_campaign_reports"]
        assert ad_report["platform"] == "Facebook Lead Ads"
        assert len(ad_report["campaigns"]) == 5

        leads = db.scalars(
            select(Lead).where(
                Lead.client_id == client.id,
                Lead.external_lead_id.like(f"{DEMO_PREFIX}-%"),
            )
        ).all()
        assert len(leads) == 31

        fast = next(lead for lead in leads if lead.external_lead_id == f"{DEMO_PREFIX}-eight-minute-booking")
        stale = next(lead for lead in leads if lead.external_lead_id == f"{DEMO_PREFIX}-twenty-four-hour-cooldown")
        assert fast.crm_stage == CRM_STAGE_MEETING_BOOKED
        assert stale.crm_stage == CRM_STAGE_CONTACTED
        assert fast.initial_sms_sent_at is not None
        assert stale.initial_sms_sent_at is not None
        assert (stale.initial_sms_sent_at - stale.created_at).total_seconds() >= 24 * 60 * 60

        fast_tags = {
            tag.tag
            for tag in db.scalars(select(LeadTag).where(LeadTag.lead_id == fast.id)).all()
        }
        assert {"hot", "speed-to-lead", "booked"}.issubset(fast_tags)

        booking = db.scalar(select(CalendarBooking).where(CalendarBooking.lead_id == fast.id))
        assert booking is not None
        assert booking.title == "Strategy call - Nina Alvarez"

        bookings_total = db.scalar(
            select(func.count(CalendarBooking.id)).where(CalendarBooking.client_id == client.id)
        )
        won_total = sum(1 for lead in leads if lead.crm_stage == CRM_STAGE_WON)
        booked_total = sum(1 for lead in leads if lead.crm_stage == CRM_STAGE_MEETING_BOOKED)
        assert bookings_total >= 14
        assert booked_total >= 8
        assert won_total >= 4

    dashboard = test_context.client.get("/ui/api/dashboard", headers=_admin_headers())
    assert dashboard.status_code == 200
    performance = dashboard.json()["campaign_performance"]
    assert performance["totals"]["campaigns"] == 5
    assert performance["totals"]["conversions"] > 0
    assert performance["totals"]["spend"] > 0
    assert len(performance["campaigns"]) == 5


def test_stackleads_demo_portal_login_and_password_rotation(test_context):
    session_factory = get_session_factory()
    with session_factory() as db:
        seed_stackleads_demo_data(db, reset=True)
        db.commit()

    login = test_context.client.post(
        "/ui/api/login/client",
        json={"email": PORTAL_EMAIL, "password": PORTAL_PASSWORD},
    )
    assert login.status_code == 200
    assert login.json()["session"]["client_key"] == CLIENT_KEY

    new_password = "ChangedStackLeads2026!"
    update = test_context.client.patch(
        f"/admin/clients/{CLIENT_KEY}",
        headers=_admin_headers(),
        json={"portal_password": new_password, "portal_enabled": True},
    )
    assert update.status_code == 200
    assert update.json()["portal_password_configured"] is True

    old_login = test_context.client.post(
        "/ui/api/login/client",
        json={"email": PORTAL_EMAIL, "password": PORTAL_PASSWORD},
    )
    assert old_login.status_code == 401

    new_login = test_context.client.post(
        "/ui/api/login/client",
        json={"email": PORTAL_EMAIL, "password": new_password},
    )
    assert new_login.status_code == 200
    assert new_login.json()["session"]["client_key"] == CLIENT_KEY

    with session_factory() as db:
        reseed = seed_stackleads_demo_data(db, reset=False)
        db.commit()
    assert reseed["seeded"] is False
    assert reseed["portal_credentials_reset"] is False
    assert reseed["portal_password"] is None

    still_rotated = test_context.client.post(
        "/ui/api/login/client",
        json={"email": PORTAL_EMAIL, "password": new_password},
    )
    assert still_rotated.status_code == 200

    with session_factory() as db:
        portal_reset = seed_stackleads_demo_data(db, reset_portal=True)
        db.commit()
    assert portal_reset["portal_credentials_reset"] is True
    assert portal_reset["portal_password"] == PORTAL_PASSWORD

    default_login = test_context.client.post(
        "/ui/api/login/client",
        json={"email": PORTAL_EMAIL, "password": PORTAL_PASSWORD},
    )
    assert default_login.status_code == 200
