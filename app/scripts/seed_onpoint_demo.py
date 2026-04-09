from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select

from app.db.models import (
    AuditLog,
    CalendarBooking,
    Client,
    ConversationState,
    ConversationStateEnum,
    Lead,
    LeadSource,
    LeadTag,
    LeadTask,
    Message,
    MessageDirection,
)
from app.db.session import get_session_factory
from app.services.crm import (
    CRM_STAGE_CONTACTED,
    CRM_STAGE_LOST,
    CRM_STAGE_MEETING_BOOKED,
    CRM_STAGE_MEETING_COMPLETED,
    CRM_STAGE_NEW_LEAD,
    CRM_STAGE_QUALIFIED,
    CRM_STAGE_WON,
    TASK_STATUS_DONE,
    TASK_STATUS_OPEN,
)
from app.services.portal_auth import hash_portal_password

CLIENT_KEY = "onpoint-building-data"
PORTAL_EMAIL = "mike@onpointbuildingdata.com"
PORTAL_PASSWORD = "OnPointDemo2026!"
DEMO_PREFIX = "onpoint-demo"
CLIENT_TIMEZONE = "America/Detroit"


@dataclass(frozen=True)
class DemoLeadSpec:
    slug: str
    full_name: str
    email: str
    phone: str
    city: str
    source: LeadSource
    crm_stage: str
    conversation_state: ConversationStateEnum
    form_answers: dict
    qualification_memory: dict
    tags: list[str]
    tasks: list[dict]
    notes: list[str]
    messages: list[dict]
    states: list[dict]
    booking: dict | None = None


def _detroit_datetime(days_offset: int, hour: int, minute: int) -> datetime:
    tz = ZoneInfo(CLIENT_TIMEZONE)
    now_local = datetime.now(tz)
    local_dt = datetime.combine(now_local.date() + timedelta(days=days_offset), time(hour, minute), tzinfo=tz)
    return local_dt.astimezone(timezone.utc)


def _client_faq_context() -> str:
    return (
        "OnPoint Building Data provides 3D laser scanning, building measurement, CAD as-built drawings, "
        "Revit/BIM models, site surveys, 360 photography, virtual tours, ADA documentation, equipment and asset "
        "inventory, and other existing-conditions documentation services. They use Leica 3D scanning workflows and "
        "support architects, builders, engineers, developers, institutions, owners, acquisitions teams, commercial "
        "real estate groups, and restaurant and retail rollouts. They are based in Grand Rapids, Michigan and serve "
        "projects in markets including Chicago, Detroit, New York City, Los Angeles, Denver, Miami, Portland, "
        "Seattle, Houston, and Las Vegas. Pricing depends on building size, site count, deliverable type, travel, "
        "and turnaround; do not quote fixed pricing over SMS."
    )


def _client_ai_context() -> str:
    return (
        "Speak like an experienced project consultant for scan-to-BIM and existing-conditions work. "
        "Use plain language. Be specific about how laser scanning, CAD as-builts, and Revit models help reduce "
        "field surprises and speed design coordination. Qualify one thing at a time: single site vs multi-site, "
        "deliverable type, building type and size, timeline, and decision-maker/contact path. "
        "Do not push booking too early. Once scope, timeline, and decision-maker are reasonably clear, offer to "
        "schedule a short scoping call."
    )


def _qualification_questions() -> list[str]:
    return [
        "Is this one building or multiple locations?",
        "Do you need CAD as-builts, Revit/BIM, or both?",
        "What kind of building is it, and roughly how large is it?",
        "What timeline are you working with for the scan or final deliverables?",
        "Are you the main decision-maker, and what is the best way to coordinate next steps?",
    ]


def _internal_booking_config() -> dict:
    return {
        "internal_calendar": {
            "slot_minutes": 30,
            "notice_minutes": 120,
            "horizon_days": 21,
            "availability": [
                {"day": 0, "start": "09:00", "end": "12:00", "enabled": True},
                {"day": 0, "start": "13:00", "end": "16:00", "enabled": True},
                {"day": 1, "start": "09:00", "end": "12:00", "enabled": True},
                {"day": 1, "start": "13:00", "end": "16:30", "enabled": True},
                {"day": 2, "start": "09:00", "end": "12:00", "enabled": True},
                {"day": 2, "start": "13:00", "end": "16:00", "enabled": True},
                {"day": 3, "start": "09:00", "end": "12:00", "enabled": True},
                {"day": 3, "start": "13:00", "end": "16:30", "enabled": True},
                {"day": 4, "start": "09:00", "end": "13:00", "enabled": True},
            ],
        }
    }


def _lead_specs() -> list[DemoLeadSpec]:
    return [
        DemoLeadSpec(
            slug="new-lead",
            full_name="Rachel Kim",
            email="rachel.kim@midtowndesign.co",
            phone="+13125550111",
            city="Chicago",
            source=LeadSource.META,
            crm_stage=CRM_STAGE_NEW_LEAD,
            conversation_state=ConversationStateEnum.NEW,
            form_answers={
                "project_type": "Restaurant renovation existing-conditions package",
                "deliverable_type": "CAD as-builts",
                "building_type": "restaurant",
                "approximate_size_sqft": "8500",
                "timeline": "pricing this month",
                "decision_maker_role": "project architect",
            },
            qualification_memory={
                "deliverable_type": "CAD as-builts",
                "building_type": "restaurant",
                "approximate_size_sqft": 8500,
                "timeline": "pricing this month",
                "decision_maker_role": "project architect",
            },
            tags=["new", "restaurant"],
            tasks=[
                {
                    "title": "Review new restaurant TI lead",
                    "description": "Confirm if Rachel needs field capture only or full scan-to-CAD scope.",
                    "due_date": date(2026, 4, 4),
                    "status": TASK_STATUS_OPEN,
                }
            ],
            notes=["Architect lead for a restaurant TI package. Good candidate for fast follow-up."],
            messages=[],
            states=[],
        ),
        DemoLeadSpec(
            slug="contacted",
            full_name="Marcus Bell",
            email="mbell@kindredhospitality.com",
            phone="+13125550112",
            city="Chicago",
            source=LeadSource.LINKEDIN,
            crm_stage=CRM_STAGE_CONTACTED,
            conversation_state=ConversationStateEnum.QUALIFYING,
            form_answers={
                "project_type": "Restaurant rollout documentation",
                "locations_scope": "Multiple locations",
                "timeline": "this month",
                "decision_maker_role": "development manager",
            },
            qualification_memory={
                "locations_scope": "multiple locations",
                "timeline": "this month",
                "decision_maker_role": "development manager",
            },
            tags=["multi-site", "restaurant"],
            tasks=[
                {
                    "title": "Follow up on deliverable type",
                    "description": "Marcus mentioned three Chicago sites; confirm whether they need CAD, BIM, or both.",
                    "due_date": date(2026, 4, 4),
                    "status": TASK_STATUS_OPEN,
                }
            ],
            notes=["Hospitality rollout lead. Could become a repeat multi-site account if the first package lands well."],
            messages=[
                {
                    "minutes": 0,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Hi Marcus — thanks for reaching out to OnPoint. We handle existing-conditions capture, CAD as-builts, and scan-to-BIM. Is this one building or multiple locations?",
                    "agent": {"action": "ask_next_question", "next_question_key": "locations_scope"},
                },
                {
                    "minutes": 12,
                    "direction": MessageDirection.INBOUND,
                    "body": "Three restaurant locations in Chicago.",
                },
                {
                    "minutes": 18,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Got it. Multi-site restaurant work is a strong fit for us. Do you need CAD as-builts, Revit/BIM, or both?",
                    "agent": {"action": "ask_next_question", "next_question_key": "deliverable_type"},
                },
            ],
            states=[
                {"minutes": 0, "previous": ConversationStateEnum.NEW, "new": ConversationStateEnum.QUALIFYING, "reason": "initial_ai_sms_sent"},
            ],
        ),
        DemoLeadSpec(
            slug="qualified",
            full_name="Alicia Monroe",
            email="alicia@cedarretailgroup.com",
            phone="+13125550113",
            city="Detroit",
            source=LeadSource.META,
            crm_stage=CRM_STAGE_QUALIFIED,
            conversation_state=ConversationStateEnum.QUALIFYING,
            form_answers={
                "project_type": "Retail fit-out existing conditions",
                "locations_scope": "One building",
                "deliverable_type": "CAD as-builts and Revit/BIM",
                "building_type": "retail box",
                "approximate_size_sqft": "12000",
                "timeline": "field capture next week; model 2 weeks after",
                "decision_maker_role": "owner rep",
                "preferred_contact_method": "email",
            },
            qualification_memory={
                "locations_scope": "one building",
                "deliverable_type": "CAD as-builts and Revit/BIM",
                "building_type": "retail box",
                "approximate_size_sqft": 12000,
                "timeline": "field capture next week; model 2 weeks after",
                "decision_maker_role": "owner rep",
                "preferred_contact_method": "email",
            },
            tags=["hot", "retail"],
            tasks=[
                {
                    "title": "Send booking nudge if Alicia goes quiet",
                    "description": "She is qualified and ready for a scoping call.",
                    "due_date": date(2026, 4, 4),
                    "status": TASK_STATUS_OPEN,
                }
            ],
            notes=["Qualified retail lead. Strong fit: one site, clear deliverables, clear timeline, owner-side contact."],
            messages=[
                {
                    "minutes": 0,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Hi Alicia — we can capture the existing conditions and turn them into the deliverables your team needs. Is this one building or multiple locations?",
                    "agent": {"action": "ask_next_question", "next_question_key": "locations_scope"},
                },
                {"minutes": 9, "direction": MessageDirection.INBOUND, "body": "Just one site."},
                {
                    "minutes": 14,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Understood. Do you need CAD as-builts, Revit/BIM, or both?",
                    "agent": {"action": "ask_next_question", "next_question_key": "deliverable_type"},
                },
                {"minutes": 21, "direction": MessageDirection.INBOUND, "body": "Both. It is a retail box around 12,000 sqft."},
                {
                    "minutes": 27,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Makes sense. When both CAD and Revit are needed, the scan helps keep the baseline consistent for design and coordination. What timeline are you working with for the scan and final model?",
                    "agent": {"action": "ask_next_question", "next_question_key": "timeline"},
                },
                {"minutes": 36, "direction": MessageDirection.INBOUND, "body": "Site visit next week, final model about two weeks after. I am the owner rep and email is easiest."},
                {
                    "minutes": 42,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Perfect. That gives us enough to scope it properly. I can send over a couple of times to review the project if you want.",
                    "agent": {"action": "offer_booking"},
                },
            ],
            states=[
                {"minutes": 0, "previous": ConversationStateEnum.NEW, "new": ConversationStateEnum.QUALIFYING, "reason": "initial_ai_sms_sent"},
            ],
        ),
        DemoLeadSpec(
            slug="meeting-booked",
            full_name="Derek Shaw",
            email="derek@atlaspropertypartners.com",
            phone="+13125550114",
            city="Grand Rapids",
            source=LeadSource.MANUAL,
            crm_stage=CRM_STAGE_MEETING_BOOKED,
            conversation_state=ConversationStateEnum.BOOKED,
            form_answers={
                "project_type": "Office tower existing conditions update",
                "locations_scope": "One building",
                "deliverable_type": "Revit/BIM",
                "building_type": "commercial office",
                "approximate_size_sqft": "28000",
                "timeline": "design kickoff in three weeks",
                "decision_maker_role": "asset manager",
                "preferred_contact_method": "email",
            },
            qualification_memory={
                "locations_scope": "one building",
                "deliverable_type": "Revit/BIM",
                "building_type": "commercial office",
                "approximate_size_sqft": 28000,
                "timeline": "design kickoff in three weeks",
                "decision_maker_role": "asset manager",
                "preferred_contact_method": "email",
            },
            tags=["booked", "office"],
            tasks=[
                {
                    "title": "Prepare scoping call notes",
                    "description": "Review tower floor count, access restrictions, and existing CAD availability.",
                    "due_date": date(2026, 4, 7),
                    "status": TASK_STATUS_OPEN,
                }
            ],
            notes=["Booked into Mike's calendar. Likely follow-on work if the first tower scan goes well."],
            messages=[
                {
                    "minutes": 0,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Hi Derek — thanks for reaching out. We handle existing-conditions capture for office properties and can take this through Revit if needed. Is this one building or part of a larger portfolio?",
                    "agent": {"action": "ask_next_question", "next_question_key": "locations_scope"},
                },
                {"minutes": 8, "direction": MessageDirection.INBOUND, "body": "One building. About 28,000 sqft and we need a Revit model."},
                {
                    "minutes": 15,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Great. That is a clean fit for scan-to-BIM. What timeline are you working with?",
                    "agent": {"action": "ask_next_question", "next_question_key": "timeline"},
                },
                {"minutes": 23, "direction": MessageDirection.INBOUND, "body": "Design kickoff is in three weeks."},
                {
                    "minutes": 30,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Perfect. I can book this directly. Here are the next available times: 1) Tue Apr 7 at 10:00 AM 2) Tue Apr 7 at 10:30 AM 3) Tue Apr 7 at 11:00 AM Reply with 1, 2, or 3.",
                    "agent": {"action": "send_booking_link"},
                },
                {"minutes": 36, "direction": MessageDirection.INBOUND, "body": "1"},
                {
                    "minutes": 38,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Booked. You are set for Tue Apr 7 at 10:00 AM. Confirmation will be sent to derek@atlaspropertypartners.com. Saved on our calendar.",
                    "agent": {"action": "mark_booked"},
                },
            ],
            states=[
                {"minutes": 0, "previous": ConversationStateEnum.NEW, "new": ConversationStateEnum.QUALIFYING, "reason": "initial_ai_sms_sent"},
                {"minutes": 30, "previous": ConversationStateEnum.QUALIFYING, "new": ConversationStateEnum.BOOKING_SENT, "reason": "calendar_booking_offer_sent"},
                {"minutes": 38, "previous": ConversationStateEnum.BOOKING_SENT, "new": ConversationStateEnum.BOOKED, "reason": "calendar_booking_created"},
            ],
            booking={"days_offset": 4, "hour": 10, "minute": 0, "duration_minutes": 30},
        ),
        DemoLeadSpec(
            slug="meeting-completed",
            full_name="Monica Reyes",
            email="monica.reyes@northshorehealth.org",
            phone="+13125550115",
            city="Detroit",
            source=LeadSource.META,
            crm_stage=CRM_STAGE_MEETING_COMPLETED,
            conversation_state=ConversationStateEnum.BOOKED,
            form_answers={
                "project_type": "Outpatient clinic expansion",
                "locations_scope": "One building",
                "deliverable_type": "CAD as-builts and Revit/BIM",
                "building_type": "hospital",
                "approximate_size_sqft": "42000",
                "timeline": "phased design over Q2",
                "decision_maker_role": "facilities director",
                "preferred_contact_method": "email",
            },
            qualification_memory={
                "locations_scope": "one building",
                "deliverable_type": "CAD as-builts and Revit/BIM",
                "building_type": "hospital",
                "approximate_size_sqft": 42000,
                "timeline": "phased design over q2",
                "decision_maker_role": "facilities director",
                "preferred_contact_method": "email",
            },
            tags=["proposal out", "institutional"],
            tasks=[
                {
                    "title": "Send clinic scope recap",
                    "description": "Prepare phased scan + model summary after completed scoping call.",
                    "due_date": date(2026, 4, 4),
                    "status": TASK_STATUS_OPEN,
                }
            ],
            notes=["Meeting completed. Monica wants a phased plan for field capture first, then Revit model packaging."],
            messages=[
                {
                    "minutes": 0,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Hi Monica — yes, we can support healthcare spaces. For this clinic expansion, is it one building or multiple connected buildings?",
                    "agent": {"action": "ask_next_question", "next_question_key": "locations_scope"},
                },
                {"minutes": 10, "direction": MessageDirection.INBOUND, "body": "One building. About 42,000 sqft and we need both CAD and Revit."},
                {
                    "minutes": 17,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Understood. That kind of scope is exactly where scan data helps keep existing conditions tight before design moves. What timeline are you working with?",
                    "agent": {"action": "ask_next_question", "next_question_key": "timeline"},
                },
                {"minutes": 25, "direction": MessageDirection.INBOUND, "body": "We need to phase design work over Q2. I am the facilities director."},
                {
                    "minutes": 31,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Thanks. I can send over a couple of times to review phasing, site access, and deliverables.",
                    "agent": {"action": "offer_booking"},
                },
            ],
            states=[
                {"minutes": 0, "previous": ConversationStateEnum.NEW, "new": ConversationStateEnum.QUALIFYING, "reason": "initial_ai_sms_sent"},
                {"minutes": 31, "previous": ConversationStateEnum.QUALIFYING, "new": ConversationStateEnum.BOOKING_SENT, "reason": "booking_link_sent"},
                {"minutes": 46, "previous": ConversationStateEnum.BOOKING_SENT, "new": ConversationStateEnum.BOOKED, "reason": "booking_confirmed"},
            ],
            booking={"days_offset": -2, "hour": 14, "minute": 0, "duration_minutes": 30},
        ),
        DemoLeadSpec(
            slug="won",
            full_name="Jason Patel",
            email="jason@unionrestaurantgroup.com",
            phone="+13125550116",
            city="New York City",
            source=LeadSource.LINKEDIN,
            crm_stage=CRM_STAGE_WON,
            conversation_state=ConversationStateEnum.BOOKED,
            form_answers={
                "project_type": "Restaurant portfolio scan package",
                "locations_scope": "Multiple locations",
                "deliverable_type": "CAD as-builts",
                "building_type": "restaurant",
                "approximate_size_sqft": "6500 average per site",
                "timeline": "pilot site this month",
                "decision_maker_role": "development director",
                "preferred_contact_method": "call",
            },
            qualification_memory={
                "locations_scope": "multiple locations",
                "deliverable_type": "CAD as-builts",
                "building_type": "restaurant",
                "timeline": "pilot site this month",
                "decision_maker_role": "development director",
                "preferred_contact_method": "call",
            },
            tags=["won", "multi-site"],
            tasks=[
                {
                    "title": "Kickoff awarded rollout",
                    "description": "Collect site list and confirm pilot restaurant access.",
                    "due_date": date(2026, 4, 8),
                    "status": TASK_STATUS_DONE,
                }
            ],
            notes=["Won account. Pilot restaurant approved and rollout package expected after first delivery."],
            messages=[
                {
                    "minutes": 0,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Hi Jason — thanks for reaching out. We support restaurant rollouts and can package consistent as-built documentation site by site. How many locations are you looking at for the pilot?",
                    "agent": {"action": "ask_next_question", "next_question_key": "locations_scope"},
                },
                {"minutes": 11, "direction": MessageDirection.INBOUND, "body": "We want to start with one pilot restaurant, then likely expand to 12 more."},
                {
                    "minutes": 18,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "That is a good rollout structure. For the pilot, do you want CAD as-builts, Revit/BIM, or both?",
                    "agent": {"action": "ask_next_question", "next_question_key": "deliverable_type"},
                },
                {"minutes": 24, "direction": MessageDirection.INBOUND, "body": "CAD as-builts first. If the pilot goes well, we may add Revit later."},
                {
                    "minutes": 31,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Perfect. Once the first site is documented well, it is much easier to scale the rollout. I can send over times for a short scoping call.",
                    "agent": {"action": "offer_booking"},
                },
            ],
            states=[
                {"minutes": 0, "previous": ConversationStateEnum.NEW, "new": ConversationStateEnum.QUALIFYING, "reason": "initial_ai_sms_sent"},
                {"minutes": 31, "previous": ConversationStateEnum.QUALIFYING, "new": ConversationStateEnum.BOOKING_SENT, "reason": "booking_link_sent"},
                {"minutes": 48, "previous": ConversationStateEnum.BOOKING_SENT, "new": ConversationStateEnum.BOOKED, "reason": "booking_confirmed"},
            ],
            booking={"days_offset": -6, "hour": 11, "minute": 30, "duration_minutes": 30},
        ),
        DemoLeadSpec(
            slug="lost",
            full_name="Ellen Brooks",
            email="ellen.brooks@example.com",
            phone="+13125550117",
            city="Grand Rapids",
            source=LeadSource.MANUAL,
            crm_stage=CRM_STAGE_LOST,
            conversation_state=ConversationStateEnum.HANDOFF,
            form_answers={
                "project_type": "Small condo remodel",
                "deliverable_type": "Basic sketch only",
                "building_type": "condo",
                "approximate_size_sqft": "900",
                "timeline": "as soon as possible",
            },
            qualification_memory={
                "deliverable_type": "Basic sketch only",
                "building_type": "condo",
                "approximate_size_sqft": 900,
                "timeline": "as soon as possible",
            },
            tags=["bad fit"],
            tasks=[],
            notes=["Lost as bad fit. Small residential scope outside OnPoint's ideal project size and service profile."],
            messages=[
                {
                    "minutes": 0,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Hi Ellen — thanks for reaching out. To point you in the right direction, what kind of property is this and roughly how large is it?",
                    "agent": {"action": "ask_next_question", "next_question_key": "building_type"},
                },
                {"minutes": 9, "direction": MessageDirection.INBOUND, "body": "It is a 900 sqft condo remodel and we just need a quick sketch."},
                {
                    "minutes": 14,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Thanks for the detail. We are usually the best fit for commercial, institutional, and multi-site documentation scopes, so this one may be better handled by a small residential drafting service.",
                    "agent": {"action": "handoff_to_human"},
                },
            ],
            states=[
                {"minutes": 0, "previous": ConversationStateEnum.NEW, "new": ConversationStateEnum.QUALIFYING, "reason": "initial_ai_sms_sent"},
                {"minutes": 14, "previous": ConversationStateEnum.QUALIFYING, "new": ConversationStateEnum.HANDOFF, "reason": "agent_transition"},
            ],
        ),
    ]


def _upsert_client(db) -> Client:
    client = db.scalar(select(Client).where(Client.client_key == CLIENT_KEY).limit(1))
    if client is None:
        client = Client(client_key=CLIENT_KEY, business_name="OnPoint Building Data")
        db.add(client)
        db.flush()

    client.business_name = "OnPoint Building Data"
    client.tone = "clear, practical, expert"
    client.timezone = CLIENT_TIMEZONE
    client.qualification_questions = _qualification_questions()
    client.booking_url = ""
    client.booking_mode = "internal"
    client.booking_config = _internal_booking_config()
    client.provider_config = {"website_url": "https://www.onpointbuildingdata.com"}
    client.fallback_handoff_number = "+16162230891"
    client.consent_text = "Reply STOP to opt out. Message/data rates may apply."
    client.portal_display_name = "Mike Smitt"
    client.portal_email = PORTAL_EMAIL
    client.portal_password_hash = hash_portal_password(PORTAL_PASSWORD)
    client.portal_enabled = True
    client.operating_hours = {"days": [0, 1, 2, 3, 4], "start": "08:00", "end": "18:00"}
    client.faq_context = _client_faq_context()
    client.ai_context = _client_ai_context()
    client.template_overrides = {}
    client.is_active = True
    db.flush()
    return client


def _reset_seeded_demo(db, client: Client) -> int:
    leads = db.scalars(
        select(Lead).where(
            Lead.client_id == client.id,
            Lead.external_lead_id.is_not(None),
            Lead.external_lead_id.like(f"{DEMO_PREFIX}-%"),
        )
    ).all()
    count = len(leads)
    for lead in leads:
        db.delete(lead)
    db.flush()
    return count


def _message_raw_payload(agent: dict | None) -> dict:
    if not agent:
        return {"seeded": True, "seed_group": DEMO_PREFIX}
    return {"seeded": True, "seed_group": DEMO_PREFIX, "agent": agent}


def _seed_lead(db, client: Client, spec: DemoLeadSpec, created_at: datetime) -> None:
    lead = Lead(
        client_id=client.id,
        external_lead_id=f"{DEMO_PREFIX}-{spec.slug}",
        source=spec.source,
        full_name=spec.full_name,
        phone=spec.phone,
        email=spec.email,
        city=spec.city,
        form_answers=spec.form_answers,
        raw_payload={
            "seeded": True,
            "seed_group": DEMO_PREFIX,
            "qualification_memory": spec.qualification_memory,
        },
        consented=True,
        opted_out=False,
        conversation_state=spec.conversation_state,
        crm_stage=spec.crm_stage,
        owner_name="Mike Smitt",
        created_at=created_at,
        updated_at=created_at,
    )
    db.add(lead)
    db.flush()

    first_outbound_at = None
    last_inbound_at = None
    last_outbound_at = None
    last_event_at = created_at

    for index, message in enumerate(spec.messages, start=1):
        at = created_at + timedelta(minutes=message["minutes"])
        db.add(
            Message(
                lead_id=lead.id,
                client_id=client.id,
                direction=message["direction"],
                body=message["body"],
                provider_message_sid=f"{DEMO_PREFIX.upper()}-{lead.id}-{index}",
                raw_payload=_message_raw_payload(message.get("agent")),
                created_at=at,
            )
        )
        last_event_at = max(last_event_at, at)
        if message["direction"] == MessageDirection.OUTBOUND:
            if first_outbound_at is None:
                first_outbound_at = at
            last_outbound_at = at
        else:
            last_inbound_at = at

    for state in spec.states:
        at = created_at + timedelta(minutes=state["minutes"])
        db.add(
            ConversationState(
                lead_id=lead.id,
                previous_state=state["previous"],
                new_state=state["new"],
                reason=state["reason"],
                metadata_json={"seeded": True, "seed_group": DEMO_PREFIX},
                created_at=at,
            )
        )
        last_event_at = max(last_event_at, at)

    for tag in spec.tags:
        db.add(LeadTag(lead_id=lead.id, client_id=client.id, tag=tag, created_at=created_at + timedelta(minutes=2)))

    for idx, task in enumerate(spec.tasks, start=1):
        due_date = task.get("due_date")
        created = created_at + timedelta(minutes=5 + idx)
        completed_at = created_at + timedelta(minutes=120 + idx) if task.get("status") == TASK_STATUS_DONE else None
        db.add(
            LeadTask(
                lead_id=lead.id,
                client_id=client.id,
                title=task["title"],
                description=task.get("description", ""),
                due_date=due_date,
                status=task.get("status", TASK_STATUS_OPEN),
                completed_at=completed_at,
                created_by="seed",
                created_at=created,
                updated_at=completed_at or created,
            )
        )
        last_event_at = max(last_event_at, completed_at or created)

    for note_index, note in enumerate(spec.notes, start=1):
        at = created_at + timedelta(minutes=40 + note_index)
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="internal_note",
                decision={"note": note, "actor_label": "Seed data"},
                created_at=at,
            )
        )
        last_event_at = max(last_event_at, at)

    if spec.crm_stage != CRM_STAGE_NEW_LEAD:
        at = created_at + timedelta(minutes=3)
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="crm_stage_auto_updated",
                decision={"previous_stage": CRM_STAGE_NEW_LEAD, "new_stage": spec.crm_stage, "reason": "seed_demo"},
                created_at=at,
            )
        )
        last_event_at = max(last_event_at, at)

    if spec.booking is not None:
        start_at = _detroit_datetime(spec.booking["days_offset"], spec.booking["hour"], spec.booking["minute"])
        end_at = start_at + timedelta(minutes=spec.booking["duration_minutes"])
        db.add(
            CalendarBooking(
                client_id=client.id,
                lead_id=lead.id,
                provider="internal",
                source="sms_ai",
                status="scheduled",
                start_at=start_at,
                end_at=end_at,
                timezone=CLIENT_TIMEZONE,
                title=f"Scoping call - {spec.full_name}",
                notes="Seeded demo booking",
                created_at=created_at + timedelta(minutes=38),
                updated_at=created_at + timedelta(minutes=38),
            )
        )
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="booking_confirmed",
                decision={"seeded": True, "start_at": start_at.isoformat(), "provider": "internal"},
                created_at=created_at + timedelta(minutes=39),
            )
        )
        last_event_at = max(last_event_at, created_at + timedelta(minutes=39))

    lead.initial_sms_sent_at = first_outbound_at
    lead.last_inbound_at = last_inbound_at
    lead.last_outbound_at = last_outbound_at
    lead.updated_at = last_event_at


def seed_onpoint_demo(*, reset: bool) -> dict:
    session_factory = get_session_factory()
    with session_factory() as db:
        client = _upsert_client(db)
        deleted = _reset_seeded_demo(db, client) if reset else 0

        specs = _lead_specs()
        start_times = [
            _detroit_datetime(-2, 9, 0),
            _detroit_datetime(-2, 11, 0),
            _detroit_datetime(-1, 10, 0),
            _detroit_datetime(-1, 14, 0),
            _detroit_datetime(-3, 13, 0),
            _detroit_datetime(-6, 11, 0),
            _detroit_datetime(-4, 15, 0),
        ]
        for spec, created_at in zip(specs, start_times, strict=True):
            _seed_lead(db, client, spec, created_at)

        db.commit()
        return {
            "client_key": client.client_key,
            "business_name": client.business_name,
            "portal_email": client.portal_email,
            "portal_password": PORTAL_PASSWORD,
            "booking_mode": client.booking_mode,
            "seeded_leads": len(specs),
            "deleted_previous_seeded_leads": deleted,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Create or refresh the OnPoint Building Data demo client.")
    parser.add_argument("--reset", action="store_true", help="Delete previous seeded OnPoint leads before reseeding.")
    args = parser.parse_args()
    result = seed_onpoint_demo(reset=args.reset)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
