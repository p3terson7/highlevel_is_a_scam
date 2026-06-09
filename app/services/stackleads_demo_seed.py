from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

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
    CRM_STAGE_QUALIFIED,
    CRM_STAGE_WON,
    TASK_STATUS_DONE,
    TASK_STATUS_OPEN,
)
from app.services.portal_auth import hash_portal_password

CLIENT_KEY = "stackleads"
BUSINESS_NAME = "StackLeads"
DEMO_PREFIX = "stackleads-demo"
PORTAL_EMAIL = "demo@stackleads.local"
PORTAL_PASSWORD = "StackLeadsDemo2026!"
CLIENT_TIMEZONE = "America/Toronto"


@dataclass(frozen=True)
class StackLeadsLeadSpec:
    slug: str
    full_name: str
    email: str
    phone: str
    city: str
    source: LeadSource
    crm_stage: str
    conversation_state: ConversationStateEnum
    owner_name: str
    created_offset: timedelta
    form_answers: dict
    qualification_memory: dict
    tags: list[str]
    tasks: list[dict]
    notes: list[str]
    messages: list[dict]
    states: list[dict]
    audit_events: list[dict]
    booking: dict | None = None


def _today() -> date:
    return datetime.now(ZoneInfo(CLIENT_TIMEZONE)).date()


def _local_datetime(days_offset: int, hour: int, minute: int) -> datetime:
    tz = ZoneInfo(CLIENT_TIMEZONE)
    local_day = datetime.now(tz).date() + timedelta(days=days_offset)
    local_dt = datetime.combine(local_day, time(hour, minute), tzinfo=tz)
    return local_dt.astimezone(timezone.utc)


def _client_ai_context() -> str:
    return (
        "StackLeads sells growth systems for service businesses that need faster lead response, cleaner CRM data, "
        "and better conversion from paid and organic campaigns.\n"
        "- Lead with business impact: speed-to-lead, lead quality, pipeline visibility, and booked calls.\n"
        "- Ask one qualification question at a time. Do not overwhelm prospects with a full audit checklist.\n"
        "- Prioritize: lead source, current response time, monthly lead volume, current CRM, decision-maker, and "
        "desired next step.\n"
        "- Do not promise guaranteed revenue, ad results, or instant pipeline improvement. Offer a short strategy "
        "call when the lead has a clear growth bottleneck.\n"
        "- If a prospect needs implementation details, route to a strategist instead of explaining internal systems."
    )


def _client_faq_context() -> str:
    return (
        "StackLeads helps local and service businesses improve lead capture, CRM follow-up, attribution clarity, "
        "and appointment-setting workflows. Best-fit prospects already generate leads from channels like Meta Ads, "
        "Google Ads, LinkedIn, SEO, referrals, or outbound, but lose revenue because response time is slow, ownership "
        "is unclear, or follow-up is inconsistent. Strategy calls review lead sources, current CRM setup, follow-up "
        "speed, sales handoff, and whether automation can help without replacing human judgment."
    )


def _internal_booking_config() -> dict:
    return {
        "internal_calendar": {
            "slot_minutes": 30,
            "notice_minutes": 90,
            "horizon_days": 21,
            "availability": [
                {"day": 0, "start": "09:30", "end": "12:00", "enabled": True},
                {"day": 0, "start": "13:00", "end": "16:30", "enabled": True},
                {"day": 1, "start": "09:30", "end": "12:00", "enabled": True},
                {"day": 1, "start": "13:00", "end": "16:30", "enabled": True},
                {"day": 2, "start": "09:30", "end": "12:00", "enabled": True},
                {"day": 2, "start": "13:00", "end": "16:30", "enabled": True},
                {"day": 3, "start": "09:30", "end": "12:00", "enabled": True},
                {"day": 3, "start": "13:00", "end": "16:30", "enabled": True},
                {"day": 4, "start": "09:30", "end": "13:00", "enabled": True},
            ],
        }
    }


def _demo_ad_campaign_reports() -> dict:
    return {
        "source": "zapier_demo",
        "platform": "Facebook Lead Ads",
        "report_range": "Last 30 days",
        "last_synced_at": datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat(),
        "campaigns": [
            {
                "campaign_id": "fb-lg-speed-hvac",
                "campaign_name": "Speed-To-Lead Audit - HVAC",
                "status": "active",
                "objective": "Lead generation",
                "impressions": 48240,
                "reach": 32180,
                "clicks": 1916,
                "conversions": 124,
                "spend": 3846.70,
                "cpc": 2.01,
                "cost_per_conversion": 31.02,
            },
            {
                "campaign_id": "fb-lg-medspa-frontdesk",
                "campaign_name": "MedSpa Front Desk Recovery",
                "status": "active",
                "objective": "Lead generation",
                "impressions": 39780,
                "reach": 28450,
                "clicks": 1612,
                "conversions": 96,
                "spend": 2948.20,
                "cpc": 1.83,
                "cost_per_conversion": 30.71,
            },
            {
                "campaign_id": "fb-lg-legal-intake",
                "campaign_name": "Legal Intake Priority Routing",
                "status": "active",
                "objective": "Lead generation",
                "impressions": 27150,
                "reach": 20610,
                "clicks": 1018,
                "conversions": 54,
                "spend": 2437.40,
                "cpc": 2.39,
                "cost_per_conversion": 45.14,
            },
            {
                "campaign_id": "fb-lg-dental-consults",
                "campaign_name": "Dental Consult Booking Lift",
                "status": "learning",
                "objective": "Lead generation",
                "impressions": 33420,
                "reach": 24980,
                "clicks": 1426,
                "conversions": 82,
                "spend": 2211.10,
                "cpc": 1.55,
                "cost_per_conversion": 26.96,
            },
            {
                "campaign_id": "fb-lg-home-services",
                "campaign_name": "Home Services Missed Lead Rescue",
                "status": "active",
                "objective": "Lead generation",
                "impressions": 55910,
                "reach": 37490,
                "clicks": 2288,
                "conversions": 139,
                "spend": 4188.65,
                "cpc": 1.83,
                "cost_per_conversion": 30.13,
            },
        ],
    }


def _showcase_lead_spec(
    *,
    index: int,
    slug: str,
    full_name: str,
    company: str,
    domain: str,
    city: str,
    source: LeadSource,
    crm_stage: str,
    conversation_state: ConversationStateEnum,
    owner_name: str,
    created_offset: timedelta,
    lead_source: str,
    monthly_volume: str,
    response_time: str,
    crm: str,
    bottleneck: str,
    tags: list[str],
    task_title: str | None,
    task_due_date: date | None,
    task_status: str = TASK_STATUS_OPEN,
    booking: dict | None = None,
) -> StackLeadsLeadSpec:
    first_name = full_name.split(" ", maxsplit=1)[0]
    has_booking = conversation_state in {ConversationStateEnum.BOOKING_SENT, ConversationStateEnum.BOOKED}
    is_booked = conversation_state == ConversationStateEnum.BOOKED
    is_handoff = conversation_state == ConversationStateEnum.HANDOFF
    is_lost = crm_stage == CRM_STAGE_LOST
    created_minutes = 2 + (index % 5)
    reply_minutes = 9 + (index % 8)
    offer_minutes = 17 + (index % 7)
    booking_minutes = 25 + (index % 9)
    webhook_event = {
        LeadSource.META: "meta_webhook_received",
        LeadSource.LINKEDIN: "linkedin_webhook_received",
        LeadSource.MANUAL: "manual_lead_created",
        LeadSource.SMS: "sms_lead_created",
    }[source]
    event_source = "manual" if source == LeadSource.MANUAL else source.value

    states = [
        {
            "minutes": created_minutes,
            "previous": ConversationStateEnum.NEW,
            "new": ConversationStateEnum.GREETED,
            "reason": "initial_sms_sent",
        }
    ]
    audit_events = [
        {
            "minutes": 0,
            "event_type": webhook_event,
            "decision": {"campaign": "StackLeads revenue CRM showcase", "source": event_source},
        },
        {"minutes": 1, "event_type": "lead_normalized", "decision": {"source": event_source}},
        {"minutes": created_minutes, "event_type": "initial_sms_sent", "decision": {"speed_to_lead_seconds": created_minutes * 60}},
    ]
    messages = [
        {
            "minutes": created_minutes,
            "direction": MessageDirection.OUTBOUND,
            "body": (
                f"Hi {first_name}, this is StackLeads. Saw your note about {bottleneck}. "
                "How fast does your team usually respond after a new lead comes in?"
            ),
            "agent": {"action": "ask_next_question", "next_question_key": "current_response_time"},
        },
        {
            "minutes": reply_minutes,
            "direction": MessageDirection.INBOUND,
            "body": f"We are around {response_time}. Most leads come from {lead_source}, and volume is about {monthly_volume} monthly.",
        },
    ]

    if not is_lost:
        states.append(
            {
                "minutes": reply_minutes + 1,
                "previous": ConversationStateEnum.GREETED,
                "new": ConversationStateEnum.QUALIFYING,
                "reason": "meaningful_reply",
            }
        )
        audit_events.append(
            {
                "minutes": reply_minutes + 1,
                "event_type": "agent_decision",
                "decision": {"actions": [{"type": "request_more_info"}], "next_state": "QUALIFYING"},
            }
        )
        messages.append(
            {
                "minutes": reply_minutes + 1,
                "direction": MessageDirection.OUTBOUND,
                "body": f"That is enough activity for follow-up speed to matter. Are those leads managed in {crm}, or is there another handoff step?",
                "agent": {"action": "ask_next_question", "next_question_key": "current_crm"},
            }
        )
        messages.append(
            {
                "minutes": reply_minutes + 7,
                "direction": MessageDirection.INBOUND,
                "body": f"{crm}. The hard part is that {bottleneck}.",
            }
        )

    if has_booking:
        states.append(
            {
                "minutes": offer_minutes,
                "previous": ConversationStateEnum.QUALIFYING,
                "new": ConversationStateEnum.BOOKING_SENT,
                "reason": "booking_offer_sent",
            }
        )
        audit_events.append(
            {
                "minutes": offer_minutes,
                "event_type": "agent_decision",
                "decision": {"actions": [{"type": "send_booking_link"}], "next_state": "BOOKING_SENT"},
            }
        )
        messages.append(
            {
                "minutes": offer_minutes,
                "direction": MessageDirection.OUTBOUND,
                "body": "Makes sense. A short strategy call can map the lead source, routing rule, and follow-up gap. Want a few times?",
                "agent": {"action": "offer_booking"},
            }
        )

    if is_booked:
        states.append(
            {
                "minutes": booking_minutes,
                "previous": ConversationStateEnum.BOOKING_SENT,
                "new": ConversationStateEnum.BOOKED,
                "reason": "booking_confirmed",
            }
        )
        audit_events.append(
            {
                "minutes": booking_minutes,
                "event_type": "booking_confirmed",
                "decision": {"seeded": True, "slot": booking or "Strategy call"},
            }
        )
        messages.extend(
            [
                {"minutes": booking_minutes - 3, "direction": MessageDirection.INBOUND, "body": "Yes, send the earliest time."},
                {
                    "minutes": booking_minutes - 2,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "I can book this directly. Earliest options: 1) Tomorrow 9:30 AM 2) Tomorrow 1:30 PM 3) Friday 10:00 AM. Reply with 1, 2, or 3.",
                    "agent": {"action": "send_booking_link"},
                },
                {"minutes": booking_minutes - 1, "direction": MessageDirection.INBOUND, "body": "1"},
                {
                    "minutes": booking_minutes,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Booked. You are set for the strategy call. Bring your current lead sources and response-time estimate.",
                    "agent": {"action": "mark_booked"},
                },
            ]
        )
    elif is_handoff:
        states.append(
            {
                "minutes": offer_minutes,
                "previous": ConversationStateEnum.QUALIFYING,
                "new": ConversationStateEnum.HANDOFF,
                "reason": "senior_strategy_review",
            }
        )
        audit_events.append(
            {
                "minutes": offer_minutes,
                "event_type": "agent_decision",
                "decision": {"actions": [{"type": "handoff_to_human"}], "next_state": "HANDOFF"},
            }
        )
        messages.append(
            {
                "minutes": offer_minutes,
                "direction": MessageDirection.OUTBOUND,
                "body": "This is worth a strategist review before we recommend changes. I am flagging the context for a human follow-up.",
                "agent": {"action": "handoff_to_human"},
            }
        )
    elif is_lost:
        states.append(
            {
                "minutes": reply_minutes + 1,
                "previous": ConversationStateEnum.GREETED,
                "new": ConversationStateEnum.HANDOFF,
                "reason": "bad_fit_review",
            }
        )
        audit_events.append(
            {
                "minutes": reply_minutes + 1,
                "event_type": "crm_stage_updated",
                "decision": {"previous_stage": "Contacted", "new_stage": "Lost", "reason": "bad_fit"},
            }
        )
        messages.append(
            {
                "minutes": reply_minutes + 1,
                "direction": MessageDirection.OUTBOUND,
                "body": "Thanks for the context. Our CRM workflow is strongest once a steady lead source is live, so this is probably better after the first campaign is running.",
                "agent": {"action": "handoff_to_human"},
            }
        )

    tasks = []
    if task_title:
        tasks.append(
            {
                "title": task_title,
                "description": f"Review {lead_source}, {crm}, and the current response-time gap before next action.",
                "due_date": task_due_date,
                "status": task_status,
            }
        )

    form_answers = {
        "company": company,
        "primary_lead_source": lead_source,
        "monthly_lead_volume": monthly_volume,
        "current_response_time": response_time,
        "crm": crm,
        "growth_bottleneck": bottleneck,
    }
    return StackLeadsLeadSpec(
        slug=slug,
        full_name=full_name,
        email=f"{full_name.lower().replace(' ', '.')}@{domain}",
        phone=f"+1416555{1000 + index:04d}",
        city=city,
        source=source,
        crm_stage=crm_stage,
        conversation_state=conversation_state,
        owner_name=owner_name,
        created_offset=created_offset,
        form_answers=form_answers,
        qualification_memory=form_answers,
        tags=tags,
        tasks=tasks,
        notes=[f"{company} is a {crm_stage.lower()} StackLeads showcase lead: {bottleneck}."],
        messages=messages,
        states=states,
        audit_events=audit_events,
        booking=booking,
    )


def _landing_page_showcase_specs(today: date) -> list[StackLeadsLeadSpec]:
    rows = [
        ("mia-watson", "Mia Watson", "Summit Smile Studio", "summitsmile.example", "Toronto", LeadSource.META, CRM_STAGE_MEETING_BOOKED, ConversationStateEnum.BOOKED, "StackLeads Strategy", timedelta(hours=-5), "Meta Ads", "95-120", "18 minutes", "HubSpot", "consult requests are not routed by urgency", ["booked", "hot", "dental"], "Prep dental consult audit", today, TASK_STATUS_OPEN, {"days_offset": 1, "hour": 9, "minute": 30, "duration_minutes": 30}),
        ("caleb-foster", "Caleb Foster", "Foster Roofing Group", "fosterroofing.example", "London", LeadSource.META, CRM_STAGE_MEETING_BOOKED, ConversationStateEnum.BOOKED, "StackLeads Strategy", timedelta(hours=-7), "Google Ads", "140+", "9 minutes", "Jobber", "storm leads need instant assignment", ["booked", "high volume", "roofing"], "Review roofing lead routing", today, TASK_STATUS_OPEN, {"days_offset": 1, "hour": 11, "minute": 0, "duration_minutes": 30}),
        ("lena-kapoor", "Lena Kapoor", "BrightPath Physio", "brightpathphysio.example", "Markham", LeadSource.LINKEDIN, CRM_STAGE_QUALIFIED, ConversationStateEnum.BOOKING_SENT, "StackLeads Strategy", timedelta(hours=-9), "LinkedIn Ads", "45-60", "under 30 minutes", "Pipedrive", "qualified consults are buried with low-intent inquiries", ["booking sent", "clinic", "hot"], "Nudge clinic booking", today, TASK_STATUS_OPEN, None),
        ("owen-rivera", "Owen Rivera", "Rivera Legal Intake", "riveralegal.example", "Ottawa", LeadSource.LINKEDIN, CRM_STAGE_MEETING_BOOKED, ConversationStateEnum.BOOKED, "StackLeads Strategy", timedelta(days=-1, hours=-2), "LinkedIn Ads", "35-50", "same day", "Clio", "urgent cases are not prioritized", ["booked", "legal", "priority routing"], "Prepare legal intake map", today + timedelta(days=1), TASK_STATUS_OPEN, {"days_offset": 2, "hour": 13, "minute": 30, "duration_minutes": 30}),
        ("harper-nguyen", "Harper Nguyen", "Oak & Iron Fitness", "oakironfitness.example", "Mississauga", LeadSource.META, CRM_STAGE_WON, ConversationStateEnum.BOOKED, "StackLeads Strategy", timedelta(days=-8), "Instagram Ads", "160+", "10-15 minutes", "manual inbox", "membership trial leads are followed up inconsistently", ["won", "fitness", "fast follow-up"], "Launch membership follow-up sprint", today - timedelta(days=2), TASK_STATUS_DONE, {"days_offset": -6, "hour": 14, "minute": 0, "duration_minutes": 30}),
        ("isabella-morgan", "Isabella Morgan", "Morgan MedSpa", "morganmedspa.example", "Vaughan", LeadSource.META, CRM_STAGE_MEETING_COMPLETED, ConversationStateEnum.BOOKED, "StackLeads Strategy", timedelta(days=-3), "Meta Ads", "110-130", "20 minutes", "GoHighLevel", "front desk cannot see treatment intent quickly", ["proposal out", "medspa", "high intent"], "Send medspa proposal recap", today, TASK_STATUS_OPEN, {"days_offset": -1, "hour": 10, "minute": 0, "duration_minutes": 30}),
        ("noah-bennett", "Noah Bennett", "Bennett Basement Co.", "bennettbasement.example", "Burlington", LeadSource.META, CRM_STAGE_CONTACTED, ConversationStateEnum.QUALIFYING, "StackLeads Strategy", timedelta(hours=-14), "Google Local Services Ads", "55-70", "2 hours", "ServiceTitan", "estimate requests are not sorted by project value", ["home services", "qualifying"], "Confirm project value signals", today + timedelta(days=1), TASK_STATUS_OPEN, None),
        ("ava-desai", "Ava Desai", "Desai Orthodontics", "desaiortho.example", "Scarborough", LeadSource.META, CRM_STAGE_MEETING_BOOKED, ConversationStateEnum.BOOKED, "StackLeads Strategy", timedelta(days=-2, hours=-3), "Meta Ads", "75-90", "45 minutes", "HubSpot", "parent inquiries need faster consult booking", ["booked", "dental", "speed-to-lead"], "Prep ortho booking review", today + timedelta(days=2), TASK_STATUS_OPEN, {"days_offset": 3, "hour": 10, "minute": 30, "duration_minutes": 30}),
        ("liam-ross", "Liam Ross", "Ross Injury Law", "rossinjurylaw.example", "Toronto", LeadSource.LINKEDIN, CRM_STAGE_QUALIFIED, ConversationStateEnum.HANDOFF, "Senior Strategist", timedelta(days=-1, hours=-6), "SEO and PPC", "65-85", "same day", "Lawmatics", "case source and intake ownership conflict", ["needs handoff", "legal", "attribution"], "Strategist case-source review", today, TASK_STATUS_OPEN, None),
        ("zoe-martin", "Zoe Martin", "Northline Plumbing", "northlineplumbing.example", "Etobicoke", LeadSource.META, CRM_STAGE_MEETING_BOOKED, ConversationStateEnum.BOOKED, "StackLeads Strategy", timedelta(days=-1, hours=-9), "Google Ads", "120+", "12 minutes", "Housecall Pro", "emergency calls and quote leads need separate flows", ["booked", "after-hours", "plumbing"], "Review emergency routing", today + timedelta(days=1), TASK_STATUS_OPEN, {"days_offset": 2, "hour": 15, "minute": 0, "duration_minutes": 30}),
        ("jackson-lee", "Jackson Lee", "Premier Auto Detail", "premierdetail.example", "Richmond Hill", LeadSource.META, CRM_STAGE_WON, ConversationStateEnum.BOOKED, "StackLeads Strategy", timedelta(days=-12), "Instagram Ads", "90-110", "under 10 minutes", "Airtable", "premium package leads were not segmented", ["won", "automotive", "segmentation"], "Complete package segmentation setup", today - timedelta(days=4), TASK_STATUS_DONE, {"days_offset": -9, "hour": 11, "minute": 30, "duration_minutes": 30}),
        ("emily-price", "Emily Price", "Price Renovation Studio", "pricereno.example", "Kitchener", LeadSource.LINKEDIN, CRM_STAGE_QUALIFIED, ConversationStateEnum.BOOKING_SENT, "StackLeads Strategy", timedelta(hours=-18), "LinkedIn Ads", "30-40", "1 hour", "Notion CRM", "designer and estimator handoffs are unclear", ["booking sent", "renovation", "handoff"], "Nudge renovation strategy booking", today + timedelta(days=1), TASK_STATUS_OPEN, None),
        ("samuel-clark", "Samuel Clark", "Clark Chiropractic", "clarkchiro.example", "Waterloo", LeadSource.META, CRM_STAGE_CONTACTED, ConversationStateEnum.QUALIFYING, "StackLeads Strategy", timedelta(days=-2, hours=-5), "Meta Ads", "50-65", "next business morning", "Jane App", "new patient leads need better first response", ["clinic", "speed-to-lead"], "Ask about first-response owner", today + timedelta(days=1), TASK_STATUS_OPEN, None),
        ("victoria-hall", "Victoria Hall", "Hall Wealth Advisors", "hallwealth.example", "Toronto", LeadSource.LINKEDIN, CRM_STAGE_MEETING_COMPLETED, ConversationStateEnum.BOOKED, "Senior Strategist", timedelta(days=-4), "LinkedIn outbound", "25-35", "same day", "Salesforce", "lead quality is high but follow-up steps vary by advisor", ["proposal out", "financial services", "high value"], "Send advisor workflow recap", today, TASK_STATUS_OPEN, {"days_offset": -2, "hour": 13, "minute": 0, "duration_minutes": 30}),
        ("grace-wilson", "Grace Wilson", "Wilson Home Care", "wilsonhomecare.example", "Oshawa", LeadSource.META, CRM_STAGE_MEETING_BOOKED, ConversationStateEnum.BOOKED, "StackLeads Strategy", timedelta(days=-2, hours=-1), "Facebook Ads", "80-100", "35 minutes", "HubSpot", "care inquiries are not assigned by location", ["booked", "home care", "routing"], "Prepare location routing review", today + timedelta(days=2), TASK_STATUS_OPEN, {"days_offset": 4, "hour": 9, "minute": 30, "duration_minutes": 30}),
        ("adam-pierce", "Adam Pierce", "Pierce Pest Control", "piercepest.example", "Hamilton", LeadSource.META, CRM_STAGE_QUALIFIED, ConversationStateEnum.BOOKING_SENT, "StackLeads Strategy", timedelta(hours=-22), "Google Ads", "100+", "25 minutes", "FieldRoutes", "urgent infestations are not flagged fast enough", ["booking sent", "urgent", "pest control"], "Nudge urgent-routing call", today, TASK_STATUS_OPEN, None),
        ("ruby-thomas", "Ruby Thomas", "Ruby Laser Clinic", "rubylaser.example", "Toronto", LeadSource.META, CRM_STAGE_WON, ConversationStateEnum.BOOKED, "StackLeads Strategy", timedelta(days=-16), "Meta Ads", "130-150", "8 minutes", "GoHighLevel", "consult no-shows needed better reminders", ["won", "medspa", "showcase"], "Close implementation checklist", today - timedelta(days=7), TASK_STATUS_DONE, {"days_offset": -12, "hour": 10, "minute": 30, "duration_minutes": 30}),
        ("benjamin-green", "Benjamin Green", "Green Commercial Cleaning", "greenclean.example", "Mississauga", LeadSource.LINKEDIN, CRM_STAGE_CONTACTED, ConversationStateEnum.QUALIFYING, "StackLeads Strategy", timedelta(days=-3, hours=-6), "LinkedIn Ads", "40-55", "2-3 hours", "spreadsheet", "facility quote requests have no owner", ["commercial", "ops cleanup"], "Clarify quote ownership", today + timedelta(days=2), TASK_STATUS_OPEN, None),
        ("nora-campbell", "Nora Campbell", "Campbell Family Law", "campbellfamilylaw.example", "Toronto", LeadSource.LINKEDIN, CRM_STAGE_MEETING_BOOKED, ConversationStateEnum.BOOKED, "Senior Strategist", timedelta(days=-1, hours=-1), "LinkedIn Ads", "20-30", "1 hour", "Clio", "consultation urgency is not captured in intake", ["booked", "legal", "high value"], "Prep family-law intake review", today + timedelta(days=1), TASK_STATUS_OPEN, {"days_offset": 2, "hour": 11, "minute": 30, "duration_minutes": 30}),
        ("elijah-stone", "Elijah Stone", "Stone Solar", "stonesolar.example", "Guelph", LeadSource.META, CRM_STAGE_QUALIFIED, ConversationStateEnum.HANDOFF, "Senior Strategist", timedelta(days=-4, hours=-2), "Google Ads", "70-85", "same day", "Salesforce", "dealer attribution and territory routing overlap", ["needs handoff", "solar", "territory routing"], "Senior territory routing review", today, TASK_STATUS_OPEN, None),
        ("madison-young", "Madison Young", "Young Dental Group", "youngdental.example", "North York", LeadSource.META, CRM_STAGE_MEETING_COMPLETED, ConversationStateEnum.BOOKED, "StackLeads Strategy", timedelta(days=-5), "Meta Ads", "100+", "15 minutes", "HubSpot", "implant consults need separate treatment follow-up", ["proposal out", "dental", "implant"], "Send treatment-routing proposal", today, TASK_STATUS_OPEN, {"days_offset": -3, "hour": 14, "minute": 30, "duration_minutes": 30}),
        ("daniel-kim", "Daniel Kim", "Kim Mortgage Team", "kimmortgage.example", "Toronto", LeadSource.LINKEDIN, CRM_STAGE_CONTACTED, ConversationStateEnum.QUALIFYING, "StackLeads Strategy", timedelta(days=-3), "LinkedIn outbound", "45-60", "same day", "Pipedrive", "pre-approval leads need better status visibility", ["mortgage", "pipeline visibility"], "Confirm status fields", today + timedelta(days=2), TASK_STATUS_OPEN, None),
        ("sarah-jones", "Sarah Jones", "Jones Pool & Spa", "jonespoolspa.example", "Barrie", LeadSource.META, CRM_STAGE_LOST, ConversationStateEnum.HANDOFF, "StackLeads Strategy", timedelta(days=-6), "seasonal referrals", "5-10", "manual", "none", "lead volume is too low for this workflow right now", ["bad fit", "seasonal"], None, None, TASK_STATUS_OPEN, None),
        ("mason-white", "Mason White", "White Security Systems", "whitesecurity.example", "Toronto", LeadSource.LINKEDIN, CRM_STAGE_MEETING_BOOKED, ConversationStateEnum.BOOKED, "StackLeads Strategy", timedelta(hours=-11), "LinkedIn Ads", "60-75", "20 minutes", "HubSpot", "commercial leads need faster rep assignment", ["booked", "b2b", "rep routing"], "Prep commercial routing audit", today + timedelta(days=1), TASK_STATUS_OPEN, {"days_offset": 3, "hour": 15, "minute": 30, "duration_minutes": 30}),
    ]
    return [
        _showcase_lead_spec(
            index=index,
            slug=slug,
            full_name=full_name,
            company=company,
            domain=domain,
            city=city,
            source=source,
            crm_stage=crm_stage,
            conversation_state=conversation_state,
            owner_name=owner_name,
            created_offset=created_offset,
            lead_source=lead_source,
            monthly_volume=monthly_volume,
            response_time=response_time,
            crm=crm,
            bottleneck=bottleneck,
            tags=tags,
            task_title=task_title,
            task_due_date=task_due_date,
            task_status=task_status,
            booking=booking,
        )
        for index, (
            slug,
            full_name,
            company,
            domain,
            city,
            source,
            crm_stage,
            conversation_state,
            owner_name,
            created_offset,
            lead_source,
            monthly_volume,
            response_time,
            crm,
            bottleneck,
            tags,
            task_title,
            task_due_date,
            task_status,
            booking,
        ) in enumerate(rows, start=8)
    ]


def _lead_specs() -> list[StackLeadsLeadSpec]:
    today = _today()
    core_specs = [
        StackLeadsLeadSpec(
            slug="eight-minute-booking",
            full_name="Nina Alvarez",
            email="nina.alvarez@northstarhvac.example",
            phone="+14165550101",
            city="Toronto",
            source=LeadSource.LINKEDIN,
            crm_stage=CRM_STAGE_MEETING_BOOKED,
            conversation_state=ConversationStateEnum.BOOKED,
            owner_name="StackLeads Strategy",
            created_offset=timedelta(hours=-3),
            form_answers={
                "primary_lead_source": "Google Ads",
                "monthly_lead_volume": "70-90",
                "current_response_time": "2-4 hours",
                "crm": "spreadsheet plus shared inbox",
                "growth_bottleneck": "qualified leads are called too late",
            },
            qualification_memory={
                "primary_lead_source": "Google Ads",
                "monthly_lead_volume": "70-90",
                "current_response_time": "2-4 hours",
                "crm": "spreadsheet plus shared inbox",
                "growth_bottleneck": "late first response",
            },
            tags=["hot", "speed-to-lead", "booked"],
            tasks=[
                {
                    "title": "Prep response-time audit",
                    "description": "Review lead source, current response lag, and CRM handoff before the call.",
                    "due_date": today,
                    "status": TASK_STATUS_OPEN,
                }
            ],
            notes=["High-intent HVAC prospect booked from first SMS thread in under 20 minutes."],
            audit_events=[
                {"minutes": 0, "event_type": "linkedin_webhook_received", "decision": {"campaign": "CRM speed-to-lead angle"}},
                {"minutes": 1, "event_type": "lead_normalized", "decision": {"source": "linkedin", "speed_to_lead_goal_minutes": 5}},
                {"minutes": 2, "event_type": "initial_sms_sent", "decision": {"speed_to_lead_seconds": 120}},
                {
                    "minutes": 8,
                    "event_type": "agent_decision",
                    "decision": {"actions": [{"type": "request_more_info"}], "next_state": "QUALIFYING"},
                },
                {
                    "minutes": 13,
                    "event_type": "agent_decision",
                    "decision": {"actions": [{"type": "send_booking_link"}], "next_state": "BOOKING_SENT"},
                },
                {"minutes": 18, "event_type": "booking_confirmed", "decision": {"slot": "Tomorrow 10:30 AM"}},
            ],
            states=[
                {"minutes": 2, "previous": ConversationStateEnum.NEW, "new": ConversationStateEnum.GREETED, "reason": "initial_sms_sent"},
                {"minutes": 8, "previous": ConversationStateEnum.GREETED, "new": ConversationStateEnum.QUALIFYING, "reason": "meaningful_reply"},
                {"minutes": 13, "previous": ConversationStateEnum.QUALIFYING, "new": ConversationStateEnum.BOOKING_SENT, "reason": "booking_offer_sent"},
                {"minutes": 18, "previous": ConversationStateEnum.BOOKING_SENT, "new": ConversationStateEnum.BOOKED, "reason": "booking_confirmed"},
            ],
            messages=[
                {
                    "minutes": 2,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Hi Nina, this is StackLeads. Saw your note about leads sitting too long. About how fast does your team usually respond after a form submit?",
                    "agent": {"action": "ask_next_question", "next_question_key": "current_response_time"},
                },
                {
                    "minutes": 6,
                    "direction": MessageDirection.INBOUND,
                    "body": "Honestly sometimes next day. We are losing people to competitors.",
                },
                {
                    "minutes": 8,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "That gap is usually where intent cools off. What CRM or handoff process are you using today?",
                    "agent": {"action": "ask_next_question", "next_question_key": "current_crm"},
                },
                {
                    "minutes": 11,
                    "direction": MessageDirection.INBOUND,
                    "body": "Mostly spreadsheets and a shared inbox. Google Ads brings in 70-90 leads monthly.",
                },
                {
                    "minutes": 13,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "That is enough volume for response time to matter. I can map the handoff leak with you on a short strategy call. Want a few times?",
                    "agent": {"action": "offer_booking"},
                },
                {"minutes": 16, "direction": MessageDirection.INBOUND, "body": "Yes, send times."},
                {
                    "minutes": 17,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "I can book this directly. Here are the next available times: 1) Tomorrow 10:30 AM 2) Tomorrow 2:00 PM 3) Monday 11:00 AM Reply with 1, 2, or 3.",
                    "agent": {"action": "send_booking_link"},
                },
                {"minutes": 18, "direction": MessageDirection.INBOUND, "body": "1"},
                {
                    "minutes": 19,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Booked. You are set for tomorrow at 10:30 AM. Bring your lead source list and current response-time estimate.",
                    "agent": {"action": "mark_booked"},
                },
            ],
            booking={"days_offset": 1, "hour": 10, "minute": 30, "duration_minutes": 30},
        ),
        StackLeadsLeadSpec(
            slug="twenty-four-hour-cooldown",
            full_name="Marcus Chen",
            email="marcus.chen@clearpathdental.example",
            phone="+14165550102",
            city="Mississauga",
            source=LeadSource.META,
            crm_stage=CRM_STAGE_CONTACTED,
            conversation_state=ConversationStateEnum.QUALIFYING,
            owner_name="StackLeads Strategy",
            created_offset=timedelta(hours=-30),
            form_answers={
                "primary_lead_source": "Meta Ads",
                "monthly_lead_volume": "35-45",
                "current_response_time": "next business day",
                "crm": "legacy CRM",
                "growth_bottleneck": "consult requests cool off before staff calls",
            },
            qualification_memory={
                "primary_lead_source": "Meta Ads",
                "monthly_lead_volume": "35-45",
                "current_response_time": "next business day",
                "crm": "legacy CRM",
                "growth_bottleneck": "slow consult follow-up",
            },
            tags=["stale", "follow up later", "speed-to-lead"],
            tasks=[
                {
                    "title": "Recover cooled-off consult lead",
                    "description": "Send a concise follow-up and ask whether the clinic is still reviewing options.",
                    "due_date": today + timedelta(days=1),
                    "status": TASK_STATUS_OPEN,
                }
            ],
            notes=["Same profile as Nina, but first response was delayed by roughly a day and buying intent cooled."],
            audit_events=[
                {"minutes": 0, "event_type": "meta_webhook_received", "decision": {"campaign": "Dental growth audit"}},
                {"minutes": 2, "event_type": "lead_normalized", "decision": {"source": "meta", "speed_to_lead_goal_minutes": 5}},
                {"minutes": 1444, "event_type": "initial_sms_sent", "decision": {"speed_to_lead_seconds": 86640}},
                {
                    "minutes": 1478,
                    "event_type": "agent_decision",
                    "decision": {"actions": [{"type": "request_more_info"}], "next_state": "QUALIFYING"},
                },
            ],
            states=[
                {"minutes": 1444, "previous": ConversationStateEnum.NEW, "new": ConversationStateEnum.GREETED, "reason": "delayed_initial_sms_sent"},
                {"minutes": 1478, "previous": ConversationStateEnum.GREETED, "new": ConversationStateEnum.QUALIFYING, "reason": "late_reply_received"},
            ],
            messages=[
                {
                    "minutes": 1444,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Hi Marcus, this is StackLeads. Saw your request about improving consult follow-up. What happens today when a new patient lead submits a form?",
                    "agent": {"action": "ask_next_question", "next_question_key": "lead_handoff"},
                },
                {
                    "minutes": 1470,
                    "direction": MessageDirection.INBOUND,
                    "body": "We already booked a call with another agency this morning, but you can send info.",
                },
                {
                    "minutes": 1478,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Totally fair. If useful, I can send a short checklist for spotting where consult leads slow down before they reach your front desk.",
                    "agent": {"action": "nurture_follow_up"},
                },
            ],
        ),
        StackLeadsLeadSpec(
            slug="fresh-five-minute-window",
            full_name="Priya Raman",
            email="priya.raman@evergreenlaw.example",
            phone="+14165550103",
            city="Brampton",
            source=LeadSource.LINKEDIN,
            crm_stage=CRM_STAGE_QUALIFIED,
            conversation_state=ConversationStateEnum.BOOKING_SENT,
            owner_name="StackLeads Strategy",
            created_offset=timedelta(minutes=-42),
            form_answers={
                "primary_lead_source": "LinkedIn Ads",
                "monthly_lead_volume": "20-30",
                "current_response_time": "under 1 hour on good days",
                "crm": "HubSpot",
                "growth_bottleneck": "intake team cannot see which leads are urgent",
            },
            qualification_memory={
                "primary_lead_source": "LinkedIn Ads",
                "monthly_lead_volume": "20-30",
                "current_response_time": "under 1 hour",
                "crm": "HubSpot",
                "growth_bottleneck": "no urgency scoring",
            },
            tags=["hot", "booking sent", "legal"],
            tasks=[
                {
                    "title": "Nudge if booking is not picked",
                    "description": "Lead has clear CRM pain and asked for times.",
                    "due_date": today,
                    "status": TASK_STATUS_OPEN,
                }
            ],
            notes=["Strong law-firm fit. Asked specifically about prioritizing urgent consultations."],
            audit_events=[
                {"minutes": 0, "event_type": "linkedin_webhook_received", "decision": {"campaign": "CRM follow-up diagnostic"}},
                {"minutes": 1, "event_type": "lead_normalized", "decision": {"source": "linkedin"}},
                {"minutes": 3, "event_type": "initial_sms_sent", "decision": {"speed_to_lead_seconds": 180}},
                {
                    "minutes": 22,
                    "event_type": "agent_decision",
                    "decision": {"actions": [{"type": "send_booking_link"}], "next_state": "BOOKING_SENT"},
                },
            ],
            states=[
                {"minutes": 3, "previous": ConversationStateEnum.NEW, "new": ConversationStateEnum.GREETED, "reason": "initial_sms_sent"},
                {"minutes": 14, "previous": ConversationStateEnum.GREETED, "new": ConversationStateEnum.QUALIFYING, "reason": "meaningful_reply"},
                {"minutes": 22, "previous": ConversationStateEnum.QUALIFYING, "new": ConversationStateEnum.BOOKING_SENT, "reason": "booking_offer_sent"},
            ],
            messages=[
                {
                    "minutes": 3,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Hi Priya, this is StackLeads. You mentioned intake leads are not getting prioritized. Which source creates the most urgent consultations right now?",
                    "agent": {"action": "ask_next_question", "next_question_key": "primary_lead_source"},
                },
                {"minutes": 12, "direction": MessageDirection.INBOUND, "body": "LinkedIn. We get fewer leads there, but they are usually better cases."},
                {
                    "minutes": 14,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "That is exactly where routing matters. Are those leads landing in HubSpot with enough context for your intake team to act quickly?",
                    "agent": {"action": "ask_next_question", "next_question_key": "current_crm"},
                },
                {"minutes": 20, "direction": MessageDirection.INBOUND, "body": "They land in HubSpot, but urgency is not obvious."},
                {
                    "minutes": 22,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Makes sense. We can review how those leads are tagged, routed, and followed up. Pick a time here: https://stackleads.example/strategy-call",
                    "agent": {"action": "send_booking_link"},
                },
            ],
        ),
        StackLeadsLeadSpec(
            slug="handoff-attribution-review",
            full_name="Ethan Brooks",
            email="ethan.brooks@ridgewayremodeling.example",
            phone="+14165550104",
            city="Hamilton",
            source=LeadSource.META,
            crm_stage=CRM_STAGE_QUALIFIED,
            conversation_state=ConversationStateEnum.HANDOFF,
            owner_name="Senior Strategist",
            created_offset=timedelta(days=-1, hours=-4),
            form_answers={
                "primary_lead_source": "Meta Ads and referrals",
                "monthly_lead_volume": "50+",
                "current_response_time": "same day",
                "crm": "GoHighLevel",
                "growth_bottleneck": "attribution is unclear across sales reps",
            },
            qualification_memory={
                "primary_lead_source": "Meta Ads and referrals",
                "monthly_lead_volume": "50+",
                "current_response_time": "same day",
                "crm": "GoHighLevel",
                "growth_bottleneck": "unclear attribution",
            },
            tags=["needs handoff", "high volume"],
            tasks=[
                {
                    "title": "Strategist attribution review",
                    "description": "Review source reporting and rep ownership before recommending a workflow.",
                    "due_date": today,
                    "status": TASK_STATUS_OPEN,
                }
            ],
            notes=["Complex attribution question. Better handled by a human strategist than a generic automation answer."],
            audit_events=[
                {"minutes": 0, "event_type": "meta_webhook_received", "decision": {"campaign": "Home services CRM audit"}},
                {"minutes": 2, "event_type": "lead_normalized", "decision": {"source": "meta"}},
                {"minutes": 4, "event_type": "initial_sms_sent", "decision": {"speed_to_lead_seconds": 240}},
                {
                    "minutes": 37,
                    "event_type": "agent_decision",
                    "decision": {"actions": [{"type": "handoff_to_human"}], "next_state": "HANDOFF"},
                },
            ],
            states=[
                {"minutes": 4, "previous": ConversationStateEnum.NEW, "new": ConversationStateEnum.GREETED, "reason": "initial_sms_sent"},
                {"minutes": 22, "previous": ConversationStateEnum.GREETED, "new": ConversationStateEnum.QUALIFYING, "reason": "meaningful_reply"},
                {"minutes": 37, "previous": ConversationStateEnum.QUALIFYING, "new": ConversationStateEnum.HANDOFF, "reason": "agent_transition"},
            ],
            messages=[
                {
                    "minutes": 4,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Hi Ethan, this is StackLeads. You mentioned CRM attribution is muddy. Which channels are you trying to separate right now?",
                    "agent": {"action": "ask_next_question", "next_question_key": "primary_lead_source"},
                },
                {"minutes": 20, "direction": MessageDirection.INBOUND, "body": "Meta, referrals, and a few rep-entered leads. We cannot tell which rep actually moved deals."},
                {
                    "minutes": 22,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "That usually needs a look at source rules and ownership handoff. Are you using one pipeline or separate pipelines by team?",
                    "agent": {"action": "ask_next_question", "next_question_key": "pipeline_structure"},
                },
                {"minutes": 35, "direction": MessageDirection.INBOUND, "body": "One pipeline, but we might need a full attribution cleanup before changing anything."},
                {
                    "minutes": 37,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "I am flagging this for a senior strategist so we do not oversimplify the setup. Someone will review the context before the next reply.",
                    "agent": {"action": "handoff_to_human"},
                },
            ],
        ),
        StackLeadsLeadSpec(
            slug="won-fast-followup",
            full_name="Sofia Mendes",
            email="sofia.mendes@peakmedspa.example",
            phone="+14165550105",
            city="Vaughan",
            source=LeadSource.MANUAL,
            crm_stage=CRM_STAGE_WON,
            conversation_state=ConversationStateEnum.BOOKED,
            owner_name="StackLeads Strategy",
            created_offset=timedelta(days=-5, hours=-2),
            form_answers={
                "primary_lead_source": "Instagram and Meta Ads",
                "monthly_lead_volume": "80+",
                "current_response_time": "15-30 minutes",
                "crm": "manual inbox",
                "growth_bottleneck": "front desk follow-up is inconsistent",
            },
            qualification_memory={
                "primary_lead_source": "Instagram and Meta Ads",
                "monthly_lead_volume": "80+",
                "current_response_time": "15-30 minutes",
                "crm": "manual inbox",
                "growth_bottleneck": "inconsistent follow-up",
            },
            tags=["won", "medspa", "fast follow-up"],
            tasks=[
                {
                    "title": "Kick off medspa CRM sprint",
                    "description": "Confirm campaign sources and front-desk ownership map.",
                    "due_date": today - timedelta(days=1),
                    "status": TASK_STATUS_DONE,
                }
            ],
            notes=["Won after fast response and focused diagnosis of front-desk follow-up gaps."],
            audit_events=[
                {"minutes": 0, "event_type": "manual_lead_created", "decision": {"source": "referral"}},
                {"minutes": 2, "event_type": "initial_sms_sent", "decision": {"speed_to_lead_seconds": 120}},
                {"minutes": 31, "event_type": "booking_confirmed", "decision": {"slot": "Strategy call"}},
                {"minutes": 4320, "event_type": "crm_stage_updated", "decision": {"previous_stage": "Meeting Completed", "new_stage": "Won"}},
            ],
            states=[
                {"minutes": 2, "previous": ConversationStateEnum.NEW, "new": ConversationStateEnum.GREETED, "reason": "initial_sms_sent"},
                {"minutes": 18, "previous": ConversationStateEnum.GREETED, "new": ConversationStateEnum.QUALIFYING, "reason": "meaningful_reply"},
                {"minutes": 24, "previous": ConversationStateEnum.QUALIFYING, "new": ConversationStateEnum.BOOKING_SENT, "reason": "booking_offer_sent"},
                {"minutes": 31, "previous": ConversationStateEnum.BOOKING_SENT, "new": ConversationStateEnum.BOOKED, "reason": "booking_confirmed"},
            ],
            messages=[
                {
                    "minutes": 2,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Hi Sofia, this is StackLeads. You mentioned front-desk follow-up is inconsistent. How many inquiry sources are they checking each day?",
                    "agent": {"action": "ask_next_question", "next_question_key": "source_count"},
                },
                {"minutes": 16, "direction": MessageDirection.INBOUND, "body": "Instagram DMs, Meta forms, website forms, and phone calls. It is messy."},
                {
                    "minutes": 18,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "That is a lot of context switching. We can map which sources need instant response and which need nurture. Want to book a short review?",
                    "agent": {"action": "offer_booking"},
                },
                {"minutes": 23, "direction": MessageDirection.INBOUND, "body": "Yes please."},
                {
                    "minutes": 24,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Here are the next options: 1) Today 3:30 PM 2) Tomorrow 9:30 AM 3) Tomorrow 1:00 PM Reply with 1, 2, or 3.",
                    "agent": {"action": "send_booking_link"},
                },
                {"minutes": 30, "direction": MessageDirection.INBOUND, "body": "1"},
                {"minutes": 31, "direction": MessageDirection.OUTBOUND, "body": "Booked for today at 3:30 PM. I will bring a simple source-to-follow-up map."},
            ],
            booking={"days_offset": -4, "hour": 15, "minute": 30, "duration_minutes": 30},
        ),
        StackLeadsLeadSpec(
            slug="after-hours-next-morning",
            full_name="Avery Scott",
            email="avery.scott@lakesideplumbing.example",
            phone="+14165550106",
            city="Oakville",
            source=LeadSource.META,
            crm_stage=CRM_STAGE_CONTACTED,
            conversation_state=ConversationStateEnum.QUALIFYING,
            owner_name="StackLeads Strategy",
            created_offset=timedelta(days=-2, hours=-8),
            form_answers={
                "primary_lead_source": "Google Local Services Ads",
                "monthly_lead_volume": "25-40",
                "current_response_time": "depends on dispatcher availability",
                "crm": "field-service software",
                "growth_bottleneck": "after-hours leads are missed",
            },
            qualification_memory={
                "primary_lead_source": "Google Local Services Ads",
                "monthly_lead_volume": "25-40",
                "current_response_time": "varies",
                "crm": "field-service software",
                "growth_bottleneck": "after-hours response",
            },
            tags=["after-hours", "service business"],
            tasks=[
                {
                    "title": "Follow up on after-hours routing",
                    "description": "Ask whether emergency and non-emergency leads should follow different paths.",
                    "due_date": today + timedelta(days=1),
                    "status": TASK_STATUS_OPEN,
                }
            ],
            notes=["Good fit for after-hours follow-up logic. Needs routing rules by urgency."],
            audit_events=[
                {"minutes": 0, "event_type": "meta_webhook_received", "decision": {"campaign": "After-hours lead response"}},
                {"minutes": 1, "event_type": "lead_normalized", "decision": {"source": "meta"}},
                {"minutes": 5, "event_type": "after_hours_initial_sms_sent", "decision": {"speed_to_lead_seconds": 300}},
                {"minutes": 720, "event_type": "follow_up_sms_sent", "decision": {"after_hours_followup_minutes": 720}},
                {
                    "minutes": 755,
                    "event_type": "agent_decision",
                    "decision": {"actions": [{"type": "request_more_info"}], "next_state": "QUALIFYING"},
                },
            ],
            states=[
                {"minutes": 5, "previous": ConversationStateEnum.NEW, "new": ConversationStateEnum.GREETED, "reason": "after_hours_initial_sms_sent"},
                {"minutes": 755, "previous": ConversationStateEnum.GREETED, "new": ConversationStateEnum.QUALIFYING, "reason": "meaningful_reply"},
            ],
            messages=[
                {"minutes": 5, "direction": MessageDirection.OUTBOUND, "body": "Thanks for reaching StackLeads after hours. We will follow up as soon as the team is back online."},
                {
                    "minutes": 720,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Good morning from StackLeads. Are after-hours leads usually emergency calls, quote requests, or both?",
                    "agent": {"action": "follow_up"},
                },
                {"minutes": 748, "direction": MessageDirection.INBOUND, "body": "Both. Emergencies need a call, quotes can wait until morning."},
                {
                    "minutes": 755,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "That split is important. We can design separate routing so urgent leads do not sit beside normal quote requests.",
                    "agent": {"action": "ask_next_question", "next_question_key": "routing_rules"},
                },
            ],
        ),
        StackLeadsLeadSpec(
            slug="bad-fit-closed",
            full_name="Brooke Ellis",
            email="brooke.ellis@example.test",
            phone="+14165550107",
            city="Toronto",
            source=LeadSource.SMS,
            crm_stage=CRM_STAGE_LOST,
            conversation_state=ConversationStateEnum.HANDOFF,
            owner_name="StackLeads Strategy",
            created_offset=timedelta(days=-3, hours=-1),
            form_answers={
                "primary_lead_source": "none yet",
                "monthly_lead_volume": "0",
                "current_response_time": "not applicable",
                "crm": "none",
                "growth_bottleneck": "needs logo and website first",
            },
            qualification_memory={
                "primary_lead_source": "none",
                "monthly_lead_volume": "0",
                "crm": "none",
                "growth_bottleneck": "pre-lead-generation stage",
            },
            tags=["bad fit", "closed"],
            tasks=[],
            notes=["Closed as bad fit for this CRM-first offer. Better suited for early brand/web work."],
            audit_events=[
                {"minutes": 0, "event_type": "sms_lead_created", "decision": {"source": "manual sms"}},
                {"minutes": 3, "event_type": "initial_sms_sent", "decision": {"speed_to_lead_seconds": 180}},
                {
                    "minutes": 24,
                    "event_type": "agent_decision",
                    "decision": {"actions": [{"type": "handoff_to_human"}], "next_state": "HANDOFF"},
                },
                {"minutes": 40, "event_type": "crm_stage_updated", "decision": {"previous_stage": "Qualified", "new_stage": "Lost"}},
            ],
            states=[
                {"minutes": 3, "previous": ConversationStateEnum.NEW, "new": ConversationStateEnum.GREETED, "reason": "initial_sms_sent"},
                {"minutes": 24, "previous": ConversationStateEnum.GREETED, "new": ConversationStateEnum.HANDOFF, "reason": "bad_fit_review"},
            ],
            messages=[
                {
                    "minutes": 3,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Hi Brooke, this is StackLeads. What lead source are you trying to improve right now?",
                    "agent": {"action": "ask_next_question", "next_question_key": "primary_lead_source"},
                },
                {"minutes": 19, "direction": MessageDirection.INBOUND, "body": "We do not have leads yet. We need a logo and website first."},
                {
                    "minutes": 24,
                    "direction": MessageDirection.OUTBOUND,
                    "body": "Thanks for the context. Our CRM workflow is strongest once leads are already coming in, so this may be better after your first acquisition channel is live.",
                    "agent": {"action": "handoff_to_human"},
                },
            ],
        ),
    ]
    return core_specs + _landing_page_showcase_specs(today)


def _upsert_client(db: Session, *, reset_portal: bool = False) -> tuple[Client, bool]:
    client = db.scalar(select(Client).where(Client.client_key == CLIENT_KEY).limit(1))
    portal_credentials_reset = reset_portal
    if client is None:
        client = Client(client_key=CLIENT_KEY, business_name=BUSINESS_NAME)
        db.add(client)
        db.flush()
        portal_credentials_reset = True

    client.business_name = BUSINESS_NAME
    client.tone = "sharp, consultative, revenue-focused"
    client.timezone = CLIENT_TIMEZONE
    client.qualification_questions = [
        "What lead source is giving you the most friction right now?",
        "How fast does your team usually respond after a form submit?",
        "What should happen next if a lead is qualified?",
    ]
    client.booking_url = "https://stackleads.example/strategy-call"
    client.booking_mode = "internal"
    client.booking_config = _internal_booking_config()
    client.provider_config = {
        "website_url": "https://stackleads.example",
        "demo_ad_campaign_reports": _demo_ad_campaign_reports(),
    }
    client.fallback_handoff_number = "+14165550100"
    client.consent_text = "Reply STOP to opt out. Message/data rates may apply."
    if portal_credentials_reset or not client.portal_email.strip() or not client.portal_password_hash.strip():
        client.portal_display_name = "StackLeads Demo"
        client.portal_email = PORTAL_EMAIL
        client.portal_password_hash = hash_portal_password(PORTAL_PASSWORD)
        portal_credentials_reset = True
    elif not client.portal_display_name.strip():
        client.portal_display_name = "StackLeads Demo"
    client.portal_enabled = True
    client.operating_hours = {"days": [0, 1, 2, 3, 4], "start": "09:00", "end": "18:00"}
    client.faq_context = _client_faq_context()
    client.ai_context = _client_ai_context()
    client.template_overrides = {}
    client.is_active = True
    db.flush()
    return client, portal_credentials_reset


def _reset_seeded_demo(db: Session, client: Client) -> int:
    leads = db.scalars(
        select(Lead).where(
            Lead.client_id == client.id,
            Lead.external_lead_id.is_not(None),
            Lead.external_lead_id.like(f"{DEMO_PREFIX}-%"),
        )
    ).all()
    deleted = len(leads)
    for lead in leads:
        db.delete(lead)
    db.flush()
    return deleted


def _message_payload(seed_group: str, agent: dict | None = None) -> dict:
    payload = {"seeded": True, "seed_group": seed_group}
    if agent:
        payload["agent"] = agent
    return payload


def _seed_lead(db: Session, client: Client, spec: StackLeadsLeadSpec, now: datetime) -> None:
    created_at = now + spec.created_offset
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
        owner_name=spec.owner_name,
        created_at=created_at,
        updated_at=created_at,
    )
    db.add(lead)
    db.flush()

    first_outbound_at = None
    last_inbound_at = None
    last_outbound_at = None
    last_event_at = created_at

    for event in spec.audit_events:
        at = created_at + timedelta(minutes=event["minutes"])
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type=event["event_type"],
                decision={"seeded": True, "seed_group": DEMO_PREFIX, **event.get("decision", {})},
                created_at=at,
            )
        )
        last_event_at = max(last_event_at, at)

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

    for idx, message in enumerate(spec.messages, start=1):
        at = created_at + timedelta(minutes=message["minutes"])
        direction = message["direction"]
        db.add(
            Message(
                lead_id=lead.id,
                client_id=client.id,
                direction=direction,
                body=message["body"],
                provider_message_sid=f"STACKLEADS-DEMO-{lead.id}-{idx}",
                raw_payload=_message_payload(DEMO_PREFIX, message.get("agent")),
                created_at=at,
            )
        )
        last_event_at = max(last_event_at, at)
        if direction == MessageDirection.OUTBOUND:
            first_outbound_at = first_outbound_at or at
            last_outbound_at = at
        else:
            last_inbound_at = at

    for tag in spec.tags:
        db.add(
            LeadTag(
                lead_id=lead.id,
                client_id=client.id,
                tag=tag,
                created_at=created_at + timedelta(minutes=2),
            )
        )

    for index, task in enumerate(spec.tasks, start=1):
        at = created_at + timedelta(minutes=12 + index)
        completed_at = at + timedelta(minutes=90) if task.get("status") == TASK_STATUS_DONE else None
        db.add(
            LeadTask(
                lead_id=lead.id,
                client_id=client.id,
                title=task["title"],
                description=task.get("description", ""),
                due_date=task.get("due_date"),
                status=task.get("status", TASK_STATUS_OPEN),
                completed_at=completed_at,
                created_by="seed",
                created_at=at,
                updated_at=completed_at or at,
            )
        )
        last_event_at = max(last_event_at, completed_at or at)

    for note_index, note in enumerate(spec.notes, start=1):
        at = created_at + timedelta(minutes=45 + note_index)
        db.add(
            AuditLog(
                client_id=client.id,
                lead_id=lead.id,
                event_type="internal_note",
                decision={"seeded": True, "seed_group": DEMO_PREFIX, "note": note, "actor_label": "StackLeads Demo"},
                created_at=at,
            )
        )
        last_event_at = max(last_event_at, at)

    if spec.booking is not None:
        start_at = _local_datetime(spec.booking["days_offset"], spec.booking["hour"], spec.booking["minute"])
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
                title=f"Strategy call - {spec.full_name}",
                notes="Seeded StackLeads showcase booking",
                created_at=created_at + timedelta(minutes=20),
                updated_at=created_at + timedelta(minutes=20),
            )
        )
        last_event_at = max(last_event_at, created_at + timedelta(minutes=20))

    lead.initial_sms_sent_at = first_outbound_at
    lead.last_inbound_at = last_inbound_at
    lead.last_outbound_at = last_outbound_at
    lead.updated_at = last_event_at


def seed_stackleads_demo_data(db: Session, *, reset: bool = False, reset_portal: bool = False) -> dict:
    client, portal_credentials_reset = _upsert_client(db, reset_portal=reset or reset_portal)
    deleted = _reset_seeded_demo(db, client) if reset else 0

    existing = db.scalar(
        select(Lead.id)
        .where(
            Lead.client_id == client.id,
            Lead.external_lead_id.is_not(None),
            Lead.external_lead_id.like(f"{DEMO_PREFIX}-%"),
        )
        .limit(1)
    )
    if existing is not None:
        return {
            "seeded": False,
            "reason": "stackleads_demo_data_already_present",
            "client_key": client.client_key,
            "business_name": client.business_name,
            "portal_email": client.portal_email,
            "portal_password": PORTAL_PASSWORD if portal_credentials_reset else None,
            "portal_credentials_reset": portal_credentials_reset,
            "deleted_previous_seeded_leads": deleted,
        }

    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    specs = _lead_specs()
    for spec in specs:
        _seed_lead(db, client, spec, now)

    db.flush()
    return {
        "seeded": True,
        "client_key": client.client_key,
        "business_name": client.business_name,
        "portal_email": client.portal_email,
        "portal_password": PORTAL_PASSWORD if portal_credentials_reset else None,
        "portal_credentials_reset": portal_credentials_reset,
        "seeded_leads": len(specs),
        "deleted_previous_seeded_leads": deleted,
        "demo_angle": "speed_to_lead",
        "recommended_showcase_lead": "Nina Alvarez",
    }


def seed_stackleads_demo(*, reset: bool = False, reset_portal: bool = False) -> dict:
    session_factory = get_session_factory()
    with session_factory() as db:
        result = seed_stackleads_demo_data(db, reset=reset, reset_portal=reset_portal)
        db.commit()
        return result
