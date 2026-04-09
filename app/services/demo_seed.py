from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.models import (
    AuditLog,
    Client,
    ConversationState,
    ConversationStateEnum,
    Lead,
    LeadTag,
    LeadTask,
    LeadSource,
    Message,
    MessageDirection,
)
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

DEMO_CLIENT_KEYS = {
    "demo-roofing",
    "demo-medspa",
    "demo-legal",
}
DEMO_PORTAL_PASSWORD = "demo-portal-2026"
SHOWCASE_EXTERNAL_PREFIX = "showcase"


@dataclass(frozen=True)
class DemoClientSpec:
    client_key: str
    business_name: str
    tone: str
    timezone: str
    qualification_questions: list[str]
    booking_url: str
    fallback_handoff_number: str
    consent_text: str
    operating_hours: dict[str, Any]
    faq_context: str
    template_overrides: dict[str, str]
    source: LeadSource
    service_label: str
    challenge_label: str
    form_field_name: str
    plan_field_name: str
    cities: list[str]
    names: list[str]


DEMO_CLIENT_SPECS: list[DemoClientSpec] = [
    DemoClientSpec(
        client_key="demo-roofing",
        business_name="Northwind Roofing Co.",
        tone="reassuring and direct",
        timezone="America/Chicago",
        qualification_questions=[
            "What kind of roof issue are you seeing right now?",
            "Did the issue start after a storm or over time?",
            "When do you want an inspection?",
        ],
        booking_url="https://demo.northwindroofing.example/book",
        fallback_handoff_number="+15125550101",
        consent_text="Reply STOP to opt out. Message/data rates may apply.",
        operating_hours={"days": [0, 1, 2, 3, 4, 5], "start": "08:00", "end": "18:00"},
        faq_context="Residential roof repair, storm inspections, and insurance documentation.",
        template_overrides={},
        source=LeadSource.META,
        service_label="roof inspection",
        challenge_label="storm damage",
        form_field_name="roof_issue",
        plan_field_name="preferred_visit",
        cities=["Austin", "Round Rock", "Cedar Park", "Georgetown", "Pflugerville", "Lakeway", "Leander"],
        names=["Mia Carter", "Liam Bennett", "Ava Ramirez", "Noah Ellis", "Ella Brooks", "Lucas Ward", "Zoe Foster"],
    ),
    DemoClientSpec(
        client_key="demo-medspa",
        business_name="Harbor MedSpa Studio",
        tone="polished and calm",
        timezone="America/Los_Angeles",
        qualification_questions=[
            "Which treatment are you most interested in?",
            "Have you had this treatment before?",
            "How soon are you hoping to come in?",
        ],
        booking_url="https://demo.harbor-medspa.example/consult",
        fallback_handoff_number="+13105550102",
        consent_text="Reply STOP to opt out. Message/data rates may apply.",
        operating_hours={"days": [0, 1, 2, 3, 4, 5], "start": "09:00", "end": "19:00"},
        faq_context="Injectables, laser services, skin consultations, and membership packages.",
        template_overrides={},
        source=LeadSource.LINKEDIN,
        service_label="consultation",
        challenge_label="skin goals",
        form_field_name="treatment_interest",
        plan_field_name="ideal_date",
        cities=["Santa Monica", "Venice", "Marina del Rey", "Beverly Hills", "West Hollywood", "Pasadena", "Culver City"],
        names=["Sofia Lane", "Olivia Hart", "Grace Kim", "Nora Wells", "Chloe Park", "Emma Reid", "Lily Torres"],
    ),
    DemoClientSpec(
        client_key="demo-legal",
        business_name="Summit Injury Law Group",
        tone="empathetic and professional",
        timezone="America/New_York",
        qualification_questions=[
            "What type of accident or injury happened?",
            "When did the incident happen?",
            "Have you already spoken with an insurance adjuster or another attorney?",
        ],
        booking_url="https://demo.summitlaw.example/review",
        fallback_handoff_number="+12125550103",
        consent_text="Reply STOP to opt out. Message/data rates may apply.",
        operating_hours={"days": [0, 1, 2, 3, 4], "start": "08:30", "end": "17:30"},
        faq_context="Personal injury consultations, case screening, and claim intake.",
        template_overrides={},
        source=LeadSource.META,
        service_label="case review",
        challenge_label="accident follow-up",
        form_field_name="incident_type",
        plan_field_name="best_time_to_call",
        cities=["Brooklyn", "Queens", "Jersey City", "Bronx", "Staten Island", "Newark", "Hoboken"],
        names=["Jordan Price", "Mason Cole", "Harper Stone", "Ethan Blake", "Amelia Ross", "Jack Turner", "Leah Grant"],
    ),
]


def can_seed_demo(settings: Settings) -> bool:
    return settings.env.lower() == "dev" or settings.enable_demo_seed


def demo_data_present(db: Session) -> bool:
    existing = db.scalar(select(Client.id).where(Client.client_key.in_(DEMO_CLIENT_KEYS)).limit(1))
    return existing is not None


def _backfill_demo_client_portals(db: Session) -> dict[str, Any]:
    updated_keys: list[str] = []
    for spec in DEMO_CLIENT_SPECS:
        client = db.scalar(select(Client).where(Client.client_key == spec.client_key).limit(1))
        if client is None:
            continue

        changed = False
        expected_email = f"owner@{spec.client_key}.demo"
        expected_display = f"{spec.business_name} Owner"

        if not client.portal_display_name.strip():
            client.portal_display_name = expected_display
            changed = True
        if not client.portal_email.strip():
            client.portal_email = expected_email
            changed = True
        if not client.portal_password_hash.strip():
            client.portal_password_hash = hash_portal_password(DEMO_PORTAL_PASSWORD)
            changed = True
        if not client.portal_enabled:
            client.portal_enabled = True
            changed = True

        if changed:
            updated_keys.append(client.client_key)

    if updated_keys:
        db.flush()
    return {"portal_clients_updated": len(updated_keys), "portal_client_keys": updated_keys}


def reset_demo_data(db: Session) -> dict[str, int]:
    demo_clients = db.scalars(select(Client).where(Client.client_key.in_(DEMO_CLIENT_KEYS))).all()
    deleted = len(demo_clients)
    for client in demo_clients:
        db.delete(client)
    db.flush()
    return {"clients_deleted": deleted}


def seed_demo_data(db: Session, *, reset: bool = False) -> dict[str, Any]:
    if reset:
        reset_result = reset_demo_data(db)
    else:
        reset_result = {"clients_deleted": 0}
        if demo_data_present(db):
            portal_backfill = _backfill_demo_client_portals(db)
            return {
                **reset_result,
                **portal_backfill,
                "clients_created": 0,
                "leads_created": 0,
                "messages_created": 0,
                "state_transitions_created": 0,
                "audit_logs_created": 0,
                "crm_tags_created": 0,
                "crm_tasks_created": 0,
                "client_keys": sorted(DEMO_CLIENT_KEYS),
                "seeded": False,
                "reason": "demo_data_already_present",
            }

    counters = {
        "clients_created": 0,
        "leads_created": 0,
        "messages_created": 0,
        "state_transitions_created": 0,
        "audit_logs_created": 0,
        "crm_tags_created": 0,
        "crm_tasks_created": 0,
    }

    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    for client_index, spec in enumerate(DEMO_CLIENT_SPECS):
        client = Client(
            client_key=spec.client_key,
            business_name=spec.business_name,
            tone=spec.tone,
            timezone=spec.timezone,
            qualification_questions=spec.qualification_questions,
            booking_url=spec.booking_url,
            booking_mode="link",
            booking_config={},
            fallback_handoff_number=spec.fallback_handoff_number,
            consent_text=spec.consent_text,
            portal_display_name=f"{spec.business_name} Owner",
            portal_email=f"owner@{spec.client_key}.demo",
            portal_password_hash=hash_portal_password(DEMO_PORTAL_PASSWORD),
            portal_enabled=True,
            operating_hours=spec.operating_hours,
            faq_context=spec.faq_context,
            template_overrides=spec.template_overrides,
            created_at=now - timedelta(days=30 - client_index),
            updated_at=now - timedelta(hours=1 + client_index),
        )
        db.add(client)
        db.flush()
        counters["clients_created"] += 1
        _seed_client_conversations(
            db=db,
            client=client,
            spec=spec,
            now=now - timedelta(hours=client_index * 2),
            counters=counters,
        )

    return {
        **reset_result,
        **counters,
        "client_keys": [spec.client_key for spec in DEMO_CLIENT_SPECS],
        "seeded": True,
    }


def seed_showcase_client_data(db: Session, *, client_key: str, reset: bool = False) -> dict[str, Any]:
    client = db.scalar(select(Client).where(Client.client_key == client_key).limit(1))
    if client is None:
        raise ValueError("Client not found")

    external_prefix = f"{SHOWCASE_EXTERNAL_PREFIX}-{client.client_key}"
    existing_showcase_leads = db.scalars(
        select(Lead).where(
            Lead.client_id == client.id,
            Lead.external_lead_id.is_not(None),
            Lead.external_lead_id.like(f"{external_prefix}-%"),
        )
    ).all()

    deleted = 0
    if reset and existing_showcase_leads:
        deleted = len(existing_showcase_leads)
        for lead in existing_showcase_leads:
            db.delete(lead)
        db.flush()
    elif existing_showcase_leads:
        return {
            "seeded": False,
            "reason": "showcase_data_already_present",
            "client_key": client.client_key,
            "leads_existing": len(existing_showcase_leads),
            "messages_created": 0,
            "state_transitions_created": 0,
            "audit_logs_created": 0,
            "crm_tags_created": 0,
            "crm_tasks_created": 0,
        }

    counters = {
        "clients_created": 0,
        "leads_created": 0,
        "messages_created": 0,
        "state_transitions_created": 0,
        "audit_logs_created": 0,
        "crm_tags_created": 0,
        "crm_tasks_created": 0,
    }

    spec = _showcase_spec_for_client(client)
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    _seed_client_conversations(
        db=db,
        client=client,
        spec=spec,
        now=now,
        counters=counters,
        external_lead_prefix=external_prefix,
    )

    return {
        "seeded": True,
        "client_key": client.client_key,
        "leads_deleted": deleted,
        **counters,
    }


def _seed_client_conversations(
    *,
    db: Session,
    client: Client,
    spec: DemoClientSpec,
    now: datetime,
    counters: dict[str, int],
    external_lead_prefix: str | None = None,
) -> None:
    scenarios = _scenario_blueprints(spec)
    lead_prefix = external_lead_prefix or client.client_key

    for idx, scenario in enumerate(scenarios):
        start = now - timedelta(days=idx, hours=(idx % 3) * 2, minutes=idx * 7)
        lead = Lead(
            client_id=client.id,
            external_lead_id=f"{lead_prefix}-lead-{idx + 1:02d}",
            source=scenario["source"],
            full_name=spec.names[idx],
            phone=f"+1555{client.id}{idx + 1:02d}{idx + 30:04d}",
            email=f"{spec.names[idx].lower().replace(' ', '.')}@example.com",
            city=spec.cities[idx],
            form_answers=scenario["form_answers"],
            raw_payload={"seeded": True, "scenario": scenario["slug"], "source": scenario["source"].value},
            consented=True,
            opted_out=scenario["opted_out"],
            conversation_state=scenario["final_state"],
            crm_stage=scenario["crm_stage"],
            created_at=start,
            updated_at=start,
        )
        db.add(lead)
        db.flush()
        counters["leads_created"] += 1

        first_outbound_at: datetime | None = None
        last_inbound_at: datetime | None = None
        last_outbound_at: datetime | None = None
        last_event_at = start

        for audit in scenario["audit_logs"]:
            created_at = start + timedelta(minutes=audit["minutes"])
            db.add(
                AuditLog(
                    client_id=client.id,
                    lead_id=lead.id if audit.get("attach_lead", True) else None,
                    event_type=audit["event_type"],
                    decision=audit.get("decision", {}),
                    created_at=created_at,
                )
            )
            counters["audit_logs_created"] += 1
            last_event_at = max(last_event_at, created_at)

        for tag in scenario.get("crm_tags", []):
            db.add(
                LeadTag(
                    lead_id=lead.id,
                    client_id=client.id,
                    tag=tag,
                    created_at=start + timedelta(minutes=2),
                )
            )
            counters["crm_tags_created"] += 1

        for task_index, task in enumerate(scenario.get("crm_tasks", []), start=1):
            created_at = start + timedelta(minutes=10 + task_index)
            completed_at = None
            if task.get("status") == TASK_STATUS_DONE:
                completed_at = start + timedelta(minutes=80 + task_index)
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
                    created_at=created_at,
                    updated_at=completed_at or created_at,
                )
            )
            counters["crm_tasks_created"] += 1
            if completed_at is not None:
                db.add(
                    AuditLog(
                        client_id=client.id,
                        lead_id=lead.id,
                        event_type="crm_task_completed",
                        decision={"title": task["title"], "actor_role": "seed"},
                        created_at=completed_at,
                    )
                )
                counters["audit_logs_created"] += 1
                last_event_at = max(last_event_at, completed_at)

        for state in scenario["states"]:
            created_at = start + timedelta(minutes=state["minutes"])
            db.add(
                ConversationState(
                    lead_id=lead.id,
                    previous_state=state["previous_state"],
                    new_state=state["new_state"],
                    reason=state["reason"],
                    metadata_json=state.get("metadata_json", {}),
                    created_at=created_at,
                )
            )
            counters["state_transitions_created"] += 1
            last_event_at = max(last_event_at, created_at)

        for msg_index, message in enumerate(scenario["messages"], start=1):
            created_at = start + timedelta(minutes=message["minutes"])
            db.add(
                Message(
                    lead_id=lead.id,
                    client_id=client.id,
                    direction=message["direction"],
                    body=message["body"].format(
                        business_name=client.business_name,
                        booking_url=client.booking_url,
                        handoff_number=client.fallback_handoff_number,
                        first_name=lead.full_name.split(" ")[0],
                    ),
                    provider_message_sid=f"DEMO-{client.id}-{lead.id}-{msg_index}",
                    raw_payload={"seeded": True, "scenario": scenario["slug"]},
                    created_at=created_at,
                )
            )
            counters["messages_created"] += 1
            last_event_at = max(last_event_at, created_at)
            if message["direction"] == MessageDirection.OUTBOUND:
                last_outbound_at = created_at
                if first_outbound_at is None:
                    first_outbound_at = created_at
            else:
                last_inbound_at = created_at

        lead.initial_sms_sent_at = first_outbound_at
        lead.last_inbound_at = last_inbound_at
        lead.last_outbound_at = last_outbound_at
        lead.updated_at = last_event_at

    db.flush()


def _showcase_spec_for_client(client: Client) -> DemoClientSpec:
    questions = [str(item).strip() for item in (client.qualification_questions or []) if str(item).strip()]
    defaults = [
        "What are you looking to improve right now?",
        "Who is your ideal customer?",
        "What timeline are you targeting?",
    ]
    while len(questions) < 3:
        questions.append(defaults[len(questions)])

    faq = (client.faq_context or "").lower()
    service_label = "consultation"
    challenge_label = "lead quality"
    if "roof" in faq:
        service_label = "roof inspection"
        challenge_label = "roof leaks"
    elif any(token in faq for token in ("ads", "advert", "lead", "crm")):
        service_label = "growth strategy call"
        challenge_label = "low lead volume"
    elif any(token in faq for token in ("spa", "treatment", "injectable", "laser")):
        service_label = "treatment consultation"
        challenge_label = "treatment planning"
    elif any(token in faq for token in ("legal", "injury", "law")):
        service_label = "case review"
        challenge_label = "case intake"

    return DemoClientSpec(
        client_key=client.client_key,
        business_name=client.business_name,
        tone=client.tone or "friendly and practical",
        timezone=client.timezone or "America/New_York",
        qualification_questions=questions[:3],
        booking_url=client.booking_url,
        fallback_handoff_number=client.fallback_handoff_number,
        consent_text=client.consent_text,
        operating_hours=client.operating_hours or {"days": [0, 1, 2, 3, 4], "start": "09:00", "end": "18:00"},
        faq_context=client.faq_context,
        template_overrides=client.template_overrides or {},
        source=LeadSource.META,
        service_label=service_label,
        challenge_label=challenge_label,
        form_field_name="service_interest",
        plan_field_name="desired_timeline",
        cities=["Austin", "Dallas", "Miami", "Denver", "Chicago", "Phoenix", "Seattle"],
        names=["Alex Rivera", "Taylor Brooks", "Jordan Kim", "Morgan Patel", "Casey Allen", "Riley Stone", "Parker Shaw"],
    )


def _scenario_blueprints(spec: DemoClientSpec) -> list[dict[str, Any]]:
    q1 = spec.qualification_questions[0]
    q2 = spec.qualification_questions[1]
    q3 = spec.qualification_questions[2]
    service = spec.service_label
    issue = spec.challenge_label
    form_field = spec.form_field_name
    plan_field = spec.plan_field_name
    is_marketing_profile = _is_marketing_profile_spec(spec)

    return [
        {
            "slug": "qualifying-open",
            "source": spec.source,
            "final_state": ConversationStateEnum.QUALIFYING,
            "crm_stage": CRM_STAGE_CONTACTED,
            "opted_out": False,
            "form_answers": {
                form_field: issue,
                plan_field: "This week",
                "budget_range": "$2k-$5k" if spec.client_key == "demo-roofing" else "Flexible",
            },
            "crm_tags": ["hot"] if spec.client_key == "demo-roofing" else ["follow up later"],
            "crm_tasks": [
                {
                    "title": "Call this lead",
                    "description": "Review qualification answers and confirm fit.",
                    "due_date": date(2026, 3, 10),
                    "status": TASK_STATUS_OPEN,
                }
            ],
            "audit_logs": [
                {"minutes": 0, "event_type": f"{spec.source.value}_webhook_received", "decision": {"seeded": True}, "attach_lead": False},
                {"minutes": 1, "event_type": "lead_normalized", "decision": {"seeded": True, "scenario": "qualifying-open"}},
                {"minutes": 4, "event_type": "initial_sms_sent", "decision": {"seeded": True}},
                {
                    "minutes": 39,
                    "event_type": "agent_decision",
                    "decision": {"actions": [{"type": "request_more_info"}], "next_state": "QUALIFYING"},
                },
            ],
            "states": [
                {"minutes": 4, "previous_state": ConversationStateEnum.NEW, "new_state": ConversationStateEnum.GREETED, "reason": "initial_sms_sent"},
                {"minutes": 39, "previous_state": ConversationStateEnum.GREETED, "new_state": ConversationStateEnum.QUALIFYING, "reason": "agent_transition"},
            ],
            "messages": [
                {
                    "minutes": 4,
                    "direction": MessageDirection.OUTBOUND,
                    "body": (
                        "Hi {first_name}, this is {business_name}. "
                        "If ad spend is up but lead quality is weak, the fix is usually offer clarity, targeting, and faster follow-up. "
                        "What service are you mainly trying to grow?"
                        if is_marketing_profile
                        else "Hi {first_name}, thanks for reaching out to {business_name}. I can help with your {business_name} request."
                    ),
                },
                {
                    "minutes": 34,
                    "direction": MessageDirection.INBOUND,
                    "body": (
                        "I am spending around $1,000 a month on ads and still getting low-quality leads."
                        if is_marketing_profile
                        else f"I need help with {issue}. Not sure what the next step is."
                    ),
                },
                {
                    "minutes": 39,
                    "direction": MessageDirection.OUTBOUND,
                    "body": (
                        "That usually means intent and landing-page messaging are misaligned. "
                        "Are you focused on one city/market or multiple regions?"
                        if is_marketing_profile
                        else q1
                    ),
                },
            ],
        },
        {
            "slug": "booking-sent",
            "source": spec.source,
            "final_state": ConversationStateEnum.BOOKING_SENT,
            "crm_stage": CRM_STAGE_QUALIFIED,
            "opted_out": False,
            "form_answers": {
                form_field: service,
                plan_field: "Next available appointment",
                "notes": "Prefers text updates",
            },
            "crm_tags": ["high budget", "hot"] if spec.client_key == "demo-roofing" else ["hot"],
            "crm_tasks": [
                {
                    "title": "Follow up tomorrow",
                    "description": "Nudge if booking is not completed.",
                    "due_date": date(2026, 3, 11),
                    "status": TASK_STATUS_OPEN,
                }
            ],
            "audit_logs": [
                {"minutes": 0, "event_type": f"{spec.source.value}_webhook_received", "decision": {"seeded": True}, "attach_lead": False},
                {"minutes": 1, "event_type": "lead_normalized", "decision": {"seeded": True, "scenario": "booking-sent"}},
                {"minutes": 5, "event_type": "initial_sms_sent", "decision": {"seeded": True}},
                {
                    "minutes": 44,
                    "event_type": "agent_decision",
                    "decision": {"actions": [{"type": "send_booking_link"}], "next_state": "BOOKING_SENT"},
                },
            ],
            "states": [
                {"minutes": 5, "previous_state": ConversationStateEnum.NEW, "new_state": ConversationStateEnum.GREETED, "reason": "initial_sms_sent"},
                {"minutes": 44, "previous_state": ConversationStateEnum.GREETED, "new_state": ConversationStateEnum.BOOKING_SENT, "reason": "agent_transition"},
            ],
            "messages": [
                {
                    "minutes": 5,
                    "direction": MessageDirection.OUTBOUND,
                    "body": (
                        "Hi {first_name}, this is {business_name}. "
                        "We improve lead quality by tightening targeting, offer framing, and speed-to-lead follow-up."
                        if is_marketing_profile
                        else "Hi {first_name}, thanks for contacting {business_name}. I can help you get scheduled."
                    ),
                },
                {
                    "minutes": 41,
                    "direction": MessageDirection.INBOUND,
                    "body": (
                        "Yes, can we book a strategy call this week?"
                        if is_marketing_profile
                        else f"Yes, can I book the {service} this week?"
                    ),
                },
                {
                    "minutes": 44,
                    "direction": MessageDirection.OUTBOUND,
                    "body": (
                        "Absolutely. Once we review your offer, targeting, and conversion path, we can map quick wins. "
                        "Book here: {booking_url}"
                        if is_marketing_profile
                        else "Absolutely. Here is the fastest way to pick a time: {booking_url}"
                    ),
                },
            ],
        },
        {
            "slug": "booked-confirmed",
            "source": spec.source,
            "final_state": ConversationStateEnum.BOOKED,
            "crm_stage": CRM_STAGE_MEETING_BOOKED
            if spec.client_key == "demo-roofing"
            else (CRM_STAGE_MEETING_COMPLETED if spec.client_key == "demo-medspa" else CRM_STAGE_WON),
            "opted_out": False,
            "form_answers": {
                form_field: service,
                plan_field: "Tuesday morning",
                "notes": "Confirmed by text",
            },
            "crm_tags": ["booked", "high budget"] if spec.client_key == "demo-legal" else ["booked"],
            "crm_tasks": [
                {
                    "title": "Review after meeting",
                    "description": "Prepare personalized proposal summary.",
                    "due_date": date(2026, 3, 12),
                    "status": TASK_STATUS_DONE if spec.client_key != "demo-roofing" else TASK_STATUS_OPEN,
                }
            ],
            "audit_logs": [
                {"minutes": 0, "event_type": f"{spec.source.value}_webhook_received", "decision": {"seeded": True}, "attach_lead": False},
                {"minutes": 2, "event_type": "lead_normalized", "decision": {"seeded": True, "scenario": "booked-confirmed"}},
                {"minutes": 6, "event_type": "initial_sms_sent", "decision": {"seeded": True}},
                {
                    "minutes": 52,
                    "event_type": "agent_decision",
                    "decision": {"actions": [{"type": "send_booking_link"}], "next_state": "BOOKING_SENT"},
                },
                {
                    "minutes": 140,
                    "event_type": "booking_confirmed",
                    "decision": {"slot": "Tuesday 10:30 AM", "seeded": True},
                },
            ],
            "states": [
                {"minutes": 6, "previous_state": ConversationStateEnum.NEW, "new_state": ConversationStateEnum.GREETED, "reason": "initial_sms_sent"},
                {"minutes": 52, "previous_state": ConversationStateEnum.GREETED, "new_state": ConversationStateEnum.BOOKING_SENT, "reason": "agent_transition"},
                {"minutes": 140, "previous_state": ConversationStateEnum.BOOKING_SENT, "new_state": ConversationStateEnum.BOOKED, "reason": "booking_confirmed"},
            ],
            "messages": [
                {
                    "minutes": 6,
                    "direction": MessageDirection.OUTBOUND,
                    "body": (
                        "Hi {first_name}, this is {business_name}. "
                        "We can usually lift conversions by fixing the offer-message match and tightening follow-up speed. "
                        "What are you trying to improve first?"
                        if is_marketing_profile
                        else "Thanks for reaching out to {business_name}. I can help you lock in time today."
                    ),
                },
                {
                    "minutes": 48,
                    "direction": MessageDirection.INBOUND,
                    "body": (
                        "Tomorrow afternoon works. Send me your calendar."
                        if is_marketing_profile
                        else "Tomorrow afternoon works. Can you send the link?"
                    ),
                },
                {"minutes": 52, "direction": MessageDirection.OUTBOUND, "body": "Here you go: {booking_url}"},
                {
                    "minutes": 140,
                    "direction": MessageDirection.OUTBOUND,
                    "body": (
                        "You are confirmed for Tuesday at 10:30 AM. "
                        "Bring your current CPL and lead volume and we will map next steps live."
                        if is_marketing_profile
                        else "You are confirmed for Tuesday at 10:30 AM. Reply here if anything changes."
                    ),
                },
            ],
        },
        {
            "slug": "opted-out",
            "source": spec.source,
            "final_state": ConversationStateEnum.OPTED_OUT,
            "crm_stage": CRM_STAGE_LOST,
            "opted_out": True,
            "form_answers": {
                form_field: service,
                plan_field: "No longer interested",
                "notes": "Requested no further contact",
            },
            "crm_tags": ["no response", "bad fit"],
            "crm_tasks": [],
            "audit_logs": [
                {"minutes": 0, "event_type": f"{spec.source.value}_webhook_received", "decision": {"seeded": True}, "attach_lead": False},
                {"minutes": 1, "event_type": "lead_normalized", "decision": {"seeded": True, "scenario": "opted-out"}},
                {"minutes": 3, "event_type": "initial_sms_sent", "decision": {"seeded": True}},
                {"minutes": 18, "event_type": "compliance_stop", "decision": {"inbound": "STOP", "seeded": True}},
            ],
            "states": [
                {"minutes": 3, "previous_state": ConversationStateEnum.NEW, "new_state": ConversationStateEnum.GREETED, "reason": "initial_sms_sent"},
                {"minutes": 18, "previous_state": ConversationStateEnum.GREETED, "new_state": ConversationStateEnum.OPTED_OUT, "reason": "STOP keyword"},
            ],
            "messages": [
                {"minutes": 3, "direction": MessageDirection.OUTBOUND, "body": "Hi {first_name}, thanks for contacting {business_name}. I can answer questions or help you book."},
                {"minutes": 17, "direction": MessageDirection.INBOUND, "body": "STOP"},
                {"minutes": 18, "direction": MessageDirection.OUTBOUND, "body": "You are unsubscribed and will not receive more messages."},
            ],
        },
        {
            "slug": "handoff",
            "source": spec.source,
            "final_state": ConversationStateEnum.HANDOFF,
            "crm_stage": CRM_STAGE_QUALIFIED,
            "opted_out": False,
            "form_answers": {
                form_field: issue,
                plan_field: "Needs senior review",
                "notes": "Complex questions that need a human follow-up",
            },
            "crm_tags": ["needs handoff", "follow up later"],
            "crm_tasks": [
                {
                    "title": "Review after meeting",
                    "description": "Assign senior rep before next outreach.",
                    "due_date": date(2026, 3, 13),
                    "status": TASK_STATUS_OPEN,
                }
            ],
            "audit_logs": [
                {"minutes": 0, "event_type": f"{spec.source.value}_webhook_received", "decision": {"seeded": True}, "attach_lead": False},
                {"minutes": 2, "event_type": "lead_normalized", "decision": {"seeded": True, "scenario": "handoff"}},
                {"minutes": 5, "event_type": "initial_sms_sent", "decision": {"seeded": True}},
                {
                    "minutes": 55,
                    "event_type": "agent_decision",
                    "decision": {"actions": [{"type": "handoff_to_human"}], "next_state": "HANDOFF"},
                },
                {
                    "minutes": 58,
                    "event_type": "internal_note",
                    "decision": {"note": "Requested a manager call before booking."},
                },
            ],
            "states": [
                {"minutes": 5, "previous_state": ConversationStateEnum.NEW, "new_state": ConversationStateEnum.GREETED, "reason": "initial_sms_sent"},
                {"minutes": 55, "previous_state": ConversationStateEnum.GREETED, "new_state": ConversationStateEnum.HANDOFF, "reason": "agent_transition"},
            ],
            "messages": [
                {
                    "minutes": 5,
                    "direction": MessageDirection.OUTBOUND,
                    "body": (
                        "Thanks for reaching out to {business_name}. I can help triage ad performance and lead handling questions."
                        if is_marketing_profile
                        else "Thanks for reaching out to {business_name}. I can help with the intake process."
                    ),
                },
                {
                    "minutes": 50,
                    "direction": MessageDirection.INBOUND,
                    "body": (
                        "I need to review attribution and pipeline setup with a senior strategist."
                        if is_marketing_profile
                        else f"I have a more involved question about {issue} and need to talk to a person."
                    ),
                },
                {"minutes": 55, "direction": MessageDirection.OUTBOUND, "body": "I am flagging this for a specialist now. For immediate help, call {handoff_number}."},
            ],
        },
        {
            "slug": "after-hours-pending",
            "source": spec.source,
            "final_state": ConversationStateEnum.GREETED,
            "crm_stage": CRM_STAGE_NEW_LEAD,
            "opted_out": False,
            "form_answers": {
                form_field: service,
                plan_field: "Evening inquiry",
                "notes": "Came in after hours",
            },
            "crm_tags": ["follow up later"],
            "crm_tasks": [
                {
                    "title": "Follow up tomorrow",
                    "description": "After-hours inquiry pending first reply.",
                    "due_date": date(2026, 3, 10),
                    "status": TASK_STATUS_OPEN,
                }
            ],
            "audit_logs": [
                {"minutes": 0, "event_type": f"{spec.source.value}_webhook_received", "decision": {"seeded": True}, "attach_lead": False},
                {"minutes": 1, "event_type": "lead_normalized", "decision": {"seeded": True, "scenario": "after-hours-pending"}},
                {"minutes": 6, "event_type": "after_hours_initial_sms_sent", "decision": {"seeded": True}},
            ],
            "states": [
                {"minutes": 6, "previous_state": ConversationStateEnum.NEW, "new_state": ConversationStateEnum.GREETED, "reason": "after_hours_initial_sms_sent"},
            ],
            "messages": [
                {"minutes": 6, "direction": MessageDirection.OUTBOUND, "body": "Thanks for contacting {business_name} after hours. We will follow up when the team is back online."},
            ],
        },
        {
            "slug": "after-hours-followup",
            "source": spec.source,
            "final_state": ConversationStateEnum.QUALIFYING,
            "crm_stage": CRM_STAGE_CONTACTED,
            "opted_out": False,
            "form_answers": {
                form_field: service,
                plan_field: "Morning follow-up requested",
                "notes": "Responded after follow-up",
            },
            "crm_tags": ["follow up later"],
            "crm_tasks": [
                {
                    "title": "Call this lead",
                    "description": "Lead replied after follow-up; confirm preferred slot.",
                    "due_date": date(2026, 3, 11),
                    "status": TASK_STATUS_OPEN,
                }
            ],
            "audit_logs": [
                {"minutes": 0, "event_type": f"{spec.source.value}_webhook_received", "decision": {"seeded": True}, "attach_lead": False},
                {"minutes": 1, "event_type": "lead_normalized", "decision": {"seeded": True, "scenario": "after-hours-followup"}},
                {"minutes": 5, "event_type": "after_hours_initial_sms_sent", "decision": {"seeded": True}},
                {"minutes": 720, "event_type": "follow_up_sms_sent", "decision": {"seeded": True}},
                {
                    "minutes": 790,
                    "event_type": "agent_decision",
                    "decision": {"actions": [{"type": "request_more_info"}], "next_state": "QUALIFYING"},
                },
            ],
            "states": [
                {"minutes": 5, "previous_state": ConversationStateEnum.NEW, "new_state": ConversationStateEnum.GREETED, "reason": "after_hours_initial_sms_sent"},
                {"minutes": 790, "previous_state": ConversationStateEnum.GREETED, "new_state": ConversationStateEnum.QUALIFYING, "reason": "agent_transition"},
            ],
            "messages": [
                {"minutes": 5, "direction": MessageDirection.OUTBOUND, "body": "Thanks for contacting {business_name} after hours. We will follow up as soon as the office opens."},
                {
                    "minutes": 720,
                    "direction": MessageDirection.OUTBOUND,
                    "body": (
                        "Good morning from {business_name}. Before we book, we can quickly diagnose the biggest leak in your ads funnel. "
                        "Where do leads drop most right now?"
                        if is_marketing_profile
                        else "Good morning from {business_name}. If you are ready, you can pick a time here: {booking_url}"
                    ),
                },
                {
                    "minutes": 782,
                    "direction": MessageDirection.INBOUND,
                    "body": (
                        "I think the biggest drop is after form submit. We call too late."
                        if is_marketing_profile
                        else f"I saw the follow-up. Before I book, {q2.lower()}"
                    ),
                },
                {
                    "minutes": 790,
                    "direction": MessageDirection.OUTBOUND,
                    "body": (
                        "That is a common bottleneck. What is your current first-response time once a lead comes in?"
                        if is_marketing_profile
                        else q3
                    ),
                },
            ],
        },
    ]


def _is_marketing_profile_spec(spec: DemoClientSpec) -> bool:
    haystack = " ".join(
        [
            spec.client_key,
            spec.business_name,
            spec.service_label,
            spec.challenge_label,
            spec.faq_context or "",
        ]
    ).lower()
    keywords = (
        "ads",
        "ad ",
        "ad-",
        "lead",
        "crm",
        "conversion",
        "funnel",
        "google ads",
        "meta ads",
        "ppc",
    )
    return any(token in haystack for token in keywords)
