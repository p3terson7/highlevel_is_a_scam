from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from app.db.models import AuditLog, Client, Lead, LeadSource, OutboundRequest
from app.db.session import get_session_factory
from app.services.llm_agent import AgentResponse, LLMAgent
from app.services.outbound_recovery import reconcile_stale_outbound_requests
from app.services.outbound_requests import fingerprint_payload
from app.services.sms_delivery import with_initial_delivery_status
from app.services.sms_service import SMSDeliveryError, classify_sms_delivery_failure
from app.services.zapier_booking import retry_zapier_booking_webhook_delivery
from app.workers import tasks


class ControlledSMSService:
    def __init__(self, failure: Exception | None = None) -> None:
        self.failure = failure
        self.send_calls = 0
        self.render_contexts: list[dict] = []
        self.bodies: list[str] = []

    def render_template(self, client: Client, template_key: str, context=None) -> str:
        _ = client, template_key
        self.render_contexts.append(dict(context or {}))
        return "Original durable message"

    def send_message(self, to_number: str, body: str) -> str:
        _ = to_number, body
        self.send_calls += 1
        self.bodies.append(body)
        if self.failure is not None:
            raise self.failure
        return f"SM-RELIABLE-{self.send_calls}"

    def with_delivery_status(self, raw_payload: dict | None, provider_sid: str) -> dict:
        return with_initial_delivery_status(
            raw_payload,
            provider_sid=provider_sid,
            provider="mock",
            callback_url="",
        )


def _create_lead(*, external_id: str, phone: str) -> int:
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = Lead(
            client_id=1,
            external_lead_id=external_id,
            source=LeadSource.MANUAL,
            full_name="Reliable Lead",
            phone=phone,
            raw_payload={},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.commit()
        return lead.id


def test_initial_sms_enqueue_uses_non_blocking_twenty_second_delay(monkeypatch):
    class FakeQueue:
        def __init__(self) -> None:
            self.calls: list[tuple] = []

        def enqueue_in(self, delay, func, *args, **kwargs):
            self.calls.append((delay, func, args, kwargs))
            return object()

    queue = FakeQueue()
    monkeypatch.setattr(
        tasks,
        "get_settings",
        lambda: SimpleNamespace(rq_eager=False, automated_sms_delay_seconds=20),
    )
    monkeypatch.setattr(tasks, "get_queue", lambda: queue)

    assert tasks.enqueue_send_initial_sms(123) is not None
    assert len(queue.calls) == 1
    delay, func, args, kwargs = queue.calls[0]
    assert delay == timedelta(seconds=20)
    assert func is tasks.send_initial_sms_task
    assert args == (123,)
    assert kwargs["retry"].max == 3
    assert kwargs["retry"].intervals == [30, 120, 300]


def test_automated_sms_pacing_normalizes_naive_database_timestamps():
    settings = SimpleNamespace(rq_eager=False, automated_sms_delay_seconds=20)
    anchor = datetime(2026, 7, 22, 16, 0)
    older_trigger = datetime(2026, 7, 22, 15, 59, tzinfo=timezone.utc)

    assert tasks._remaining_automated_sms_delay(
        settings,
        older_trigger,
        anchor,
        now=datetime(2026, 7, 22, 16, 0, tzinfo=timezone.utc),
    ) == 20
    assert tasks._remaining_automated_sms_delay(
        settings,
        anchor,
        now=datetime(2026, 7, 22, 16, 0, 20, tzinfo=timezone.utc),
    ) == 0


def test_missing_queue_never_turns_delayed_followup_into_immediate_send(monkeypatch):
    monkeypatch.setattr(
        tasks,
        "get_settings",
        lambda: SimpleNamespace(rq_eager=False),
    )
    monkeypatch.setattr(tasks, "get_queue", lambda: None)

    assert tasks.enqueue_followup_sms(123, reason="after_hours_followup") is False


def test_delayed_initial_sms_is_suppressed_after_lead_starts_conversation(
    test_context,
    monkeypatch,
):
    lead_id = _create_lead(
        external_id="conversation-started-before-greeting",
        phone="+15550001000",
    )
    with get_session_factory()() as db:
        lead = db.get(Lead, lead_id)
        assert lead is not None
        lead.last_inbound_at = datetime.now(timezone.utc)
        db.commit()

    service = ControlledSMSService()
    monkeypatch.setattr(tasks, "build_sms_service", lambda *args, **kwargs: service)
    monkeypatch.setattr(tasks, "_acquire_lead_workflow_lock", lambda **kwargs: None)

    result = tasks.send_initial_sms_task(lead_id)

    assert result == {"status": "skipped", "reason": "conversation_already_started"}
    assert service.send_calls == 0
    with get_session_factory()() as db:
        audit = db.scalar(
            select(AuditLog)
            .where(
                AuditLog.lead_id == lead_id,
                AuditLog.event_type == "initial_sms_skipped",
            )
            .order_by(AuditLog.id.desc())
        )
        assert audit is not None
        assert audit.decision["reason"] == "conversation_already_started"


def test_initial_ai_sms_uses_submitted_lead_language(test_context, monkeypatch):
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        lead = Lead(
            client_id=1,
            external_lead_id="manual-french-form-language",
            source=LeadSource.MANUAL,
            full_name="Julie Gagnon",
            phone="+15550001019",
            email="julie-form-language@example.com",
            form_answers={"lang": "fr"},
            raw_payload={},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.commit()
        lead_id = lead.id

    service = ControlledSMSService()
    monkeypatch.setattr(tasks, "build_sms_service", lambda *args, **kwargs: service)
    monkeypatch.setattr(tasks, "_acquire_lead_workflow_lock", lambda **kwargs: None)
    class FrenchProvider:
        name = "test"

        def generate_json(self, system_prompt, user_prompt):
            import json
            assert json.loads(user_prompt)["response_language"] == "fr"
            return {"reply_text": "Bonjour Julie, ici Hermes, l’assistante de Test Business. Quel résultat recherchez-vous?"}

    monkeypatch.setattr(tasks, "build_llm_agent", lambda **kwargs: LLMAgent(FrenchProvider()))

    result = tasks.send_initial_sms_task(lead_id)

    assert result["status"] == "ok"
    assert service.render_contexts == []
    assert service.bodies[0].startswith("Bonjour Julie")
    assert "Hermes" in service.bodies[0]


def test_sms_failure_classification_distinguishes_rejection_from_unknown_result():
    rejected = classify_sms_delivery_failure(
        SMSDeliveryError("rejected", provider_status=400, provider_code="21610")
    )
    timed_out = classify_sms_delivery_failure(SMSDeliveryError("read timed out"))
    unexpected = classify_sms_delivery_failure(RuntimeError("socket failed"))

    assert rejected.ambiguous is False
    assert rejected.safe_to_retry is True
    assert rejected.reason == "delivery_rejected"
    assert rejected.provider_code == "21610"
    assert timed_out.ambiguous is True
    assert timed_out.safe_to_retry is False
    assert unexpected.ambiguous is True
    assert unexpected.safe_to_retry is False


def test_initial_sms_retries_only_explicit_definitive_failure(test_context, monkeypatch):
    lead_id = _create_lead(
        external_id="definitive-sms-failure",
        phone="+15550001001",
    )
    service = ControlledSMSService(
        SMSDeliveryError("Twilio rejected it", provider_status=400, provider_code="21610")
    )
    monkeypatch.setattr(tasks, "build_sms_service", lambda *args, **kwargs: service)
    monkeypatch.setattr(tasks, "_acquire_lead_workflow_lock", lambda **kwargs: None)

    class ChangingAgent:
        calls = 0

        def run_turn(self, **kwargs):
            self.calls += 1
            return AgentResponse(
                reply_text=f"Opening number {self.calls}",
                collected_fields={"service_needed": f"Project {self.calls}"},
                runtime_payload={"intent_level": "HIGH_INTENT"},
            )

    agent = ChangingAgent()
    monkeypatch.setattr(tasks, "build_llm_agent", lambda **kwargs: agent)

    first = tasks.send_initial_sms_task(lead_id)
    duplicate = tasks.send_initial_sms_task(lead_id)

    assert first["reason"] == "delivery_rejected"
    assert duplicate["reason"] == "delivery_failed"
    assert service.send_calls == 1
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        record = db.scalar(
            select(OutboundRequest).where(OutboundRequest.lead_id == lead_id)
        )
        assert record is not None
        assert record.status == "failed"
        assert record.response_json["safe_to_retry"] is True
        assert record.response_json["attempt_count"] == 1

        assert "qualification_memory" not in db.get(Lead, lead_id).raw_payload
    assert agent.calls == 1

    service.failure = None
    retried = tasks.send_initial_sms_task(lead_id, retry_definitive_failure=True)

    assert retried["status"] == "ok"
    assert service.send_calls == 2
    with SessionLocal() as db:
        record = db.scalar(
            select(OutboundRequest).where(OutboundRequest.lead_id == lead_id)
        )
        assert record is not None
        assert record.status == "completed"
        assert record.response_json["attempt_count"] == 2
        lead = db.get(Lead, lead_id)
        assert lead.raw_payload["qualification_memory"]["service_needed"] == "Project 1"
        assert lead.raw_payload["intent_level"] == "HIGH_INTENT"
    assert agent.calls == 1
    assert service.bodies == ["Opening number 1", "Opening number 1"]


def test_initial_sms_never_retries_ambiguous_provider_result(test_context, monkeypatch):
    lead_id = _create_lead(
        external_id="ambiguous-sms-failure",
        phone="+15550001002",
    )
    service = ControlledSMSService(SMSDeliveryError("read timed out"))
    monkeypatch.setattr(tasks, "build_sms_service", lambda *args, **kwargs: service)
    monkeypatch.setattr(tasks, "_acquire_lead_workflow_lock", lambda **kwargs: None)

    first = tasks.send_initial_sms_task(lead_id)
    retry = tasks.send_initial_sms_task(lead_id, retry_definitive_failure=True)

    assert first["reason"] == "delivery_result_unknown"
    assert retry["reason"] == "delivery_ambiguous"
    assert service.send_calls == 1
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        record = db.scalar(
            select(OutboundRequest).where(OutboundRequest.lead_id == lead_id)
        )
        assert record is not None
        assert record.status == "ambiguous"
        assert record.response_json["safe_to_retry"] is False


def test_initial_sms_cancels_if_send_time_consent_check_fails(test_context, monkeypatch):
    lead_id = _create_lead(
        external_id="consent-changed-before-send",
        phone="+15550001003",
    )
    service = ControlledSMSService()
    monkeypatch.setattr(tasks, "build_sms_service", lambda *args, **kwargs: service)
    monkeypatch.setattr(tasks, "_acquire_lead_workflow_lock", lambda **kwargs: None)
    monkeypatch.setattr(tasks, "lock_lead_for_outbound_delivery", lambda **kwargs: None)

    result = tasks.send_initial_sms_task(lead_id)

    assert result["reason"] == "consent_withdrawn_before_send"
    assert service.send_calls == 0
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        record = db.scalar(
            select(OutboundRequest).where(OutboundRequest.lead_id == lead_id)
        )
        assert record is not None
        assert record.status == "cancelled"


def test_stale_outbound_recovery_is_bounded_and_conservative(test_context):
    lead_id = _create_lead(
        external_id="stale-outbound-recovery",
        phone="+15550001004",
    )
    observed_at = datetime(2026, 7, 13, 15, 0, tzinfo=timezone.utc)
    old = observed_at - timedelta(days=2)
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        rows = [
            OutboundRequest(
                client_id=1,
                lead_id=lead_id,
                idempotency_key="stale-pending",
                request_kind="automated_initial_sms",
                request_fingerprint=fingerprint_payload({"kind": "pending"}),
                status="pending",
                response_json={"attempt_count": 1, "max_attempts": 3},
                created_at=old,
                updated_at=old,
            ),
            OutboundRequest(
                client_id=1,
                lead_id=lead_id,
                idempotency_key="stale-ambiguous",
                request_kind="automated_initial_sms",
                request_fingerprint=fingerprint_payload({"kind": "ambiguous"}),
                status="ambiguous",
                response_json={
                    "attempt_count": 1,
                    "max_attempts": 3,
                    "ambiguous_since": old.isoformat(),
                },
                created_at=old,
                updated_at=old,
            ),
            OutboundRequest(
                client_id=1,
                lead_id=lead_id,
                idempotency_key="retry-safe",
                request_kind="automated_initial_sms",
                request_fingerprint=fingerprint_payload({"kind": "retry"}),
                status="failed",
                response_json={
                    "reason": "initial_ai_sms_sent",
                    "safe_to_retry": True,
                    "attempt_count": 1,
                    "max_attempts": 3,
                },
                created_at=old,
                updated_at=old,
            ),
            OutboundRequest(
                client_id=1,
                lead_id=lead_id,
                idempotency_key="retry-cap-reached",
                request_kind="automated_initial_sms",
                request_fingerprint=fingerprint_payload({"kind": "cap"}),
                status="failed",
                response_json={
                    "safe_to_retry": True,
                    "attempt_count": 3,
                    "max_attempts": 3,
                },
                created_at=old,
                updated_at=old,
            ),
        ]
        db.add_all(rows)
        db.commit()

        result = reconcile_stale_outbound_requests(db=db, now=observed_at)
        db.commit()

        status_by_key = {
            row.idempotency_key: row.status
            for row in db.scalars(
                select(OutboundRequest).where(OutboundRequest.lead_id == lead_id)
            ).all()
        }

    assert result.pending_marked_ambiguous == 1
    assert result.dead_lettered == 2
    assert len(result.retry_directives) == 1
    assert result.retry_directives[0].request_kind == "automated_initial_sms"
    assert status_by_key == {
        "stale-pending": "ambiguous",
        "stale-ambiguous": "dead_letter",
        "retry-safe": "failed",
        "retry-cap-reached": "dead_letter",
    }


def test_zapier_retry_stops_before_a_fourth_provider_attempt(test_context):
    lead_id = _create_lead(
        external_id="zapier-retry-cap",
        phone="+15550001005",
    )
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        record = OutboundRequest(
            client_id=1,
            lead_id=lead_id,
            idempotency_key="zapier-retry-cap",
            request_kind="zapier_booking_webhook",
            request_fingerprint=fingerprint_payload({"kind": "zapier-cap"}),
            status="failed",
            response_json={"delivery_payload": {"event_id": "evt-1"}, "attempt": 3},
        )
        db.add(record)
        db.commit()

        result = retry_zapier_booking_webhook_delivery(db=db, request_id=record.id)
        db.refresh(record)

        assert result == {"status": "failed", "reason": "attempt_cap_reached", "attempt": 3}
        assert record.status == "dead_letter"
        assert record.response_json["dead_letter_reason"] == "attempt_cap_reached"


def test_periodic_recovery_uses_single_tokenized_schedule(test_context, monkeypatch):
    class FakeRedis:
        def __init__(self) -> None:
            self.value: str | None = None

        def set(self, key, value, *, nx, ex):
            _ = key, ex
            if nx and self.value is not None:
                return False
            self.value = value
            return True

        def eval(self, script, key_count, key, token):
            _ = script, key_count, key
            if self.value == token:
                self.value = None
                return 1
            return 0

    class FakeQueue:
        def __init__(self) -> None:
            self.calls: list[tuple] = []

        def enqueue_in(self, *args):
            self.calls.append(args)
            return object()

    redis = FakeRedis()
    queue = FakeQueue()
    monkeypatch.setattr(tasks, "get_settings", lambda: SimpleNamespace(rq_eager=False))
    monkeypatch.setattr(tasks, "get_redis_connection", lambda: redis)
    monkeypatch.setattr(tasks, "get_queue", lambda: queue)

    assert tasks.ensure_outbound_delivery_recovery_scheduled() is True
    assert tasks.ensure_outbound_delivery_recovery_scheduled() is False
    assert len(queue.calls) == 1
    scheduled_token = queue.calls[0][2]
    assert redis.value == scheduled_token

    tasks._clear_outbound_recovery_schedule_marker(redis, "stale-token")
    assert redis.value == scheduled_token
    tasks._clear_outbound_recovery_schedule_marker(redis, scheduled_token)
    assert redis.value is None


@pytest.mark.parametrize("source", list(LeadSource))
@pytest.mark.parametrize("permission", ["missing_consent", "opted_out", "missing_phone"])
def test_initial_ai_outreach_checks_permission_before_model(test_context, monkeypatch, source, permission):
    lead_id = _create_lead(external_id="permission-before-ai", phone="+15550001087")
    with get_session_factory()() as db:
        lead = db.get(Lead, lead_id)
        lead.source = source
        if permission == "missing_consent":
            lead.consented = False
        elif permission == "opted_out":
            lead.opted_out = True
        else:
            lead.phone = ""
        db.commit()

    def unexpected_model(**kwargs):
        pytest.fail("Initial outreach must check permission before invoking the model")

    service = ControlledSMSService()
    monkeypatch.setattr(tasks, "build_llm_agent", unexpected_model)
    monkeypatch.setattr(tasks, "build_sms_service", lambda *args, **kwargs: service)
    result = tasks.send_initial_sms_task(lead_id)
    assert result["reason"] == "missing_sms_permission_or_phone"
    assert service.send_calls == 0


def test_legacy_after_hours_jobs_do_not_schedule_or_send(monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("Retired after-hours jobs must not access providers or enqueue work")

    monkeypatch.setattr(tasks, "get_queue", unexpected)
    monkeypatch.setattr(tasks, "build_sms_service", unexpected)
    monkeypatch.setattr(tasks, "get_session_factory", unexpected)
    assert tasks.enqueue_followup_sms(123) is False
    assert tasks.send_followup_sms_task(123)["reason"] == "after_hours_automation_retired"
    assert tasks.send_followup_sms_task(123, retry_definitive_failure=True)["status"] == "skipped"


@pytest.mark.parametrize("kind, reason", [
    ("automated_initial_sms", "after_hours_initial_sms_sent"),
    ("automated_followup_sms", "after_hours_followup"),
])
def test_failed_legacy_after_hours_outreach_is_never_resent(test_context, monkeypatch, kind, reason):
    lead_id = _create_lead(external_id="retired-hours", phone="+15550001088")
    with get_session_factory()() as db:
        record = OutboundRequest(
            client_id=1, lead_id=lead_id,
            idempotency_key=f"automated-initial-sms:{lead_id}",
            request_kind=kind,
            request_fingerprint=fingerprint_payload({"lead_id": lead_id, "reason": reason}),
            status="failed",
            response_json={"reason": reason, "body": "We are currently offline.", "safe_to_retry": True},
        )
        db.add(record)
        db.commit()
        record_id = record.id

    service = ControlledSMSService()
    monkeypatch.setattr(tasks, "build_sms_service", lambda *args, **kwargs: service)
    task = tasks.send_initial_sms_task if kind == "automated_initial_sms" else tasks.send_followup_sms_task
    assert task(lead_id, retry_definitive_failure=True)["reason"] == "after_hours_automation_retired"
    assert service.send_calls == 0
    with get_session_factory()() as db:
        result = reconcile_stale_outbound_requests(db=db)
        db.commit()
        assert result.retry_directives == ()
        assert db.get(OutboundRequest, record_id).status == "cancelled"


def test_historical_after_hours_logs_do_not_mark_conversation_pending(test_context):
    from app.api.ui.shared import _conversation_tags

    lead_id = _create_lead(external_id="old-hours-tag", phone="+15550001089")
    with get_session_factory()() as db:
        lead = db.get(Lead, lead_id)
        tags = _conversation_tags(lead, [AuditLog(
            client_id=1, lead_id=lead_id,
            event_type="after_hours_initial_sms_sent", decision={},
            created_at=datetime.now(timezone.utc),
        )])
    assert "After-hours pending" not in tags
