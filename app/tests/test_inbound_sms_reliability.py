from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Barrier
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from app.api import routes_sms
from app.core.config import Settings, get_settings
from app.core.deps import get_app_settings
from app.db.models import (
    AuditLog,
    InboundWebhookEvent,
    Lead,
    LeadSource,
    Message,
    MessageDirection,
)
from app.db.session import get_session_factory
from app.services.inbound_work import (
    INBOUND_WORK_COMPLETED,
    INBOUND_WORK_DEAD_LETTER,
    INBOUND_WORK_ENQUEUED,
    INBOUND_WORK_QUEUED,
    INBOUND_WORK_RETRYABLE_FAILED,
    claim_inbound_work,
)
from app.services.twilio_inbound_admission import TwilioInboundAdmission
from app.workers import tasks


def _seed_queued_message(*, phone: str, sid: str, body: str = "I need a quote") -> tuple[int, int]:
    with get_session_factory()() as db:
        lead = Lead(
            client_id=1,
            external_lead_id=f"reliability-{sid}",
            source=LeadSource.SMS,
            full_name="",
            phone=phone,
            email="",
            city="",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.flush()
        message = Message(
            client_id=1,
            lead_id=lead.id,
            direction=MessageDirection.INBOUND,
            body=body,
            provider_message_sid=sid,
            raw_payload={},
            inbound_work_status=INBOUND_WORK_QUEUED,
            inbound_work_updated_at=datetime.now(timezone.utc),
        )
        db.add(message)
        db.commit()
        return lead.id, message.id


def _route_settings(*, rq_eager: bool) -> Settings:
    values = get_settings().model_dump()
    values.update(
        {
            "env": "test",
            "rq_eager": rq_eager,
            "allow_unsigned_twilio_webhooks": True,
        }
    )
    return Settings(**values)


def test_quota_rejection_returns_twiml_without_creating_work(test_context, monkeypatch) -> None:
    monkeypatch.setattr(
        routes_sms,
        "admit_twilio_inbound",
        lambda **kwargs: TwilioInboundAdmission(
            False,
            "limit_exceeded",
            "redis",
            limiting_scope="tenant",
            retry_after_seconds=60,
            sid_fingerprint="abc123",
        ),
    )

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={
            "From": "+15559990001",
            "Body": "private lead message",
            "MessageSid": "SM-RAW-PRIVATE-001",
            "NumMedia": "1",
            "MediaUrl0": "https://api.twilio.com/private-media",
            "MediaContentType0": "image/jpeg",
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/xml")
    assert "<Response></Response>" in response.text
    with get_session_factory()() as db:
        assert db.scalar(select(Lead).where(Lead.phone == "+15559990001")) is None
        assert db.scalar(select(Message).where(Message.provider_message_sid == "SM-RAW-PRIVATE-001")) is None
        assert db.scalar(select(InboundWebhookEvent)) is None
        audit = db.scalar(
            select(AuditLog).where(AuditLog.event_type == "twilio_inbound_admission_rejected")
        )
        assert audit is not None
        rendered_audit = str(audit.decision)
        assert "+15559990001" not in rendered_audit
        assert "private lead message" not in rendered_audit
        assert "SM-RAW-PRIVATE-001" not in rendered_audit


def test_admission_is_not_called_before_twilio_authentication(test_context, monkeypatch) -> None:
    from app.main import app

    values = _route_settings(rq_eager=False).model_dump()
    values.update(
        {
            "allow_unsigned_twilio_webhooks": False,
            "twilio_account_sid": "AC-expected",
            "twilio_auth_token": "twilio-auth-secret",
            "twilio_from_number": "+15550000000",
        }
    )
    app.dependency_overrides[get_app_settings] = lambda: Settings(**values)

    def unexpected_admission(**kwargs):
        raise AssertionError("admission ran before Twilio authentication")

    monkeypatch.setattr(routes_sms, "admit_twilio_inbound", unexpected_admission)
    try:
        response = test_context.client.post(
            f"/sms/inbound/{test_context.client_key}",
            data={
                "AccountSid": "AC-attacker",
                "To": "+15550000000",
                "From": "+15559990009",
                "Body": "hello",
                "MessageSid": "SM-UNSIGNED",
            },
        )
    finally:
        app.dependency_overrides.pop(get_app_settings, None)

    assert response.status_code == 403


def test_rotating_sender_numbers_cannot_bypass_tenant_admission(test_context, monkeypatch) -> None:
    from app.main import app

    values = _route_settings(rq_eager=False).model_dump()
    values.update(
        {
            "twilio_inbound_tenant_limit": 1,
            "twilio_inbound_account_limit": 10,
            "twilio_inbound_window_seconds": 60,
        }
    )
    app.dependency_overrides[get_app_settings] = lambda: Settings(**values)
    monkeypatch.setattr(routes_sms, "enqueue_process_inbound_sms", lambda **kwargs: False)
    try:
        first = test_context.client.post(
            f"/sms/inbound/{test_context.client_key}",
            data={
                "From": "+15559990101",
                "Body": "first lead",
                "MessageSid": "SM-ROTATE-ONE",
            },
        )
        second = test_context.client.post(
            f"/sms/inbound/{test_context.client_key}",
            data={
                "From": "+15559990102",
                "Body": "second lead",
                "MessageSid": "SM-ROTATE-TWO",
            },
        )
    finally:
        app.dependency_overrides.pop(get_app_settings, None)

    assert first.status_code == 200
    assert second.status_code == 200
    with get_session_factory()() as db:
        leads = db.scalars(
            select(Lead).where(Lead.phone.in_(("+15559990101", "+15559990102")))
        ).all()
        assert [lead.phone for lead in leads] == ["+15559990101"]
        assert db.scalar(
            select(AuditLog).where(AuditLog.event_type == "twilio_inbound_admission_rejected")
        ) is not None


def test_known_lead_stop_is_preserved_when_admission_is_unavailable(test_context, monkeypatch) -> None:
    with get_session_factory()() as db:
        lead = Lead(
            client_id=1,
            external_lead_id="known-stop-lead",
            source=LeadSource.MANUAL,
            full_name="",
            phone="+15559990002",
            email="",
            city="",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
        )
        db.add(lead)
        db.commit()
        lead_id = lead.id

    monkeypatch.setattr(
        routes_sms,
        "admit_twilio_inbound",
        lambda **kwargs: TwilioInboundAdmission(
            False,
            "coordination_unavailable",
            "unavailable",
            limiting_scope="coordination",
            retry_after_seconds=60,
            sid_fingerprint="stop123",
        ),
    )
    sent_before = len(test_context.fake_sms.sent)

    response = test_context.client.post(
        f"/sms/inbound/{test_context.client_key}",
        data={"From": "+15559990002", "Body": "STOP", "MessageSid": "SM-STOP-OUTAGE"},
    )

    assert response.status_code == 200
    assert len(test_context.fake_sms.sent) == sent_before
    with get_session_factory()() as db:
        lead = db.get(Lead, lead_id)
        message = db.scalar(
            select(Message).where(Message.provider_message_sid == "SM-STOP-OUTAGE")
        )
        assert lead is not None and lead.opted_out is True and lead.consented is False
        assert message is not None and message.inbound_work_status == INBOUND_WORK_COMPLETED


def test_queue_failure_leaves_durable_work_and_duplicate_reenqueues(test_context, monkeypatch) -> None:
    from app.main import app

    app.dependency_overrides[get_app_settings] = lambda: _route_settings(rq_eager=False)
    monkeypatch.setattr(routes_sms, "enqueue_process_inbound_sms", lambda **kwargs: False)
    payload = {
        "From": "+15559990003",
        "Body": "I need a solar quote",
        "MessageSid": "SM-QUEUE-OUTAGE",
    }
    try:
        first = test_context.client.post(f"/sms/inbound/{test_context.client_key}", data=payload)
        assert first.status_code == 200
        assert test_context.fake_llm.calls == 0
        assert test_context.fake_sms.sent == []

        with get_session_factory()() as db:
            message = db.scalar(
                select(Message).where(Message.provider_message_sid == "SM-QUEUE-OUTAGE")
            )
            assert message is not None
            message_id = message.id
            assert message.inbound_work_status == INBOUND_WORK_QUEUED
            assert db.scalar(
                select(AuditLog).where(
                    AuditLog.lead_id == message.lead_id,
                    AuditLog.event_type == "inbound_sms_queue_handoff_failed",
                )
            ) is not None

        queued: list[int] = []
        monkeypatch.setattr(
            routes_sms,
            "enqueue_process_inbound_sms",
            lambda **kwargs: queued.append(kwargs["inbound_message_id"]) or object(),
        )
        duplicate = test_context.client.post(f"/sms/inbound/{test_context.client_key}", data=payload)
        assert duplicate.status_code == 200
        assert queued == [message_id]
    finally:
        app.dependency_overrides.pop(get_app_settings, None)


def test_database_claim_allows_only_one_concurrent_worker(test_context) -> None:
    _, message_id = _seed_queued_message(phone="+15559990004", sid="SM-CLAIM-ONE")
    worker_count = 8
    barrier = Barrier(worker_count)

    def claim() -> bool:
        barrier.wait()
        with get_session_factory()() as db:
            return claim_inbound_work(db=db, message_id=message_id)

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        results = list(executor.map(lambda _: claim(), range(worker_count)))

    assert sum(results) == 1
    with get_session_factory()() as db:
        message = db.get(Message, message_id)
        assert message is not None
        assert message.inbound_work_attempt_count == 1


def test_queue_handoff_is_deduplicated_before_worker_claim(test_context, monkeypatch) -> None:
    lead_id, message_id = _seed_queued_message(
        phone="+15559990010",
        sid="SM-ONE-QUEUE-HANDOFF",
    )

    class FakeQueue:
        def __init__(self) -> None:
            self.calls: list[tuple] = []

        def enqueue(self, func, **kwargs):
            self.calls.append((func, kwargs))
            return object()

    queue = FakeQueue()
    monkeypatch.setattr(tasks, "get_settings", lambda: SimpleNamespace(rq_eager=False))
    monkeypatch.setattr(tasks, "get_queue", lambda: queue)

    assert tasks.enqueue_process_inbound_sms(lead_id, message_id) is not False
    assert tasks.enqueue_process_inbound_sms(lead_id, message_id) is True
    assert len(queue.calls) == 1
    with get_session_factory()() as db:
        message = db.get(Message, message_id)
        assert message is not None
        assert message.inbound_work_status == INBOUND_WORK_ENQUEUED


def test_startup_webhook_recovery_also_recovers_inbound_text(test_context, monkeypatch) -> None:
    _, message_id = _seed_queued_message(
        phone="+15559990012",
        sid="SM-STARTUP-RECOVERY",
    )
    recovered: list[int] = []
    monkeypatch.setattr(
        tasks,
        "enqueue_process_inbound_sms",
        lambda **kwargs: recovered.append(kwargs["inbound_message_id"]) or object(),
    )

    assert tasks.recover_webhook_inbox_events(limit=10) == 0
    assert recovered == [message_id]


def test_worker_claim_produces_only_one_ai_reply(test_context, monkeypatch) -> None:
    lead_id, message_id = _seed_queued_message(
        phone="+15559990005",
        sid="SM-WORKER-ONCE",
        body="Please tell me about a solar quote",
    )
    monkeypatch.setattr(tasks, "_acquire_lead_workflow_lock", lambda **kwargs: None)
    monkeypatch.setattr(tasks, "build_sms_service", lambda *args, **kwargs: test_context.fake_sms)
    monkeypatch.setattr(tasks, "build_llm_agent", lambda *args, **kwargs: test_context.fake_llm)
    monkeypatch.setattr(tasks, "build_booking_service", lambda *args, **kwargs: test_context.fake_booking)

    first = tasks.process_inbound_sms_task(lead_id, message_id)
    duplicate = tasks.process_inbound_sms_task(lead_id, message_id)

    assert first["status"] == "ok"
    assert duplicate["status"] == "skipped"
    assert test_context.fake_llm.calls == 1
    assert len(test_context.fake_sms.sent) == 1
    with get_session_factory()() as db:
        message = db.get(Message, message_id)
        assert message is not None
        assert message.inbound_work_status == INBOUND_WORK_COMPLETED
        assert message.inbound_work_attempt_count == 1


def test_failed_work_is_recovered_and_completed(test_context, monkeypatch) -> None:
    lead_id, message_id = _seed_queued_message(
        phone="+15559990006",
        sid="SM-WORKER-RECOVER",
        body="Please prepare my solar quote",
    )

    class RaisingAgent:
        def run_turn(self, **kwargs):
            raise RuntimeError("provider failed before a reply")

    monkeypatch.setattr(tasks, "_acquire_lead_workflow_lock", lambda **kwargs: None)
    monkeypatch.setattr(tasks, "build_sms_service", lambda *args, **kwargs: test_context.fake_sms)
    monkeypatch.setattr(tasks, "build_llm_agent", lambda *args, **kwargs: RaisingAgent())
    monkeypatch.setattr(tasks, "build_booking_service", lambda *args, **kwargs: test_context.fake_booking)

    with pytest.raises(RuntimeError):
        tasks.process_inbound_sms_task(lead_id, message_id)

    with get_session_factory()() as db:
        message = db.get(Message, message_id)
        assert message is not None
        assert message.inbound_work_status == INBOUND_WORK_RETRYABLE_FAILED
        assert message.inbound_work_attempt_count == 1
        assert "provider failed" not in message.inbound_work_error

    recovered: list[int] = []
    monkeypatch.setattr(
        tasks,
        "enqueue_process_inbound_sms",
        lambda **kwargs: recovered.append(kwargs["inbound_message_id"]) or object(),
    )
    assert tasks.recover_inbound_sms_work(limit=10) == 1
    assert recovered == [message_id]

    monkeypatch.setattr(tasks, "build_llm_agent", lambda *args, **kwargs: test_context.fake_llm)
    result = tasks.process_inbound_sms_task(lead_id, message_id)
    assert result["status"] == "ok"
    assert test_context.fake_llm.calls == 1
    assert len(test_context.fake_sms.sent) == 1
    with get_session_factory()() as db:
        message = db.get(Message, message_id)
        assert message is not None
        assert message.inbound_work_status == INBOUND_WORK_COMPLETED
        assert message.inbound_work_attempt_count == 2


def test_failed_work_stops_after_bounded_attempts(test_context, monkeypatch) -> None:
    lead_id, message_id = _seed_queued_message(
        phone="+15559990011",
        sid="SM-WORKER-DEAD-LETTER",
    )
    with get_session_factory()() as db:
        message = db.get(Message, message_id)
        assert message is not None
        message.inbound_work_attempt_count = 2
        db.commit()

    class RaisingAgent:
        def run_turn(self, **kwargs):
            raise RuntimeError("do not persist this detail")

    monkeypatch.setattr(tasks, "_acquire_lead_workflow_lock", lambda **kwargs: None)
    monkeypatch.setattr(tasks, "build_sms_service", lambda *args, **kwargs: test_context.fake_sms)
    monkeypatch.setattr(tasks, "build_llm_agent", lambda *args, **kwargs: RaisingAgent())
    monkeypatch.setattr(tasks, "build_booking_service", lambda *args, **kwargs: test_context.fake_booking)

    with pytest.raises(RuntimeError):
        tasks.process_inbound_sms_task(lead_id, message_id)

    with get_session_factory()() as db:
        message = db.get(Message, message_id)
        assert message is not None
        assert message.inbound_work_status == INBOUND_WORK_DEAD_LETTER
        assert message.inbound_work_attempt_count == 3
    assert tasks.recover_inbound_sms_work(limit=10) == 0


def test_periodic_inbound_recovery_has_one_tokenized_schedule(test_context, monkeypatch) -> None:
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

    assert tasks.ensure_inbound_sms_recovery_scheduled() is True
    assert tasks.ensure_inbound_sms_recovery_scheduled() is False
    assert len(queue.calls) == 1
    scheduled_token = queue.calls[0][2]
    assert redis.value == scheduled_token

    tasks._clear_inbound_sms_recovery_schedule_marker(redis, "stale-token")
    assert redis.value == scheduled_token
    tasks._clear_inbound_sms_recovery_schedule_marker(redis, scheduled_token)
    assert redis.value is None
