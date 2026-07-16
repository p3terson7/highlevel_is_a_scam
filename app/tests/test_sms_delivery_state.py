from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Barrier, Lock

from sqlalchemy.dialects import postgresql

from app.api.routes_sms import _lock_delivery_message_for_callback
from app.db.models import Message, MessageDirection
from app.services.sms_delivery import (
    apply_twilio_delivery_callback,
    with_initial_delivery_status,
)


def _outbound_message() -> Message:
    return Message(
        id=1,
        client_id=1,
        lead_id=1,
        direction=MessageDirection.OUTBOUND,
        body="hello",
        provider_message_sid="SM123",
        raw_payload=with_initial_delivery_status(
            {},
            provider_sid="SM123",
            provider="twilio",
            callback_url="https://crm.example/sms/status-callback",
        ),
    )


def test_delivery_callback_is_monotonic_and_terminal():
    message = _outbound_message()

    queued = apply_twilio_delivery_callback(
        message,
        payload={"MessageSid": "SM123", "MessageStatus": "queued"},
        now=datetime(2026, 7, 13, 10, 0, tzinfo=timezone.utc),
    )
    assert queued.applied is True
    assert queued.delivery["status"] == "queued"

    regression = apply_twilio_delivery_callback(
        message,
        payload={"MessageSid": "SM123", "MessageStatus": "accepted"},
    )
    assert regression.applied is False
    assert regression.reason == "status_regression"
    assert message.raw_payload["delivery"]["status"] == "queued"

    delivered = apply_twilio_delivery_callback(
        message,
        payload={"MessageSid": "SM123", "MessageStatus": "delivered"},
    )
    assert delivered.applied is True
    assert message.raw_payload["delivery"]["status"] == "delivered"

    late_sent = apply_twilio_delivery_callback(
        message,
        payload={"MessageSid": "SM123", "MessageStatus": "sent"},
    )
    assert late_sent.applied is False
    assert late_sent.reason == "terminal_status"
    assert message.raw_payload["delivery"]["status"] == "delivered"


def test_delivery_callback_duplicate_and_unknown_status_are_idempotent():
    message = _outbound_message()
    first = apply_twilio_delivery_callback(
        message,
        payload={"MessageSid": "SM123", "MessageStatus": "sent"},
    )
    assert first.applied is True
    first_payload = dict(message.raw_payload)

    duplicate = apply_twilio_delivery_callback(
        message,
        payload={"MessageSid": "SM123", "MessageStatus": "sent"},
    )
    assert duplicate.applied is False
    assert duplicate.reason == "duplicate"
    assert message.raw_payload == first_payload

    unsupported = apply_twilio_delivery_callback(
        message,
        payload={"MessageSid": "SM123", "MessageStatus": "attacker-controlled"},
    )
    assert unsupported.applied is False
    assert unsupported.reason == "unsupported_status"
    assert message.raw_payload == first_payload


def test_delivery_failure_can_transition_from_sent_but_cannot_be_overwritten():
    message = _outbound_message()
    assert apply_twilio_delivery_callback(
        message,
        payload={"MessageSid": "SM123", "MessageStatus": "sent"},
    ).applied

    failed = apply_twilio_delivery_callback(
        message,
        payload={
            "MessageSid": "SM123",
            "MessageStatus": "undelivered",
            "ErrorCode": "30005",
            "ErrorMessage": "Unknown destination handset",
        },
    )
    assert failed.applied is True
    assert failed.delivery["severity"] == "warning"

    delivered_after_failure = apply_twilio_delivery_callback(
        message,
        payload={"MessageSid": "SM123", "MessageStatus": "delivered"},
    )
    assert delivered_after_failure.applied is False
    assert delivered_after_failure.reason == "terminal_status"
    assert message.raw_payload["delivery"]["status"] == "undelivered"
    assert message.raw_payload["delivery"]["error_code"] == "30005"
    assert message.raw_payload["delivery"]["error_message"] == "Unknown destination handset"


def test_duplicate_warning_can_fill_missing_error_detail_once():
    message = _outbound_message()
    first = apply_twilio_delivery_callback(
        message,
        payload={"MessageSid": "SM123", "MessageStatus": "undelivered"},
    )
    assert first.applied is True
    assert first.delivery["error_code"] == ""

    enriched = apply_twilio_delivery_callback(
        message,
        payload={
            "MessageSid": "SM123",
            "MessageStatus": "undelivered",
            "ErrorCode": "30005",
            "ErrorMessage": "Unknown destination handset",
        },
    )
    assert enriched.applied is True
    assert enriched.reason == "error_detail_enriched"
    assert enriched.delivery["error_code"] == "30005"
    assert enriched.delivery["error_message"] == "Unknown destination handset"

    enriched_payload = dict(message.raw_payload)
    duplicate = apply_twilio_delivery_callback(
        message,
        payload={
            "MessageSid": "SM123",
            "MessageStatus": "undelivered",
            "ErrorCode": "30005",
            "ErrorMessage": "Unknown destination handset",
        },
    )
    assert duplicate.applied is False
    assert duplicate.reason == "duplicate"
    assert message.raw_payload == enriched_payload


def test_delivery_callback_lock_query_reloads_the_current_row():
    current = _outbound_message()
    captured = {}

    class RecordingSession:
        def scalar(self, statement):
            captured["statement"] = statement
            return current

    locked = _lock_delivery_message_for_callback(
        db=RecordingSession(),
        message_id=current.id,
        client_id=current.client_id,
        provider_sid=current.provider_message_sid,
    )

    assert locked is current
    statement = captured["statement"]
    assert statement.get_execution_options()["populate_existing"] is True
    compiled = str(statement.compile(dialect=postgresql.dialect()))
    assert "FOR UPDATE" in compiled


def test_serialized_concurrent_callbacks_finish_at_terminal_status_with_error_detail():
    message = _outbound_message()
    start = Barrier(2)
    row_lock = Lock()

    def apply(payload):
        start.wait()
        with row_lock:
            return apply_twilio_delivery_callback(message, payload=payload)

    with ThreadPoolExecutor(max_workers=2) as executor:
        queued = executor.submit(
            apply,
            {"MessageSid": "SM123", "MessageStatus": "queued"},
        )
        failed = executor.submit(
            apply,
            {
                "MessageSid": "SM123",
                "MessageStatus": "undelivered",
                "ErrorCode": "30005",
                "ErrorMessage": "Unknown destination handset",
            },
        )
        results = [queued.result(), failed.result()]

    assert sum(result.applied and result.delivery["status"] == "undelivered" for result in results) == 1
    assert message.raw_payload["delivery"]["status"] == "undelivered"
    assert message.raw_payload["delivery"]["error_code"] == "30005"
    assert message.raw_payload["delivery"]["error_message"] == "Unknown destination handset"
