from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from app.core.config import Settings
from app.services.twilio_inbound_admission import (
    admit_twilio_inbound,
    reset_local_twilio_admission_state,
)


@pytest.fixture(autouse=True)
def _reset_local_state():
    reset_local_twilio_admission_state()
    yield
    reset_local_twilio_admission_state()


def _settings(**overrides) -> Settings:
    values = {
        "env": "test",
        "twilio_inbound_tenant_limit": 10,
        "twilio_inbound_account_limit": 100,
        "twilio_inbound_window_seconds": 60,
    }
    values.update(overrides)
    return Settings(**values)


def test_local_admission_is_atomic_under_concurrency() -> None:
    settings = _settings(twilio_inbound_tenant_limit=7)

    def reserve(index: int) -> bool:
        return admit_twilio_inbound(
            redis_client=None,
            settings=settings,
            client_id=11,
            account_sid="AC-shared",
            message_sid=f"SM-{index}",
        ).admitted

    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(reserve, range(50)))

    assert sum(results) == 7


def test_duplicate_sid_does_not_consume_another_slot() -> None:
    settings = _settings(twilio_inbound_tenant_limit=1)

    first = admit_twilio_inbound(
        redis_client=None,
        settings=settings,
        client_id=1,
        account_sid="AC-one",
        message_sid="SM-same",
    )
    duplicate = admit_twilio_inbound(
        redis_client=None,
        settings=settings,
        client_id=1,
        account_sid="AC-one",
        message_sid="SM-same",
    )
    novel = admit_twilio_inbound(
        redis_client=None,
        settings=settings,
        client_id=1,
        account_sid="AC-one",
        message_sid="SM-new",
    )

    assert first.admitted is True
    assert duplicate.admitted is True
    assert duplicate.reason == "duplicate"
    assert novel.admitted is False
    assert novel.limiting_scope == "tenant"


def test_shared_account_cap_applies_across_tenants() -> None:
    settings = _settings(twilio_inbound_account_limit=1)

    first = admit_twilio_inbound(
        redis_client=None,
        settings=settings,
        client_id=1,
        account_sid="AC-shared",
        message_sid="SM-one",
    )
    second = admit_twilio_inbound(
        redis_client=None,
        settings=settings,
        client_id=2,
        account_sid="AC-shared",
        message_sid="SM-two",
    )

    assert first.admitted is True
    assert second.admitted is False
    assert second.limiting_scope == "account"


def test_deployed_environment_fails_closed_without_redis() -> None:
    result = admit_twilio_inbound(
        redis_client=None,
        settings=_settings(env="production"),
        client_id=7,
        account_sid="AC-production",
        message_sid="SM-production",
    )

    assert result.admitted is False
    assert result.reason == "coordination_unavailable"
    assert result.backend == "unavailable"


def test_redis_error_only_falls_back_in_local_environment() -> None:
    class BrokenRedis:
        def eval(self, *args, **kwargs):
            raise ConnectionError("redis unavailable")

    local = admit_twilio_inbound(
        redis_client=BrokenRedis(),
        settings=_settings(),
        client_id=1,
        account_sid="AC-local",
        message_sid="SM-local",
    )
    deployed = admit_twilio_inbound(
        redis_client=BrokenRedis(),
        settings=_settings(env="staging"),
        client_id=1,
        account_sid="AC-staging",
        message_sid="SM-staging",
    )

    assert local.admitted is True and local.backend == "local"
    assert deployed.admitted is False and deployed.backend == "unavailable"


def test_redis_admission_uses_two_atomic_scopes_and_hashed_sid() -> None:
    class RecordingRedis:
        def __init__(self) -> None:
            self.args = None

        def eval(self, *args):
            self.args = args
            return [1, 0]

    redis = RecordingRedis()
    result = admit_twilio_inbound(
        redis_client=redis,
        settings=_settings(),
        client_id=22,
        account_sid="AC-secret-ish",
        message_sid="SM-raw-provider-id",
    )

    assert result.admitted is True
    assert redis.args is not None
    assert redis.args[1] == 2
    rendered_args = " ".join(str(value) for value in redis.args[2:])
    assert "SM-raw-provider-id" not in rendered_args
    assert "AC-secret-ish" not in rendered_args
