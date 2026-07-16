import pytest
from redis.exceptions import RedisError

from app.services.compliance import clear_local_rate_limit_state, is_rate_limited


@pytest.fixture(autouse=True)
def reset_local_admission_state():
    clear_local_rate_limit_state()
    yield
    clear_local_rate_limit_state()


def test_missing_redis_uses_bounded_local_fallback_instead_of_failing_open():
    assert is_rate_limited(None, lead_id=42, max_messages=2, window_minutes=1) is False
    assert is_rate_limited(None, lead_id=42, max_messages=2, window_minutes=1) is False
    assert is_rate_limited(None, lead_id=42, max_messages=2, window_minutes=1) is True


def test_redis_error_uses_local_fallback():
    class UnavailableRedis:
        def incr(self, key):
            raise RedisError("offline")

    redis_client = UnavailableRedis()
    assert is_rate_limited(redis_client, lead_id=7, max_messages=1, window_minutes=1) is False
    assert is_rate_limited(redis_client, lead_id=7, max_messages=1, window_minutes=1) is True


def test_compliance_reply_scope_is_independent_from_general_admission_scope():
    assert is_rate_limited(None, lead_id=9, max_messages=1, window_minutes=1) is False
    assert is_rate_limited(
        None,
        lead_id=9,
        max_messages=1,
        window_minutes=1,
        scope="compliance-help",
    ) is False
    assert is_rate_limited(
        None,
        lead_id=9,
        max_messages=1,
        window_minutes=1,
        scope="compliance-help",
    ) is True
