from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from redis.exceptions import RedisError

from app.api.ui import sandbox_routes
from app.core.config import Settings
from app.core.deps import get_app_settings
from app.services import sandbox_admission


class _AtomicRedis:
    def __init__(self) -> None:
        self._lock = Lock()
        self.members: dict[str, set[str]] = {}

    def eval(self, script: str, key_count: int, *args):
        assert "ZCARD" in script
        assert key_count == 1
        key, limit, _window_seconds, reservation_id = args
        with self._lock:
            members = self.members.setdefault(str(key), set())
            if str(reservation_id) in members:
                return 1
            if len(members) >= int(limit):
                return 0
            members.add(str(reservation_id))
            return 1


class _UnavailableRedis:
    def eval(self, *_args, **_kwargs):
        raise RedisError("redis unavailable")


def _admin_headers() -> dict[str, str]:
    return {"X-Admin-Token": "test-admin-token-32-characters-long!"}


def _start_payload() -> dict:
    return {
        "mode": "gpt_only",
        "full_name": "Sandbox Rate Test",
        "form_answers": [{"question": "Project scope", "answer": "One implementation"}],
    }


def test_redis_sandbox_admission_is_atomic_and_idempotent(monkeypatch):
    redis = _AtomicRedis()
    monkeypatch.setattr(sandbox_admission, "_redis_client", lambda _: redis)
    settings = Settings(env="production", redis_url="redis://coordination.invalid/0")

    def reserve(index: int):
        return sandbox_admission.admit_sandbox_action(
            settings=settings,
            client_id=42,
            action="message",
            limit=7,
            window_seconds=60,
            reservation_id=f"request-{index}",
        )

    with ThreadPoolExecutor(max_workers=32) as executor:
        results = list(executor.map(reserve, range(80)))

    assert sum(result.admitted for result in results) == 7
    assert all(result.backend == "redis" for result in results)

    first = sandbox_admission.admit_sandbox_action(
        settings=settings,
        client_id=99,
        action="start",
        limit=1,
        window_seconds=600,
        reservation_id="same-logical-request",
    )
    repeated = sandbox_admission.admit_sandbox_action(
        settings=settings,
        client_id=99,
        action="start",
        limit=1,
        window_seconds=600,
        reservation_id="same-logical-request",
    )
    different = sandbox_admission.admit_sandbox_action(
        settings=settings,
        client_id=99,
        action="start",
        limit=1,
        window_seconds=600,
        reservation_id="different-request",
    )

    assert first.admitted is True
    assert repeated.admitted is True
    assert different.admitted is False
    assert different.reason == "limit_exceeded"


def test_local_sandbox_fallback_is_concurrency_safe_and_test_only(monkeypatch):
    sandbox_admission.reset_local_sandbox_admission_state()
    monkeypatch.setattr(sandbox_admission, "_redis_client", lambda _: None)
    local_settings = Settings(env="test", redis_url="redis://coordination.invalid/0")

    def reserve(index: int):
        return sandbox_admission.admit_sandbox_action(
            settings=local_settings,
            client_id=17,
            action="message",
            limit=5,
            window_seconds=60,
            reservation_id=f"local-request-{index}",
        )

    try:
        with ThreadPoolExecutor(max_workers=24) as executor:
            results = list(executor.map(reserve, range(60)))
        assert sum(result.admitted for result in results) == 5
        assert all(result.backend == "local" for result in results)

        deployed = sandbox_admission.admit_sandbox_action(
            settings=Settings(
                env="staging", redis_url="redis://coordination.invalid/0"
            ),
            client_id=18,
            action="start",
            limit=10,
            window_seconds=600,
        )
        assert deployed.admitted is False
        assert deployed.reason == "coordination_unavailable"
        assert deployed.backend == "unavailable"
    finally:
        sandbox_admission.reset_local_sandbox_admission_state()


def test_sandbox_has_tenant_scoped_start_and_message_admission_limits(
    test_context, monkeypatch
):
    monkeypatch.setattr(sandbox_routes, "_SANDBOX_START_LIMIT", 1)
    monkeypatch.setattr(sandbox_routes, "_SANDBOX_MESSAGE_LIMIT", 1)

    first_start = test_context.client.post(
        f"/ui/api/owner/{test_context.client_key}/sandbox/start",
        headers=_admin_headers(),
        json=_start_payload(),
    )
    assert first_start.status_code == 200
    lead_id = first_start.json()["lead_id"]

    second_start = test_context.client.post(
        f"/ui/api/owner/{test_context.client_key}/sandbox/start",
        headers=_admin_headers(),
        json=_start_payload(),
    )
    assert second_start.status_code == 429
    assert second_start.headers["Retry-After"]

    first_message = test_context.client.post(
        f"/ui/api/conversations/{lead_id}/sandbox/messages",
        headers=_admin_headers(),
        json={"body": "What does the implementation include?"},
    )
    assert first_message.status_code == 200

    second_message = test_context.client.post(
        f"/ui/api/conversations/{lead_id}/sandbox/messages",
        headers=_admin_headers(),
        json={"body": "And how does onboarding work?"},
    )
    assert second_message.status_code == 429
    assert second_message.headers["Retry-After"]


def test_sandbox_rejects_oversized_model_inputs_before_route_work(test_context):
    oversized_answer = test_context.client.post(
        f"/ui/api/owner/{test_context.client_key}/sandbox/start",
        headers=_admin_headers(),
        json={
            **_start_payload(),
            "form_answers": [{"question": "Project scope", "answer": "x" * 1_001}],
        },
    )
    assert oversized_answer.status_code == 422

    oversized_message = test_context.client.post(
        "/ui/api/conversations/1/sandbox/messages",
        headers=_admin_headers(),
        json={"body": "x" * 2_001},
    )
    assert oversized_message.status_code == 422


def test_sandbox_coordination_outage_fails_closed_before_provider_work(
    test_context, monkeypatch
):
    setup = test_context.client.post(
        f"/ui/api/owner/{test_context.client_key}/sandbox/start",
        headers=_admin_headers(),
        json=_start_payload(),
    )
    assert setup.status_code == 200
    lead_id = setup.json()["lead_id"]

    unavailable = _UnavailableRedis()
    monkeypatch.setattr(sandbox_admission, "_redis_client", lambda _: unavailable)
    provider_calls: list[bool] = []

    def forbidden_provider(*_args, **_kwargs):
        provider_calls.append(True)
        raise AssertionError(
            "provider construction must not run without admission coordination"
        )

    monkeypatch.setattr(sandbox_routes, "build_llm_agent", forbidden_provider)

    from app.main import app

    deployed_settings = Settings(
        env="production",
        redis_url="redis://coordination.invalid/0",
        admin_token="test-admin-token-32-characters-long!",
    )
    app.dependency_overrides[get_app_settings] = lambda: deployed_settings
    try:
        start_response = test_context.client.post(
            f"/ui/api/owner/{test_context.client_key}/sandbox/start",
            headers=_admin_headers(),
            json=_start_payload(),
        )
        message_response = test_context.client.post(
            f"/ui/api/conversations/{lead_id}/sandbox/messages",
            headers=_admin_headers(),
            json={"body": "Does this reach the provider?"},
        )
    finally:
        app.dependency_overrides.pop(get_app_settings, None)

    assert start_response.status_code == 503
    assert (
        start_response.json()["detail"]
        == "AI sandbox start is temporarily unavailable. Try again shortly."
    )
    assert start_response.headers["Retry-After"] == "30"
    assert message_response.status_code == 503
    assert (
        message_response.json()["detail"]
        == "AI sandbox message is temporarily unavailable. Try again shortly."
    )
    assert message_response.headers["Retry-After"] == "30"
    assert provider_calls == []
