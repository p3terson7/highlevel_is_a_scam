from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from app.core.config import Settings
from app.services import login_rate_limit


def test_redis_login_admission_uses_single_atomic_script(monkeypatch):
    class FakeRedis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, int, tuple[object, ...]]] = []

        def eval(self, script: str, key_count: int, *arguments: object) -> int:
            self.calls.append((script, key_count, arguments))
            return 1

    fake_redis = FakeRedis()
    monkeypatch.setattr(login_rate_limit, "_redis_client", lambda _: fake_redis)

    admission = login_rate_limit.admit_portal_login_attempt(
        settings=Settings(redis_url="redis://example.invalid/0"),
        email="owner@example.com",
        remote_ip="192.0.2.20",
    )

    assert admission is not None
    assert admission.backend == "redis"
    assert len(fake_redis.calls) == 1
    script, key_count, arguments = fake_redis.calls[0]
    assert key_count == 3
    assert "ZCARD" in script and "ZADD" in script
    assert len(arguments) == 8


def test_local_login_admission_is_atomic_under_concurrency(monkeypatch):
    monkeypatch.setattr(login_rate_limit, "_redis_client", lambda _: None)
    login_rate_limit.reset_local_portal_login_limits()
    barrier = Barrier(20)
    settings = Settings()

    def attempt() -> bool:
        barrier.wait()
        return (
            login_rate_limit.admit_portal_login_attempt(
                settings=settings,
                email="target@example.com",
                remote_ip="192.0.2.25",
            )
            is not None
        )

    try:
        with ThreadPoolExecutor(max_workers=20) as executor:
            admitted = list(executor.map(lambda _: attempt(), range(20)))
        assert sum(admitted) == 5
    finally:
        login_rate_limit.reset_local_portal_login_limits()


def test_success_clears_account_buckets_and_its_ip_reservation(monkeypatch):
    monkeypatch.setattr(login_rate_limit, "_redis_client", lambda _: None)
    login_rate_limit.reset_local_portal_login_limits()
    settings = Settings()
    arguments = {
        "settings": settings,
        "email": "owner@example.com",
        "remote_ip": "192.0.2.30",
    }

    try:
        admissions = [
            login_rate_limit.admit_portal_login_attempt(**arguments) for _ in range(5)
        ]
        assert all(admission is not None for admission in admissions)
        assert login_rate_limit.admit_portal_login_attempt(**arguments) is None

        successful_admission = admissions[-1]
        assert successful_admission is not None
        login_rate_limit.clear_portal_login_failures(
            **arguments,
            admission=successful_admission,
        )

        assert all(
            login_rate_limit.admit_portal_login_attempt(**arguments) is not None
            for _ in range(5)
        )
        assert login_rate_limit.admit_portal_login_attempt(**arguments) is None
    finally:
        login_rate_limit.reset_local_portal_login_limits()
