from __future__ import annotations

import json
import socket
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select

from app.core.config import Settings
from app.db.models import AuditLog, Client, ConversationStateEnum, Lead, LeadSource
from app.db.session import get_session_factory
from app.services.knowledge import (
    FetchResult,
    KnowledgeIngestionError,
    KnowledgeIngestionService,
    chunk_text,
    extract_page_text,
)
from app.services.llm_agent import LLMAgent
from app.workers import knowledge_tasks
from app.workers.knowledge_tasks import (
    KnowledgeIngestionBusy,
    KnowledgeIngestionQueueUnavailable,
    enqueue_knowledge_ingestion,
    process_knowledge_ingestion_task,
)


def _admin_headers() -> dict[str, str]:
    return {"X-Admin-Token": "test-admin-token-32-characters-long!"}


def test_knowledge_ingestion_endpoint_queues_then_worker_extracts(test_context, monkeypatch):
    fetch_calls: list[str] = []
    queued: dict[str, object] = {}

    def fake_fetch(self, url: str) -> FetchResult:
        _ = self
        fetch_calls.append(url)
        return FetchResult(
            url=url,
            status_code=200,
            html="""
            <html>
              <head><title>Acme Solar Services</title></head>
              <body>
                <nav>Home Services Contact</nav>
                <main>
                  <h1>Solar panel installation and battery backup</h1>
                  <p>We install residential solar systems, battery storage, and EV charger-ready electrical upgrades.</p>
                  <p>Service areas include Austin, Round Rock, and nearby suburbs.</p>
                </main>
              </body>
            </html>
            """,
        )

    monkeypatch.setattr(KnowledgeIngestionService, "_fetch_url", fake_fetch)
    monkeypatch.setattr(
        "app.api.ui.client_routes.enqueue_knowledge_ingestion",
        lambda **kwargs: queued.update(kwargs) or "knowledge-job-1",
    )

    response = test_context.client.post(
        f"/ui/api/owner/{test_context.client_key}/knowledge/ingest",
        headers=_admin_headers(),
        json={
            "urls": ["https://acme.example/services?token=private-crawl-token"],
            "replace": True,
        },
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["status"] == "queued"
    assert payload["job_id"] == "knowledge-job-1"
    assert payload["extraction"]["pages"][0]["status"] == "queued"
    assert payload["extraction"]["pages"][0]["url"] == "https://acme.example/services"
    assert "private-crawl-token" not in json.dumps(payload)
    assert fetch_calls == []
    assert queued["urls"] == [
        "https://acme.example/services?token=private-crawl-token"
    ]

    monkeypatch.setattr(
        knowledge_tasks,
        "_claim_or_renew_knowledge_admission",
        lambda **kwargs: True,
    )
    released: list[dict[str, object]] = []
    monkeypatch.setattr(
        knowledge_tasks,
        "_release_knowledge_admission",
        lambda **kwargs: released.append(kwargs),
    )
    worker_result = process_knowledge_ingestion_task(
        int(queued["client_id"]),
        list(queued["urls"]),
        bool(queued["replace"]),
        str(queued["actor_role"]),
        "admission-token",
    )

    assert worker_result["status"] == "ok"
    assert fetch_calls == [
        "https://acme.example/services?token=private-crawl-token"
    ]
    assert released == [
        {
            "client_id": queued["client_id"],
            "admission_token": "admission-token",
        }
    ]

    refreshed = test_context.client.get(
        f"/ui/api/owner/{test_context.client_key}/knowledge",
        headers=_admin_headers(),
    )
    assert refreshed.status_code == 200
    refreshed_payload = refreshed.json()
    source = refreshed_payload["sources"][0]
    assert source["url"] == "https://acme.example/services"
    assert source["normalized_url"] == "https://acme.example/services"
    assert "private-crawl-token" not in json.dumps(refreshed_payload)
    assert source["title"] == "Acme Solar Services"
    assert source["chunks"]
    assert "battery storage" in refreshed_payload["business_profile_context"].lower()
    with get_session_factory()() as db:
        audit = db.scalar(
            select(AuditLog).where(AuditLog.event_type == "knowledge_urls_ingested")
        )
        assert audit is not None
        assert audit.decision["url_hosts"] == ["acme.example"]
        assert "urls" not in audit.decision


def test_knowledge_ingestion_admission_is_bounded_per_tenant(monkeypatch):
    class FakeRedis:
        def __init__(self) -> None:
            self.values: dict[str, str] = {}

        def set(self, key, value, *, nx, ex):
            _ = ex
            if nx and key in self.values:
                return False
            self.values[str(key)] = str(value)
            return True

    class FakeQueue:
        def __init__(self) -> None:
            self.calls: list[tuple[tuple, dict]] = []

        def enqueue(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return SimpleNamespace(id=f"knowledge-job-{len(self.calls)}")

    redis = FakeRedis()
    queue = FakeQueue()
    monkeypatch.setattr(knowledge_tasks, "get_settings", lambda: Settings(rq_eager=False))
    monkeypatch.setattr(knowledge_tasks, "get_redis_connection", lambda: redis)
    monkeypatch.setattr(knowledge_tasks, "get_knowledge_queue", lambda: queue)

    first_job = enqueue_knowledge_ingestion(
        client_id=10,
        urls=["https://docs.example/services"],
        replace=True,
        actor_role="client",
    )
    with pytest.raises(KnowledgeIngestionBusy):
        enqueue_knowledge_ingestion(
            client_id=10,
            urls=["https://docs.example/pricing"],
            replace=True,
            actor_role="client",
        )
    second_tenant_job = enqueue_knowledge_ingestion(
        client_id=11,
        urls=["https://docs.example/services"],
        replace=True,
        actor_role="client",
    )

    assert first_job == "knowledge-job-1"
    assert second_tenant_job == "knowledge-job-2"
    assert len(queue.calls) == 2


def test_knowledge_queue_failure_releases_tenant_admission(monkeypatch):
    class FakeRedis:
        def __init__(self) -> None:
            self.values: dict[str, str] = {}

        def set(self, key, value, *, nx, ex):
            _ = ex
            if nx and key in self.values:
                return False
            self.values[str(key)] = str(value)
            return True

        def eval(self, script, key_count, key, token, *args):
            _ = script, key_count, args
            if self.values.get(str(key)) == str(token):
                self.values.pop(str(key), None)
                return 1
            return 0

    class FailingQueue:
        def enqueue(self, *args, **kwargs):
            _ = args, kwargs
            raise RuntimeError("queue unavailable")

    redis = FakeRedis()
    monkeypatch.setattr(knowledge_tasks, "get_settings", lambda: Settings(rq_eager=False))
    monkeypatch.setattr(knowledge_tasks, "get_redis_connection", lambda: redis)
    monkeypatch.setattr(knowledge_tasks, "get_knowledge_queue", lambda: FailingQueue())

    with pytest.raises(KnowledgeIngestionQueueUnavailable):
        enqueue_knowledge_ingestion(
            client_id=12,
            urls=["https://docs.example/services"],
            replace=True,
            actor_role="admin",
        )

    assert redis.values == {}


def test_knowledge_ingestion_never_falls_back_inline_in_eager_mode(monkeypatch):
    monkeypatch.setattr(knowledge_tasks, "get_settings", lambda: Settings(rq_eager=True))

    def unexpected_redis():
        raise AssertionError("Eager knowledge ingestion must not reach Redis or run inline")

    monkeypatch.setattr(knowledge_tasks, "get_redis_connection", unexpected_redis)

    with pytest.raises(KnowledgeIngestionQueueUnavailable, match="dedicated background worker"):
        enqueue_knowledge_ingestion(
            client_id=13,
            urls=["https://docs.example/services"],
            replace=True,
            actor_role="admin",
        )


def test_knowledge_endpoint_reports_tenant_admission_conflict(test_context, monkeypatch):
    def busy(**kwargs):
        _ = kwargs
        raise KnowledgeIngestionBusy("Knowledge ingestion already running")

    monkeypatch.setattr(
        "app.api.ui.client_routes.enqueue_knowledge_ingestion",
        busy,
    )

    response = test_context.client.post(
        f"/ui/api/owner/{test_context.client_key}/knowledge/ingest",
        headers=_admin_headers(),
        json={"urls": ["https://acme.example/services"], "replace": True},
    )

    assert response.status_code == 409
    assert response.headers["retry-after"] == "30"


def test_agent_receives_retrieved_website_knowledge_context(test_context, monkeypatch):
    def fake_fetch(self, url: str) -> FetchResult:
        _ = self
        return FetchResult(
            url=url,
            status_code=200,
            html="""
            <html>
              <head><title>Commercial Building Documentation</title></head>
              <body>
                <main>
                  <h1>Commercial BIM and Revit surveys</h1>
                  <p>We provide commercial BIM, Revit models, CAD as-builts, measured surveys, and retail rollout documentation.</p>
                </main>
              </body>
            </html>
            """,
        )

    class KnowledgeAwareProvider:
        name = "knowledge-aware"

        def generate_json(self, system_prompt: str, user_prompt: str):
            assert "private-rag-token" not in system_prompt
            assert "private-rag-token" not in user_prompt
            assert "knowledge_context" in system_prompt
            payload = json.loads(user_prompt)
            assert "commercial bim" in payload["knowledge_context"].lower()
            assert "commercial bim" in payload["business_profile_context"].lower()
            return {
                "reply_text": "Yes, we handle commercial BIM and Revit models for retail spaces.",
                "next_state": "QUALIFYING",
                "collected_fields": payload["qualification_memory"],
                "next_question_key": None,
                "action": "none",
                "tool_call": {"name": "none", "args": {}},
            }

    monkeypatch.setattr(KnowledgeIngestionService, "_fetch_url", fake_fetch)

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        KnowledgeIngestionService().ingest_urls(
            db=db,
            client_id=client.id,
            urls=["https://docs.example/services?token=private-rag-token"],
            replace=True,
        )
        lead = Lead(
            client_id=client.id,
            source=LeadSource.MANUAL,
            full_name="Knowledge Lead",
            phone="+15550002222",
            email="knowledge@example.com",
            city="Austin",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.QUALIFYING,
        )
        db.add(lead)
        db.flush()

        response = LLMAgent(provider=KnowledgeAwareProvider()).run_turn(
            client=client,
            lead=lead,
            inbound_text="Do you do BIM for commercial retail?",
            history=[],
            booking_service=None,
            db=db,
        )

    assert "commercial bim" in response.reply_text.lower()


def test_agent_receives_business_profile_context_when_retrieval_has_no_exact_match(test_context, monkeypatch):
    def fake_fetch(self, url: str) -> FetchResult:
        _ = self
        return FetchResult(
            url=url,
            status_code=200,
            html="""
            <html>
              <head><title>Acme Energy Services</title></head>
              <body>
                <main>
                  <h1>Solar installation and battery storage</h1>
                  <p>We install residential solar systems, battery storage, and EV charger-ready electrical upgrades.</p>
                  <p>Service areas include Austin, Round Rock, and nearby suburbs.</p>
                </main>
              </body>
            </html>
            """,
        )

    class BusinessProfileAwareProvider:
        name = "business-profile-aware"

        def generate_json(self, system_prompt: str, user_prompt: str):
            assert "business_profile_context" in system_prompt
            payload = json.loads(user_prompt)
            assert not payload["knowledge_context"].strip()
            assert "battery storage" in payload["business_profile_context"].lower()
            return {
                "reply_text": "We can help with solar installation and battery storage questions.",
                "next_state": "QUALIFYING",
                "collected_fields": payload["qualification_memory"],
                "next_question_key": None,
                "action": "none",
                "tool_call": {"name": "none", "args": {}},
            }

    monkeypatch.setattr(KnowledgeIngestionService, "_fetch_url", fake_fetch)

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        client = db.scalar(select(Client).where(Client.client_key == test_context.client_key))
        assert client is not None
        KnowledgeIngestionService().ingest_urls(
            db=db,
            client_id=client.id,
            urls=["https://acme.example/services"],
            replace=True,
        )
        lead = Lead(
            client_id=client.id,
            source=LeadSource.MANUAL,
            full_name="Business Memory Lead",
            phone="+15550003333",
            email="business-memory@example.com",
            city="Austin",
            form_answers={},
            raw_payload={},
            consented=True,
            opted_out=False,
            conversation_state=ConversationStateEnum.QUALIFYING,
        )
        db.add(lead)
        db.flush()

        response = LLMAgent(provider=BusinessProfileAwareProvider()).run_turn(
            client=client,
            lead=lead,
            inbound_text="Can you help me understand the basics?",
            history=[],
            booking_service=None,
            db=db,
        )

    assert "battery storage" in response.reply_text.lower()


def test_extraction_filters_navigation_footer_and_chunks_without_mid_word_overlap():
    page = extract_page_text(
        """
        <html>
          <head><title>Case Study: Site Surveys for Starbucks Remodels in California</title></head>
          <body>
            <nav>About Services 3d Scanning & Modeling Building Measurement Portfolio Blog Contact</nav>
            <main>
              <h1>Case Study: Site Surveys for Starbucks Remodels in California</h1>
              <h2>Project Overview</h2>
              <p>Onpoint was awarded a contract to provide site surveys for 23 Starbucks locations across California, spanning from San Diego and Los Angeles up to San Francisco.</p>
              <p>The purpose of the surveys was to support upcoming store remodels, requiring precise documentation of each location's current layout and features.</p>
              <h2>Scope of Work</h2>
              <p>Each site survey included detailed CAD files consisting of floor plans, reflected ceiling plans, and exterior elevations.</p>
              <p>In addition, high-resolution photographs were taken to provide visual context and support the remodel design process.</p>
              <p>The surveys were conducted efficiently, with each location requiring only one day on site, minimizing disruption to store operations.</p>
              <h2>Deliverables</h2>
              <p>Onpoint delivered the survey data within 5 business days of completing each site.</p>
              <p>All 23 Starbucks locations were surveyed and deliverables submitted within 30 days, meeting the project timeline.</p>
            </main>
            <footer>Be the first to know Subscribe to our newsletter Powered by Design Force Marketing Privacy Policy Terms & Conditions</footer>
          </body>
        </html>
        """,
        url="https://www.onpointbuildingdata.com/case-study",
    )

    text = page.text.lower()
    assert "project overview" in text
    assert "about services 3d scanning" not in text
    assert "subscribe to our newsletter" not in text
    assert page.chunks
    assert all(not chunk.startswith(("s.", "treamlined", "cess.", "ing ")) for chunk in page.chunks)


def test_chunk_text_does_not_repeat_previous_chunk_tail():
    text = (
        "Project overview. "
        "Onpoint documented 23 stores across California. "
        "Each survey included CAD floor plans, reflected ceiling plans, and exterior elevations. "
        "High-resolution photographs supported the remodel design process. "
        "Each location required one day on site. "
        "Deliverables were submitted within 5 business days."
    )

    chunks = chunk_text(text, target_chars=115)

    assert len(chunks) > 1
    for previous, current in zip(chunks, chunks[1:]):
        previous_tail = previous[-35:].strip()
        assert previous_tail not in current


def test_knowledge_fetch_rejects_hostname_resolving_to_private_address(monkeypatch):
    def private_dns(host: str, port: int, *, type: int):
        _ = host
        return [(socket.AF_INET, type, socket.IPPROTO_TCP, "", ("127.0.0.1", port))]

    monkeypatch.setattr("app.services.knowledge.socket.getaddrinfo", private_dns)
    transport = httpx.MockTransport(
        lambda request: pytest.fail(f"private target should not be requested: {request.url}")
    )

    with pytest.raises(KnowledgeIngestionError, match="non-public"):
        KnowledgeIngestionService(transport=transport)._fetch_url("https://public-looking.example")


def test_knowledge_fetch_revalidates_and_blocks_private_redirect_target(monkeypatch):
    dns_queries: list[str] = []

    def controlled_dns(host: str, port: int, *, type: int):
        dns_queries.append(host)
        address = "93.184.216.34" if host == "public.example" else "10.20.30.40"
        return [(socket.AF_INET, type, socket.IPPROTO_TCP, "", (address, port))]

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(302, headers={"Location": "http://internal.example/admin"})

    monkeypatch.setattr("app.services.knowledge.socket.getaddrinfo", controlled_dns)

    with pytest.raises(KnowledgeIngestionError, match="non-public"):
        KnowledgeIngestionService(transport=httpx.MockTransport(handler))._fetch_url(
            "https://public.example/start"
        )

    assert dns_queries == ["public.example", "internal.example"]
    assert len(requests) == 1
    assert requests[0].url.host == "93.184.216.34"
    assert requests[0].headers["host"] == "public.example"


def test_knowledge_fetch_preserves_host_while_using_validated_address(monkeypatch):
    def public_dns(host: str, port: int, *, type: int):
        assert host == "public.example"
        return [(socket.AF_INET, type, socket.IPPROTO_TCP, "", ("93.184.216.34", port))]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "93.184.216.34"
        assert request.headers["host"] == "public.example"
        return httpx.Response(
            200,
            headers={"Content-Type": "text/html; charset=utf-8"},
            content=b"<html><body><main>Public website content</main></body></html>",
        )

    monkeypatch.setattr("app.services.knowledge.socket.getaddrinfo", public_dns)

    result = KnowledgeIngestionService(transport=httpx.MockTransport(handler))._fetch_url(
        "https://public.example/services"
    )

    assert result.status_code == 200
    assert result.url == "https://public.example/services"
    assert "Public website content" in result.html


def test_knowledge_fetch_rejects_urls_with_embedded_credentials():
    with pytest.raises(KnowledgeIngestionError, match="credentials"):
        transport = httpx.MockTransport(lambda request: httpx.Response(200))
        KnowledgeIngestionService(transport=transport)._fetch_url(
            "https://user:password@93.184.216.34/private"
        )


def test_knowledge_fetch_rejects_nonstandard_public_ports():
    transport = httpx.MockTransport(
        lambda request: pytest.fail(f"nonstandard port should not be requested: {request.url}")
    )

    with pytest.raises(KnowledgeIngestionError, match="standard HTTPS or HTTP port"):
        KnowledgeIngestionService(transport=transport)._fetch_url(
            "https://93.184.216.34:8443/services"
        )
