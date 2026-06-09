from __future__ import annotations

import json

from sqlalchemy import select

from app.db.models import Client, ConversationStateEnum, Lead, LeadSource
from app.db.session import get_session_factory
from app.services.knowledge import FetchResult, KnowledgeIngestionService, chunk_text, extract_page_text
from app.services.llm_agent import LLMAgent


def _admin_headers() -> dict[str, str]:
    return {"X-Admin-Token": "test-admin-token"}


def test_knowledge_ingestion_endpoint_returns_extraction_result(test_context, monkeypatch):
    def fake_fetch(self, url: str) -> FetchResult:
        _ = self
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

    response = test_context.client.post(
        f"/ui/api/owner/{test_context.client_key}/knowledge/ingest",
        headers=_admin_headers(),
        json={"urls": ["https://acme.example/services"], "replace": True},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_sources"] == 1
    assert payload["total_chunks"] >= 1
    assert payload["extraction"]["pages"][0]["status"] == "ok"
    assert "battery storage" in payload["extraction"]["pages"][0]["text_excerpt"].lower()

    refreshed = test_context.client.get(
        f"/ui/api/owner/{test_context.client_key}/knowledge",
        headers=_admin_headers(),
    )
    assert refreshed.status_code == 200
    source = refreshed.json()["sources"][0]
    assert source["title"] == "Acme Solar Services"
    assert source["chunks"]


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
            assert "knowledge_context" in system_prompt
            payload = json.loads(user_prompt)
            assert "commercial bim" in payload["knowledge_context"].lower()
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
            urls=["https://docs.example/services"],
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
