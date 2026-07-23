from __future__ import annotations

import json
from copy import deepcopy

from app.db.models import Client, ConversationStateEnum, Lead, LeadSource
from app.services.handoff_policy import _summary


def test_handoff_summary_filters_stored_form_metadata_without_mutating_lead():
    client = Client(
        client_key="handoff-filter",
        business_name="Handoff Filter",
        provider_config={},
    )
    lead = Lead(
        client_id=1,
        source=LeadSource.META,
        full_name="Stored Metadata Lead",
        phone="+15145550191",
        email="stored-metadata@example.com",
        city="Montréal",
        form_answers={
            "form_type": "quote_request",
            "lang": "fr",
            "services": "Scan 3D",
            "timeline": "Cette semaine",
        },
        raw_payload={
            "intent_level": "MEDIUM_INTENT",
            "lead_summary": {
                "form_type": "quote_request",
                "lang": "fr",
                "form_answers_summary": (
                    "Form Type: quote_request | Lang: fr | "
                    "Services: Scan 3D | Timeline: Cette semaine"
                ),
                "service_interest": "quote_request",
                "timeline": "Cette semaine",
                "recommended_follow_up": "Review the project.",
            },
        },
        consented=True,
        opted_out=False,
        conversation_state=ConversationStateEnum.QUALIFYING,
    )
    original_answers = deepcopy(lead.form_answers)
    original_raw_payload = deepcopy(lead.raw_payload)

    payload = _summary(
        client=client,
        lead=lead,
        inbound_text="Please have someone contact me.",
        history=[],
        level="required",
        reason="explicit_human_request",
        media_attachments=None,
    )

    assert payload["form_answers"] == {
        "services": "Scan 3D",
        "when_to_start": "Cette semaine",
    }
    assert payload["lead_summary"] == {
        "form_answers_summary": "Timeline: Cette semaine | Services: Scan 3D",
        "timeline": "Cette semaine",
        "recommended_follow_up": "Review the project.",
    }
    serialized = json.dumps(payload)
    assert "quote_request" not in serialized
    assert "Form Type" not in serialized
    assert '"form_type"' not in serialized
    assert '"lang"' not in serialized
    assert lead.form_answers == original_answers
    assert lead.raw_payload == original_raw_payload
