from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from evals.chatbot.schema import (
    CheckResult,
    EvalReport,
    EvalScenario,
    ScenarioResult,
    SchemaError,
    ToolExpectation,
    TurnObservation,
    TurnResult,
    load_scenario,
    load_scenarios,
)


def _scenario_payload(*, scenario_id: str = "support-fr", kind: str = "checkpoint") -> dict:
    turns = [
        {
            "turn_id": "support-question",
            "inbound": "Pouvez-vous expliquer votre service?",
            "replay_outputs": [{"reply_text": "Bien sûr."}],
            "expect": {
                "expected_language": "fr",
                "expected_state": "QUALIFYING",
                "max_questions": 1,
                "expected_tool_names": [],
            },
        }
    ]
    if kind == "journey":
        turns.append({"inbound": "Oui, mardi me convient.", "expect": {"booking_expected": True}})
    return {
        "schema_version": 1,
        "id": scenario_id,
        "owner": "conversation-platform",
        "kind": kind,
        "tags": ["language", "support"],
        "risk": "high",
        "tenant": {
            "business_name": "Entreprise Démo",
            "timezone": "America/Toronto",
            "provider_config": {"language": "fr"},
        },
        "lead": {"full_name": "Marie Tremblay", "state": "QUALIFYING"},
        "initial_history": [
            {"direction": "OUTBOUND", "body": "Bonjour Marie! Comment puis-je vous aider?"}
        ],
        "tool_world": {
            "slots": [
                {
                    "start": "2026-07-21T14:00:00-04:00",
                    "end": "2026-07-21T14:30:00-04:00",
                    "display_time": "mardi 21 juillet à 14 h",
                }
            ]
        },
        "turns": turns,
    }


def test_scenario_schema_parses_nested_fixture_and_serializes() -> None:
    scenario = EvalScenario.from_dict(_scenario_payload())

    assert scenario.id == "support-fr"
    assert scenario.tenant.provider_config == {"language": "fr"}
    assert scenario.lead.state == "QUALIFYING"
    assert scenario.turns[0].expect.expected_tool_names == ()
    assert scenario.tool_world.slots[0].display_time.startswith("mardi")
    assert scenario.to_dict()["turns"][0]["expect"]["expected_language"] == "fr"


def test_richer_expectations_parse_without_turning_semantics_into_snapshots() -> None:
    payload = _scenario_payload()
    payload["turns"][0]["expect"] = {
        "expected_language": "fr",
        "language_switch_from": "en",
        "allowed_actions": ["none", "ask_next_question"],
        "allowed_conversation_acts": ["answer_question", "nurture"],
        "expected_next_states": ["QUALIFYING", "BOOKING_SENT"],
        "allowed_resolution_paths": ["agent_response", "pre_llm_handoff_policy"],
        "required_facts": ["The response explains the warranty."],
        "forbidden_claims": ["The response invents a lifetime warranty."],
        "forbidden_terms": ["lifetime"],
        "forbidden_tools": ["book_slot"],
        "visible_slot_indexes": [1, 2],
        "semantic_criteria": ["Answer before offering a meeting."],
        "tool": {
            "proposed_name": "find_slots",
            "args_subset": {"preferred_day": "tuesday"},
            "max_calls": 1,
        },
        "max_ctas": 1,
    }

    expectation = EvalScenario.from_dict(payload).turns[0].expect

    assert expectation.allowed_actions == ("none", "ask_next_question")
    assert expectation.expected_next_states == ("QUALIFYING", "BOOKING_SENT")
    assert expectation.required_facts == ("The response explains the warranty.",)
    assert expectation.visible_slot_indexes == (1, 2)
    assert expectation.max_meeting_ctas == 1
    assert expectation.max_ctas == 1
    assert expectation.tool == ToolExpectation(
        proposed_name="find_slots",
        args_subset={"preferred_day": "tuesday"},
        max_calls=1,
    )


def test_full_repository_fixture_corpus_matches_schema() -> None:
    fixture_root = Path(__file__).resolve().parents[3] / "evals" / "chatbot" / "fixtures"

    scenarios = load_scenarios(fixture_root)

    assert len(scenarios) >= 10
    assert all(scenario.owner for scenario in scenarios)


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    [
        (lambda payload: payload.update({"unexpected": True}), "unknown field(s): unexpected"),
        (lambda payload: payload.update({"schema_version": 2}), "unsupported version 2"),
        (lambda payload: payload.update({"id": "Not Valid"}), "scenario.id"),
        (lambda payload: payload.update({"kind": "other"}), "checkpoint or journey"),
        (lambda payload: payload["turns"].append({"inbound": "Second"}), "exactly one turn"),
        (
            lambda payload: payload["turns"][0]["expect"].update({"expected_language": "es"}),
            "must be en or fr",
        ),
    ],
)
def test_scenario_schema_rejects_invalid_fixtures(mutation, expected_error: str) -> None:
    payload = _scenario_payload()
    mutation(payload)

    with pytest.raises(SchemaError, match=re.escape(expected_error)):
        EvalScenario.from_dict(payload)


def test_journey_requires_at_least_two_turns() -> None:
    payload = _scenario_payload(kind="journey")
    payload["turns"] = payload["turns"][:1]

    with pytest.raises(SchemaError, match="at least two turns"):
        EvalScenario.from_dict(payload)


def test_expectation_aliases_must_not_conflict() -> None:
    payload = _scenario_payload()
    payload["turns"][0]["expect"].update({"max_ctas": 0, "max_meeting_ctas": 1})

    with pytest.raises(SchemaError, match="max_ctas and max_meeting_ctas must match"):
        EvalScenario.from_dict(payload)


def test_nested_tool_expectation_rejects_unknown_fields() -> None:
    payload = _scenario_payload()
    payload["turns"][0]["expect"]["tool"] = {
        "proposed_name": "none",
        "unexpected": True,
    }

    with pytest.raises(SchemaError, match=r"scenario.turns\[0\].expect.tool"):
        EvalScenario.from_dict(payload)


def test_scenario_owner_is_required() -> None:
    payload = _scenario_payload()
    del payload["owner"]

    with pytest.raises(SchemaError, match="scenario.owner"):
        EvalScenario.from_dict(payload)


def test_load_scenarios_loads_directory_in_order_and_rejects_duplicate_ids(tmp_path) -> None:
    first = tmp_path / "a.json"
    second = tmp_path / "nested" / "b.json"
    second.parent.mkdir()
    first.write_text(json.dumps(_scenario_payload(scenario_id="first")), encoding="utf-8")
    second.write_text(json.dumps(_scenario_payload(scenario_id="second")), encoding="utf-8")

    scenarios = load_scenarios(tmp_path)

    assert [scenario.id for scenario in scenarios] == ["first", "second"]
    assert load_scenario(first).id == "first"

    second.write_text(json.dumps(_scenario_payload(scenario_id="first")), encoding="utf-8")
    with pytest.raises(SchemaError, match="duplicate scenario id.*first"):
        load_scenarios(tmp_path)


def test_report_exposes_aggregate_pass_state_as_json() -> None:
    check = CheckResult(category="content", code="fact", passed=True, detail="present")
    turn = TurnResult(
        turn_index=0,
        observation=TurnObservation(reply="Helpful answer"),
        checks=(check,),
    )
    report = EvalReport(
        agent="v3",
        provider="replay",
        scenarios=(ScenarioResult(scenario_id="support-en", kind="checkpoint", turns=(turn,)),),
    )

    payload = json.loads(report.to_json())

    assert payload["passed"] is True
    assert payload["passed_scenarios"] == 1
    assert payload["failed_scenarios"] == 0
    assert payload["scenarios"][0]["passed"] is True
    assert payload["scenarios"][0]["turns"][0]["passed"] is True
