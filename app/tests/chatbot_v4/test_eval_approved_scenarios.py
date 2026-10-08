from __future__ import annotations

import json

from app.core.config import Settings
from evals.chatbot.adapters.v3 import ReplayProvider, V3ScenarioAdapter
from evals.chatbot.runner import (
    deterministic_gate_passed,
    discover_scenarios,
    run_evaluations,
)


APPROVED_SCENARIO_GROUPS = {
    "urgent_callback": {"lead_led_urgent_callback_from_contact_form"},
    "negated_callback": {"negated_callback_stays_in_sms"},
    "answer_before_stale_script": {"answer_then_pricing_question_first"},
    "exact_time_available": {"exact_requested_time_confirmation_books_once"},
    "exact_time_unavailable": {
        "exact_requested_time_unavailable_nearest_alternatives"
    },
    "supersede_visible_offer": {"new_time_request_supersedes_active_offer"},
    "decision_maker_not_handoff": {"decision_maker_response_is_not_handoff"},
    "source_and_form_shape_independence": {
        "source_independence_website_arbitrary_fields",
        "source_independence_meta_arbitrary_fields",
        "source_independence_linkedin_arbitrary_fields",
    },
    "language_switch_applies_to_composed_copy": {
        "english_to_french_composed_copy_stays_french"
    },
    "ambiguous_delivery_freezes_booking": {
        "ambiguous_offer_delivery_freezes_selection"
    },
}

APPROVED_SCENARIO_IDS = {
    scenario_id
    for scenario_ids in APPROVED_SCENARIO_GROUPS.values()
    for scenario_id in scenario_ids
}

CURRENTLY_GREEN_APPROVED_IDS = {
    "english_to_french_composed_copy_stays_french",
    "exact_requested_time_confirmation_books_once",
    "new_time_request_supersedes_active_offer",
    "source_independence_website_arbitrary_fields",
    "source_independence_meta_arbitrary_fields",
    "source_independence_linkedin_arbitrary_fields",
    "ambiguous_offer_delivery_freezes_selection",
}


def _run(scenario_ids: set[str], *, suite: str):
    scenarios = discover_scenarios("all", scenario_ids=scenario_ids)
    report = run_evaluations(
        scenarios,
        provider="replay",
        workers=min(4, len(scenarios)),
        settings=Settings(_env_file=None, openai_api_key=""),
        suite=suite,
    )
    return scenarios, report


def test_approved_pack_contains_ten_behavior_groups_with_structural_checks() -> None:
    scenarios = discover_scenarios("all", scenario_ids=APPROVED_SCENARIO_IDS)

    assert len(APPROVED_SCENARIO_GROUPS) == 10
    assert len(scenarios) == 12
    assert {scenario.id for scenario in scenarios} == APPROVED_SCENARIO_IDS
    for scenario in scenarios:
        assert scenario.risk in {"high", "critical"}
        for turn in scenario.turns:
            expectation = turn.expect
            assert expectation.expected_state or expectation.expected_next_states
            assert expectation.expected_action or expectation.allowed_actions
            assert expectation.booking_expected is not None
            assert expectation.handoff_expected is not None
            assert expectation.expected_tool_names is not None
            assert expectation.must_include or expectation.any_of


def test_currently_green_approved_scenarios_are_regression_gated() -> None:
    scenarios, report = _run(
        CURRENTLY_GREEN_APPROVED_IDS,
        suite="approved-agent-scenarios-green",
    )

    assert {scenario.id for scenario in scenarios} == CURRENTLY_GREEN_APPROVED_IDS
    assert report.passed, report.to_json()
    assert deterministic_gate_passed(report)


def test_source_and_arbitrary_form_shapes_have_equivalent_behavior() -> None:
    source_ids = APPROVED_SCENARIO_GROUPS["source_and_form_shape_independence"]
    scenarios, report = _run(source_ids, suite="approved-source-equivalence")

    assert report.passed, report.to_json()
    observations = [scenario.turns[0].observation for scenario in report.scenarios]
    assert {observation.lead_source for observation in observations} == {
        "manual",
        "meta",
        "linkedin",
    }
    assert len(
        {
            (
                observation.reply,
                observation.state,
                observation.action,
                observation.conversation_act,
                observation.language,
                observation.booking_created,
                observation.handoff_requested,
                tuple(call.get("name") for call in observation.tool_calls),
            )
            for observation in observations
        }
    ) == 1

    prompt_key_sets: set[frozenset[str]] = set()
    for scenario in scenarios:
        inbound = scenario.turns[0].inbound.casefold()
        for submitted_fact in (
            "partenaire à long terme",
            "rétro-concevoir",
            "step",
            "stl",
            "septembre",
        ):
            assert submitted_fact not in inbound

        provider = ReplayProvider(
            [output for turn in scenario.turns for output in turn.replay_outputs],
            scenario_id=scenario.id,
        )
        adapter_result = V3ScenarioAdapter(scenario, provider=provider).run()
        assert not adapter_result.errors
        assert len(provider.calls) == 1

        prompt = json.loads(provider.calls[0].user_prompt)
        prompt_answers = prompt["lead_form_answers"]
        assert prompt_answers == scenario.lead.form_answers
        prompt_key_sets.add(frozenset(prompt_answers))

        submitted_values = json.dumps(prompt_answers, ensure_ascii=False).casefold()
        for submitted_fact in ("partenaire", "septembre", "step", "stl"):
            assert submitted_fact in submitted_values

    assert len(prompt_key_sets) == 3
