from __future__ import annotations

import pytest

from app.core.config import Settings
from evals.chatbot.runner import (
    EvalConfigurationError,
    deterministic_gate_passed,
    discover_scenarios,
    run_evaluations,
)
from evals.chatbot.schema import (
    CheckResult,
    EvalReport,
    ScenarioResult,
    TurnObservation,
    TurnResult,
)


def _scenario(scenario_id: str):
    scenarios = discover_scenarios("all", scenario_ids=[scenario_id])
    assert len(scenarios) == 1
    return scenarios[0]


def test_replay_runner_executes_real_v3_pipeline_without_credentials() -> None:
    report = run_evaluations(
        [_scenario("code_switch_to_french")],
        provider="replay",
        settings=Settings(_env_file=None, openai_api_key=""),
        suite="test",
    )

    assert report.passed
    assert deterministic_gate_passed(report)
    assert report.metadata["model"] == "replay"
    observation = report.scenarios[0].turns[0].observation
    assert observation.language == "fr"
    assert observation.conversation_act == "answer_question"
    assert not observation.booking_created


def test_journey_completes_booking_through_the_fake_calendar_boundary() -> None:
    report = run_evaluations(
        [_scenario("support_to_booking")],
        provider="replay",
        settings=Settings(_env_file=None, openai_api_key=""),
        suite="journeys",
    )

    assert report.passed, report.to_json()
    booked_turn = report.scenarios[0].turns[-1].observation
    assert booked_turn.state == "BOOKED"
    assert booked_turn.booking_created
    assert booked_turn.resolution_path == "deterministic_booking_flow"
    assert [call["name"] for call in booked_turn.tool_calls] == ["book_slot"]


def test_live_runner_requires_an_explicit_environment_api_key() -> None:
    with pytest.raises(EvalConfigurationError, match="OPENAI_API_KEY"):
        run_evaluations(
            [_scenario("code_switch_to_french")],
            provider="live",
            settings=Settings(_env_file=None, openai_api_key=""),
        )


def test_model_judge_cannot_be_enabled_accidentally_in_replay_mode() -> None:
    with pytest.raises(EvalConfigurationError, match="requires --provider live"):
        run_evaluations(
            [_scenario("code_switch_to_french")],
            provider="replay",
            judge="model",
            settings=Settings(_env_file=None, openai_api_key=""),
        )


def test_semantic_judge_failures_remain_advisory_for_the_deterministic_gate() -> None:
    report = EvalReport(
        agent="v3",
        provider="live",
        scenarios=(
            ScenarioResult(
                scenario_id="advisory-judge",
                kind="checkpoint",
                turns=(
                    TurnResult(
                        turn_index=0,
                        observation=TurnObservation(reply="A grounded answer."),
                        checks=(
                            CheckResult("safety", "no_write", True, "no write"),
                            CheckResult(
                                "semantic.runtime",
                                "model_judge_error",
                                False,
                                "judge unavailable",
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )

    assert not report.passed
    assert deterministic_gate_passed(report)
