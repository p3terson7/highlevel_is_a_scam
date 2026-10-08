from __future__ import annotations

from app.core.config import Settings
from evals.chatbot.runner import deterministic_gate_passed, discover_scenarios, run_evaluations


def test_offline_smoke_suite_is_green_without_network_or_secrets() -> None:
    scenarios = discover_scenarios("smoke")

    report = run_evaluations(
        scenarios,
        provider="replay",
        workers=2,
        settings=Settings(_env_file=None, openai_api_key=""),
        suite="smoke",
    )

    assert len(scenarios) == 5
    assert "fr_concise_expert_booking" in {scenario.id for scenario in scenarios}
    assert report.passed, report.to_json()
    assert deterministic_gate_passed(report)


def test_recent_intro_language_and_specific_time_regressions_are_green() -> None:
    scenario_ids = {
        "duplicate_intro_after_initial_sms",
        "french_handoff_has_no_english_suffix",
        "french_friday_noon_uses_requested_slot",
    }
    scenarios = discover_scenarios("all", scenario_ids=scenario_ids)

    report = run_evaluations(
        scenarios,
        provider="replay",
        workers=3,
        settings=Settings(_env_file=None, openai_api_key=""),
        suite="regression-proof",
    )

    assert {scenario.id for scenario in scenarios} == scenario_ids
    assert report.passed, report.to_json()
    assert deterministic_gate_passed(report)
