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
