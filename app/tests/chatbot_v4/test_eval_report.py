from __future__ import annotations

import json

from evals.chatbot.report import category_scores, render_markdown, write_report
from evals.chatbot.schema import (
    CheckResult,
    EvalReport,
    ScenarioResult,
    TurnObservation,
    TurnResult,
)


def _report() -> EvalReport:
    return EvalReport(
        agent="v3",
        provider="replay",
        scenarios=(
            ScenarioResult(
                scenario_id="report-example",
                kind="checkpoint",
                duration_ms=12,
                turns=(
                    TurnResult(
                        turn_index=0,
                        observation=TurnObservation(reply="Helpful response"),
                        checks=(
                            CheckResult("safety", "no_write", True, "no write"),
                            CheckResult("content", "answer", False, "missing answer"),
                        ),
                    ),
                ),
            ),
        ),
        metadata={"suite": "smoke", "duration_ms": 12},
    )


def test_report_summarizes_categories_and_failures() -> None:
    report = _report()

    scores = {score.category: score for score in category_scores(report)}
    assert scores["safety"].rate == 1.0
    assert scores["content"].rate == 0.0
    rendered = render_markdown(report)
    assert "**FAIL**" in rendered
    assert "content / answer" in rendered
    assert "missing answer" in rendered


def test_report_writes_json_and_markdown(tmp_path) -> None:
    json_path, markdown_path = write_report(_report(), tmp_path)

    assert json.loads(json_path.read_text())["failed_scenarios"] == 1
    assert "Chatbot evaluation report" in markdown_path.read_text()
