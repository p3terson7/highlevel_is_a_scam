from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from evals.chatbot.schema import CheckResult, EvalReport


@dataclass(frozen=True)
class CategoryScore:
    category: str
    passed: int
    total: int

    @property
    def rate(self) -> float:
        return self.passed / self.total if self.total else 0.0


def category_scores(report: EvalReport) -> tuple[CategoryScore, ...]:
    counts: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for scenario in report.scenarios:
        for turn in scenario.turns:
            for check in turn.checks:
                counts[check.category][1] += 1
                if check.passed:
                    counts[check.category][0] += 1
    return tuple(
        CategoryScore(category=category, passed=values[0], total=values[1])
        for category, values in sorted(counts.items())
    )


def failed_checks(report: EvalReport) -> tuple[tuple[str, int, CheckResult], ...]:
    failures: list[tuple[str, int, CheckResult]] = []
    for scenario in report.scenarios:
        for turn in scenario.turns:
            failures.extend(
                (scenario.scenario_id, turn.turn_index, check)
                for check in turn.checks
                if not check.passed
            )
    return tuple(failures)


def _cell(value: Any) -> str:
    text = str(value if value is not None else "")
    return " ".join(text.replace("|", "\\|").split())


def render_markdown(report: EvalReport) -> str:
    metadata = report.metadata or {}
    lines = [
        "# Chatbot evaluation report",
        "",
        f"- Result: **{'PASS' if report.passed else 'FAIL'}**",
        f"- Agent: `{_cell(report.agent)}`",
        f"- Provider: `{_cell(report.provider)}`",
        f"- Scenarios: {report.passed_scenarios}/{len(report.scenarios)} passed",
        f"- Generated: `{report.generated_at.isoformat()}`",
    ]
    for key in ("suite", "samples", "judge", "duration_ms", "model"):
        if key in metadata and metadata[key] not in (None, ""):
            lines.append(f"- {key.replace('_', ' ').title()}: `{_cell(metadata[key])}`")

    scores = category_scores(report)
    if scores:
        lines.extend(
            [
                "",
                "## Category scores",
                "",
                "| Category | Passed | Rate |",
                "| --- | ---: | ---: |",
            ]
        )
        lines.extend(
            f"| {_cell(score.category)} | {score.passed}/{score.total} | {score.rate:.1%} |"
            for score in scores
        )

    lines.extend(
        [
            "",
            "## Scenarios",
            "",
            "| Scenario | Kind | Turns | Result | Duration |",
            "| --- | --- | ---: | --- | ---: |",
        ]
    )
    for scenario in report.scenarios:
        result = "PASS" if scenario.passed else "FAIL"
        duration = f"{scenario.duration_ms} ms" if scenario.duration_ms is not None else ""
        lines.append(
            f"| `{_cell(scenario.scenario_id)}` | {_cell(scenario.kind)} | "
            f"{len(scenario.turns)} | **{result}** | {duration} |"
        )

    failures = failed_checks(report)
    errors = [
        (scenario.scenario_id, scenario.error)
        for scenario in report.scenarios
        if scenario.error
    ]
    turn_errors = [
        (scenario.scenario_id, turn.turn_index, turn.error)
        for scenario in report.scenarios
        for turn in scenario.turns
        if turn.error
    ]
    if failures or errors or turn_errors:
        lines.extend(
            [
                "",
                "## Failures",
                "",
                "| Scenario | Turn | Check | Detail |",
                "| --- | ---: | --- | --- |",
            ]
        )
        for scenario_id, error in errors:
            lines.append(f"| `{_cell(scenario_id)}` |  | runtime | {_cell(error)} |")
        for scenario_id, turn_index, error in turn_errors:
            lines.append(
                f"| `{_cell(scenario_id)}` | {turn_index + 1} | runtime | {_cell(error)} |"
            )
        for scenario_id, turn_index, check in failures:
            lines.append(
                f"| `{_cell(scenario_id)}` | {turn_index + 1} | "
                f"{_cell(check.category)} / {_cell(check.code)} | {_cell(check.detail)} |"
            )

    lines.extend(
        [
            "",
            "Reports contain synthetic evaluation data. Live runs are opt-in and must not use production transcripts.",
            "",
        ]
    )
    return "\n".join(lines)


def write_report(report: EvalReport, output_dir: str | Path) -> tuple[Path, Path]:
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    json_path = destination / "report.json"
    markdown_path = destination / "report.md"
    json_path.write_text(
        json.dumps(report.to_dict(), indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, markdown_path


__all__ = [
    "CategoryScore",
    "category_scores",
    "failed_checks",
    "render_markdown",
    "write_report",
]
