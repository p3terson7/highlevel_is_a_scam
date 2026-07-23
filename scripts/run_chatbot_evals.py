#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from evals.chatbot.report import category_scores, write_report  # noqa: E402
from evals.chatbot.runner import (  # noqa: E402
    EvalConfigurationError,
    deterministic_gate_passed,
    discover_scenarios,
    run_evaluations,
)
from evals.chatbot.schema import SchemaError  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Batch-test the production chatbot path with synthetic scenarios and fake external services."
        )
    )
    parser.add_argument("--agent", choices=("v3", "v4", "both"), default="v3")
    parser.add_argument(
        "--suite",
        choices=("smoke", "regression", "journeys", "all"),
        default="smoke",
    )
    parser.add_argument("--provider", choices=("replay", "live"), default="replay")
    parser.add_argument("--samples", type=int, default=1)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--judge", choices=("off", "model"), default="off")
    parser.add_argument("--model", default=None, help="Override OPENAI_MODEL for this live run.")
    parser.add_argument(
        "--judge-model",
        default=None,
        help="Model used by the optional semantic judge; defaults to the evaluated model.",
    )
    parser.add_argument(
        "--scenario",
        action="append",
        default=[],
        help="Run one scenario ID. Repeat to select multiple IDs.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Run only the first N fixtures.")
    parser.add_argument(
        "--max-live-turns",
        type=int,
        default=25,
        help="Refuse live runs containing more than this many sampled lead turns.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPOSITORY_ROOT / "artifacts" / "chatbot-evals",
    )
    parser.add_argument(
        "--fail-on",
        choices=("deterministic", "all", "never"),
        default="deterministic",
        help="Exit policy. Semantic model scores are advisory by default.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List selected fixture IDs without executing the agent.",
    )
    return parser


def _estimated_live_calls(*, turns: int, scenarios: int, samples: int, judge: str) -> int:
    # An active offer can add slot resolution before the planner and post-tool call.
    estimate = turns * samples * 3
    if judge == "model":
        estimate += scenarios * samples
    return estimate


def _gate_passed(report: object, policy: str) -> bool:
    if policy == "never":
        return True
    if policy == "all":
        return bool(getattr(report, "passed", False))
    return deterministic_gate_passed(report)  # type: ignore[arg-type]


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        scenarios = discover_scenarios(args.suite, scenario_ids=args.scenario)
        if args.limit is not None:
            if args.limit < 1:
                raise EvalConfigurationError("--limit must be greater than zero")
            scenarios = scenarios[: args.limit]
        if not scenarios:
            raise EvalConfigurationError("no scenarios selected")

        if args.list:
            for scenario in scenarios:
                print(
                    f"{scenario.id}\t{scenario.kind}\t{scenario.risk}\t"
                    f"owner={scenario.owner}\tturns={len(scenario.turns)}"
                )
            return 0

        if args.provider == "live":
            selected_turns = sum(len(scenario.turns) for scenario in scenarios) * args.samples
            estimated_calls = _estimated_live_calls(
                turns=sum(len(scenario.turns) for scenario in scenarios),
                scenarios=len(scenarios),
                samples=args.samples,
                judge=args.judge,
            )
            if args.max_live_turns < 1:
                raise EvalConfigurationError("--max-live-turns must be greater than zero")
            if selected_turns > args.max_live_turns:
                raise EvalConfigurationError(
                    f"live run contains {selected_turns} sampled lead turns, above the "
                    f"--max-live-turns budget of {args.max_live_turns}; narrow the suite or explicitly raise the budget"
                )
            print(
                f"Live evaluation explicitly enabled: {len(scenarios)} fixture(s), "
                f"{args.samples} sample(s), {selected_turns} lead turn(s), approximately "
                f"{estimated_calls} logical model calls before any JSON repair or provider retry."
            )

        report = run_evaluations(
            scenarios,
            agent=args.agent,
            provider=args.provider,
            samples=args.samples,
            workers=args.workers,
            judge=args.judge,
            model=args.model,
            judge_model=args.judge_model,
            suite=args.suite,
        )
        json_path, markdown_path = write_report(report, args.output)
    except (EvalConfigurationError, SchemaError) as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    except OSError as exc:
        print(f"output error: {exc}", file=sys.stderr)
        return 2

    print(
        f"{'PASS' if report.passed else 'FAIL'}: "
        f"{report.passed_scenarios}/{len(report.scenarios)} scenarios passed "
        f"in {report.metadata.get('duration_ms', 0)} ms"
    )
    for score in category_scores(report):
        print(f"  {score.category}: {score.passed}/{score.total} ({score.rate:.1%})")
    print(f"JSON report: {json_path}")
    print(f"Markdown report: {markdown_path}")

    gate_passed = _gate_passed(report, args.fail_on)
    if args.judge == "model" and args.fail_on == "deterministic" and not report.passed:
        print(
            "Semantic judge failures are advisory; use --fail-on all only after calibrating the judge against human labels."
        )
    return 0 if gate_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
