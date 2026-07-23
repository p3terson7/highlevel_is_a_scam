from __future__ import annotations

import re
import time
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from pathlib import Path
from typing import Any

from app.core.config import Settings
from evals.chatbot.adapters.v3 import ReplayProvider, V3ScenarioAdapter, build_live_agent
from evals.chatbot.graders.deterministic import grade_turn
from evals.chatbot.graders.model_judge import ModelJudge, OpenAIJudgeProvider
from evals.chatbot.schema import (
    CheckResult,
    EvalReport,
    EvalScenario,
    ScenarioResult,
    TurnObservation,
    TurnResult,
    load_scenarios,
)

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures"
_SECRET_PATTERN = re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b")
_LABELED_SECRET_PATTERN = re.compile(
    r"(?i)\b(authorization|api[_ -]?key|token)(\s*[:=]\s*)([^\s,;]+)"
)


class EvalConfigurationError(ValueError):
    pass


def discover_scenarios(
    suite: str,
    *,
    fixture_root: str | Path = FIXTURE_ROOT,
    scenario_ids: Iterable[str] = (),
) -> tuple[EvalScenario, ...]:
    normalized_suite = suite.strip().lower()
    root = Path(fixture_root)
    if normalized_suite == "all":
        source = root
    elif normalized_suite in {"smoke", "regression", "journeys"}:
        source = root / normalized_suite
    else:
        raise EvalConfigurationError(
            "suite must be one of: smoke, regression, journeys, all"
        )
    scenarios = load_scenarios(source)
    requested = {item.strip() for item in scenario_ids if item.strip()}
    if not requested:
        return scenarios
    selected = tuple(scenario for scenario in scenarios if scenario.id in requested)
    missing = sorted(requested - {scenario.id for scenario in selected})
    if missing:
        raise EvalConfigurationError(f"unknown scenario id(s): {', '.join(missing)}")
    return selected


def _safe_error(exc: BaseException) -> str:
    detail = " ".join(str(exc).split())
    detail = _SECRET_PATTERN.sub("[secret redacted]", detail)
    detail = _LABELED_SECRET_PATTERN.sub(r"\1\2[secret redacted]", detail)[:500]
    return f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__


def _enum_value(value: Any) -> str | None:
    if value is None:
        return None
    raw = getattr(value, "value", value)
    text = str(raw).strip()
    return text or None


def _turn_observation(adapter_turn: Any) -> TurnObservation:
    tool_calls = getattr(adapter_turn, "tool_calls", ()) or ()
    return TurnObservation(
        reply=str(getattr(adapter_turn, "reply", "") or ""),
        state=_enum_value(getattr(adapter_turn, "state", None)),
        action=_enum_value(getattr(adapter_turn, "action", None)),
        conversation_act=_enum_value(getattr(adapter_turn, "conversation_act", None)),
        resolution_path=_enum_value(getattr(adapter_turn, "resolution_path", None)),
        pending_step=_enum_value(getattr(adapter_turn, "pending_step", None)),
        language=_enum_value(getattr(adapter_turn, "language", None)),
        booking_created=bool(getattr(adapter_turn, "booking_created", False)),
        handoff_requested=bool(getattr(adapter_turn, "handoff_requested", False)),
        tool_calls=tuple(dict(item) for item in tool_calls if isinstance(item, dict)),
    )


def _runtime_check(code: str, passed: bool, detail: str) -> CheckResult:
    return CheckResult(
        category="runtime",
        code=code,
        passed=passed,
        detail=detail,
    )


def _build_adapter(
    scenario: EvalScenario,
    *,
    provider_mode: str,
    settings: Settings,
    model: str | None,
) -> tuple[V3ScenarioAdapter, ReplayProvider | None]:
    if provider_mode == "replay":
        replay = ReplayProvider(
            [output for turn in scenario.turns for output in turn.replay_outputs],
            scenario_id=scenario.id,
        )
        return V3ScenarioAdapter(scenario, provider=replay), replay
    if provider_mode == "live":
        agent = build_live_agent(settings=settings, model=model)
        return V3ScenarioAdapter(scenario, agent=agent), None
    raise EvalConfigurationError("provider must be replay or live")


def _run_scenario(
    scenario: EvalScenario,
    *,
    provider_mode: str,
    settings: Settings,
    model: str | None,
    judge: ModelJudge | None,
    result_id: str,
) -> ScenarioResult:
    started = time.perf_counter()
    try:
        adapter, replay = _build_adapter(
            scenario,
            provider_mode=provider_mode,
            settings=settings,
            model=model,
        )
        adapter_result = adapter.run()
        adapter_turns = tuple(getattr(adapter_result, "turns", ()) or ())
        if len(adapter_turns) != len(scenario.turns):
            raise RuntimeError(
                f"adapter returned {len(adapter_turns)} turns for {len(scenario.turns)} inputs"
            )

        turn_results: list[TurnResult] = []
        observations: list[TurnObservation] = []
        for index, (expected_turn, adapter_turn) in enumerate(
            zip(scenario.turns, adapter_turns, strict=True)
        ):
            observation = _turn_observation(adapter_turn)
            observations.append(observation)
            checks = list(grade_turn(expected_turn.expect, observation))
            if provider_mode == "live":
                turn_provider = str(
                    getattr(adapter_turn, "provider", "") or ""
                ).strip().casefold()
                provider_error = str(
                    getattr(adapter_turn, "provider_error", "") or ""
                ).strip()
                live_provider_healthy = not provider_error and turn_provider != "fallback"
                checks.append(
                    _runtime_check(
                        "live_provider_healthy",
                        live_provider_healthy,
                        (
                            "live provider completed without fallback"
                            if live_provider_healthy
                            else _safe_error(
                                RuntimeError(provider_error or "agent used its fallback provider")
                            )
                        ),
                    )
                )
            adapter_error = getattr(adapter_turn, "error", None)
            turn_results.append(
                TurnResult(
                    turn_index=index,
                    turn_id=expected_turn.turn_id,
                    observation=observation,
                    checks=tuple(checks),
                    error=str(adapter_error)[:500] if adapter_error else None,
                )
            )

        adapter_errors = tuple(getattr(adapter_result, "errors", ()) or ())
        scenario_error = "; ".join(str(error) for error in adapter_errors) or getattr(
            adapter_result, "error", None
        )
        if replay is not None and replay.remaining:
            unused = _runtime_check(
                "replay_outputs_consumed",
                False,
                f"{replay.remaining} replay output(s) were not consumed",
            )
            final = turn_results[-1]
            turn_results[-1] = replace(final, checks=final.checks + (unused,))

        if judge is not None and observations and not scenario_error:
            try:
                semantic_checks = judge.grade(scenario, observations)
            except Exception as exc:
                semantic_checks = (
                    CheckResult(
                        category="semantic.runtime",
                        code="model_judge_error",
                        passed=False,
                        detail=_safe_error(exc),
                    ),
                )
            final = turn_results[-1]
            turn_results[-1] = replace(
                final,
                checks=final.checks + semantic_checks,
            )

        duration_ms = round((time.perf_counter() - started) * 1_000)
        return ScenarioResult(
            scenario_id=result_id,
            kind=scenario.kind,
            turns=tuple(turn_results),
            error=str(scenario_error)[:500] if scenario_error else None,
            duration_ms=duration_ms,
        )
    except Exception as exc:
        duration_ms = round((time.perf_counter() - started) * 1_000)
        return ScenarioResult(
            scenario_id=result_id,
            kind=scenario.kind,
            error=_safe_error(exc),
            duration_ms=duration_ms,
        )


def run_evaluations(
    scenarios: Sequence[EvalScenario],
    *,
    agent: str = "v3",
    provider: str = "replay",
    samples: int = 1,
    workers: int = 1,
    judge: str = "off",
    settings: Settings | None = None,
    model: str | None = None,
    judge_model: str | None = None,
    suite: str = "custom",
) -> EvalReport:
    normalized_agent = agent.strip().lower()
    normalized_provider = provider.strip().lower()
    normalized_judge = judge.strip().lower()
    if normalized_agent != "v3":
        raise EvalConfigurationError(
            "only agent v3 is implemented in this first evaluation slice; v4/both become available with the V4 adapter"
        )
    if normalized_provider not in {"replay", "live"}:
        raise EvalConfigurationError("provider must be replay or live")
    if normalized_judge not in {"off", "model"}:
        raise EvalConfigurationError("judge must be off or model")
    if normalized_judge == "model" and normalized_provider != "live":
        raise EvalConfigurationError("the model judge is networked and requires --provider live")
    if not scenarios:
        raise EvalConfigurationError("at least one scenario is required")
    if samples < 1 or samples > 20:
        raise EvalConfigurationError("samples must be between 1 and 20")
    if workers < 1 or workers > 16:
        raise EvalConfigurationError("workers must be between 1 and 16")

    effective_settings = settings or Settings()
    effective_model = str(model or effective_settings.openai_model).strip()
    if normalized_provider == "live" and not effective_settings.openai_api_key.strip():
        raise EvalConfigurationError(
            "OPENAI_API_KEY is required for an explicitly requested live evaluation"
        )
    if normalized_provider == "live" and not effective_model:
        raise EvalConfigurationError(
            "OPENAI_MODEL or --model is required for an explicitly requested live evaluation"
        )

    model_judge: ModelJudge | None = None
    effective_judge_model = ""
    if normalized_judge == "model":
        effective_judge_model = str(judge_model or effective_model).strip()
        judge_provider = OpenAIJudgeProvider(
            api_key=effective_settings.openai_api_key.strip(),
            model=effective_judge_model,
            timeout_seconds=effective_settings.request_timeout_seconds,
        )
        model_judge = ModelJudge(judge_provider)

    jobs: list[tuple[EvalScenario, str]] = []
    for sample_index in range(samples):
        for scenario in scenarios:
            result_id = scenario.id if samples == 1 else f"{scenario.id}#sample-{sample_index + 1}"
            jobs.append((scenario, result_id))

    started = time.perf_counter()
    results: list[ScenarioResult | None] = [None] * len(jobs)

    def execute(index: int, scenario: EvalScenario, result_id: str) -> tuple[int, ScenarioResult]:
        return (
            index,
            _run_scenario(
                scenario,
                provider_mode=normalized_provider,
                settings=effective_settings,
                model=effective_model,
                judge=model_judge,
                result_id=result_id,
            ),
        )

    if workers == 1:
        for index, (scenario, result_id) in enumerate(jobs):
            _, results[index] = execute(index, scenario, result_id)
    else:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="chatbot-eval") as pool:
            futures = {
                pool.submit(execute, index, scenario, result_id): index
                for index, (scenario, result_id) in enumerate(jobs)
            }
            for future in as_completed(futures):
                index, result = future.result()
                results[index] = result

    completed_results = tuple(result for result in results if result is not None)
    duration_ms = round((time.perf_counter() - started) * 1_000)
    return EvalReport(
        agent=normalized_agent,
        provider=normalized_provider,
        scenarios=completed_results,
        metadata={
            "suite": suite,
            "samples": samples,
            "workers": workers,
            "judge": normalized_judge,
            "model": effective_model if normalized_provider == "live" else "replay",
            "judge_model": effective_judge_model or None,
            "duration_ms": duration_ms,
            "fixture_count": len(scenarios),
        },
    )


def deterministic_gate_passed(report: EvalReport) -> bool:
    if not report.scenarios:
        return False
    for scenario in report.scenarios:
        if scenario.error or not scenario.turns:
            return False
        for turn in scenario.turns:
            if turn.error:
                return False
            if any(
                not check.passed and not check.category.startswith("semantic.")
                for check in turn.checks
            ):
                return False
    return True


__all__ = [
    "EvalConfigurationError",
    "FIXTURE_ROOT",
    "deterministic_gate_passed",
    "discover_scenarios",
    "run_evaluations",
]
