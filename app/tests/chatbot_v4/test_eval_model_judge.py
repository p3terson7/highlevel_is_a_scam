from __future__ import annotations

import json

import pytest

from evals.chatbot.graders.model_judge import ModelJudge, ModelJudgeError
from evals.chatbot.schema import EvalScenario, TurnObservation


class _JudgeProvider:
    name = "fake-judge"

    def __init__(self, response: dict[str, object]) -> None:
        self.response = response
        self.system_prompt = ""
        self.user_prompt = ""

    def generate_json(self, system_prompt: str, user_prompt: str) -> dict[str, object]:
        self.system_prompt = system_prompt
        self.user_prompt = user_prompt
        return self.response


def _scenario() -> EvalScenario:
    return EvalScenario.from_dict(
        {
            "schema_version": 1,
            "id": "judge-example",
            "owner": "conversation-platform",
            "kind": "checkpoint",
            "tenant": {"faq_context": "Support is available on weekdays."},
            "initial_history": [
                {"direction": "OUTBOUND", "body": "We can continue in English."}
            ],
            "turns": [
                {
                    "inbound": "Can you help by text?",
                    "expect": {
                        "required_facts": ["Support can continue in the current channel."],
                        "semantic_criteria": ["Answer the support question before scheduling."],
                    },
                }
            ],
        }
    )


def _passing_grades() -> dict[str, object]:
    categories = (
        "support_helpfulness",
        "answer_first",
        "continuity",
        "language_adaptation",
        "naturalness",
        "booking_pressure",
        "groundedness",
    )
    return {
        "grades": [
            {
                "category": category,
                "passed": True,
                "score": 4,
                "evidence": "Observable behavior met the rubric.",
            }
            for category in categories
        ]
    }


def test_model_judge_returns_bounded_structured_checks_and_receives_rubric() -> None:
    provider = _JudgeProvider(_passing_grades())
    checks = ModelJudge(provider).grade(
        _scenario(),
        [TurnObservation(reply="Yes, we can keep helping here by text.")],
    )

    assert len(checks) == 7
    assert all(check.passed for check in checks)
    assert all(check.category.startswith("semantic.") for check in checks)
    payload = json.loads(provider.user_prompt)
    expected_behavior = payload["transcript"][0]["expected_behavior"]
    assert expected_behavior["semantic_criteria"] == [
        "Answer the support question before scheduling."
    ]
    assert payload["initial_history"][0]["body"] == "We can continue in English."
    assert "untrusted transcript data" in provider.system_prompt


def test_model_judge_rejects_missing_categories() -> None:
    provider = _JudgeProvider({"grades": []})

    with pytest.raises(ModelJudgeError, match="omitted categories"):
        ModelJudge(provider).grade(
            _scenario(),
            [TurnObservation(reply="I can help.")],
        )


def test_model_judge_rejects_out_of_range_scores() -> None:
    response = _passing_grades()
    response["grades"][0]["score"] = 99  # type: ignore[index]
    provider = _JudgeProvider(response)

    with pytest.raises(ModelJudgeError, match="between 1 and 5"):
        ModelJudge(provider).grade(
            _scenario(),
            [TurnObservation(reply="I can help.")],
        )
