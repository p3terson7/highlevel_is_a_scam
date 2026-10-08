from __future__ import annotations

from evals.chatbot.graders.deterministic import (
    count_meeting_ctas,
    count_questions,
    detect_reply_language,
    grade_turn,
)
from evals.chatbot.schema import ToolExpectation, TurnExpectation, TurnObservation


def test_grade_turn_passes_semantic_and_structural_constraints() -> None:
    expectation = TurnExpectation(
        expected_language="fr",
        expected_state="BOOKING_SENT",
        expected_action="offer_booking",
        expected_pending_step="slot_selection",
        must_include=("mardi",),
        any_of=("14 h", "quatorze heures"),
        must_not_include=("guaranteed price",),
        max_questions=1,
        max_chars=240,
        max_meeting_ctas=1,
        booking_expected=False,
        handoff_expected=False,
        expected_tool_names=("find_slots",),
        max_tool_calls=1,
    )
    observation = TurnObservation(
        reply="Je peux vous proposer un rendez-vous mardi à 14 h. Est-ce que ce créneau vous convient?",
        state="BOOKING_SENT",
        action="offer_booking",
        pending_step="slot_selection",
        language="fr",
        booking_created=False,
        handoff_requested=False,
        tool_calls=({"name": "find_slots", "args": {}},),
    )

    checks = grade_turn(expectation, observation)

    assert checks
    assert all(check.passed for check in checks)
    assert {check.code for check in checks} >= {
        "language_match",
        "state_match",
        "question_limit",
        "meeting_cta_limit",
        "tool_sequence",
    }


def test_grade_turn_reports_action_content_side_effect_and_tool_failures() -> None:
    expectation = TurnExpectation(
        expected_state="BOOKED",
        expected_action="mark_booked",
        expected_pending_step="none",
        must_include=("confirmed",),
        any_of=("Tuesday", "Wednesday"),
        must_not_include=("$500",),
        max_questions=0,
        max_meeting_ctas=0,
        booking_expected=True,
        handoff_expected=False,
        expected_tool_names=("book_slot",),
        max_tool_calls=1,
    )
    observation = TurnObservation(
        reply="Would you like another meeting for $500?",
        state="QUALIFYING",
        action="ask_next_question",
        pending_step="qualification",
        booking_created=False,
        handoff_requested=True,
        tool_calls=({"name": "find_slots"}, {"name": "book_slot"}),
    )

    failed = [check for check in grade_turn(expectation, observation) if not check.passed]

    assert {check.code for check in failed} >= {
        "state_match",
        "action_match",
        "pending_step_match",
        "required_text_1",
        "any_required_text",
        "forbidden_text_1",
        "question_limit",
        "meeting_cta_limit",
        "booking_side_effect",
        "handoff_side_effect",
        "tool_sequence",
        "tool_call_limit",
    }


def test_language_detector_distinguishes_supported_languages_and_uses_fallback() -> None:
    assert detect_reply_language("Bonjour, je peux vous aider avec votre demande.") == "fr"
    assert detect_reply_language("Hello, I can help with your request.") == "en"
    assert detect_reply_language("OK", fallback="fr") == "fr"
    assert detect_reply_language("1234") is None


def test_french_exact_slot_confirmation_is_one_cta_group() -> None:
    reply = (
        "Oui, jeudi 16 juillet à 15 h 00 (EDT) est disponible pour l'appel de consultation. "
        "Voulez-vous que je le réserve? "
        "Si ce créneau ne fonctionne pas, envoyez-moi un autre moment."
    )
    assert count_meeting_ctas(reply) == 1
    assert count_meeting_ctas("Voulez-vous que je le réserve?") == 1


def test_question_and_cta_counts_are_clause_based() -> None:
    reply = "Would a meeting help?? Here is the service information. What time can I book?"

    assert count_questions(reply) == 2
    assert count_meeting_ctas(reply) == 2


def test_text_matching_is_case_insensitive_without_exact_snapshot() -> None:
    checks = grade_turn(
        TurnExpectation(must_include=("SERVICE AREA",), must_not_include=("invented",)),
        TurnObservation(reply="Our service area includes Montréal and Laval."),
    )

    assert all(check.passed for check in checks)


def test_richer_deterministic_expectations_grade_observable_contracts() -> None:
    expectation = TurnExpectation(
        allowed_actions=("none", "offer_booking"),
        allowed_conversation_acts=("offer_slots",),
        expected_next_states=("BOOKING_SENT",),
        allowed_resolution_paths=("agent_response",),
        forbidden_terms=("already booked",),
        forbidden_tools=("book_slot", "mark_booked"),
        visible_slot_indexes=(1, 2),
        tool=ToolExpectation(
            proposed_name="find_slots",
            args_subset={"preferred_day": "tuesday", "filters": {"period": "afternoon"}},
            max_calls=1,
        ),
    )
    observation = TurnObservation(
        reply="I found 1) Tuesday at 1 PM and 2) Tuesday at 3 PM. Which option works?",
        state="BOOKING_SENT",
        action="none",
        conversation_act="offer_slots",
        resolution_path="agent_response",
        tool_calls=(
            {
                "name": "find_slots",
                "args": {
                    "preferred_day": "tuesday",
                    "filters": {"period": "afternoon", "timezone": "America/Toronto"},
                    "limit": 3,
                },
            },
        ),
    )

    checks = grade_turn(expectation, observation)

    assert all(check.passed for check in checks)
    assert {check.code for check in checks} >= {
        "action_allowed",
        "conversation_act_allowed",
        "next_state_allowed",
        "resolution_path_allowed",
        "visible_slot_indexes",
        "proposed_tool_name",
        "proposed_tool_args_subset",
        "proposed_tool_call_limit",
    }


def test_richer_grader_reports_forbidden_terms_tools_and_invalid_args() -> None:
    expectation = TurnExpectation(
        allowed_actions=("none",),
        expected_next_states=("QUALIFYING",),
        forbidden_terms=("API_KEY",),
        forbidden_tools=("book_slot",),
        visible_slot_indexes=(1, 2),
        tool=ToolExpectation(
            proposed_name="find_slots",
            args_subset={"preferred_day": "tuesday"},
            max_calls=1,
        ),
    )
    observation = TurnObservation(
        reply="API_key is hidden. I found option 1.",
        state="BOOKED",
        action="mark_booked",
        tool_calls=(
            {"name": "book_slot", "args": {"slot_index": 1}},
            {"name": "find_slots", "args": {"preferred_day": "wednesday"}},
        ),
    )

    failed_codes = {
        check.code for check in grade_turn(expectation, observation) if not check.passed
    }

    assert failed_codes >= {
        "action_allowed",
        "next_state_allowed",
        "forbidden_term_1",
        "forbidden_tool_1",
        "visible_slot_indexes",
        "proposed_tool_name",
        "proposed_tool_args_subset",
        "proposed_tool_call_limit",
    }


def test_semantic_descriptions_are_advisory_but_missing_structured_signals_fail() -> None:
    expectation = TurnExpectation(
        required_facts=("The answer preserves all relevant prior context.",),
        forbidden_claims=("The assistant claims an unsupported outcome.",),
        semantic_criteria=("The reply should feel natural and helpful.",),
        language_switch_from="en",
        allowed_conversation_acts=("answer_question",),
        allowed_resolution_paths=("agent_response",),
    )

    checks = grade_turn(
        expectation,
        TurnObservation(reply="Oui, je peux vous aider.", language="fr"),
    )

    assert [check.code for check in checks] == [
        "reply_non_empty",
        "conversation_act_allowed",
        "resolution_path_allowed",
    ]
    assert checks[0].passed is True
    assert checks[1].passed is False
    assert checks[2].passed is False
    assert not any(
        "required_fact" in check.code or "forbidden_claim" in check.code for check in checks
    )


def test_persisted_side_effect_expectations_are_deterministic() -> None:
    expectation = TurnExpectation(
        expected_total_lead_task_count=1,
        expected_total_booking_event_count=0,
        expected_total_outbound_message_count=2,
        expected_booking_confirmation_unknown=True,
        expected_handoff_reason="booking_confirmation_unknown",
        expected_lead_source="linkedin",
    )
    observation = TurnObservation(
        reply="I could not confirm the booking, so the team will verify it.",
        total_lead_task_count=1,
        total_booking_event_count=0,
        total_outbound_message_count=2,
        booking_confirmation_unknown=True,
        handoff_reason="booking_confirmation_unknown",
        lead_source="linkedin",
    )

    checks = grade_turn(expectation, observation)

    assert all(check.passed for check in checks)
    assert {check.code for check in checks} >= {
        "total_lead_task_count",
        "total_booking_event_count",
        "total_outbound_message_count",
        "booking_confirmation_unknown",
        "handoff_reason",
        "lead_source",
    }


def test_asserted_observability_capabilities_fail_when_adapter_did_not_expose_them() -> None:
    expectation = TurnExpectation(
        expected_total_lead_task_count=0,
        expected_total_booking_event_count=0,
        expected_total_outbound_message_count=0,
        expected_booking_confirmation_unknown=False,
    )

    checks = grade_turn(expectation, TurnObservation(reply="No observable state."))
    asserted = {
        check.code: check
        for check in checks
        if check.code
        in {
            "total_lead_task_count",
            "total_booking_event_count",
            "total_outbound_message_count",
            "booking_confirmation_unknown",
        }
    }

    assert set(asserted) == {
        "total_lead_task_count",
        "total_booking_event_count",
        "total_outbound_message_count",
        "booking_confirmation_unknown",
    }
    assert all(not check.passed for check in asserted.values())
    assert all("did not expose" in check.detail for check in asserted.values())


def test_observability_mismatches_report_cumulative_values() -> None:
    expectation = TurnExpectation(
        expected_total_lead_task_count=1,
        expected_total_booking_event_count=1,
        expected_total_outbound_message_count=2,
        expected_booking_confirmation_unknown=True,
    )
    observation = TurnObservation(
        reply="Observed state differs.",
        total_lead_task_count=0,
        total_booking_event_count=0,
        total_outbound_message_count=1,
        booking_confirmation_unknown=False,
    )

    checks = grade_turn(expectation, observation)
    mismatches = [check for check in checks if check.code != "reply_non_empty"]

    assert mismatches
    assert all(not check.passed for check in mismatches)
    assert "cumulative" in mismatches[0].detail
