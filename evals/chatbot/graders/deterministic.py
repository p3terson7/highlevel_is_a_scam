from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable, Mapping, Sequence

from evals.chatbot.schema import CheckResult, TurnExpectation, TurnObservation

_WORD_PATTERN = re.compile(r"[a-zA-ZÀ-ÖØ-öø-ÿ']+")
_QUESTION_PATTERN = re.compile(r"\?+")
_SEGMENT_PATTERN = re.compile(r"[.!?;\n]+")
_NUMBERED_SLOT_PATTERN = re.compile(r"(?<!\w)(\d{1,2})\s*[.)](?=\s)")
_NAMED_SLOT_PATTERN = re.compile(r"\boption\s+#?(\d{1,2})\b", re.IGNORECASE)
_MEETING_CTA_PATTERN = re.compile(
    r"\b(?:"
    r"book(?:ing)?|schedule|meeting|appointment|consultation|calendar|"
    r"available\s+times?|availability|time\s+slots?|send\s+(?:me\s+)?times?|"
    r"réserv(?:er|ez|ation|é)|rendez[- ]vous|rencontre|appel|disponibilit(?:é|és)|"
    r"créneau(?:x)?|planifi(?:er|ez)|calendrier"
    r")\b",
    re.IGNORECASE,
)

_ENGLISH_MARKERS = {
    "a",
    "about",
    "and",
    "are",
    "available",
    "book",
    "business",
    "call",
    "can",
    "do",
    "for",
    "help",
    "hello",
    "how",
    "i",
    "is",
    "it",
    "meeting",
    "need",
    "of",
    "our",
    "please",
    "schedule",
    "that",
    "the",
    "this",
    "time",
    "to",
    "we",
    "what",
    "when",
    "with",
    "would",
    "you",
    "your",
}
_FRENCH_MARKERS = {
    "à",
    "appel",
    "avec",
    "bonjour",
    "besoin",
    "ce",
    "comment",
    "créneau",
    "de",
    "des",
    "disponible",
    "du",
    "en",
    "est",
    "et",
    "je",
    "la",
    "le",
    "les",
    "merci",
    "nous",
    "notre",
    "pour",
    "pouvez",
    "réserver",
    "rendez-vous",
    "sur",
    "un",
    "une",
    "vous",
    "votre",
}
_FRENCH_CLITICS = ("c'", "d'", "j'", "l'", "m'", "n'", "qu'", "s'", "t'")


def count_questions(reply: str) -> int:
    """Count question clauses without treating repeated punctuation as extra questions."""

    return len(_QUESTION_PATTERN.findall(reply))


def count_meeting_ctas(reply: str) -> int:
    """Count distinct groups of meeting/booking calls to action.

    Adjacent clauses such as an offer followed by "does that slot work?" are one
    CTA. A separate CTA after unrelated copy is counted again.
    """

    count = 0
    previous_was_cta = False
    for segment in _SEGMENT_PATTERN.split(reply):
        if not segment.strip():
            continue
        is_cta = bool(_MEETING_CTA_PATTERN.search(segment))
        if is_cta and not previous_was_cta:
            count += 1
        previous_was_cta = is_cta
    return count


def detect_reply_language(reply: str, *, fallback: str | None = None) -> str | None:
    """Return a conservative English/French signal for deterministic smoke checks.

    This deliberately is not a general-purpose language detector. It avoids a new
    runtime dependency and is only used for the application's supported en/fr
    evaluation suites. Ambiguous very short replies use the adapter's language
    observation when available.
    """

    normalized = unicodedata.normalize("NFC", reply).casefold().replace("’", "'")
    tokens = _WORD_PATTERN.findall(normalized)
    if not tokens:
        return fallback if fallback in {"en", "fr"} else None

    english_score = sum(token in _ENGLISH_MARKERS for token in tokens)
    french_score = sum(token in _FRENCH_MARKERS for token in tokens)
    french_score += 2 * sum(character in "àâçéèêëîïôùûüÿœ" for character in normalized)
    french_score += sum(normalized.startswith(prefix) or f" {prefix}" in normalized for prefix in _FRENCH_CLITICS)

    if english_score > french_score:
        return "en"
    if french_score > english_score:
        return "fr"
    if fallback in {"en", "fr"}:
        return fallback
    return None


def _check(
    *,
    category: str,
    code: str,
    passed: bool,
    detail: str,
    observed: object = None,
    expected: object = None,
) -> CheckResult:
    return CheckResult(
        category=category,
        code=code,
        passed=passed,
        detail=detail,
        observed=observed,
        expected=expected,
    )


def _normalize_optional(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped.casefold() if stripped else None


def _tool_names(tool_calls: Iterable[dict[str, object]]) -> tuple[str, ...]:
    names: list[str] = []
    for call in tool_calls:
        raw_name = call.get("name")
        names.append(str(raw_name).casefold() if raw_name is not None else "<missing>")
    return tuple(names)


def _visible_slot_indexes(reply: str) -> tuple[int, ...]:
    indexes = {
        int(match)
        for pattern in (_NUMBERED_SLOT_PATTERN, _NAMED_SLOT_PATTERN)
        for match in pattern.findall(reply)
    }
    return tuple(sorted(indexes))


def _is_json_subset(expected: object, observed: object) -> bool:
    if isinstance(expected, Mapping):
        if not isinstance(observed, Mapping):
            return False
        return all(
            key in observed and _is_json_subset(value, observed[key])
            for key, value in expected.items()
        )
    if isinstance(expected, Sequence) and not isinstance(expected, (str, bytes, bytearray)):
        if not isinstance(observed, Sequence) or isinstance(observed, (str, bytes, bytearray)):
            return False
        return len(expected) == len(observed) and all(
            _is_json_subset(expected_item, observed_item)
            for expected_item, observed_item in zip(expected, observed, strict=True)
        )
    return type(expected) is type(observed) and expected == observed


def grade_turn(
    expectation: TurnExpectation,
    observation: TurnObservation,
) -> tuple[CheckResult, ...]:
    """Evaluate structural and textual constraints for a single agent turn.

    Checks are intentionally semantic constraints rather than exact response
    snapshots, so harmless copy changes do not invalidate the suite.
    """

    checks: list[CheckResult] = []
    reply = observation.reply or ""
    folded_reply = reply.casefold()
    checks.append(
        _check(
            category="response",
            code="reply_non_empty",
            passed=bool(reply.strip()),
            detail="agent returned a non-empty reply" if reply.strip() else "agent reply was empty",
            observed=len(reply),
            expected="> 0 characters",
        )
    )

    if expectation.expected_language is not None:
        detected = detect_reply_language(reply, fallback=observation.language)
        passed = detected == expectation.expected_language
        checks.append(
            _check(
                category="language",
                code="language_match",
                passed=passed,
                detail=(
                    f"reply language matched {expectation.expected_language}"
                    if passed
                    else f"reply language was {detected or 'ambiguous'}"
                ),
                observed=detected,
                expected=expectation.expected_language,
            )
        )

    if expectation.expected_state is not None:
        observed_state = observation.state.upper() if observation.state else None
        passed = observed_state == expectation.expected_state
        checks.append(
            _check(
                category="state",
                code="state_match",
                passed=passed,
                detail=(
                    f"lead reached {expectation.expected_state}"
                    if passed
                    else f"lead state was {observed_state or 'unset'}"
                ),
                observed=observed_state,
                expected=expectation.expected_state,
            )
        )

    if expectation.expected_action is not None:
        observed_action = _normalize_optional(observation.action)
        expected_action = _normalize_optional(expectation.expected_action)
        passed = observed_action == expected_action
        checks.append(
            _check(
                category="policy",
                code="action_match",
                passed=passed,
                detail=(
                    f"action matched {expectation.expected_action}"
                    if passed
                    else f"action was {observation.action or 'unset'}"
                ),
                observed=observation.action,
                expected=expectation.expected_action,
            )
        )

    if expectation.allowed_actions is not None:
        observed_action = _normalize_optional(observation.action)
        allowed_actions = tuple(action.casefold() for action in expectation.allowed_actions)
        passed = observed_action in allowed_actions
        checks.append(
            _check(
                category="policy",
                code="action_allowed",
                passed=passed,
                detail=(
                    f"action {observation.action or 'unset'} was allowed"
                    if passed
                    else f"action {observation.action or 'unset'} was outside the allowed set"
                ),
                observed=observation.action,
                expected=list(expectation.allowed_actions),
            )
        )

    if expectation.allowed_conversation_acts is not None:
        observed_act = _normalize_optional(observation.conversation_act)
        allowed_acts = tuple(act.casefold() for act in expectation.allowed_conversation_acts)
        passed = observed_act in allowed_acts
        checks.append(
            _check(
                category="conversation",
                code="conversation_act_allowed",
                passed=passed,
                detail=(
                    f"conversation act {observation.conversation_act} was allowed"
                    if passed
                    else (
                        "conversation act observation was missing"
                        if observation.conversation_act is None
                        else f"conversation act {observation.conversation_act} was outside the allowed set"
                    )
                ),
                observed=observation.conversation_act,
                expected=list(expectation.allowed_conversation_acts),
            )
        )

    if expectation.expected_next_states is not None:
        observed_state = observation.state.upper() if observation.state else None
        passed = observed_state in expectation.expected_next_states
        checks.append(
            _check(
                category="state",
                code="next_state_allowed",
                passed=passed,
                detail=(
                    f"lead reached allowed state {observed_state}"
                    if passed
                    else f"lead state {observed_state or 'unset'} was outside the expected set"
                ),
                observed=observed_state,
                expected=list(expectation.expected_next_states),
            )
        )

    if expectation.allowed_resolution_paths is not None:
        observed_path = _normalize_optional(observation.resolution_path)
        allowed_paths = tuple(path.casefold() for path in expectation.allowed_resolution_paths)
        passed = observed_path in allowed_paths
        checks.append(
            _check(
                category="policy",
                code="resolution_path_allowed",
                passed=passed,
                detail=(
                    f"resolution path {observation.resolution_path} was allowed"
                    if passed
                    else (
                        "resolution path observation was missing"
                        if observation.resolution_path is None
                        else f"resolution path {observation.resolution_path} was outside the allowed set"
                    )
                ),
                observed=observation.resolution_path,
                expected=list(expectation.allowed_resolution_paths),
            )
        )

    if expectation.expected_pending_step is not None:
        expected_pending = _normalize_optional(expectation.expected_pending_step)
        if expected_pending in {"none", "null"}:
            expected_pending = None
        observed_pending = _normalize_optional(observation.pending_step)
        passed = observed_pending == expected_pending
        checks.append(
            _check(
                category="state",
                code="pending_step_match",
                passed=passed,
                detail=(
                    "pending step matched"
                    if passed
                    else f"pending step was {observation.pending_step or 'unset'}"
                ),
                observed=observation.pending_step,
                expected=expectation.expected_pending_step,
            )
        )

    for index, required in enumerate(expectation.must_include):
        passed = required.casefold() in folded_reply
        checks.append(
            _check(
                category="content",
                code=f"required_text_{index + 1}",
                passed=passed,
                detail=(
                    f"reply included required concept {required!r}"
                    if passed
                    else f"reply omitted required concept {required!r}"
                ),
                observed=passed,
                expected=required,
            )
        )

    if expectation.any_of:
        matches = tuple(item for item in expectation.any_of if item.casefold() in folded_reply)
        passed = bool(matches)
        checks.append(
            _check(
                category="content",
                code="any_required_text",
                passed=passed,
                detail=(
                    f"reply included {matches[0]!r}"
                    if passed
                    else "reply omitted every accepted concept"
                ),
                observed=list(matches),
                expected=list(expectation.any_of),
            )
        )

    for index, forbidden in enumerate(expectation.must_not_include):
        present = forbidden.casefold() in folded_reply
        checks.append(
            _check(
                category="safety",
                code=f"forbidden_text_{index + 1}",
                passed=not present,
                detail=(
                    f"reply contained forbidden text {forbidden!r}"
                    if present
                    else f"reply avoided forbidden text {forbidden!r}"
                ),
                observed=present,
                expected=False,
            )
        )

    for index, forbidden in enumerate(expectation.forbidden_terms):
        present = forbidden.casefold() in folded_reply
        checks.append(
            _check(
                category="safety",
                code=f"forbidden_term_{index + 1}",
                passed=not present,
                detail=(
                    f"reply contained forbidden term {forbidden!r}"
                    if present
                    else f"reply avoided forbidden term {forbidden!r}"
                ),
                observed=present,
                expected=False,
            )
        )

    if expectation.visible_slot_indexes is not None:
        visible_indexes = _visible_slot_indexes(reply)
        expected_indexes = tuple(sorted(expectation.visible_slot_indexes))
        passed = visible_indexes == expected_indexes
        checks.append(
            _check(
                category="booking",
                code="visible_slot_indexes",
                passed=passed,
                detail=(
                    f"reply showed slot indexes {list(visible_indexes)}"
                    if passed
                    else f"reply showed slot indexes {list(visible_indexes)}, expected {list(expected_indexes)}"
                ),
                observed=list(visible_indexes),
                expected=list(expected_indexes),
            )
        )

    if expectation.max_questions is not None:
        question_count = count_questions(reply)
        passed = question_count <= expectation.max_questions
        checks.append(
            _check(
                category="conversation",
                code="question_limit",
                passed=passed,
                detail=f"reply asked {question_count} question(s)",
                observed=question_count,
                expected=f"<= {expectation.max_questions}",
            )
        )

    if expectation.max_chars is not None:
        char_count = len(reply)
        passed = char_count <= expectation.max_chars
        checks.append(
            _check(
                category="conversation",
                code="character_limit",
                passed=passed,
                detail=f"reply used {char_count} character(s)",
                observed=char_count,
                expected=f"<= {expectation.max_chars}",
            )
        )

    if expectation.max_meeting_ctas is not None:
        cta_count = count_meeting_ctas(reply)
        passed = cta_count <= expectation.max_meeting_ctas
        checks.append(
            _check(
                category="booking",
                code="meeting_cta_limit",
                passed=passed,
                detail=f"reply contained {cta_count} meeting CTA segment(s)",
                observed=cta_count,
                expected=f"<= {expectation.max_meeting_ctas}",
            )
        )

    if expectation.booking_expected is not None:
        passed = observation.booking_created is expectation.booking_expected
        checks.append(
            _check(
                category="booking",
                code="booking_side_effect",
                passed=passed,
                detail=(
                    "booking side effect matched expectation"
                    if passed
                    else f"booking_created was {observation.booking_created}"
                ),
                observed=observation.booking_created,
                expected=expectation.booking_expected,
            )
        )

    if expectation.handoff_expected is not None:
        passed = observation.handoff_requested is expectation.handoff_expected
        checks.append(
            _check(
                category="handoff",
                code="handoff_side_effect",
                passed=passed,
                detail=(
                    "handoff side effect matched expectation"
                    if passed
                    else f"handoff_requested was {observation.handoff_requested}"
                ),
                observed=observation.handoff_requested,
                expected=expectation.handoff_expected,
            )
        )

    tool_names = _tool_names(observation.tool_calls)

    for index, forbidden_tool in enumerate(expectation.forbidden_tools):
        normalized_tool = forbidden_tool.casefold()
        present = normalized_tool in tool_names
        checks.append(
            _check(
                category="tools",
                code=f"forbidden_tool_{index + 1}",
                passed=not present,
                detail=(
                    f"agent called forbidden tool {forbidden_tool!r}"
                    if present
                    else f"agent avoided forbidden tool {forbidden_tool!r}"
                ),
                observed=list(tool_names),
                expected=f"does not contain {normalized_tool}",
            )
        )

    if expectation.tool is not None:
        proposed_name = expectation.tool.proposed_name.casefold()
        alternate_resolution = bool(
            not tool_names
            and observation.resolution_path
            and expectation.allowed_resolution_paths
            and observation.resolution_path.casefold()
            in {path.casefold() for path in expectation.allowed_resolution_paths}
        )
        if proposed_name == "none":
            proposed_name_passed = not tool_names
        else:
            proposed_name_passed = bool(tool_names and tool_names[0] == proposed_name)
        proposed_name_passed = proposed_name_passed or alternate_resolution
        checks.append(
            _check(
                category="tools",
                code="proposed_tool_name",
                passed=proposed_name_passed,
                detail=(
                    f"tool proposal matched {proposed_name}"
                    if proposed_name_passed and not alternate_resolution
                    else (
                        f"allowed deterministic resolution path {observation.resolution_path} required no tool"
                        if alternate_resolution
                        else f"first tool was {tool_names[0] if tool_names else 'none'}"
                    )
                ),
                observed=tool_names[0] if tool_names else "none",
                expected=proposed_name,
            )
        )

        if expectation.tool.max_calls is not None:
            call_count = len(observation.tool_calls)
            passed = call_count <= expectation.tool.max_calls
            checks.append(
                _check(
                    category="tools",
                    code="proposed_tool_call_limit",
                    passed=passed,
                    detail=f"agent made {call_count} logical tool call(s)",
                    observed=call_count,
                    expected=f"<= {expectation.tool.max_calls}",
                )
            )

        if expectation.tool.args_subset:
            matching_call = next(
                (
                    call
                    for call in observation.tool_calls
                    if str(call.get("name") or "").casefold() == proposed_name
                ),
                None,
            )
            observed_args = matching_call.get("args") if matching_call else None
            passed = _is_json_subset(expectation.tool.args_subset, observed_args)
            checks.append(
                _check(
                    category="tools",
                    code="proposed_tool_args_subset",
                    passed=passed,
                    detail=(
                        "tool arguments contained the required subset"
                        if passed
                        else "tool arguments omitted or changed the required subset"
                    ),
                    observed=observed_args,
                    expected=expectation.tool.args_subset,
                )
            )

    if expectation.expected_tool_names is not None:
        expected_tool_names = tuple(name.casefold() for name in expectation.expected_tool_names)
        passed = tool_names == expected_tool_names
        checks.append(
            _check(
                category="tools",
                code="tool_sequence",
                passed=passed,
                detail=(
                    "tool call sequence matched expectation"
                    if passed
                    else f"tool call sequence was {list(tool_names)}"
                ),
                observed=list(tool_names),
                expected=list(expected_tool_names),
            )
        )

    if expectation.max_tool_calls is not None:
        call_count = len(observation.tool_calls)
        passed = call_count <= expectation.max_tool_calls
        checks.append(
            _check(
                category="tools",
                code="tool_call_limit",
                passed=passed,
                detail=f"agent made {call_count} tool call(s)",
                observed=call_count,
                expected=f"<= {expectation.max_tool_calls}",
            )
        )

    return tuple(checks)
