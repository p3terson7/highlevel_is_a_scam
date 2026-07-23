from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_SCHEMA_VERSION = 1
_SCENARIO_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")
_LEAD_STATES = {
    "NEW",
    "GREETED",
    "QUALIFYING",
    "BOOKING_SENT",
    "BOOKED",
    "HANDOFF",
    "OPTED_OUT",
}
_LEAD_SOURCES = {"manual", "meta", "linkedin", "sms"}


class SchemaError(ValueError):
    """Raised when an evaluation fixture does not match the supported schema."""


def _fail(path: str, message: str) -> SchemaError:
    return SchemaError(f"{path}: {message}")


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _fail(path, "expected an object")
    if not all(isinstance(key, str) for key in value):
        raise _fail(path, "object keys must be strings")
    return value


def _reject_unknown(data: Mapping[str, Any], allowed: set[str], path: str) -> None:
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise _fail(path, f"unknown field(s): {', '.join(unknown)}")


def _string(
    value: Any,
    path: str,
    *,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise _fail(path, "expected a string")
    if not allow_empty and not value.strip():
        raise _fail(path, "must not be empty")
    return value


def _optional_string(value: Any, path: str, *, allow_empty: bool = False) -> str | None:
    if value is None:
        return None
    return _string(value, path, allow_empty=allow_empty)


def _boolean(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise _fail(path, "expected a boolean")
    return value


def _integer(value: Any, path: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _fail(path, "expected an integer")
    if value < minimum:
        raise _fail(path, f"must be at least {minimum}")
    return value


def _sequence(value: Any, path: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise _fail(path, "expected an array")
    return value


def _string_tuple(value: Any, path: str, *, unique: bool = True) -> tuple[str, ...]:
    values = _sequence(value, path)
    parsed = tuple(_string(item, f"{path}[{index}]") for index, item in enumerate(values))
    if unique and len(set(parsed)) != len(parsed):
        raise _fail(path, "must not contain duplicates")
    return parsed


def _integer_tuple(
    value: Any,
    path: str,
    *,
    minimum: int = 0,
    unique: bool = True,
) -> tuple[int, ...]:
    values = _sequence(value, path)
    parsed = tuple(
        _integer(item, f"{path}[{index}]", minimum=minimum)
        for index, item in enumerate(values)
    )
    if unique and len(set(parsed)) != len(parsed):
        raise _fail(path, "must not contain duplicates")
    return parsed


def _dict_copy(value: Any, path: str) -> dict[str, Any]:
    return dict(_mapping(value, path))


def _json_safe(value: Any, path: str) -> Any:
    try:
        json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise _fail(path, f"must contain JSON-serializable values ({exc})") from exc
    return value


class _Serializable:
    def to_dict(self) -> dict[str, Any]:
        return _serialize(asdict(self))


def _serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {key: _serialize(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_serialize(item) for item in value]
    return value


@dataclass(frozen=True)
class EvalTenant(_Serializable):
    client_key: str = "chatbot-eval"
    business_name: str = "Evaluation Business"
    tone: str = "friendly, helpful, and concise"
    timezone: str = "America/Toronto"
    qualification_questions: tuple[str, ...] = ()
    booking_url: str = ""
    booking_mode: str = "internal"
    booking_config: dict[str, Any] = field(default_factory=dict)
    provider_config: dict[str, Any] = field(default_factory=dict)
    fallback_handoff_number: str = ""
    consent_text: str = "Reply STOP to opt out. Msg/data rates may apply."
    operating_hours: dict[str, Any] = field(default_factory=dict)
    faq_context: str = ""
    ai_context: str = ""
    template_overrides: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, value: Any, path: str = "tenant") -> EvalTenant:
        data = _mapping(value, path)
        allowed = {
            "client_key",
            "business_name",
            "tone",
            "timezone",
            "qualification_questions",
            "booking_url",
            "booking_mode",
            "booking_config",
            "provider_config",
            "fallback_handoff_number",
            "consent_text",
            "operating_hours",
            "faq_context",
            "ai_context",
            "template_overrides",
        }
        _reject_unknown(data, allowed, path)
        template_overrides = _dict_copy(data.get("template_overrides", {}), f"{path}.template_overrides")
        for key, item in template_overrides.items():
            _string(item, f"{path}.template_overrides.{key}", allow_empty=True)
        return cls(
            client_key=_string(data.get("client_key", "chatbot-eval"), f"{path}.client_key"),
            business_name=_string(
                data.get("business_name", "Evaluation Business"), f"{path}.business_name"
            ),
            tone=_string(data.get("tone", "friendly, helpful, and concise"), f"{path}.tone"),
            timezone=_string(data.get("timezone", "America/Toronto"), f"{path}.timezone"),
            qualification_questions=_string_tuple(
                data.get("qualification_questions", []), f"{path}.qualification_questions"
            ),
            booking_url=_string(data.get("booking_url", ""), f"{path}.booking_url", allow_empty=True),
            booking_mode=_string(data.get("booking_mode", "internal"), f"{path}.booking_mode"),
            booking_config=_json_safe(
                _dict_copy(data.get("booking_config", {}), f"{path}.booking_config"),
                f"{path}.booking_config",
            ),
            provider_config=_json_safe(
                _dict_copy(data.get("provider_config", {}), f"{path}.provider_config"),
                f"{path}.provider_config",
            ),
            fallback_handoff_number=_string(
                data.get("fallback_handoff_number", ""),
                f"{path}.fallback_handoff_number",
                allow_empty=True,
            ),
            consent_text=_string(
                data.get("consent_text", "Reply STOP to opt out. Msg/data rates may apply."),
                f"{path}.consent_text",
                allow_empty=True,
            ),
            operating_hours=_json_safe(
                _dict_copy(data.get("operating_hours", {}), f"{path}.operating_hours"),
                f"{path}.operating_hours",
            ),
            faq_context=_string(data.get("faq_context", ""), f"{path}.faq_context", allow_empty=True),
            ai_context=_string(data.get("ai_context", ""), f"{path}.ai_context", allow_empty=True),
            template_overrides=template_overrides,
        )


@dataclass(frozen=True)
class EvalLead(_Serializable):
    full_name: str = "Evaluation Lead"
    phone: str = "+15555550100"
    email: str = "lead@example.test"
    city: str = ""
    external_lead_id: str = "eval-lead"
    source: str = "manual"
    form_answers: dict[str, Any] = field(default_factory=dict)
    raw_payload: dict[str, Any] = field(default_factory=dict)
    state: str = "QUALIFYING"
    consented: bool = True
    opted_out: bool = False

    @classmethod
    def from_dict(cls, value: Any, path: str = "lead") -> EvalLead:
        data = _mapping(value, path)
        allowed = {
            "full_name",
            "phone",
            "email",
            "city",
            "external_lead_id",
            "source",
            "form_answers",
            "raw_payload",
            "state",
            "consented",
            "opted_out",
        }
        _reject_unknown(data, allowed, path)
        state = _string(data.get("state", "QUALIFYING"), f"{path}.state").upper()
        if state not in _LEAD_STATES:
            raise _fail(f"{path}.state", f"unsupported lead state {state!r}")
        source = _string(data.get("source", "manual"), f"{path}.source").lower()
        if source not in _LEAD_SOURCES:
            raise _fail(f"{path}.source", f"unsupported lead source {source!r}")
        return cls(
            full_name=_string(data.get("full_name", "Evaluation Lead"), f"{path}.full_name"),
            phone=_string(data.get("phone", "+15555550100"), f"{path}.phone"),
            email=_string(data.get("email", "lead@example.test"), f"{path}.email"),
            city=_string(data.get("city", ""), f"{path}.city", allow_empty=True),
            external_lead_id=_string(
                data.get("external_lead_id", "eval-lead"), f"{path}.external_lead_id"
            ),
            source=source,
            form_answers=_json_safe(
                _dict_copy(data.get("form_answers", {}), f"{path}.form_answers"),
                f"{path}.form_answers",
            ),
            raw_payload=_json_safe(
                _dict_copy(data.get("raw_payload", {}), f"{path}.raw_payload"),
                f"{path}.raw_payload",
            ),
            state=state,
            consented=_boolean(data.get("consented", True), f"{path}.consented"),
            opted_out=_boolean(data.get("opted_out", False), f"{path}.opted_out"),
        )


@dataclass(frozen=True)
class EvalHistoryMessage(_Serializable):
    direction: str
    body: str
    raw_payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, value: Any, path: str) -> EvalHistoryMessage:
        data = _mapping(value, path)
        _reject_unknown(data, {"direction", "body", "raw_payload"}, path)
        direction = _string(data.get("direction"), f"{path}.direction").upper()
        if direction not in {"INBOUND", "OUTBOUND"}:
            raise _fail(f"{path}.direction", "must be INBOUND or OUTBOUND")
        return cls(
            direction=direction,
            body=_string(data.get("body"), f"{path}.body"),
            raw_payload=_json_safe(
                _dict_copy(data.get("raw_payload", {}), f"{path}.raw_payload"),
                f"{path}.raw_payload",
            ),
        )


@dataclass(frozen=True)
class ToolSlot(_Serializable):
    start: str
    end: str
    display_time: str
    timezone: str = "America/Toronto"
    slot_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, value: Any, path: str) -> ToolSlot:
        data = _mapping(value, path)
        _reject_unknown(data, {"start", "end", "display_time", "timezone", "slot_id", "metadata"}, path)
        start = _string(data.get("start"), f"{path}.start")
        end = _string(data.get("end"), f"{path}.end")
        try:
            start_at = datetime.fromisoformat(start.replace("Z", "+00:00"))
            end_at = datetime.fromisoformat(end.replace("Z", "+00:00"))
        except ValueError as exc:
            raise _fail(path, "start and end must be ISO-8601 datetimes") from exc
        if (start_at.tzinfo is None) != (end_at.tzinfo is None):
            raise _fail(path, "start and end must use the same timezone style")
        if end_at <= start_at:
            raise _fail(f"{path}.end", "must be later than start")
        return cls(
            start=start,
            end=end,
            display_time=_string(data.get("display_time"), f"{path}.display_time"),
            timezone=_string(data.get("timezone", "America/Toronto"), f"{path}.timezone"),
            slot_id=_optional_string(data.get("slot_id"), f"{path}.slot_id"),
            metadata=_json_safe(
                _dict_copy(data.get("metadata", {}), f"{path}.metadata"), f"{path}.metadata"
            ),
        )


@dataclass(frozen=True)
class ToolWorld(_Serializable):
    slots: tuple[ToolSlot, ...] = ()
    booking_succeeds: bool = True
    booking_reference: str = "eval-booking"

    @classmethod
    def from_dict(cls, value: Any, path: str = "tool_world") -> ToolWorld:
        data = _mapping(value, path)
        _reject_unknown(data, {"slots", "booking_succeeds", "booking_reference"}, path)
        slots_value = _sequence(data.get("slots", []), f"{path}.slots")
        return cls(
            slots=tuple(
                ToolSlot.from_dict(item, f"{path}.slots[{index}]")
                for index, item in enumerate(slots_value)
            ),
            booking_succeeds=_boolean(
                data.get("booking_succeeds", True), f"{path}.booking_succeeds"
            ),
            booking_reference=_string(
                data.get("booking_reference", "eval-booking"), f"{path}.booking_reference"
            ),
        )


@dataclass(frozen=True)
class ToolExpectation(_Serializable):
    proposed_name: str
    args_subset: dict[str, Any] = field(default_factory=dict)
    max_calls: int | None = None

    @classmethod
    def from_dict(cls, value: Any, path: str) -> ToolExpectation:
        data = _mapping(value, path)
        _reject_unknown(data, {"proposed_name", "args_subset", "max_calls"}, path)
        max_calls = None
        if "max_calls" in data and data["max_calls"] is not None:
            max_calls = _integer(data["max_calls"], f"{path}.max_calls")
        return cls(
            proposed_name=_string(data.get("proposed_name"), f"{path}.proposed_name").casefold(),
            args_subset=_json_safe(
                _dict_copy(data.get("args_subset", {}), f"{path}.args_subset"),
                f"{path}.args_subset",
            ),
            max_calls=max_calls,
        )


@dataclass(frozen=True)
class TurnExpectation(_Serializable):
    expected_language: str | None = None
    expected_state: str | None = None
    expected_action: str | None = None
    expected_pending_step: str | None = None
    allowed_actions: tuple[str, ...] | None = None
    allowed_conversation_acts: tuple[str, ...] | None = None
    expected_next_states: tuple[str, ...] | None = None
    language_switch_from: str | None = None
    allowed_resolution_paths: tuple[str, ...] | None = None
    must_include: tuple[str, ...] = ()
    any_of: tuple[str, ...] = ()
    must_not_include: tuple[str, ...] = ()
    required_facts: tuple[str, ...] = ()
    forbidden_claims: tuple[str, ...] = ()
    forbidden_terms: tuple[str, ...] = ()
    forbidden_tools: tuple[str, ...] = ()
    visible_slot_indexes: tuple[int, ...] | None = None
    semantic_criteria: tuple[str, ...] = ()
    max_questions: int | None = None
    max_chars: int | None = None
    max_meeting_ctas: int | None = None
    booking_expected: bool | None = None
    handoff_expected: bool | None = None
    expected_tool_names: tuple[str, ...] | None = None
    max_tool_calls: int | None = None
    tool: ToolExpectation | None = None

    @property
    def max_ctas(self) -> int | None:
        """Compatibility alias used by the richer fixture corpus."""

        return self.max_meeting_ctas

    @classmethod
    def from_dict(cls, value: Any, path: str) -> TurnExpectation:
        data = _mapping(value, path)
        allowed = {
            "expected_language",
            "expected_state",
            "expected_action",
            "expected_pending_step",
            "allowed_actions",
            "allowed_conversation_acts",
            "expected_next_states",
            "language_switch_from",
            "allowed_resolution_paths",
            "must_include",
            "any_of",
            "must_not_include",
            "required_facts",
            "forbidden_claims",
            "forbidden_terms",
            "forbidden_tools",
            "visible_slot_indexes",
            "semantic_criteria",
            "max_questions",
            "max_chars",
            "max_meeting_ctas",
            "max_ctas",
            "booking_expected",
            "handoff_expected",
            "expected_tool_names",
            "max_tool_calls",
            "tool",
        }
        _reject_unknown(data, allowed, path)
        expected_language = _optional_string(
            data.get("expected_language"), f"{path}.expected_language"
        )
        if expected_language is not None:
            expected_language = expected_language.lower()
            if expected_language not in {"en", "fr"}:
                raise _fail(f"{path}.expected_language", "must be en or fr")
        expected_state = _optional_string(data.get("expected_state"), f"{path}.expected_state")
        if expected_state is not None:
            expected_state = expected_state.upper()
            if expected_state not in _LEAD_STATES:
                raise _fail(f"{path}.expected_state", f"unsupported lead state {expected_state!r}")

        def optional_strings(key: str, *, unique: bool = True) -> tuple[str, ...] | None:
            if key not in data or data[key] is None:
                return None
            return _string_tuple(data[key], f"{path}.{key}", unique=unique)

        allowed_actions = optional_strings("allowed_actions")
        allowed_conversation_acts = optional_strings("allowed_conversation_acts")
        expected_next_states = optional_strings("expected_next_states")
        if expected_next_states is not None:
            expected_next_states = tuple(state.upper() for state in expected_next_states)
            invalid_states = sorted(set(expected_next_states) - _LEAD_STATES)
            if invalid_states:
                raise _fail(
                    f"{path}.expected_next_states",
                    f"unsupported lead state(s): {', '.join(invalid_states)}",
                )
        if expected_state and expected_next_states is not None and expected_state not in expected_next_states:
            raise _fail(
                path,
                "expected_state must also appear in expected_next_states when both are provided",
            )

        expected_action = _optional_string(data.get("expected_action"), f"{path}.expected_action")
        if expected_action and allowed_actions is not None:
            allowed_action_values = {action.casefold() for action in allowed_actions}
            if expected_action.casefold() not in allowed_action_values:
                raise _fail(
                    path,
                    "expected_action must also appear in allowed_actions when both are provided",
                )

        language_switch_from = _optional_string(
            data.get("language_switch_from"), f"{path}.language_switch_from"
        )
        if language_switch_from is not None:
            language_switch_from = language_switch_from.casefold()
            if language_switch_from not in {"en", "fr"}:
                raise _fail(f"{path}.language_switch_from", "must be en or fr")

        def optional_int(key: str) -> int | None:
            if key not in data or data[key] is None:
                return None
            return _integer(data[key], f"{path}.{key}")

        def optional_bool(key: str) -> bool | None:
            if key not in data or data[key] is None:
                return None
            return _boolean(data[key], f"{path}.{key}")

        expected_tool_names: tuple[str, ...] | None = None
        if "expected_tool_names" in data:
            expected_tool_names = _string_tuple(
                data["expected_tool_names"], f"{path}.expected_tool_names", unique=False
            )

        max_meeting_ctas = optional_int("max_meeting_ctas")
        max_ctas = optional_int("max_ctas")
        if max_meeting_ctas is not None and max_ctas is not None and max_meeting_ctas != max_ctas:
            raise _fail(path, "max_ctas and max_meeting_ctas must match when both are provided")
        if max_meeting_ctas is None:
            max_meeting_ctas = max_ctas

        tool = None
        if "tool" in data and data["tool"] is not None:
            tool = ToolExpectation.from_dict(data["tool"], f"{path}.tool")

        return cls(
            expected_language=expected_language,
            expected_state=expected_state,
            expected_action=expected_action,
            expected_pending_step=_optional_string(
                data.get("expected_pending_step"), f"{path}.expected_pending_step"
            ),
            allowed_actions=allowed_actions,
            allowed_conversation_acts=allowed_conversation_acts,
            expected_next_states=expected_next_states,
            language_switch_from=language_switch_from,
            allowed_resolution_paths=optional_strings("allowed_resolution_paths"),
            must_include=_string_tuple(data.get("must_include", []), f"{path}.must_include"),
            any_of=_string_tuple(data.get("any_of", []), f"{path}.any_of"),
            must_not_include=_string_tuple(
                data.get("must_not_include", []), f"{path}.must_not_include"
            ),
            required_facts=_string_tuple(data.get("required_facts", []), f"{path}.required_facts"),
            forbidden_claims=_string_tuple(
                data.get("forbidden_claims", []), f"{path}.forbidden_claims"
            ),
            forbidden_terms=_string_tuple(
                data.get("forbidden_terms", []), f"{path}.forbidden_terms"
            ),
            forbidden_tools=_string_tuple(
                data.get("forbidden_tools", []), f"{path}.forbidden_tools"
            ),
            visible_slot_indexes=(
                _integer_tuple(
                    data["visible_slot_indexes"], f"{path}.visible_slot_indexes", minimum=1
                )
                if "visible_slot_indexes" in data and data["visible_slot_indexes"] is not None
                else None
            ),
            semantic_criteria=_string_tuple(
                data.get("semantic_criteria", []), f"{path}.semantic_criteria"
            ),
            max_questions=optional_int("max_questions"),
            max_chars=optional_int("max_chars"),
            max_meeting_ctas=max_meeting_ctas,
            booking_expected=optional_bool("booking_expected"),
            handoff_expected=optional_bool("handoff_expected"),
            expected_tool_names=expected_tool_names,
            max_tool_calls=optional_int("max_tool_calls"),
            tool=tool,
        )


@dataclass(frozen=True)
class EvalTurn(_Serializable):
    inbound: str
    expect: TurnExpectation = field(default_factory=TurnExpectation)
    replay_outputs: tuple[dict[str, Any], ...] = ()
    turn_id: str | None = None

    @classmethod
    def from_dict(cls, value: Any, path: str) -> EvalTurn:
        data = _mapping(value, path)
        _reject_unknown(data, {"inbound", "expect", "replay_outputs", "turn_id"}, path)
        replay_values = _sequence(data.get("replay_outputs", []), f"{path}.replay_outputs")
        replay_outputs = tuple(
            _json_safe(_dict_copy(item, f"{path}.replay_outputs[{index}]"), f"{path}.replay_outputs[{index}]")
            for index, item in enumerate(replay_values)
        )
        return cls(
            inbound=_string(data.get("inbound"), f"{path}.inbound"),
            expect=TurnExpectation.from_dict(data.get("expect", {}), f"{path}.expect"),
            replay_outputs=replay_outputs,
            turn_id=_optional_string(data.get("turn_id"), f"{path}.turn_id"),
        )


@dataclass(frozen=True)
class EvalScenario(_Serializable):
    schema_version: int
    id: str
    owner: str
    kind: str
    turns: tuple[EvalTurn, ...]
    tags: tuple[str, ...] = ()
    risk: str = "normal"
    tenant: EvalTenant = field(default_factory=EvalTenant)
    lead: EvalLead = field(default_factory=EvalLead)
    initial_history: tuple[EvalHistoryMessage, ...] = ()
    tool_world: ToolWorld = field(default_factory=ToolWorld)

    @classmethod
    def from_dict(cls, value: Any, path: str = "scenario") -> EvalScenario:
        data = _mapping(value, path)
        allowed = {
            "schema_version",
            "id",
            "owner",
            "kind",
            "tags",
            "risk",
            "tenant",
            "lead",
            "initial_history",
            "turns",
            "tool_world",
        }
        _reject_unknown(data, allowed, path)
        schema_version = _integer(data.get("schema_version"), f"{path}.schema_version", minimum=1)
        if schema_version != _SCHEMA_VERSION:
            raise _fail(
                f"{path}.schema_version",
                f"unsupported version {schema_version}; expected {_SCHEMA_VERSION}",
            )
        scenario_id = _string(data.get("id"), f"{path}.id")
        if not _SCENARIO_ID_PATTERN.fullmatch(scenario_id):
            raise _fail(
                f"{path}.id",
                "must start with a lowercase letter or digit and contain only lowercase letters, digits, '.', '_' or '-'",
            )
        kind = _string(data.get("kind"), f"{path}.kind").lower()
        if kind not in {"checkpoint", "journey"}:
            raise _fail(f"{path}.kind", "must be checkpoint or journey")

        turns_value = _sequence(data.get("turns"), f"{path}.turns")
        turns = tuple(
            EvalTurn.from_dict(item, f"{path}.turns[{index}]")
            for index, item in enumerate(turns_value)
        )
        if kind == "checkpoint" and len(turns) != 1:
            raise _fail(f"{path}.turns", "checkpoint scenarios must contain exactly one turn")
        if kind == "journey" and len(turns) < 2:
            raise _fail(f"{path}.turns", "journey scenarios must contain at least two turns")

        history_value = _sequence(data.get("initial_history", []), f"{path}.initial_history")
        return cls(
            schema_version=schema_version,
            id=scenario_id,
            owner=_string(data.get("owner"), f"{path}.owner"),
            kind=kind,
            turns=turns,
            tags=_string_tuple(data.get("tags", []), f"{path}.tags"),
            risk=_string(data.get("risk", "normal"), f"{path}.risk"),
            tenant=EvalTenant.from_dict(data.get("tenant", {}), f"{path}.tenant"),
            lead=EvalLead.from_dict(data.get("lead", {}), f"{path}.lead"),
            initial_history=tuple(
                EvalHistoryMessage.from_dict(item, f"{path}.initial_history[{index}]")
                for index, item in enumerate(history_value)
            ),
            tool_world=ToolWorld.from_dict(data.get("tool_world", {}), f"{path}.tool_world"),
        )


@dataclass(frozen=True)
class CheckResult(_Serializable):
    category: str
    code: str
    passed: bool
    detail: str
    observed: Any = None
    expected: Any = None


@dataclass(frozen=True)
class TurnObservation(_Serializable):
    reply: str
    state: str | None = None
    action: str | None = None
    conversation_act: str | None = None
    resolution_path: str | None = None
    pending_step: str | None = None
    language: str | None = None
    booking_created: bool = False
    handoff_requested: bool = False
    tool_calls: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class TurnResult(_Serializable):
    turn_index: int
    observation: TurnObservation
    checks: tuple[CheckResult, ...]
    turn_id: str | None = None
    error: str | None = None

    @property
    def passed(self) -> bool:
        return self.error is None and all(check.passed for check in self.checks)


@dataclass(frozen=True)
class ScenarioResult(_Serializable):
    scenario_id: str
    kind: str
    turns: tuple[TurnResult, ...] = ()
    error: str | None = None
    duration_ms: int | None = None

    @property
    def passed(self) -> bool:
        return self.error is None and bool(self.turns) and all(turn.passed for turn in self.turns)


@dataclass(frozen=True)
class EvalReport(_Serializable):
    agent: str
    provider: str
    scenarios: tuple[ScenarioResult, ...]
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return bool(self.scenarios) and all(scenario.passed for scenario in self.scenarios)

    @property
    def passed_scenarios(self) -> int:
        return sum(scenario.passed for scenario in self.scenarios)

    @property
    def failed_scenarios(self) -> int:
        return len(self.scenarios) - self.passed_scenarios

    def to_dict(self) -> dict[str, Any]:
        payload = super().to_dict()
        payload.update(
            {
                "passed": self.passed,
                "passed_scenarios": self.passed_scenarios,
                "failed_scenarios": self.failed_scenarios,
            }
        )
        for scenario_payload, scenario in zip(payload["scenarios"], self.scenarios, strict=True):
            scenario_payload["passed"] = scenario.passed
            for turn_payload, turn in zip(scenario_payload["turns"], scenario.turns, strict=True):
                turn_payload["passed"] = turn.passed
        return payload

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True, ensure_ascii=False)


def load_scenario(source: str | Path | Mapping[str, Any]) -> EvalScenario:
    """Load one scenario from a mapping or JSON file."""

    if isinstance(source, Mapping):
        return EvalScenario.from_dict(source)
    path = Path(source)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise SchemaError(f"{path}: unable to read scenario ({exc})") from exc
    except json.JSONDecodeError as exc:
        raise SchemaError(
            f"{path}:{exc.lineno}:{exc.colno}: invalid JSON ({exc.msg})"
        ) from exc
    try:
        return EvalScenario.from_dict(value)
    except SchemaError as exc:
        raise SchemaError(f"{path}: {exc}") from exc


def load_scenarios(source: str | Path | Iterable[str | Path]) -> tuple[EvalScenario, ...]:
    """Load scenarios from one file, a directory tree, or a sequence of paths."""

    if isinstance(source, (str, Path)):
        path = Path(source)
        paths = sorted(path.rglob("*.json")) if path.is_dir() else [path]
    else:
        paths = [Path(item) for item in source]
    if not paths:
        raise SchemaError("no scenario JSON files found")
    scenarios = tuple(load_scenario(path) for path in paths)
    seen: set[str] = set()
    duplicate_ids: set[str] = set()
    for scenario in scenarios:
        if scenario.id in seen:
            duplicate_ids.add(scenario.id)
        seen.add(scenario.id)
    if duplicate_ids:
        raise SchemaError(f"duplicate scenario id(s): {', '.join(sorted(duplicate_ids))}")
    return scenarios
