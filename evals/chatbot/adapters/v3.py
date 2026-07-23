"""Evaluation adapter for the production Agent V3 conversation path.

The adapter deliberately runs :func:`process_inbound_turn` rather than calling
the model in isolation.  This keeps the deterministic handoff, booking,
conversation-memory, outbox, and guardrail behavior in the evaluation loop.
Only the external edges are replaced: model output can be replayed, SMS uses
the production mock provider, and calendar behavior is data driven and local.

The public API accepts dictionaries as well as Pydantic models.  This keeps the
runtime adapter independent from the fixture schema and lets that schema evolve
without duplicating it here.
"""

from __future__ import annotations

import json
import re
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import Settings
from app.db.models import (
    AuditLog,
    Base,
    Client,
    ConversationStateEnum,
    Lead,
    LeadSource,
    Message,
    MessageDirection,
)
from app.services.agent_v3 import LLMAgentV3, OpenAIProvider
from app.services.booking import (
    BookingProviderError,
    BookingSelectionResult,
    BookingSlot,
    SlotOffer,
    looks_like_booking_commitment,
    looks_like_slot_selection_message,
)
from app.services.i18n import client_language, format_datetime_for_language
from app.services.inbound_sms import process_inbound_turn
from app.services.sms_service import build_mock_sms_service


class ReplayOutputExhausted(RuntimeError):
    """Raised after Agent V3 requested more model calls than a replay defines."""


class ReplayOutputsUnused(RuntimeError):
    """Raised when a replay defines outputs that Agent V3 never consumed."""


class LiveProviderConfigurationError(RuntimeError):
    """Raised before a live run when its explicit OpenAI configuration is absent."""


class JSONProvider(Protocol):
    name: str

    def generate_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class ProviderCall:
    index: int
    phase: str
    system_prompt: str
    user_prompt: str
    response: dict[str, Any] | None = None
    error: str | None = None

    def to_dict(self, *, include_prompts: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "index": self.index,
            "phase": self.phase,
            "response": _bounded_metadata(self.response) if self.response is not None else None,
            "error": self.error,
        }
        if include_prompts:
            payload["system_prompt"] = self.system_prompt
            payload["user_prompt"] = self.user_prompt
        return payload


def _prompt_phase(system_prompt: str) -> str:
    normalized = str(system_prompt or "").lower()
    if "currently active booking offer" in normalized:
        return "slot_resolution"
    if "after a backend tool returned" in normalized:
        return "tool_followup"
    if "fix this invalid response" in normalized or "return valid json only" in normalized:
        return "json_repair"
    return "decision"


def _provider_error(exc: Exception) -> str:
    # Never interpolate provider/request objects, which may retain credentials.
    detail = str(exc).replace("\n", " ").strip()
    detail = re.sub(r"\bsk-[A-Za-z0-9_-]{8,}\b", "[secret redacted]", detail)
    detail = re.sub(
        r"(?i)\b(authorization|api[_ -]?key|token)(\s*[:=]\s*)([^\s,;]+)",
        r"\1\2[secret redacted]",
        detail,
    )
    return f"{type(exc).__name__}: {detail[:400]}" if detail else type(exc).__name__


class ReplayProvider:
    """Deterministic provider that consumes one JSON object per model call."""

    name = "replay"

    def __init__(self, outputs: Iterable[Mapping[str, Any]], *, scenario_id: str = "") -> None:
        self.scenario_id = str(scenario_id or "").strip()
        self._outputs = [_plain_mapping(output, label="replay output") for output in outputs]
        self._cursor = 0
        self.calls: list[ProviderCall] = []
        self.exhaustion_errors: list[str] = []

    @property
    def consumed(self) -> int:
        return self._cursor

    @property
    def remaining(self) -> int:
        return max(0, len(self._outputs) - self._cursor)

    def generate_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        index = len(self.calls)
        phase = _prompt_phase(system_prompt)
        if self._cursor >= len(self._outputs):
            scenario = f" for scenario '{self.scenario_id}'" if self.scenario_id else ""
            detail = (
                f"Replay output exhausted{scenario}: Agent V3 requested model call "
                f"#{index + 1} ({phase}) but only {len(self._outputs)} output(s) were provided."
            )
            self.calls.append(
                ProviderCall(
                    index=index,
                    phase=phase,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    error=detail,
                )
            )
            self.exhaustion_errors.append(detail)
            raise ReplayOutputExhausted(detail)

        response = dict(self._outputs[self._cursor])
        self._cursor += 1
        self.calls.append(
            ProviderCall(
                index=index,
                phase=phase,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response=response,
            )
        )
        return response

    def assert_healthy(self) -> None:
        if self.exhaustion_errors:
            raise ReplayOutputExhausted(self.exhaustion_errors[-1])

    def assert_consumed(self) -> None:
        self.assert_healthy()
        if not self.remaining:
            return
        scenario = f" for scenario '{self.scenario_id}'" if self.scenario_id else ""
        raise ReplayOutputsUnused(
            f"Replay has {self.remaining} unused model output(s){scenario}; "
            f"Agent V3 consumed {self.consumed} of {len(self._outputs)}. "
            "This usually means a deterministic guardrail handled a turn before the model."
        )


class RecordingProvider:
    """Record an explicitly supplied provider without changing its behavior."""

    def __init__(self, delegate: JSONProvider) -> None:
        self._delegate = delegate
        self.name = str(getattr(delegate, "name", "unknown") or "unknown")
        self.calls: list[ProviderCall] = []

    def generate_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        index = len(self.calls)
        phase = _prompt_phase(system_prompt)
        try:
            response = self._delegate.generate_json(system_prompt=system_prompt, user_prompt=user_prompt)
        except Exception as exc:
            self.calls.append(
                ProviderCall(
                    index=index,
                    phase=phase,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    error=_provider_error(exc),
                )
            )
            raise
        self.calls.append(
            ProviderCall(
                index=index,
                phase=phase,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response=dict(response),
            )
        )
        return response


class _ObservingAgent:
    """Capture the post-guardrail response without altering Agent V3."""

    def __init__(self, delegate: LLMAgentV3) -> None:
        self._delegate = delegate
        self.responses: list[Any] = []
        self.slot_resolutions: list[dict[str, Any] | None] = []

    def run_turn(self, *args: Any, **kwargs: Any) -> Any:
        response = self._delegate.run_turn(*args, **kwargs)
        self.responses.append(response)
        return response

    def next_reply(self, *args: Any, **kwargs: Any) -> Any:
        response = self._delegate.next_reply(*args, **kwargs)
        self.responses.append(response)
        return response

    def resolve_booking_selection(self, *args: Any, **kwargs: Any) -> dict[str, Any] | None:
        resolution = self._delegate.resolve_booking_selection(*args, **kwargs)
        self.slot_resolutions.append(resolution)
        return resolution


def build_live_provider(
    *,
    settings: Settings | None = None,
    model: str | None = None,
    timeout_seconds: int | None = None,
) -> RecordingProvider:
    """Build a live provider only after the caller explicitly chooses live mode.

    Merely importing this module or building a replay adapter never creates an
    OpenAI client.  Credentials are read through the application's Settings and
    are never copied into results, prompts, diagnostics, or object reprs.
    """

    effective = settings or Settings()
    api_key = str(effective.openai_api_key or "").strip()
    if not api_key:
        raise LiveProviderConfigurationError(
            "Live chatbot evaluation requires OPENAI_API_KEY. Replay mode remains fully offline."
        )
    selected_model = str(model or effective.openai_model or "").strip()
    if not selected_model:
        raise LiveProviderConfigurationError(
            "Live chatbot evaluation requires OPENAI_MODEL or Settings.openai_model."
        )
    timeout = int(timeout_seconds or effective.request_timeout_seconds or 20)
    return RecordingProvider(
        OpenAIProvider(api_key=api_key, model=selected_model, timeout_seconds=timeout)
    )


def build_live_agent(
    *,
    settings: Settings | None = None,
    model: str | None = None,
    timeout_seconds: int | None = None,
) -> LLMAgentV3:
    return LLMAgentV3(
        provider=build_live_provider(
            settings=settings,
            model=model,
            timeout_seconds=timeout_seconds,
        )
    )


@dataclass(frozen=True)
class AdapterTurnResult:
    index: int
    inbound: str
    reply: str
    state: str
    action: str = "none"
    pending_step: str | None = None
    language: str = "en"
    booking_created: bool = False
    handoff_requested: bool = False
    tool_calls: tuple[dict[str, Any], ...] = ()
    provider: str = ""
    provider_error: str | None = None
    conversation_act: str | None = None
    resolution_path: str | None = None
    raw_outbound_metadata: dict[str, Any] = field(default_factory=dict)
    inbound_message_id: int | None = None
    outbound_message_id: int | None = None
    suppressed: bool = False

    @property
    def outbound(self) -> str:
        """Compatibility alias used by some report renderers."""

        return self.reply

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AdapterScenarioResult:
    scenario_id: str
    turns: tuple[AdapterTurnResult, ...]
    replay_remaining: int = 0
    errors: tuple[str, ...] = ()
    provider_calls: tuple[dict[str, Any], ...] = ()
    booking_calls: tuple[dict[str, Any], ...] = ()

    @property
    def passed_adapter(self) -> bool:
        return not self.errors

    @property
    def error(self) -> str | None:
        """Compatibility view for runners that store one scenario-level error."""

        return "; ".join(self.errors) if self.errors else None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class DeterministicBookingService:
    """A data-driven calendar boundary with no network or provider writes.

    ``tool_world`` may contain:

    - ``slots``: the default slot dictionaries.
    - ``slot_sets``: ordered ``{"when": {...}, "slots": [...]}`` overrides.
    - ``strict_preferences``: return no slots instead of falling back when a
      requested day/time has no match.
    - ``booking_outcome``: ``success`` (default), ``rejected``, or ``ambiguous``.
    - localized ``offer_reply``/``booking_reply``/``no_slots_reply`` strings or
      ``{"en": ..., "fr": ...}`` maps.
    """

    def __init__(self, tool_world: Any = None) -> None:
        self.world = _plain_mapping(tool_world or {}, label="tool_world")
        if "booking_succeeds" in self.world and "booking_outcome" not in self.world:
            self.world["booking_outcome"] = (
                "success" if bool(self.world.get("booking_succeeds")) else "rejected"
            )
        if self.world.get("booking_reference") and not self.world.get("booking_id"):
            self.world["booking_id"] = str(self.world["booking_reference"])
        self.calls: list[dict[str, Any]] = []
        raw_slots = self.world.get("slots") if "slots" in self.world else _default_slots()
        self._default_slots = _normalize_slots(raw_slots)

    def _record(self, name: str, **arguments: Any) -> None:
        self.calls.append(
            {
                "source": "booking_service",
                "name": name,
                "args": _bounded_metadata(arguments),
            }
        )

    def preview_slots(self, client: Client, limit: int = 3, db: Session | None = None) -> SlotOffer:
        return self.find_slots(client=client, lead=None, limit=limit, db=db)

    def offer_slots(
        self,
        client: Client,
        lead: Lead | None,
        limit: int = 3,
        db: Session | None = None,
    ) -> SlotOffer:
        return self.find_slots(client=client, lead=lead, limit=limit, db=db)

    def find_slots(
        self,
        *,
        client: Client,
        lead: Lead | None,
        preferred_day: str | None = None,
        avoid_day: str | None = None,
        preferred_period: str | None = None,
        exact_time: str | None = None,
        range_start: str | None = None,
        range_end: str | None = None,
        request_text: str | None = None,
        limit: int = 3,
        db: Session | None = None,
    ) -> SlotOffer:
        _ = db
        arguments = {
            "preferred_day": preferred_day,
            "avoid_day": avoid_day,
            "preferred_period": preferred_period,
            "exact_time": exact_time,
            "range_start": range_start,
            "range_end": range_end,
            "request_text": request_text,
            "limit": limit,
        }
        self._record("find_slots", **arguments)
        slots, configured_reply = self._slots_for(arguments)
        slots = slots[: max(1, min(int(limit or 3), 10))]
        language = client_language(client, lead=lead, inbound_text=request_text)
        reply = _localized_value(configured_reply, language)
        if not slots:
            reply = reply or _localized_value(self.world.get("no_slots_reply"), language)
            reply = reply or (
                "Je ne vois aucun créneau correspondant dans ce calendrier de test."
                if language == "fr"
                else "I do not see a matching time in this test calendar."
            )
        else:
            reply = reply or _localized_value(self.world.get("offer_reply"), language)
            reply = reply or _render_offer_reply(slots, language=language, timezone_name=client.timezone)
        offer = {
            "provider": "eval",
            "slots": [asdict(slot) for slot in slots],
            "generated_at": "eval-deterministic",
            "request": _bounded_metadata(arguments),
            "matched_preference": bool(slots),
            "match_mode": "eval_fixture",
        }
        return SlotOffer(reply_text=reply, slots=slots, raw_payload={"booking_offer": offer})

    def _slots_for(self, arguments: Mapping[str, Any]) -> tuple[list[BookingSlot], Any]:
        for raw_set in self.world.get("slot_sets") or []:
            slot_set = _plain_mapping(raw_set, label="slot set")
            conditions = _plain_mapping(slot_set.get("when") or {}, label="slot set conditions")
            if _conditions_match(conditions, arguments):
                return _normalize_slots(slot_set.get("slots") or []), slot_set.get("reply")

        slots = list(self._default_slots)
        filtered = _filter_slots(slots, arguments)
        has_preference = any(
            arguments.get(key)
            for key in ("preferred_day", "avoid_day", "preferred_period", "exact_time", "range_start", "range_end")
        )
        if filtered or not has_preference:
            return filtered or slots, None
        if bool(self.world.get("strict_preferences")):
            return [], None
        return slots, None

    def book_requested_slot(
        self,
        *,
        client: Client,
        lead: Lead,
        latest_offer: dict[str, Any] | None,
        slot_index: int | None = None,
        slot_start_time: str | None = None,
        slot_text: str | None = None,
        db: Session | None = None,
    ) -> dict[str, Any]:
        _ = db
        self._record(
            "book_slot",
            slot_index=slot_index,
            slot_start_time=slot_start_time,
            slot_text=slot_text,
        )
        slots = _offer_slots(latest_offer) or [asdict(slot) for slot in self._default_slots]
        selected = _match_slot(
            slots,
            slot_index=slot_index,
            slot_start_time=slot_start_time,
            slot_text=slot_text,
        )
        if selected is None:
            return self._unmatched_booking_result(client=client, lead=lead, latest_offer=latest_offer)

        outcome = str(self.world.get("booking_outcome") or "success").strip().lower()
        if outcome == "ambiguous":
            raise BookingProviderError(
                "The evaluation calendar returned an intentionally ambiguous result.",
                ambiguous=True,
                provider_status=503,
            )
        if outcome == "rejected":
            return self._unmatched_booking_result(client=client, lead=lead, latest_offer=latest_offer)
        return self._successful_booking_result(client=client, lead=lead, selected=selected)

    def _unmatched_booking_result(
        self,
        *,
        client: Client,
        lead: Lead,
        latest_offer: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        language = client_language(client, lead=lead)
        slots = _offer_slots(latest_offer)
        reply = _localized_value(self.world.get("booking_rejected_reply"), language) or (
            "Je n'ai pas pu confirmer ce créneau. Choisissez une des options proposées."
            if language == "fr"
            else "I could not confirm that time. Please choose one of the offered options."
        )
        return {
            "reply_text": reply,
            "slots": slots,
            "runtime_payload": {
                "booking_offer": dict(latest_offer or {}),
                "pending_step": "slot_selection_pending" if slots else None,
            },
        }

    def _successful_booking_result(
        self,
        *,
        client: Client,
        lead: Lead,
        selected: Mapping[str, Any],
    ) -> dict[str, Any]:
        language = client_language(client, lead=lead)
        display = _slot_display(selected, language=language, timezone_name=client.timezone)
        configured = _localized_value(self.world.get("booking_reply"), language)
        reply = configured or (
            f"Réservé. Votre appel est prévu pour {display}."
            if language == "fr"
            else f"Booked. Your call is set for {display}."
        )
        booking_id = str(self.world.get("booking_id") or "eval-booking-1")
        booking = {"provider": "eval", "event_uri": f"eval://booking/{booking_id}", "id": booking_id}
        return {
            "reply_text": reply,
            "booking": booking,
            "runtime_payload": {
                "calendar_booking": {
                    "provider": "eval",
                    "slot": dict(selected),
                    "booking": booking,
                },
                "pending_step": None,
            },
        }

    def handle_slot_selection(
        self,
        *,
        client: Client,
        lead: Lead,
        inbound_text: str,
        history: Sequence[Message],
        active_offer: dict[str, Any] | None = None,
        resolved_slot_index: int | None = None,
        resolved_slot_start_time: str | None = None,
        db: Session | None = None,
    ) -> BookingSelectionResult | None:
        _ = db
        latest_offer = active_offer or _latest_offer(history)
        if not latest_offer:
            return None
        should_attempt = bool(
            resolved_slot_index
            or resolved_slot_start_time
            or looks_like_slot_selection_message(inbound_text)
            or looks_like_booking_commitment(inbound_text)
        )
        if not should_attempt:
            return None
        self._record(
            "handle_slot_selection",
            inbound_text=inbound_text,
            resolved_slot_index=resolved_slot_index,
            resolved_slot_start_time=resolved_slot_start_time,
        )
        slots = _offer_slots(latest_offer)
        selected = _match_slot(
            slots,
            slot_index=resolved_slot_index,
            slot_start_time=resolved_slot_start_time,
            slot_text=inbound_text,
        )
        if selected is None:
            # A specific new time belongs back in the normal Agent V3 path so
            # it can ask find_slots instead of pretending it matched an offer.
            if re.search(r"\b\d{1,2}(?::\d{2})?\s*(?:am|pm|h)\b", inbound_text, re.IGNORECASE):
                return None
            language = client_language(client, lead=lead, inbound_text=inbound_text)
            reply = _localized_value(self.world.get("clarification_reply"), language)
            reply = reply or _render_clarification(slots, language=language)
            return BookingSelectionResult(
                handled=True,
                reply_text=reply,
                next_state=ConversationStateEnum.BOOKING_SENT,
                raw_payload={"booking_offer": dict(latest_offer), "pending_step": "slot_selection_pending"},
                audit_event_type="calendar_booking_offer_repeated",
                audit_decision={"inbound": inbound_text, "provider": "eval"},
                transition_reason="calendar_booking_offer_repeated",
            )

        try:
            result = self.book_requested_slot(
                client=client,
                lead=lead,
                latest_offer=latest_offer,
                slot_index=int(selected.get("index") or 0) or None,
                slot_start_time=str(selected.get("start_time") or "") or None,
                slot_text=inbound_text,
                db=db,
            )
        except BookingProviderError as exc:
            if not exc.ambiguous:
                raise
            language = client_language(client, lead=lead, inbound_text=inbound_text)
            return BookingSelectionResult(
                handled=True,
                reply_text=(
                    "Je ne peux pas confirmer le résultat; une personne va le vérifier."
                    if language == "fr"
                    else "I cannot confirm the result; a person will verify it."
                ),
                next_state=ConversationStateEnum.HANDOFF,
                raw_payload={
                    "pending_step": None,
                    "booking_confirmation_unknown": True,
                    "booking_provider_status": exc.provider_status,
                },
                audit_event_type="calendar_booking_confirmation_unknown",
                audit_decision={"inbound": inbound_text, "provider": "eval"},
                transition_reason="calendar_booking_confirmation_unknown",
            )

        calendar_booking = (result.get("runtime_payload") or {}).get("calendar_booking")
        if not calendar_booking:
            return BookingSelectionResult(
                handled=True,
                reply_text=str(result.get("reply_text") or ""),
                next_state=ConversationStateEnum.BOOKING_SENT,
                raw_payload=dict(result.get("runtime_payload") or {}),
                audit_event_type="calendar_booking_offer_repeated",
                audit_decision={"inbound": inbound_text, "provider": "eval"},
                transition_reason="calendar_booking_offer_repeated",
            )
        return BookingSelectionResult(
            handled=True,
            reply_text=str(result.get("reply_text") or ""),
            next_state=ConversationStateEnum.BOOKED,
            raw_payload=dict(result.get("runtime_payload") or {}),
            audit_event_type="calendar_booking_created",
            audit_decision={"inbound": inbound_text, "provider": "eval", "slot": dict(selected)},
            transition_reason="calendar_booking_created",
        )

    def handle_reschedule_confirmation(
        self,
        *,
        client: Client,
        lead: Lead,
        inbound_text: str,
        history: Sequence[Message] | None = None,
        db: Session | None = None,
    ) -> BookingSelectionResult | None:
        _ = (client, lead, inbound_text, history, db)
        # Reschedule state is still evaluated by Agent V3. A future fixture can
        # opt into a richer fake without silently changing current behavior.
        return None


class V3ScenarioAdapter:
    """Run one synthetic conversation through the real inbound-turn pipeline."""

    def __init__(
        self,
        scenario: Any,
        *,
        provider: JSONProvider | None = None,
        agent: LLMAgentV3 | None = None,
        booking_service: DeterministicBookingService | None = None,
        database_path: str | Path | None = None,
    ) -> None:
        self.scenario = _plain_mapping(scenario, label="scenario")
        self.scenario_id = str(self.scenario.get("id") or self.scenario.get("scenario_id") or "scenario")
        if provider is not None and agent is not None:
            raise ValueError("Pass either provider or agent, not both")
        if provider is None and agent is None:
            provider = ReplayProvider(_scenario_replay_outputs(self.scenario), scenario_id=self.scenario_id)
        self.provider = provider or getattr(agent, "_provider", None)
        delegate = agent or LLMAgentV3(provider=provider)  # type: ignore[arg-type]
        self.agent = _ObservingAgent(delegate)
        self.booking_service = booking_service or DeterministicBookingService(self.scenario.get("tool_world"))
        self._database_path = Path(database_path).expanduser().resolve() if database_path else None
        self._temporary_directory: tempfile.TemporaryDirectory[str] | None = None
        self._engine: Engine | None = None
        self._session_factory: sessionmaker[Session] | None = None
        self._db: Session | None = None
        self._client_id: int | None = None
        self._lead_id: int | None = None
        self._base_time = _parse_datetime(
            self.scenario.get("started_at") or self.scenario.get("now"),
            default=datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc),
        )
        self._turn_counter = 0

    @classmethod
    def live(
        cls,
        scenario: Any,
        *,
        settings: Settings | None = None,
        model: str | None = None,
        timeout_seconds: int | None = None,
        database_path: str | Path | None = None,
    ) -> "V3ScenarioAdapter":
        provider = build_live_provider(
            settings=settings,
            model=model,
            timeout_seconds=timeout_seconds,
        )
        return cls(scenario, provider=provider, database_path=database_path)

    @property
    def db(self) -> Session:
        self._ensure_started()
        assert self._db is not None
        return self._db

    @property
    def client(self) -> Client:
        self._ensure_started()
        assert self._client_id is not None
        client = self.db.get(Client, self._client_id)
        if client is None:
            raise RuntimeError("Evaluation client disappeared")
        return client

    @property
    def lead(self) -> Lead:
        self._ensure_started()
        assert self._lead_id is not None
        lead = self.db.get(Lead, self._lead_id)
        if lead is None:
            raise RuntimeError("Evaluation lead disappeared")
        return lead

    def __enter__(self) -> "V3ScenarioAdapter":
        self._ensure_started()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        _ = (exc_type, exc, traceback)
        self.close()

    def close(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        if self._temporary_directory is not None:
            self._temporary_directory.cleanup()
            self._temporary_directory = None

    def _ensure_started(self) -> None:
        if self._db is not None:
            return
        if self._database_path is None:
            self._temporary_directory = tempfile.TemporaryDirectory(prefix="chatbot-eval-")
            path = Path(self._temporary_directory.name) / "scenario.db"
        else:
            path = self._database_path
            if path.exists():
                raise FileExistsError(f"Evaluation database already exists: {path}")
            path.parent.mkdir(parents=True, exist_ok=True)
        self._engine = create_engine(
            f"sqlite+pysqlite:///{path.as_posix()}",
            connect_args={"check_same_thread": False},
            future=True,
        )
        Base.metadata.create_all(self._engine)
        self._session_factory = sessionmaker(
            bind=self._engine,
            class_=Session,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
        self._db = self._session_factory()
        self._seed_scenario()

    def _seed_scenario(self) -> None:
        tenant = _plain_mapping(
            self.scenario.get("tenant") or self.scenario.get("client") or {},
            label="tenant",
        )
        lead_data = _plain_mapping(self.scenario.get("lead") or {}, label="lead")
        provider_config = _plain_mapping(tenant.get("provider_config") or {}, label="provider_config")
        language = tenant.get("language")
        if language:
            provider_config["language"] = str(language)
        provider_config.setdefault("business_profile_context", str(tenant.get("business_profile_context") or ""))
        client = Client(
            client_key=f"eval-{_safe_identifier(self.scenario_id)}-{uuid4().hex[:8]}",
            business_name=str(tenant.get("business_name") or "Evaluation Business"),
            tone=str(tenant.get("tone") or "friendly"),
            timezone=str(tenant.get("timezone") or "UTC"),
            qualification_questions=list(tenant.get("qualification_questions") or []),
            booking_url=str(tenant.get("booking_url") or ""),
            booking_mode=str(tenant.get("booking_mode") or "internal"),
            booking_config=_plain_mapping(tenant.get("booking_config") or {}, label="booking_config"),
            provider_config=provider_config,
            fallback_handoff_number=str(tenant.get("fallback_handoff_number") or ""),
            consent_text=str(tenant.get("consent_text") or "Reply STOP to opt out."),
            operating_hours=_plain_mapping(
                tenant.get("operating_hours")
                or {"days": [0, 1, 2, 3, 4, 5, 6], "start": "00:00", "end": "23:59"},
                label="operating_hours",
            ),
            faq_context=str(tenant.get("faq_context") or ""),
            ai_context=str(tenant.get("ai_context") or ""),
            template_overrides=_plain_mapping(tenant.get("template_overrides") or {}, label="template_overrides"),
            is_active=True,
        )
        self.db.add(client)
        self.db.flush()

        raw_payload = _plain_mapping(lead_data.get("raw_payload") or {}, label="lead raw_payload")
        # This production sandbox marker is the application's explicit switch
        # that suppresses Zapier booking delivery. It is forced after fixture
        # data so an evaluation can never opt itself into an external webhook.
        raw_payload["created_from"] = "ui_ai_sandbox"
        raw_payload["test_configuration"] = "gpt_only"
        initial_state = (
            lead_data.get("conversation_state")
            or lead_data.get("state")
            or self.scenario.get("initial_state")
            or "QUALIFYING"
        )
        lead = Lead(
            client_id=client.id,
            external_lead_id=f"eval-{uuid4().hex}",
            source=_lead_source(lead_data.get("source")),
            full_name=str(lead_data.get("full_name") or "Alex Evaluation"),
            phone=str(lead_data.get("phone") or "+15555550100"),
            email=str(lead_data.get("email") or "alex.eval@example.invalid"),
            city=str(lead_data.get("city") or ""),
            form_answers=_plain_mapping(lead_data.get("form_answers") or {}, label="form_answers"),
            raw_payload=raw_payload,
            consented=bool(lead_data.get("consented", True)),
            opted_out=bool(lead_data.get("opted_out", False)),
            conversation_state=_conversation_state(initial_state),
            crm_stage=str(lead_data.get("crm_stage") or "New Lead"),
            owner_name=str(lead_data.get("owner_name") or ""),
            created_at=self._base_time - timedelta(minutes=10),
            updated_at=self._base_time - timedelta(minutes=10),
        )
        self.db.add(lead)
        self.db.flush()
        self._client_id = client.id
        self._lead_id = lead.id

        history = self.scenario.get("initial_history") or self.scenario.get("history") or []
        history_items = list(history)
        for index, raw_message in enumerate(history_items):
            item = _plain_mapping(raw_message, label="history message")
            direction = _message_direction(item.get("direction"))
            created_at = _parse_datetime(
                item.get("created_at") or item.get("at"),
                default=self._base_time - timedelta(minutes=max(1, len(history_items) - index)),
            )
            self.db.add(
                Message(
                    lead_id=lead.id,
                    client_id=client.id,
                    direction=direction,
                    body=str(item.get("body") or item.get("text") or ""),
                    provider_message_sid=f"EVAL-HISTORY-{index + 1}-{uuid4().hex[:8]}",
                    raw_payload=_plain_mapping(item.get("raw_payload") or {}, label="history raw_payload"),
                    created_at=created_at,
                )
            )
        self.db.commit()

    def run_turn(self, inbound_text: str, *, at: datetime | str | None = None) -> AdapterTurnResult:
        self._ensure_started()
        index = self._turn_counter
        turn_time = _parse_datetime(at, default=self._base_time + timedelta(minutes=index))
        client = self.client
        lead = self.lead
        inbound = Message(
            lead_id=lead.id,
            client_id=client.id,
            direction=MessageDirection.INBOUND,
            body=str(inbound_text),
            provider_message_sid=f"EVAL-IN-{index + 1}-{uuid4().hex[:10]}",
            raw_payload={"source": "chatbot_eval", "external_delivery_bypassed": True},
            created_at=turn_time,
        )
        self.db.add(inbound)
        self.db.flush()
        lead.last_inbound_at = turn_time
        lead.updated_at = turn_time
        self.db.commit()

        provider_start = len(_provider_calls(self.provider))
        booking_start = len(self.booking_service.calls)
        response_start = len(self.agent.responses)
        resolution_start = len(self.agent.slot_resolutions)
        process_inbound_turn(
            db=self.db,
            client=client,
            lead=lead,
            inbound_text=str(inbound_text),
            now=turn_time,
            sms_service=build_mock_sms_service(),
            booking_service=self.booking_service,  # type: ignore[arg-type]
            llm_agent=self.agent,
            inbound_message_id=inbound.id,
        )
        self.db.expire_all()
        current_lead = self.lead
        outbound = self._outbound_for_inbound(inbound.id)
        metadata = dict(outbound.raw_payload or {}) if outbound else {}
        raw_agent = metadata.get("agent") if isinstance(metadata.get("agent"), dict) else {}
        booking_flow = metadata.get("booking_flow") if isinstance(metadata.get("booking_flow"), dict) else {}
        booking_created = bool(metadata.get("calendar_booking")) or str(
            booking_flow.get("event_type") or ""
        ) == "calendar_booking_created"
        handoff_requested = current_lead.conversation_state == ConversationStateEnum.HANDOFF
        action = str(raw_agent.get("action") or "").strip()
        if booking_created:
            action = "mark_booked"
        elif handoff_requested:
            action = "handoff_to_human"
        elif not action:
            action = "none"
        provider = str(raw_agent.get("provider") or "").strip()
        if not provider and isinstance(metadata.get("booking_flow"), dict):
            provider = "deterministic_booking_flow"
        underlying_provider = str(getattr(self.provider, "name", "") or "").strip()
        if provider == "openai" and underlying_provider:
            provider = underlying_provider
        provider_error = str(raw_agent.get("provider_error") or "").strip() or None
        lead_payload = current_lead.raw_payload if isinstance(current_lead.raw_payload, dict) else {}
        pending_step = metadata.get("pending_step_after")
        if pending_step is None:
            pending_step = lead_payload.get("pending_step")
        language = str(lead_payload.get("lead_language") or client_language(client, lead=current_lead)).strip() or "en"
        provider_end = len(_provider_calls(self.provider))
        model_tools = _model_tool_calls(_provider_calls(self.provider)[provider_start:provider_end])
        booking_tools = [dict(call) for call in self.booking_service.calls[booking_start:]]
        observed_response = self.agent.responses[-1] if len(self.agent.responses) > response_start else None
        slot_resolution = next(
            (
                resolution
                for resolution in reversed(self.agent.slot_resolutions[resolution_start:])
                if isinstance(resolution, dict)
            ),
            None,
        )
        conversation_act = str(getattr(observed_response, "conversation_act", "") or "").strip() or None
        if conversation_act is None and handoff_requested:
            conversation_act = "handoff"
        elif conversation_act is None and booking_created:
            conversation_act = "book_selected_slot"
        elif conversation_act is None and slot_resolution:
            resolution_decision = str(slot_resolution.get("decision") or "").strip().lower()
            if resolution_decision == "ask_clarification":
                conversation_act = "ask_clarifying_question"
            elif (
                resolution_decision == "new_times"
                and current_lead.conversation_state == ConversationStateEnum.BOOKING_SENT
                and bool(metadata.get("booking_offer"))
            ):
                conversation_act = "offer_slots"
            elif resolution_decision == "select_slot" and booking_created:
                conversation_act = "book_selected_slot"
        resolution_path = self._resolution_path(
            handoff_requested=handoff_requested,
            booking_flow=booking_flow,
            observed_response=observed_response,
            model_tools=model_tools,
            suppressed=outbound is None,
        )
        result = AdapterTurnResult(
            index=index,
            inbound=str(inbound_text),
            reply=str(outbound.body or "") if outbound else "",
            state=current_lead.conversation_state.value,
            action=action,
            pending_step=str(pending_step) if pending_step else None,
            language=language,
            booking_created=booking_created,
            handoff_requested=handoff_requested,
            tool_calls=tuple(_logical_tool_calls(model_tools, booking_tools)),
            provider=provider,
            provider_error=provider_error,
            conversation_act=conversation_act,
            resolution_path=resolution_path,
            raw_outbound_metadata=_bounded_metadata(metadata),
            inbound_message_id=inbound.id,
            outbound_message_id=outbound.id if outbound else None,
            suppressed=outbound is None,
        )
        self._turn_counter += 1
        return result

    def _resolution_path(
        self,
        *,
        handoff_requested: bool,
        booking_flow: Mapping[str, Any],
        observed_response: Any,
        model_tools: Sequence[dict[str, Any]],
        suppressed: bool,
    ) -> str | None:
        if booking_flow:
            return "deterministic_booking_flow"
        if handoff_requested:
            audit = self.db.scalar(
                select(AuditLog)
                .where(
                    AuditLog.lead_id == self.lead.id,
                    AuditLog.event_type == "agent_handoff_triggered",
                )
                .order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
                .limit(1)
            )
            decision = audit.decision if audit and isinstance(audit.decision, dict) else {}
            source = str(decision.get("source") or "").strip().lower()
            if source == "pre_llm":
                return "pre_llm_handoff_policy"
            if source == "post_llm":
                return "post_llm_handoff_policy"
            if any(call.get("name") == "handoff_to_human" for call in model_tools):
                return "agent_handoff_tool"
            return "handoff_policy"
        if suppressed:
            return "suppressed"
        if observed_response is not None:
            if str(getattr(observed_response, "provider", "") or "") == "fallback":
                return "agent_fallback"
            if model_tools:
                return "agent_tool"
            return "agent_v3"
        return None

    def run(self) -> AdapterScenarioResult:
        try:
            results: list[AdapterTurnResult] = []
            errors: list[str] = []
            turns = list(self.scenario.get("turns") or [])
            for raw_turn in turns:
                turn = _plain_mapping(raw_turn, label="turn")
                inbound = turn.get("inbound") or turn.get("inbound_text") or turn.get("message")
                if inbound is None:
                    errors.append(f"Turn {len(results) + 1} has no inbound message")
                    break
                try:
                    results.append(self.run_turn(str(inbound), at=turn.get("at") or turn.get("now")))
                except Exception as exc:
                    errors.append(f"Turn {len(results) + 1} failed: {_provider_error(exc)}")
                    break

            replay_remaining = 0
            if isinstance(self.provider, ReplayProvider):
                replay_remaining = self.provider.remaining
                errors.extend(error for error in self.provider.exhaustion_errors if error not in errors)
                if replay_remaining:
                    try:
                        self.provider.assert_consumed()
                    except ReplayOutputsUnused as exc:
                        errors.append(str(exc))

            return AdapterScenarioResult(
                scenario_id=self.scenario_id,
                turns=tuple(results),
                replay_remaining=replay_remaining,
                errors=tuple(errors),
                provider_calls=tuple(call.to_dict() for call in _provider_calls(self.provider)),
                booking_calls=tuple(_bounded_metadata(call) for call in self.booking_service.calls),
            )
        finally:
            self.close()

    def _outbound_for_inbound(self, inbound_message_id: int) -> Message | None:
        candidates = self.db.scalars(
            select(Message)
            .where(
                Message.lead_id == self.lead.id,
                Message.direction == MessageDirection.OUTBOUND,
            )
            .order_by(Message.created_at.desc(), Message.id.desc())
        ).all()
        for message in candidates:
            payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
            try:
                linked_id = int(payload.get("inbound_message_id"))
            except (TypeError, ValueError):
                continue
            if linked_id == int(inbound_message_id):
                return message
        return None


def _plain_mapping(value: Any, *, label: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump(mode="python")
        if isinstance(dumped, Mapping):
            return dict(dumped)
    if is_dataclass(value):
        dumped = asdict(value)
        if isinstance(dumped, Mapping):
            return dict(dumped)
    raise TypeError(f"{label} must be a mapping or model with model_dump()")


def _scenario_replay_outputs(scenario: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_outputs = scenario.get("replay_outputs")
    if raw_outputs is not None:
        return [_plain_mapping(item, label="replay output") for item in raw_outputs]
    outputs: list[dict[str, Any]] = []
    for raw_turn in scenario.get("turns") or []:
        turn = _plain_mapping(raw_turn, label="turn")
        turn_outputs = turn.get("replay_outputs")
        if turn_outputs is None and turn.get("replay_output") is not None:
            turn_outputs = [turn.get("replay_output")]
        outputs.extend(_plain_mapping(item, label="replay output") for item in (turn_outputs or []))
    return outputs


def _provider_calls(provider: Any) -> list[ProviderCall]:
    calls = getattr(provider, "calls", None)
    return list(calls) if isinstance(calls, list) else []


def _model_tool_calls(calls: Sequence[ProviderCall]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for call in calls:
        response = call.response if isinstance(call.response, dict) else {}
        raw_tool = response.get("tool_call") if isinstance(response.get("tool_call"), dict) else {}
        name = str(raw_tool.get("name") or "none").strip()
        if name and name != "none":
            output.append(
                {
                    "source": "model",
                    "phase": call.phase,
                    "name": name,
                    "args": _bounded_metadata(raw_tool.get("args") or {}),
                }
            )
    return output


def _logical_tool_calls(
    model_tools: Sequence[dict[str, Any]],
    booking_tools: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Expose one logical call, not both a proposal and its fake execution.

    Detailed fake-service invocations remain available on
    ``AdapterScenarioResult.booking_calls`` for debugging.  Turn graders should
    reason about the Agent V3 tool contract, where a proposed ``find_slots``
    followed by its service execution is one call.
    """

    logical = [dict(call) for call in model_tools]
    seen = {str(call.get("name") or "") for call in logical}
    for call in booking_tools:
        name = str(call.get("name") or "").strip()
        if name == "handle_slot_selection":
            continue
        if not name or name in seen:
            continue
        logical.append(dict(call))
        seen.add(name)
    return logical


def _default_slots() -> list[dict[str, Any]]:
    return [
        {
            "index": 1,
            "start_time": "2026-07-16T14:00:00+00:00",
            "end_time": "2026-07-16T14:30:00+00:00",
            "display_time": "Thu Jul 16 at 2:00 PM",
            "display_hint": "Thursday 2:00 PM",
            "search_blob": "thursday 2pm thu 2 pm",
        },
        {
            "index": 2,
            "start_time": "2026-07-17T15:00:00+00:00",
            "end_time": "2026-07-17T15:30:00+00:00",
            "display_time": "Fri Jul 17 at 3:00 PM",
            "display_hint": "Friday 3:00 PM",
            "search_blob": "friday 3pm fri 3 pm",
        },
        {
            "index": 3,
            "start_time": "2026-07-20T16:00:00+00:00",
            "end_time": "2026-07-20T16:30:00+00:00",
            "display_time": "Mon Jul 20 at 4:00 PM",
            "display_hint": "Monday 4:00 PM",
            "search_blob": "monday 4pm mon 4 pm",
        },
    ]


def _normalize_slots(raw_slots: Iterable[Any]) -> list[BookingSlot]:
    slots: list[BookingSlot] = []
    for position, raw_slot in enumerate(raw_slots, start=1):
        item = _plain_mapping(raw_slot, label="booking slot")
        start = str(item.get("start_time") or item.get("start") or "").strip()
        if not start:
            raise ValueError(f"Booking slot {position} requires start_time")
        end = str(item.get("end_time") or item.get("end") or "").strip() or None
        display = str(item.get("display_time") or start).strip()
        metadata = item.get("metadata") if isinstance(item.get("metadata"), Mapping) else {}
        hint = str(item.get("display_hint") or metadata.get("display_hint") or display).strip()
        search = str(
            item.get("search_blob")
            or metadata.get("search_blob")
            or f"{display} {hint} {start}"
        ).strip().lower()
        slots.append(
            BookingSlot(
                index=int(item.get("index") or position),
                start_time=start,
                end_time=end,
                display_time=display,
                display_hint=hint,
                search_blob=search,
            )
        )
    return slots


def _conditions_match(conditions: Mapping[str, Any], arguments: Mapping[str, Any]) -> bool:
    for key, expected in conditions.items():
        actual = arguments.get(key)
        if isinstance(expected, str):
            if expected.strip().lower() not in str(actual or "").strip().lower():
                return False
        elif actual != expected:
            return False
    return True


def _filter_slots(slots: Sequence[BookingSlot], arguments: Mapping[str, Any]) -> list[BookingSlot]:
    filtered = list(slots)
    preferred_day = str(arguments.get("preferred_day") or "").strip().lower()
    avoid_day = str(arguments.get("avoid_day") or "").strip().lower()
    exact_time = str(arguments.get("exact_time") or "").strip().lower()
    period = str(arguments.get("preferred_period") or "").strip().lower()
    if preferred_day:
        day_tokens = _day_tokens(preferred_day)
        filtered = [slot for slot in filtered if any(token in _slot_haystack(slot) for token in day_tokens)]
    if avoid_day:
        day_tokens = _day_tokens(avoid_day)
        filtered = [slot for slot in filtered if not any(token in _slot_haystack(slot) for token in day_tokens)]
    if exact_time:
        time_tokens = _time_tokens(exact_time)
        filtered = [slot for slot in filtered if any(token in _slot_haystack(slot) for token in time_tokens)]
    if period in {"morning", "matin"}:
        filtered = [slot for slot in filtered if (_slot_hour(slot) or 24) < 12]
    elif period in {"afternoon", "apres-midi", "après-midi"}:
        filtered = [slot for slot in filtered if 12 <= (_slot_hour(slot) or -1) < 17]
    elif period in {"evening", "soir", "soirée", "soiree"}:
        filtered = [slot for slot in filtered if (_slot_hour(slot) or -1) >= 17]
    return filtered


def _day_tokens(day: str) -> tuple[str, ...]:
    aliases = {
        "mon": ("mon", "monday", "lundi"),
        "tue": ("tue", "tuesday", "mardi"),
        "wed": ("wed", "wednesday", "mercredi"),
        "thu": ("thu", "thursday", "jeudi"),
        "fri": ("fri", "friday", "vendredi"),
        "sat": ("sat", "saturday", "samedi"),
        "sun": ("sun", "sunday", "dimanche"),
    }
    normalized = day.strip().lower()
    for values in aliases.values():
        if any(normalized.startswith(value) for value in values):
            return values
    return (normalized,)


def _time_tokens(value: str) -> tuple[str, ...]:
    compact = re.sub(r"\s+", "", value.lower())
    digits = re.findall(r"\d{1,2}(?::\d{2})?", compact)
    output = {value.lower(), compact}
    output.update(digits)
    output.update(digit.replace(":00", "") for digit in digits)
    return tuple(token for token in output if token)


def _slot_haystack(slot: BookingSlot) -> str:
    return f"{slot.search_blob} {slot.display_time} {slot.display_hint} {slot.start_time}".lower()


def _slot_hour(slot: BookingSlot) -> int | None:
    try:
        return datetime.fromisoformat(slot.start_time.replace("Z", "+00:00")).hour
    except ValueError:
        return None


def _localized_value(value: Any, language: str) -> str:
    if isinstance(value, Mapping):
        return str(value.get(language) or value.get("en") or "").strip()
    return str(value or "").strip()


def _render_offer_reply(slots: Sequence[BookingSlot], *, language: str, timezone_name: str) -> str:
    labels = [
        _slot_display(asdict(slot), language=language, timezone_name=timezone_name)
        for slot in slots
    ]
    if language == "fr":
        return "Voici les disponibilités :\n" + "\n".join(
            f"{index}) {label}" for index, label in enumerate(labels, start=1)
        ) + "\nRépondez avec le numéro du créneau."
    return "Here are the available times:\n" + "\n".join(
        f"{index}) {label}" for index, label in enumerate(labels, start=1)
    ) + "\nReply with the slot number."


def _render_clarification(slots: Sequence[Mapping[str, Any]], *, language: str) -> str:
    labels = [f"{slot.get('index')}) {slot.get('display_time')}" for slot in slots]
    if language == "fr":
        return "Quel créneau voulez-vous réserver?\n" + "\n".join(labels)
    return "Which time should I book?\n" + "\n".join(labels)


def _slot_display(slot: Mapping[str, Any], *, language: str, timezone_name: str) -> str:
    if language == "fr":
        try:
            parsed = datetime.fromisoformat(str(slot.get("start_time") or "").replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return format_datetime_for_language(parsed, timezone_name=timezone_name or "UTC", language="fr")
        except ValueError:
            pass
    return str(slot.get("display_time") or slot.get("display_hint") or slot.get("start_time") or "").strip()


def _offer_slots(offer: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(offer, Mapping):
        return []
    raw = offer.get("slots")
    if not isinstance(raw, list):
        return []
    return [dict(slot) for slot in raw if isinstance(slot, Mapping)]


def _latest_offer(history: Sequence[Message]) -> dict[str, Any] | None:
    for message in reversed(history):
        payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
        for key in ("active_booking_offer", "booking_offer"):
            offer = payload.get(key)
            if isinstance(offer, dict) and isinstance(offer.get("slots"), list):
                return dict(offer)
    return None


def _match_slot(
    slots: Sequence[Mapping[str, Any]],
    *,
    slot_index: int | None,
    slot_start_time: str | None,
    slot_text: str | None,
) -> dict[str, Any] | None:
    if slot_start_time:
        for slot in slots:
            if str(slot.get("start_time") or "").strip() == str(slot_start_time).strip():
                return dict(slot)
    if slot_index:
        for slot in slots:
            try:
                if int(slot.get("index") or 0) == int(slot_index):
                    return dict(slot)
            except (TypeError, ValueError):
                continue
    text = str(slot_text or "").strip().lower()
    numeric = re.fullmatch(r"(?:option\s*)?(\d+)", text)
    if numeric:
        return _match_slot(
            slots,
            slot_index=int(numeric.group(1)),
            slot_start_time=None,
            slot_text=None,
        )
    if text:
        normalized = re.sub(r"[^a-z0-9à-ÿ]+", " ", text).strip()
        for slot in slots:
            haystack = " ".join(
                str(slot.get(key) or "")
                for key in ("display_time", "display_hint", "search_blob", "start_time")
            ).lower()
            if normalized and normalized in re.sub(r"[^a-z0-9à-ÿ]+", " ", haystack):
                return dict(slot)
    return None


def _conversation_state(value: Any) -> ConversationStateEnum:
    normalized = str(getattr(value, "value", value) or "QUALIFYING").strip().upper()
    try:
        return ConversationStateEnum(normalized)
    except ValueError as exc:
        raise ValueError(f"Unknown conversation state: {normalized}") from exc


def _lead_source(value: Any) -> LeadSource:
    normalized = str(getattr(value, "value", value) or "manual").strip().lower()
    try:
        return LeadSource(normalized)
    except ValueError:
        return LeadSource.MANUAL


def _message_direction(value: Any) -> MessageDirection:
    normalized = str(getattr(value, "value", value) or "INBOUND").strip().upper()
    try:
        return MessageDirection(normalized)
    except ValueError as exc:
        raise ValueError(f"Unknown message direction: {normalized}") from exc


def _parse_datetime(value: Any, *, default: datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif value:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"Invalid ISO datetime: {value}") from exc
    else:
        parsed = default
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _safe_identifier(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", str(value).lower()).strip("-")
    return (normalized or "scenario")[:32]


def _bounded_metadata(value: Any, *, depth: int = 0) -> Any:
    if depth >= 7:
        return "[nested data omitted]"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value if len(value) <= 2_000 else f"{value[:1985]}...[truncated]"
    if isinstance(value, Mapping):
        return {
            str(key)[:160]: _bounded_metadata(item, depth=depth + 1)
            for key, item in list(value.items())[:60]
        }
    if isinstance(value, (list, tuple, set)):
        return [_bounded_metadata(item, depth=depth + 1) for item in list(value)[:60]]
    if is_dataclass(value):
        return _bounded_metadata(asdict(value), depth=depth + 1)
    try:
        return _bounded_metadata(json.loads(json.dumps(value, default=str)), depth=depth + 1)
    except Exception:
        return str(value)[:2_000]


__all__ = [
    "AdapterScenarioResult",
    "AdapterTurnResult",
    "DeterministicBookingService",
    "LiveProviderConfigurationError",
    "ProviderCall",
    "RecordingProvider",
    "ReplayOutputExhausted",
    "ReplayOutputsUnused",
    "ReplayProvider",
    "V3ScenarioAdapter",
    "build_live_agent",
    "build_live_provider",
]
