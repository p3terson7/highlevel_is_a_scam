from __future__ import annotations

from datetime import datetime
from typing import Any

from app.db.models import ConversationStateEnum, Lead

AGENT_CONTROL_KEY = "agent_control"


def _raw_payload(lead: Lead) -> dict[str, Any]:
    return dict(lead.raw_payload or {}) if isinstance(lead.raw_payload, dict) else {}


def get_agent_control(lead: Lead) -> dict[str, Any]:
    raw = _raw_payload(lead)
    control = raw.get(AGENT_CONTROL_KEY)
    if not isinstance(control, dict):
        control = {}
    paused = bool(control.get("paused"))
    mode = str(control.get("mode") or ("paused" if paused else "active")).strip().lower()
    if lead.conversation_state == ConversationStateEnum.HANDOFF:
        mode = "handoff"
        paused = True
    elif lead.opted_out or lead.conversation_state == ConversationStateEnum.OPTED_OUT:
        mode = "opted_out"
        paused = True
    return {
        "paused": paused,
        "mode": mode,
        "reason": str(control.get("reason") or "").strip(),
        "note": str(control.get("note") or "").strip(),
        "actor_role": str(control.get("actor_role") or "").strip(),
        "updated_at": str(control.get("updated_at") or "").strip(),
    }


def set_agent_control(
    lead: Lead,
    *,
    paused: bool,
    actor_role: str,
    now: datetime,
    reason: str = "",
    note: str = "",
) -> dict[str, Any]:
    raw = _raw_payload(lead)
    control = {
        "paused": bool(paused),
        "mode": "paused" if paused else "active",
        "reason": reason.strip() or ("operator_paused" if paused else "operator_resumed"),
        "note": note.strip(),
        "actor_role": actor_role,
        "updated_at": now.isoformat(),
    }
    raw[AGENT_CONTROL_KEY] = control
    lead.raw_payload = raw
    lead.updated_at = now
    return get_agent_control(lead)


def should_suppress_ai_reply(lead: Lead) -> tuple[bool, str]:
    control = get_agent_control(lead)
    if control["mode"] == "handoff":
        return True, "human_handoff"
    if control["mode"] == "opted_out":
        return True, "opted_out"
    if control["paused"]:
        return True, control["reason"] or "operator_paused"
    return False, ""
