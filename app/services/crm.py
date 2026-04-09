from __future__ import annotations

import re
from datetime import date

CRM_STAGE_NEW_LEAD = "New Lead"
CRM_STAGE_CONTACTED = "Contacted"
CRM_STAGE_QUALIFIED = "Qualified"
CRM_STAGE_MEETING_BOOKED = "Meeting Booked"
CRM_STAGE_MEETING_COMPLETED = "Meeting Completed"
CRM_STAGE_WON = "Won"
CRM_STAGE_LOST = "Lost"

CRM_STAGES = [
    CRM_STAGE_NEW_LEAD,
    CRM_STAGE_CONTACTED,
    CRM_STAGE_QUALIFIED,
    CRM_STAGE_MEETING_BOOKED,
    CRM_STAGE_MEETING_COMPLETED,
    CRM_STAGE_WON,
    CRM_STAGE_LOST,
]
CRM_STAGE_SET = set(CRM_STAGES)
CRM_STAGE_INDEX = {stage: idx for idx, stage in enumerate(CRM_STAGES)}

TASK_STATUS_OPEN = "open"
TASK_STATUS_DONE = "done"
TASK_STATUS_SET = {TASK_STATUS_OPEN, TASK_STATUS_DONE}

_CRM_STAGE_ALIASES = {
    "new": CRM_STAGE_NEW_LEAD,
    "new lead": CRM_STAGE_NEW_LEAD,
    "new_lead": CRM_STAGE_NEW_LEAD,
    "contacted": CRM_STAGE_CONTACTED,
    "qualified": CRM_STAGE_QUALIFIED,
    "meeting booked": CRM_STAGE_MEETING_BOOKED,
    "meeting_booked": CRM_STAGE_MEETING_BOOKED,
    "booked": CRM_STAGE_MEETING_BOOKED,
    "meeting completed": CRM_STAGE_MEETING_COMPLETED,
    "meeting_completed": CRM_STAGE_MEETING_COMPLETED,
    "won": CRM_STAGE_WON,
    "lost": CRM_STAGE_LOST,
}

_SHORT_ACKS = {"ok", "okay", "k", "yes", "y", "thanks", "thank", "great", "cool", "sure", "1", "2", "3"}


def normalize_crm_stage(raw: str | None) -> str:
    if raw is None:
        return CRM_STAGE_NEW_LEAD
    text = str(raw).strip()
    if not text:
        return CRM_STAGE_NEW_LEAD
    if text in CRM_STAGE_SET:
        return text
    key = re.sub(r"\s+", " ", text.replace("-", " ").replace("_", " ").strip().lower())
    return _CRM_STAGE_ALIASES.get(key, CRM_STAGE_NEW_LEAD)


def crm_stage_rank(stage: str | None) -> int:
    normalized = normalize_crm_stage(stage)
    return CRM_STAGE_INDEX.get(normalized, 0)


def progress_crm_stage(current: str | None, target: str | None) -> str:
    current_norm = normalize_crm_stage(current)
    target_norm = normalize_crm_stage(target)
    return target_norm if crm_stage_rank(target_norm) > crm_stage_rank(current_norm) else current_norm


def normalize_task_status(raw: str | None) -> str:
    if not raw:
        return TASK_STATUS_OPEN
    text = str(raw).strip().lower()
    return text if text in TASK_STATUS_SET else TASK_STATUS_OPEN


def normalize_tag(raw: str | None) -> str:
    if raw is None:
        return ""
    text = re.sub(r"\s+", " ", str(raw).strip().lower())
    return text[:64]


def is_meaningful_inbound(text: str) -> bool:
    tokens = re.findall(r"[a-z0-9]+", (text or "").lower())
    if not tokens:
        return False
    if len(tokens) == 1 and tokens[0] in _SHORT_ACKS:
        return False
    return len("".join(tokens)) >= 5


def parse_due_date(raw: str | None) -> date | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    return date.fromisoformat(text)
