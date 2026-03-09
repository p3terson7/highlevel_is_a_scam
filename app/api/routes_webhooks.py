from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import PlainTextResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.core.deps import get_app_settings
from app.core.metrics import incr
from app.core.security import (
    verify_linkedin_signature,
    verify_meta_challenge,
    verify_meta_signature,
)
from app.db.models import AuditLog, Client
from app.db.session import get_db
from app.services.lead_summary import normalize_form_answers
from app.services.runtime_config import get_effective_runtime_map_for_client, load_runtime_overrides
from app.workers.tasks import enqueue_process_webhook

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


def _load_client(db: Session, client_key: str) -> Client:
    client = db.scalar(select(Client).where(Client.client_key == client_key, Client.is_active.is_(True)))
    if client is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    return client


def _coerce_zapier_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if any(key in payload for key in ("entry", "lead", "leads")):
        return payload

    leadgen_id = str(
        payload.get("leadgen_id")
        or payload.get("lead_id")
        or payload.get("id")
        or payload.get("external_lead_id")
        or ""
    ).strip()

    if isinstance(payload.get("field_data"), list):
        return {
            "entry": [
                {
                    "changes": [
                        {
                            "value": {
                                "leadgen_id": leadgen_id,
                                "field_data": payload["field_data"],
                            }
                        }
                    ]
                }
            ]
        }

    form_answers: dict[str, Any] = {}

    # Some Zapier setups send a single text blob under an empty key:
    # {"": "Full Name : \"...\", Email : \"...\", ..."}
    blob = payload.get("")
    if isinstance(blob, str):
        pairs = re.findall(r'([^:,][^:]*?)\s*:\s*"([^"]*)"', blob)
        for raw_key, raw_value in pairs:
            key = raw_key.strip()
            value = raw_value.strip()
            if not key or value == "":
                continue
            form_answers.setdefault(key, value)
            normalized_key = re.sub(r"[^a-z0-9]+", "_", key.lower()).strip("_")
            if normalized_key:
                form_answers.setdefault(normalized_key, value)

    if isinstance(payload.get("form_answers"), dict):
        form_answers.update(payload["form_answers"])
    if isinstance(payload.get("fields"), dict):
        form_answers.update(payload["fields"])

    for key in (
        "full_name",
        "name",
        "first_name",
        "phone_number",
        "phone",
        "mobile_phone",
        "email",
        "email_address",
        "city",
        "location_city",
    ):
        value = payload.get(key)
        if value not in (None, ""):
            form_answers.setdefault(key, value)

    if not form_answers:
        for key, value in payload.items():
            if key in {"leadgen_id", "lead_id", "id", "external_lead_id"}:
                continue
            if isinstance(value, (str, int, float, bool)):
                form_answers[key] = value
    form_answers = normalize_form_answers(form_answers)

    lead_payload: dict[str, Any] = {"form_answers": form_answers}
    if leadgen_id:
        lead_payload["id"] = leadgen_id
    return {"lead": lead_payload}


@router.get("/meta/{client_key}", response_class=PlainTextResponse)
def verify_meta_webhook(
    client_key: str,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    hub_mode: str | None = Query(default=None, alias="hub.mode"),
    hub_verify_token: str | None = Query(default=None, alias="hub.verify_token"),
    hub_challenge: str | None = Query(default=None, alias="hub.challenge"),
) -> str:
    client = _load_client(db, client_key)
    effective_runtime = get_effective_runtime_map_for_client(
        settings=settings,
        overrides=load_runtime_overrides(db),
        client=client,
    )
    challenge = verify_meta_challenge(
        mode=hub_mode,
        verify_token=hub_verify_token,
        challenge=hub_challenge,
        expected_verify_token=effective_runtime["meta_verify_token"],
    )
    if challenge is None:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Verification failed")
    return challenge


@router.post("/meta/{client_key}", status_code=status.HTTP_202_ACCEPTED)
async def meta_webhook(
    client_key: str,
    request: Request,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
) -> dict[str, Any]:
    client = _load_client(db, client_key)
    effective_runtime = get_effective_runtime_map_for_client(
        settings=settings,
        overrides=load_runtime_overrides(db),
        client=client,
    )
    if not verify_meta_signature(request, effective_runtime["meta_verify_token"]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid signature")

    payload = await request.json()
    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=None,
            event_type="meta_webhook_received",
            decision={"payload": payload},
        )
    )
    db.commit()

    enqueue_process_webhook(client_id=client.id, source="meta", payload=payload)
    incr("leads_received_total")
    return {"status": "accepted"}


@router.post("/zapier/{client_key}", status_code=status.HTTP_202_ACCEPTED)
async def zapier_webhook(
    client_key: str,
    request: Request,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    client = _load_client(db, client_key)
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Payload must be a JSON object")

    normalized_payload = _coerce_zapier_payload(payload)
    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=None,
            event_type="zapier_webhook_received",
            decision={
                "status": "accepted",
                "queued_source": "meta",
                "payload": payload,
                "normalized_payload": normalized_payload,
            },
        )
    )
    db.commit()

    enqueue_process_webhook(client_id=client.id, source="meta", payload=normalized_payload)
    incr("leads_received_total")
    return {
        "status": "accepted",
        "source": "zapier",
        "queued_source": "meta",
        "client_key": client_key,
    }


@router.post("/linkedin/{client_key}", status_code=status.HTTP_202_ACCEPTED)
async def linkedin_webhook(
    client_key: str,
    request: Request,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
) -> dict[str, Any]:
    client = _load_client(db, client_key)
    effective_runtime = get_effective_runtime_map_for_client(
        settings=settings,
        overrides=load_runtime_overrides(db),
        client=client,
    )
    if not verify_linkedin_signature(request, effective_runtime["linkedin_verify_token"]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid signature")

    payload = await request.json()
    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=None,
            event_type="linkedin_webhook_received",
            decision={"payload": payload},
        )
    )
    db.commit()

    enqueue_process_webhook(client_id=client.id, source="linkedin", payload=payload)
    incr("leads_received_total")
    return {"status": "accepted"}
