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


def _verify_optional_webhook_secret(request: Request, effective_runtime: dict[str, Any]) -> None:
    webhook_secret = str(effective_runtime.get("zapier_webhook_secret") or "").strip()
    if not webhook_secret:
        return
    provided_secret = (
        request.headers.get("X-CRM-Webhook-Secret")
        or request.headers.get("X-Zapier-Webhook-Secret")
        or request.headers.get("X-Zapier-Token")
        or request.query_params.get("webhook_secret")
        or request.query_params.get("zapier_secret")
        or ""
    ).strip()
    if provided_secret != webhook_secret:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid webhook secret")


def _question_answer_rows(answers: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"question": key, "answer": value} for key, value in answers.items()]


def _merge_dicts(*values: Any) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for value in values:
        if isinstance(value, dict):
            merged.update({str(key): item for key, item in value.items()})
    return merged


def _tracking_from_payload(payload: dict[str, Any], answers: dict[str, Any]) -> dict[str, Any]:
    tracking = _merge_dicts(payload.get("tracking"), payload.get("utm"), payload.get("utms"))
    for key, value in payload.items():
        if str(key).startswith("utm_") or key in {"gclid", "fbclid", "li_fat_id", "msclkid", "ad_id"}:
            tracking[str(key)] = value
    for key, value in answers.items():
        if str(key).startswith("utm_") or key in {"gclid", "fbclid", "li_fat_id", "msclkid", "ad_id"}:
            tracking.setdefault(str(key), value)
    return normalize_form_answers(tracking)


def _source_from_tracking(payload: dict[str, Any], tracking: dict[str, Any]) -> str:
    raw_source = str(
        payload.get("source")
        or payload.get("lead_source")
        or tracking.get("utm_source")
        or tracking.get("source")
        or ""
    ).strip().lower()
    if "linkedin" in raw_source or raw_source in {"li", "linkedin_ads", "linkedin ads"}:
        return "linkedin"
    if raw_source in {"meta", "facebook", "fb", "instagram", "ig"} or "facebook" in raw_source or "instagram" in raw_source:
        return "meta"
    return "manual"


def _coerce_website_form_payload(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    lead_payload = payload.get("lead") if isinstance(payload.get("lead"), dict) else {}
    form_answers = _merge_dicts(
        payload.get("form_answers"),
        payload.get("fields"),
        payload.get("answers"),
        lead_payload.get("form_answers"),
    )
    for key in (
        "full_name",
        "name",
        "first_name",
        "last_name",
        "phone",
        "phone_number",
        "mobile_phone",
        "email",
        "email_address",
        "city",
        "location",
        "location_city",
        "company",
        "message",
    ):
        value = lead_payload.get(key, payload.get(key))
        if value not in (None, ""):
            form_answers.setdefault(key, value)

    form_answers = normalize_form_answers(form_answers)
    tracking = _tracking_from_payload(payload, form_answers)
    form_answers.update(tracking)

    external_lead_id = str(
        lead_payload.get("id")
        or lead_payload.get("external_lead_id")
        or payload.get("id")
        or payload.get("lead_id")
        or payload.get("external_lead_id")
        or ""
    ).strip()

    source = _source_from_tracking(payload, tracking)
    normalized = {
        "lead": {
            "id": external_lead_id or None,
            "form_answers": form_answers,
            "submitted_form_answers": _question_answer_rows(form_answers),
            "tracking": tracking,
            "source_page_url": payload.get("source_page_url") or payload.get("page_url") or payload.get("url") or "",
            "referrer": payload.get("referrer") or payload.get("referrer_url") or "",
            "raw_website_payload": payload,
        }
    }
    return source, normalized


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
    original_answers: list[dict[str, Any]] = []

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
            original_answers.append({"question": key, "answer": value})
            normalized_key = re.sub(r"[^a-z0-9]+", "_", key.lower()).strip("_")
            if normalized_key:
                form_answers.setdefault(normalized_key, value)

    if isinstance(payload.get("form_answers"), dict):
        form_answers.update(payload["form_answers"])
        original_answers.extend(
            {"question": key, "answer": value} for key, value in payload["form_answers"].items()
        )
    if isinstance(payload.get("fields"), dict):
        form_answers.update(payload["fields"])
        original_answers.extend({"question": key, "answer": value} for key, value in payload["fields"].items())

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
            original_answers.append({"question": key, "answer": value})

    if not form_answers:
        for key, value in payload.items():
            if key in {"leadgen_id", "lead_id", "id", "external_lead_id"}:
                continue
            if isinstance(value, (str, int, float, bool)):
                form_answers[key] = value
                original_answers.append({"question": key, "answer": value})
    form_answers = normalize_form_answers(form_answers)

    lead_payload: dict[str, Any] = {
        "form_answers": form_answers,
        "submitted_form_answers": original_answers,
    }
    if leadgen_id:
        lead_payload["id"] = leadgen_id
    return {"lead": lead_payload}


@router.post("/form/{client_key}", status_code=status.HTTP_202_ACCEPTED)
async def website_form_webhook(
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
    _verify_optional_webhook_secret(request, effective_runtime)
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Payload must be a JSON object")

    source, normalized_payload = _coerce_website_form_payload(payload)
    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=None,
            event_type="website_form_webhook_received",
            decision={
                "status": "accepted",
                "queued_source": source,
                "payload": payload,
                "normalized_payload": normalized_payload,
            },
        )
    )
    db.commit()

    enqueue_process_webhook(client_id=client.id, source=source, payload=normalized_payload)
    incr("leads_received_total")
    return {
        "status": "accepted",
        "source": source,
        "client_key": client_key,
    }


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
    settings: Settings = Depends(get_app_settings),
) -> dict[str, Any]:
    client = _load_client(db, client_key)
    effective_runtime = get_effective_runtime_map_for_client(
        settings=settings,
        overrides=load_runtime_overrides(db),
        client=client,
    )
    zapier_secret = str(effective_runtime.get("zapier_webhook_secret") or "").strip()
    if zapier_secret:
        provided_secret = (
            request.headers.get("X-Zapier-Webhook-Secret")
            or request.headers.get("X-Zapier-Token")
            or request.query_params.get("zapier_secret")
            or ""
        ).strip()
        if provided_secret != zapier_secret:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Zapier secret")
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
