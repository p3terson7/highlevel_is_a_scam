from __future__ import annotations

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
from app.services.runtime_config import get_effective_runtime_value, load_runtime_overrides
from app.workers.tasks import enqueue_process_webhook

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


def _load_client(db: Session, client_key: str) -> Client:
    client = db.scalar(select(Client).where(Client.client_key == client_key, Client.is_active.is_(True)))
    if client is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    return client


@router.get("/meta/{client_key}", response_class=PlainTextResponse)
def verify_meta_webhook(
    client_key: str,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    hub_mode: str | None = Query(default=None, alias="hub.mode"),
    hub_verify_token: str | None = Query(default=None, alias="hub.verify_token"),
    hub_challenge: str | None = Query(default=None, alias="hub.challenge"),
) -> str:
    _load_client(db, client_key)
    runtime_overrides = load_runtime_overrides(db)
    challenge = verify_meta_challenge(
        mode=hub_mode,
        verify_token=hub_verify_token,
        challenge=hub_challenge,
        expected_verify_token=get_effective_runtime_value(
            settings=settings,
            overrides=runtime_overrides,
            key="meta_verify_token",
        ),
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
    runtime_overrides = load_runtime_overrides(db)
    meta_verify_token = get_effective_runtime_value(
        settings=settings,
        overrides=runtime_overrides,
        key="meta_verify_token",
    )
    if not verify_meta_signature(request, meta_verify_token):
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


@router.post("/linkedin/{client_key}", status_code=status.HTTP_202_ACCEPTED)
async def linkedin_webhook(
    client_key: str,
    request: Request,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
) -> dict[str, Any]:
    client = _load_client(db, client_key)
    runtime_overrides = load_runtime_overrides(db)
    linkedin_verify_token = get_effective_runtime_value(
        settings=settings,
        overrides=runtime_overrides,
        key="linkedin_verify_token",
    )
    if not verify_linkedin_signature(request, linkedin_verify_token):
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
