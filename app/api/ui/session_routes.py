from fastapi import APIRouter
from .shared import *

router = APIRouter()

@router.get("/ui/api/session")
def ui_session(
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(db=db, settings=settings, admin_token=admin_token, portal_token=portal_token)
    return _session_payload(actor=actor, settings=settings, db=db)


@router.post("/ui/api/login/client")
def ui_client_login(
    payload: ClientPortalLoginRequest,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
) -> dict[str, Any]:
    email = payload.email.strip().lower()
    password = payload.password
    if not email or not password:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email and password are required")

    client = db.scalar(
        select(Client)
        .where(
            Client.portal_email == email,
            Client.portal_enabled.is_(True),
            Client.is_active.is_(True),
        )
        .limit(1)
    )
    if client is None or not client.portal_password_hash or not verify_portal_password(password, client.portal_password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password")

    token = issue_portal_token(
        settings=settings,
        client_id=client.id,
        client_key=client.client_key,
        email=client.portal_email,
    )
    actor = UIActor(role="client", client=client)
    return {
        "status": "ok",
        "token": token,
        "session": _session_payload(actor=actor, settings=settings, db=db),
    }

