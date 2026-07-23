from fastapi import APIRouter, Request, Response
from .shared import *
from app.services.login_rate_limit import (
    admit_portal_login_attempt,
    clear_portal_login_failures,
)
from app.services.portal_auth import hash_portal_password, portal_auth_version
from app.services.ui_session_auth import (
    UI_CSRF_COOKIE,
    UI_SESSION_COOKIE,
    issue_ui_session_token,
    new_csrf_token,
    ui_session_cookies_secure,
)

router = APIRouter()
_DUMMY_PORTAL_PASSWORD_HASH = hash_portal_password("dummy-portal-password-not-used")
_ADMIN_SESSION_SECONDS = 8 * 60 * 60
_CLIENT_SESSION_SECONDS = 7 * 24 * 60 * 60


def _set_ui_session_cookies(
    *,
    response: Response,
    settings: Settings,
    session_token: str,
    csrf_token: str,
    max_age: int,
) -> None:
    secure = ui_session_cookies_secure(settings)
    response.set_cookie(
        UI_SESSION_COOKIE,
        session_token,
        max_age=max_age,
        httponly=True,
        secure=secure,
        samesite="strict",
        path="/",
    )
    response.set_cookie(
        UI_CSRF_COOKIE,
        csrf_token,
        max_age=max_age,
        httponly=False,
        secure=secure,
        samesite="strict",
        path="/",
    )


@router.post("/ui/api/login/admin")
def ui_admin_login(
    payload: AdminLoginRequest,
    request: Request,
    response: Response,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
) -> dict[str, Any]:
    remote_ip = request.client.host if request.client is not None else "unknown"
    admission = admit_portal_login_attempt(
        settings=settings,
        email="__leadops_admin__",
        remote_ip=remote_ip,
    )
    if admission is None:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many login attempts. Try again later.",
            headers={"Retry-After": "900"},
        )
    if not verify_admin_token(payload.admin_token, settings.admin_token):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid admin token")
    clear_portal_login_failures(
        settings=settings,
        email="__leadops_admin__",
        remote_ip=remote_ip,
        admission=admission,
    )
    csrf_token = new_csrf_token()
    session_token = issue_ui_session_token(
        settings=settings,
        role="admin",
        csrf_token=csrf_token,
    )
    _set_ui_session_cookies(
        response=response,
        settings=settings,
        session_token=session_token,
        csrf_token=csrf_token,
        max_age=_ADMIN_SESSION_SECONDS,
    )
    return {
        "status": "ok",
        "session": _session_payload(actor=UIActor(role="admin"), settings=settings, db=db),
    }


@router.get("/ui/api/session")
def ui_session(
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    portal_token: str | None = Header(default=None, alias="X-Portal-Token"),
) -> dict[str, Any]:
    actor = _resolve_ui_actor(
        db=db,
        settings=settings,
        admin_token=admin_token,
        portal_token=portal_token,
    )
    return _session_payload(actor=actor, settings=settings, db=db)


def _authenticate_portal_client(
    *,
    payload: ClientPortalLoginRequest,
    request: Request,
    db: Session,
    settings: Settings,
) -> Client:
    email = payload.email.strip().lower()
    password = payload.password
    if not email or not password:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email and password are required")
    remote_ip = request.client.host if request.client is not None else "unknown"
    admission = admit_portal_login_attempt(
        settings=settings,
        email=email,
        remote_ip=remote_ip,
    )
    if admission is None:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many login attempts. Try again later.",
            headers={"Retry-After": "900"},
        )

    matching_clients = db.scalars(
        select(Client)
        .where(
            func.lower(func.trim(Client.portal_email)) == email,
            Client.portal_enabled.is_(True),
            Client.is_active.is_(True),
        )
        .limit(2)
    ).all()
    # Refuse ambiguous identity rather than authenticating an arbitrary tenant
    # if a deployment has not applied the portal-email uniqueness migration yet.
    client = matching_clients[0] if len(matching_clients) == 1 else None
    password_hash = (
        client.portal_password_hash
        if client is not None and client.portal_password_hash
        else _DUMMY_PORTAL_PASSWORD_HASH
    )
    password_valid = verify_portal_password(password, password_hash)
    if client is None or not client.portal_password_hash or not password_valid:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password")

    clear_portal_login_failures(
        settings=settings,
        email=email,
        remote_ip=remote_ip,
        admission=admission,
    )
    return client


@router.post("/ui/api/login/client")
def ui_client_login(
    payload: ClientPortalLoginRequest,
    request: Request,
    response: Response,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
) -> dict[str, Any]:
    client = _authenticate_portal_client(
        payload=payload,
        request=request,
        db=db,
        settings=settings,
    )

    csrf_token = new_csrf_token()
    session_token = issue_ui_session_token(
        settings=settings,
        role="client",
        csrf_token=csrf_token,
        client_id=client.id,
        client_key=client.client_key,
        email=client.portal_email,
        auth_version=portal_auth_version(client.portal_password_hash),
    )
    _set_ui_session_cookies(
        response=response,
        settings=settings,
        session_token=session_token,
        csrf_token=csrf_token,
        max_age=_CLIENT_SESSION_SECONDS,
    )
    actor = UIActor(role="client", client=client)
    return {
        "status": "ok",
        "session": _session_payload(actor=actor, settings=settings, db=db),
    }


@router.post("/ui/api/login/client/token")
def ui_client_legacy_token_login(
    payload: ClientPortalLoginRequest,
    request: Request,
    response: Response,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
) -> dict[str, Any]:
    if not settings.enable_legacy_portal_token_login:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    client = _authenticate_portal_client(
        payload=payload,
        request=request,
        db=db,
        settings=settings,
    )
    token = issue_portal_token(
        settings=settings,
        client_id=client.id,
        client_key=client.client_key,
        email=client.portal_email,
        auth_version=portal_auth_version(client.portal_password_hash),
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    actor = UIActor(role="client", client=client)
    return {
        "status": "ok",
        "token": token,
        "session": _session_payload(actor=actor, settings=settings, db=db),
    }


@router.post("/ui/api/logout")
def ui_logout(
    response: Response,
    settings: Settings = Depends(get_app_settings),
) -> dict[str, str]:
    secure = ui_session_cookies_secure(settings)
    response.delete_cookie(
        UI_SESSION_COOKIE,
        path="/",
        httponly=True,
        secure=secure,
        samesite="strict",
    )
    response.delete_cookie(
        UI_CSRF_COOKIE,
        path="/",
        httponly=False,
        secure=secure,
        samesite="strict",
    )
    return {"status": "ok"}
