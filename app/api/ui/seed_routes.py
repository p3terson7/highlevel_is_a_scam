from fastapi import APIRouter
from .shared import *

router = APIRouter()

@router.post("/ui/api/seed-demo")
def ui_seed_demo(
    reset: bool = Query(default=False),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> dict[str, Any]:
    _require_admin(settings, admin_token)
    if not can_seed_demo(settings):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Demo seed is disabled")
    result = seed_demo_data(db, reset=reset)
    db.commit()
    return result


@router.delete("/ui/api/seed-demo")
def ui_reset_demo(
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> dict[str, Any]:
    _require_admin(settings, admin_token)
    if not can_seed_demo(settings):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Demo seed is disabled")
    result = reset_demo_data(db)
    db.commit()
    return {**result, "status": "ok"}


@router.post("/ui/api/seed-showcase/{client_key}")
def ui_seed_showcase_client(
    client_key: str,
    reset: bool = Query(default=False),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> dict[str, Any]:
    _require_admin(settings, admin_token)
    if not can_seed_demo(settings):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Demo seed is disabled")
    try:
        result = seed_showcase_client_data(db, client_key=client_key, reset=reset)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    db.commit()
    return result
