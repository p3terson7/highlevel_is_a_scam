from pathlib import Path

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import HTMLResponse, Response

router = APIRouter()

_UI_DIR = Path(__file__).resolve().parents[2] / "templates"
_UI_FILE = _UI_DIR / "ui.html"
_UI_ASSET_DIR = _UI_DIR / "ui_assets"
_UI_ASSET_TYPES = {
    "ui.css": "text/css; charset=utf-8",
    "ui-i18n.js": "application/javascript; charset=utf-8",
    "ui-core.js": "application/javascript; charset=utf-8",
    "ui-navigation.js": "application/javascript; charset=utf-8",
    "ui-dashboard.js": "application/javascript; charset=utf-8",
    "ui-views.js": "application/javascript; charset=utf-8",
    "ui-actions.js": "application/javascript; charset=utf-8",
    "ui-bootstrap.js": "application/javascript; charset=utf-8",
}


@router.get("/ui", response_class=HTMLResponse)
def ui_index() -> HTMLResponse:
    return HTMLResponse(_UI_FILE.read_text(encoding="utf-8"))


@router.get("/ui/", response_class=HTMLResponse)
def ui_index_slash() -> HTMLResponse:
    return HTMLResponse(_UI_FILE.read_text(encoding="utf-8"))


@router.get("/ui/assets/{asset_name}")
def ui_asset(asset_name: str) -> Response:
    media_type = _UI_ASSET_TYPES.get(asset_name)
    if media_type is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="UI asset not found")
    response = Response((_UI_ASSET_DIR / asset_name).read_text(encoding="utf-8"), media_type=media_type)
    response.headers["Cache-Control"] = "no-store, max-age=0"
    return response
