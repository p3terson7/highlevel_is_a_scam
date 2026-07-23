import json
import mimetypes
import os
from pathlib import Path

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import HTMLResponse, Response

from app.core.config import get_settings

router = APIRouter()

_REPO_DIR = Path(__file__).resolve().parents[3]
_UI_DIR = Path(__file__).resolve().parents[2] / "templates"
_UI_FILE = _UI_DIR / "ui.html"
_UI_ASSET_DIR = _UI_DIR / "ui_assets"
_REACT_ROOT_MARKERS = {
    "dashboard": "<!-- react-dashboard-root -->",
    "clients": "<!-- react-clients-root -->",
    "inbox": "<!-- react-inbox-root -->",
    "pipeline": "<!-- react-pipeline-root -->",
    "records": "<!-- react-records-root -->",
    "calendar": "<!-- react-calendar-root -->",
    "tasks": "<!-- react-tasks-root -->",
    "logs": "<!-- react-logs-root -->",
    "settings": "<!-- react-settings-root -->",
    "test-lab": "<!-- react-test-lab-root -->",
}
_REACT_INJECTION_MARKER = "<!-- react-island-assets -->"
_FRONTEND_DIST_DIR = _REPO_DIR / "frontend" / "dist"
_FRONTEND_MANIFEST_FILE = _FRONTEND_DIST_DIR / ".vite" / "manifest.json"
_FRONTEND_ENTRY_CANDIDATES = ("index.html", "src/main.tsx")
_REACT_ISLAND_FEATURE_FLAG = "UI_REACT_ISLAND_ENABLED"
_REACT_APP_SHELL_FEATURE_FLAG = "UI_REACT_APP_SHELL_ENABLED"
_LEGACY_SHELL_FEATURE_FLAG = "UI_LEGACY_SHELL_ENABLED"
_UI_ASSET_TYPES = {
    "ui.css": "text/css; charset=utf-8",
    "ui-i18n.js": "application/javascript; charset=utf-8",
    "ui-core.js": "application/javascript; charset=utf-8",
    "ui-navigation.js": "application/javascript; charset=utf-8",
    "ui-dashboard.js": "application/javascript; charset=utf-8",
    "ui-views.js": "application/javascript; charset=utf-8",
    "ui-actions.js": "application/javascript; charset=utf-8",
    "ui-bootstrap.js": "application/javascript; charset=utf-8",
    "landscape.jpg": "image/jpeg",
}


def _env_flag_enabled(name: str, *, default: bool = False) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        setting_name = name.strip().lower()
        return bool(getattr(get_settings(), setting_name, default))
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def _legacy_shell_enabled() -> bool:
    return _env_flag_enabled(_LEGACY_SHELL_FEATURE_FLAG)


def _react_app_shell_enabled() -> bool:
    # React is the production UI. The old shell remains available only as an
    # explicit rollback path while the migration is being verified.
    return not _legacy_shell_enabled() and _env_flag_enabled(
        _REACT_APP_SHELL_FEATURE_FLAG,
        default=True,
    )


def _react_island_enabled() -> bool:
    return (
        not _legacy_shell_enabled()
        and not _react_app_shell_enabled()
        and _env_flag_enabled(_REACT_ISLAND_FEATURE_FLAG)
    )


def _react_assets_enabled() -> bool:
    return _react_island_enabled() or _react_app_shell_enabled()


def _load_frontend_manifest() -> dict[str, object] | None:
    if not _FRONTEND_MANIFEST_FILE.exists():
        return None
    try:
        manifest = json.loads(_FRONTEND_MANIFEST_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return manifest if isinstance(manifest, dict) else None


def _frontend_entry() -> dict[str, object] | None:
    if not _react_assets_enabled():
        return None
    manifest = _load_frontend_manifest()
    if not manifest:
        return None
    for entry_name in _FRONTEND_ENTRY_CANDIDATES:
        entry = manifest.get(entry_name)
        if isinstance(entry, dict) and isinstance(entry.get("file"), str):
            return entry
    return None


def _asset_url(asset_path: str) -> str:
    return f"/ui/react-assets/{asset_path.lstrip('/')}"


def _react_root_html(entry: dict[str, object] | None, island: str) -> str:
    if not entry:
        return ""
    return f'<div class="react-island-root react-{island}-root" data-react-island="{island}"></div>'


def _react_asset_tags(entry: dict[str, object] | None) -> str:
    if not entry:
        return ""
    tags: list[str] = []
    for css_asset in entry.get("css", []):
        if isinstance(css_asset, str):
            tags.append(f'<link rel="stylesheet" href="{_asset_url(css_asset)}" />')
    tags.append(f'<script type="module" src="{_asset_url(entry["file"])}"></script>')
    return "\n    ".join(tags)


def _react_app_shell_html(entry: dict[str, object]) -> str:
    react_tags = _react_asset_tags(entry)
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta http-equiv="Cache-Control" content="no-store, max-age=0" />
    <meta http-equiv="Pragma" content="no-cache" />
    <title>Lead Ops Console</title>
    <link rel="stylesheet" href="/ui/assets/ui.css" />
    {react_tags}
  </head>
  <body data-theme="dark">
    <div class="app-background" aria-hidden="true"></div>
    <div id="react-root" data-react-app-shell="true"></div>
  </body>
</html>"""


def ui_shell_response() -> HTMLResponse:
    entry = _frontend_entry()
    if _react_app_shell_enabled():
        if entry:
            return HTMLResponse(_react_app_shell_html(entry))
        return HTMLResponse(
            """<!doctype html>
<html lang="en"><head><meta charset="utf-8" /><meta name="viewport" content="width=device-width" />
<title>React frontend unavailable</title></head><body>
<main><h1>React frontend build unavailable</h1>
<p>The server is configured for React, but its build manifest or entry asset is missing.</p></main>
</body></html>""",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            headers={"Cache-Control": "no-store, max-age=0", "Retry-After": "30"},
        )

    shell_html = _UI_FILE.read_text(encoding="utf-8")
    for island, marker in _REACT_ROOT_MARKERS.items():
        shell_html = shell_html.replace(marker, _react_root_html(entry, island))
    shell_html = shell_html.replace(_REACT_INJECTION_MARKER, _react_asset_tags(entry))
    return HTMLResponse(shell_html)


@router.get("/ui", response_class=HTMLResponse)
def ui_index() -> HTMLResponse:
    return ui_shell_response()


@router.get("/ui/", response_class=HTMLResponse)
def ui_index_slash() -> HTMLResponse:
    return ui_shell_response()


@router.get("/ui/assets/{asset_name}")
def ui_asset(asset_name: str) -> Response:
    media_type = _UI_ASSET_TYPES.get(asset_name)
    if media_type is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="UI asset not found")
    asset_path = _UI_ASSET_DIR / asset_name
    if media_type.startswith("image/"):
        response = Response(asset_path.read_bytes(), media_type=media_type)
    else:
        response = Response(asset_path.read_text(encoding="utf-8"), media_type=media_type)
    response.headers["Cache-Control"] = "no-store, max-age=0"
    return response


@router.get("/ui/react-assets/{asset_path:path}")
def ui_react_asset(asset_path: str) -> Response:
    resolved_asset = (_FRONTEND_DIST_DIR / asset_path).resolve()
    if not resolved_asset.is_relative_to(_FRONTEND_DIST_DIR.resolve()) or not resolved_asset.is_file():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="React asset not found")

    media_type = mimetypes.guess_type(resolved_asset.name)[0] or "application/octet-stream"
    response = Response(resolved_asset.read_bytes(), media_type=media_type)
    if asset_path.startswith("assets/"):
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    else:
        response.headers["Cache-Control"] = "no-store, max-age=0"
    return response
