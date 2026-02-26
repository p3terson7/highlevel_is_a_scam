from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["ui"])

_UI_FILE = Path(__file__).resolve().parents[1] / "templates" / "ui.html"


@router.get("/ui", response_class=HTMLResponse)
def ui_index() -> HTMLResponse:
    return HTMLResponse(_UI_FILE.read_text(encoding="utf-8"))


@router.get("/ui/", response_class=HTMLResponse)
def ui_index_slash() -> HTMLResponse:
    return HTMLResponse(_UI_FILE.read_text(encoding="utf-8"))
