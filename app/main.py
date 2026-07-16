from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import HTMLResponse

from app.api.routes_health import router as health_router
from app.api.routes_sms import router as sms_router
from app.api.routes_ui import router as ui_router
from app.api.routes_webhooks import router as webhook_router
from app.api.ui.shell import ui_shell_response
from app.core.config import get_settings, validate_security_settings
from app.core.logging import configure_logging, get_logger, reset_request_id, set_request_id
from app.core.metrics import incr
from app.core.request_limits import RequestBodyLimitMiddleware
from app.core.ui_session_middleware import UISessionMiddleware
from app.db.models import Base
from app.db.session import get_engine, get_session_factory
from app.services.runtime_config import protect_existing_persisted_secrets
from app.services.secret_storage import validate_secret_storage_settings
from app.workers.tasks import recover_webhook_inbox_events

settings = get_settings()
configure_logging(settings.log_level)
logger = get_logger(__name__)
_IS_PRODUCTION = settings.env.strip().lower() in {"prod", "production"}


@asynccontextmanager
async def lifespan(_: FastAPI):
    validate_security_settings(settings)
    validate_secret_storage_settings(settings)
    if settings.auto_create_tables:
        Base.metadata.create_all(bind=get_engine())
    with get_session_factory()() as db:
        protected_count = protect_existing_persisted_secrets(db)
        if protected_count:
            logger.info("persisted_secrets_encrypted", extra={"secret_count": protected_count})
    recovered_webhooks = recover_webhook_inbox_events()
    if recovered_webhooks:
        logger.info(
            "webhook_inbox_recovered",
            extra={"event_count": recovered_webhooks},
        )
    incr("app_startups_total")
    yield


app = FastAPI(
    title="Lead Conversion SMS Agent",
    version="0.1.0",
    lifespan=lifespan,
    docs_url=None if _IS_PRODUCTION else "/docs",
    redoc_url=None if _IS_PRODUCTION else "/redoc",
    openapi_url=None if _IS_PRODUCTION else "/openapi.json",
)
app.add_middleware(
    RequestBodyLimitMiddleware,
    max_bytes=settings.request_body_max_bytes,
    upload_max_bytes=settings.message_media_max_bytes + (1024 * 1024),
)
app.add_middleware(UISessionMiddleware, settings=settings)
app.include_router(health_router)
app.include_router(webhook_router)
app.include_router(sms_router)
app.include_router(ui_router)

_NON_UI_ROUTE_PREFIXES = {
    "admin",
    "api",
    "docs",
    "health",
    "metrics",
    "openapi.json",
    "redoc",
    "sms",
    "webhooks",
}
_UI_ROUTE_PREFIXES = {
    "ui",
    "home",
    "dashboard",
    "clients",
    "inbox",
    "conversations",
    "pipeline",
    "crm",
    "records",
    "leads",
    "calendar",
    "tasks",
    "logs",
    "settings",
    "test-lab",
    "test_lab",
}


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", uuid.uuid4().hex)
    token = set_request_id(request_id)
    start = time.perf_counter()

    try:
        response = await call_next(request)
    except Exception:
        incr("app_request_errors_total")
        logger.exception("request_failed", extra={"path": request.url.path})
        raise
    finally:
        elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
        logger.info("request_completed", extra={"method": request.method, "path": request.url.path, "elapsed_ms": elapsed_ms})
        reset_request_id(token)

    response.headers["X-Request-ID"] = request_id
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "base-uri 'self'; "
        "connect-src 'self'; "
        "font-src 'self' data:; "
        "form-action 'self'; "
        "frame-ancestors 'none'; "
        "img-src 'self' data: blob: https:; "
        "object-src 'none'; "
        "script-src 'self'; "
        "style-src 'self' 'unsafe-inline'"
    )
    if _IS_PRODUCTION:
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    incr("app_requests_total")
    return response


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def root() -> HTMLResponse:
    return ui_shell_response()


def _should_serve_ui_fallback(path: str, request: Request) -> bool:
    normalized = path.strip("/")
    if not normalized:
        return True
    if normalized.startswith(("ui/api", "ui/assets")):
        return False
    segments = normalized.split("/")
    first_segment = segments[0]
    if first_segment in _NON_UI_ROUTE_PREFIXES:
        return False
    route_segment = segments[1] if first_segment == "ui" and len(segments) > 1 else first_segment
    if route_segment not in _UI_ROUTE_PREFIXES:
        return False
    accept_header = request.headers.get("accept", "")
    return not accept_header or "text/html" in accept_header or "*/*" in accept_header


@app.get("/{path:path}", response_class=HTMLResponse, include_in_schema=False)
def ui_fallback(path: str, request: Request) -> HTMLResponse:
    if _should_serve_ui_fallback(path, request):
        return ui_shell_response()
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
