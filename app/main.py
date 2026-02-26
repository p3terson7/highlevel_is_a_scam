from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.api.routes_health import router as health_router
from app.api.routes_sms import router as sms_router
from app.api.routes_ui import router as ui_router
from app.api.routes_webhooks import router as webhook_router
from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger, reset_request_id, set_request_id
from app.core.metrics import incr
from app.db.models import Base
from app.db.session import get_engine

settings = get_settings()
configure_logging(settings.log_level)
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    if settings.auto_create_tables:
        Base.metadata.create_all(bind=get_engine())
    incr("app_startups_total")
    yield


app = FastAPI(title="Lead Conversion SMS Agent", version="0.1.0", lifespan=lifespan)
app.include_router(health_router)
app.include_router(webhook_router)
app.include_router(sms_router)
app.include_router(ui_router)


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
    incr("app_requests_total")
    return response


@app.get("/")
def root() -> dict[str, str]:
    return {"service": settings.app_name, "status": "running"}
