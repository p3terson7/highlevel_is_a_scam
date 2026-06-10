from __future__ import annotations

from fastapi import APIRouter

from app.api.ui import (
    client_routes,
    conversation_routes,
    crm_routes,
    dashboard_routes,
    sandbox_routes,
    seed_routes,
    session_routes,
    shell,
)

router = APIRouter(tags=["ui"])
router.include_router(shell.router)
router.include_router(session_routes.router)
router.include_router(dashboard_routes.router)
router.include_router(client_routes.router)
router.include_router(conversation_routes.router)
router.include_router(crm_routes.router)
router.include_router(sandbox_routes.router)
router.include_router(seed_routes.router)
