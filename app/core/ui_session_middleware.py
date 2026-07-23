from __future__ import annotations

from http.cookies import SimpleCookie
import hmac

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from app.core.config import Settings
from app.core.security import verify_admin_token
from app.services.portal_auth import verify_portal_token
from app.services.ui_session_auth import (
    UI_CSRF_COOKIE,
    UI_CSRF_HEADER,
    UI_SESSION_COOKIE,
    csrf_matches_session,
    reset_current_ui_session_token,
    set_current_ui_session_token,
    verify_ui_session_token,
)


_UNSAFE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_CSRF_EXEMPT_PATHS = {
    "/ui/api/login/admin",
    "/ui/api/login/client",
    "/ui/api/login/client/token",
}


class UISessionMiddleware:
    """Expose signed HttpOnly UI sessions and enforce double-submit CSRF."""

    def __init__(self, app: ASGIApp, *, settings: Settings) -> None:
        self.app = app
        self.settings = settings

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        cookies = _cookies(scope)
        session_token = cookies.get(UI_SESSION_COOKIE, "")
        context_token = set_current_ui_session_token(session_token)
        try:
            if self._csrf_required(scope=scope, session_token=session_token):
                payload = verify_ui_session_token(self.settings, session_token)
                csrf_cookie = cookies.get(UI_CSRF_COOKIE, "")
                csrf_headers = _header_values(scope, UI_CSRF_HEADER.lower().encode("ascii"))
                supplied = csrf_headers[0] if len(csrf_headers) == 1 else ""
                if (
                    payload is None
                    or not csrf_cookie
                    or not supplied
                    or not hmac.compare_digest(csrf_cookie, supplied)
                    or not csrf_matches_session(payload, supplied)
                ):
                    response = JSONResponse({"detail": "Invalid CSRF token"}, status_code=403)
                    await response(scope, receive, send)
                    return
            await self.app(scope, receive, send)
        finally:
            reset_current_ui_session_token(context_token)

    def _csrf_required(self, *, scope: Scope, session_token: str) -> bool:
        method = str(scope.get("method") or "GET").upper()
        path = str(scope.get("path") or "")
        if method not in _UNSAFE_METHODS or path in _CSRF_EXEMPT_PATHS:
            return False
        if not (path.startswith("/ui/api/") or path.startswith("/admin/")):
            return False
        session_payload = verify_ui_session_token(self.settings, session_token)
        admin_headers = _header_values(scope, b"x-admin-token")
        if len(admin_headers) == 1 and verify_admin_token(admin_headers[0], self.settings.admin_token):
            return False
        portal_headers = _header_values(scope, b"x-portal-token")
        # A portal bearer may waive CSRF only when it is the sole valid
        # credential. Otherwise a low-privilege bearer could suppress CSRF
        # while an admin-only route authorizes the ambient admin cookie.
        if (
            session_payload is None
            and len(portal_headers) == 1
            and verify_portal_token(self.settings, portal_headers[0]) is not None
        ):
            return False
        return session_payload is not None


def _cookies(scope: Scope) -> dict[str, str]:
    values = _header_values(scope, b"cookie")
    if not values:
        return {}
    jar = SimpleCookie()
    try:
        jar.load("; ".join(values))
    except Exception:
        return {}
    return {key: morsel.value for key, morsel in jar.items()}


def _header_values(scope: Scope, name: bytes) -> list[str]:
    values: list[str] = []
    for raw_name, raw_value in scope.get("headers", []):
        if raw_name.lower() != name:
            continue
        try:
            values.append(raw_value.decode("latin-1"))
        except UnicodeError:
            return []
    return values
