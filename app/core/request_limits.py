from __future__ import annotations

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class RequestBodyTooLarge(Exception):
    pass


class RequestBodyLimitMiddleware:
    """Reject oversized bodies before FastAPI parses JSON or multipart data."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        max_bytes: int,
        upload_max_bytes: int,
    ) -> None:
        self.app = app
        self.max_bytes = max(int(max_bytes), 1)
        self.upload_max_bytes = max(int(upload_max_bytes), self.max_bytes)

    @staticmethod
    def _is_media_upload(path: str) -> bool:
        return path.startswith("/ui/api/conversations/") and path.endswith(
            "/messages/manual-media"
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = str(scope.get("path") or "")
        limit = self.upload_max_bytes if self._is_media_upload(path) else self.max_bytes
        content_lengths = [
            value
            for name, value in scope.get("headers", [])
            if name.lower() == b"content-length"
        ]
        if len(content_lengths) > 1:
            await self._reject(scope, receive, send, status_code=400, detail="Invalid Content-Length")
            return
        if content_lengths:
            try:
                content_length = int(content_lengths[0].decode("ascii"))
            except (UnicodeError, ValueError):
                await self._reject(scope, receive, send, status_code=400, detail="Invalid Content-Length")
                return
            if content_length < 0:
                await self._reject(scope, receive, send, status_code=400, detail="Invalid Content-Length")
                return
            if content_length > limit:
                await self._reject(scope, receive, send, status_code=413, detail="Request body too large")
                return

        received = 0

        async def limited_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > limit:
                    raise RequestBodyTooLarge
            return message

        response_started = False

        async def tracked_send(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, limited_receive, tracked_send)
        except RequestBodyTooLarge:
            if response_started:
                raise
            await self._reject(scope, receive, send, status_code=413, detail="Request body too large")

    @staticmethod
    async def _reject(
        scope: Scope,
        receive: Receive,
        send: Send,
        *,
        status_code: int,
        detail: str,
    ) -> None:
        response = JSONResponse({"detail": detail}, status_code=status_code)
        await response(scope, receive, send)
