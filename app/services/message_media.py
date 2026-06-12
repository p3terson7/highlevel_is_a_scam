from __future__ import annotations

import mimetypes
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import httpx

from app.core.config import Settings
from app.db.models import Lead, Message, MessageAttachment


ALLOWED_MEDIA_PREFIXES = ("image/", "video/")
SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(frozen=True)
class StoredMedia:
    filename: str
    content_type: str
    media_kind: str
    size_bytes: int
    storage_path: str
    provider_media_url: str = ""
    raw_payload: dict | None = None


class MessageMediaError(RuntimeError):
    pass


def media_kind_for_content_type(content_type: str) -> str:
    normalized = str(content_type or "").split(";")[0].strip().lower()
    if normalized.startswith("image/"):
        return "image"
    if normalized.startswith("video/"):
        return "video"
    return ""


def ensure_allowed_media_type(content_type: str) -> str:
    normalized = str(content_type or "").split(";")[0].strip().lower()
    if not normalized or not normalized.startswith(ALLOWED_MEDIA_PREFIXES):
        raise MessageMediaError("Only image and video attachments are supported.")
    return normalized


def safe_media_filename(filename: str, content_type: str, fallback_prefix: str = "media") -> str:
    raw_name = Path(str(filename or "")).name.strip()
    if not raw_name:
        extension = mimetypes.guess_extension(content_type) or ""
        raw_name = f"{fallback_prefix}{extension}"
    cleaned = SAFE_FILENAME_RE.sub("-", raw_name).strip(".-")
    return cleaned or f"{fallback_prefix}{mimetypes.guess_extension(content_type) or ''}"


def media_storage_root(settings: Settings) -> Path:
    root = Path(settings.message_media_storage_dir)
    if not root.is_absolute():
        root = Path.cwd() / root
    root.mkdir(parents=True, exist_ok=True)
    return root


def _relative_storage_path(client_id: int, message_id: int, filename: str) -> Path:
    return Path(str(client_id)) / str(message_id) / f"{uuid4().hex}_{filename}"


def store_message_media(
    *,
    settings: Settings,
    client_id: int,
    message_id: int,
    filename: str,
    content_type: str,
    content: bytes,
    provider_media_url: str = "",
    raw_payload: dict | None = None,
) -> StoredMedia:
    normalized_content_type = ensure_allowed_media_type(content_type)
    if not content:
        raise MessageMediaError("Attachment file is empty.")
    max_bytes = int(settings.message_media_max_bytes or 0)
    if max_bytes > 0 and len(content) > max_bytes:
        raise MessageMediaError("Attachment is too large.")

    safe_name = safe_media_filename(filename, normalized_content_type)
    relative_path = _relative_storage_path(client_id, message_id, safe_name)
    absolute_path = media_storage_root(settings) / relative_path
    absolute_path.parent.mkdir(parents=True, exist_ok=True)
    absolute_path.write_bytes(content)

    return StoredMedia(
        filename=safe_name,
        content_type=normalized_content_type,
        media_kind=media_kind_for_content_type(normalized_content_type),
        size_bytes=len(content),
        storage_path=relative_path.as_posix(),
        provider_media_url=provider_media_url,
        raw_payload=raw_payload or {},
    )


def create_message_attachment(
    *,
    message: Message,
    lead: Lead,
    stored: StoredMedia,
) -> MessageAttachment:
    return MessageAttachment(
        message_id=message.id,
        lead_id=lead.id,
        client_id=lead.client_id,
        filename=stored.filename,
        content_type=stored.content_type,
        media_kind=stored.media_kind,
        size_bytes=stored.size_bytes,
        storage_path=stored.storage_path,
        provider_media_url=stored.provider_media_url,
        public_token=uuid4().hex,
        raw_payload=stored.raw_payload or {},
    )


def attachment_file_path(settings: Settings, attachment: MessageAttachment) -> Path:
    root = media_storage_root(settings)
    path = root / attachment.storage_path
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise MessageMediaError("Invalid attachment storage path.") from exc
    return path


def provider_public_base_url(settings: Settings, client_provider_config: dict | None = None) -> str:
    raw = ""
    if isinstance(client_provider_config, dict):
        raw = str(client_provider_config.get("public_base_url") or "").strip()
    if not raw:
        raw = str(settings.public_base_url or "").strip()
    return raw.rstrip("/")


def attachment_public_url(settings: Settings, attachment: MessageAttachment, client_provider_config: dict | None = None) -> str:
    base = provider_public_base_url(settings, client_provider_config)
    if not base:
        return ""
    return f"{base}/media/public/{attachment.public_token}"


def filename_from_url(url: str, content_type: str, index: int = 0) -> str:
    path = urlparse(str(url or "")).path
    name = Path(path).name
    return safe_media_filename(name or f"twilio-media-{index}", content_type, fallback_prefix=f"twilio-media-{index}")


async def download_twilio_media(
    *,
    media_url: str,
    content_type: str,
    account_sid: str,
    auth_token: str,
    timeout_seconds: int,
) -> bytes:
    ensure_allowed_media_type(content_type)
    auth = (account_sid, auth_token) if account_sid and auth_token else None
    async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
        response = await client.get(media_url, auth=auth)
        response.raise_for_status()
        return response.content
