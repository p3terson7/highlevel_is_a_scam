from __future__ import annotations

import asyncio
import ipaddress
import mimetypes
import os
import re
import socket
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse
from uuid import uuid4

import httpx

from app.core.config import Settings
from app.db.models import Lead, Message, MessageAttachment

ALLOWED_MEDIA_TYPES = {
    "image/gif",
    "image/heic",
    "image/heif",
    "image/jpeg",
    "image/png",
    "image/webp",
    "video/mp4",
    "video/mpeg",
    "video/quicktime",
    "video/webm",
}
SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
_DEFAULT_MEDIA_MAX_BYTES = 25 * 1024 * 1024
_MAX_MEDIA_REDIRECTS = 2
_MAX_MEDIA_URL_LENGTH = 4096
_MEDIA_REDIRECT_STATUSES = {301, 302, 303, 307, 308}


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
    if normalized not in ALLOWED_MEDIA_TYPES:
        raise MessageMediaError("This image or video format is not supported.")
    return normalized


def ensure_media_content_matches_type(content_type: str, content: bytes) -> None:
    normalized = ensure_allowed_media_type(content_type)
    head = bytes(content[:32])
    signatures = {
        "image/jpeg": head.startswith(b"\xff\xd8\xff"),
        "image/png": head.startswith(b"\x89PNG\r\n\x1a\n"),
        "image/gif": head.startswith((b"GIF87a", b"GIF89a")),
        "image/webp": head.startswith(b"RIFF") and head[8:12] == b"WEBP",
        "video/webm": head.startswith(b"\x1aE\xdf\xa3"),
        "video/mpeg": head.startswith((b"\x00\x00\x01\xba", b"\x00\x00\x01\xb3")),
    }
    if normalized in {"image/heic", "image/heif"}:
        signatures[normalized] = head[4:8] == b"ftyp" and head[8:12] in {
            b"heic",
            b"heix",
            b"hevc",
            b"hevx",
            b"heim",
            b"heis",
            b"mif1",
            b"msf1",
        }
    if normalized in {"video/mp4", "video/quicktime"}:
        signatures[normalized] = head[4:8] == b"ftyp"
    if not signatures.get(normalized, False):
        raise MessageMediaError("Attachment contents do not match the declared media type.")


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
    _ensure_private_directory(root)
    return root


def _ensure_private_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.chmod(0o700)
    if path.stat().st_mode & 0o777 != 0o700:
        raise MessageMediaError("Attachment storage directory permissions are unsafe.")


def _write_private_file(path: Path, content: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(content)
            handle.flush()
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        path.unlink(missing_ok=True)
        raise
    if path.stat().st_mode & 0o777 != 0o600:
        path.unlink(missing_ok=True)
        raise MessageMediaError("Attachment file permissions are unsafe.")


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
    ensure_media_content_matches_type(normalized_content_type, content)

    safe_name = safe_media_filename(filename, normalized_content_type)
    relative_path = _relative_storage_path(client_id, message_id, safe_name)
    storage_root = media_storage_root(settings)
    current_directory = storage_root
    for part in relative_path.parent.parts:
        current_directory /= part
        _ensure_private_directory(current_directory)
    absolute_path = storage_root / relative_path
    _write_private_file(absolute_path, content)

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
        public_expires_at=datetime.now(timezone.utc) + timedelta(minutes=15),
        raw_payload=stored.raw_payload or {},
    )


def refresh_attachment_public_access(
    attachment: MessageAttachment,
    *,
    lifetime: timedelta = timedelta(minutes=15),
) -> None:
    attachment.public_token = uuid4().hex
    attachment.public_expires_at = datetime.now(timezone.utc) + lifetime


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
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        port = parsed.port or 443
    except ValueError:
        return ""
    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or port != 443
        or parsed.query
        or parsed.fragment
    ):
        return ""
    hostname = parsed.hostname.encode("idna").decode("ascii").lower()
    netloc = f"[{hostname}]" if ":" in hostname else hostname
    path = (parsed.path or "").rstrip("/")
    return urlunparse(("https", netloc, path, "", "", ""))


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
    max_bytes: int = _DEFAULT_MEDIA_MAX_BYTES,
    transport: httpx.AsyncBaseTransport | None = None,
) -> bytes:
    expected_content_type = ensure_allowed_media_type(content_type)
    byte_limit = int(max_bytes or 0)
    if byte_limit <= 0:
        byte_limit = _DEFAULT_MEDIA_MAX_BYTES

    current_url = str(media_url or "").strip()
    auth = (account_sid, auth_token) if account_sid and auth_token else None
    client_kwargs: dict = {
        "timeout": max(float(timeout_seconds), 1.0),
        "follow_redirects": False,
        "trust_env": False,
    }
    if transport is not None:
        client_kwargs["transport"] = transport

    async with httpx.AsyncClient(**client_kwargs) as client:
        for redirect_count in range(_MAX_MEDIA_REDIRECTS + 1):
            parsed = _validate_media_url(
                current_url,
                require_twilio_api_host=redirect_count == 0,
                expected_account_sid=account_sid if redirect_count == 0 else "",
            )
            addresses = await _resolve_public_addresses(parsed.hostname or "", parsed.port or 443)
            request_url, request_headers, request_extensions = _pinned_request_target(current_url, addresses[0])
            request_auth = auth if redirect_count == 0 else None

            async with client.stream(
                "GET",
                request_url,
                headers=request_headers,
                extensions=request_extensions,
                auth=request_auth,
            ) as response:
                if response.status_code in _MEDIA_REDIRECT_STATUSES:
                    if redirect_count >= _MAX_MEDIA_REDIRECTS:
                        raise MessageMediaError("Twilio media returned too many redirects.")
                    location = response.headers.get("location", "").strip()
                    if not location:
                        raise MessageMediaError("Twilio media redirect did not include a destination.")
                    current_url = urljoin(current_url, location)
                    continue

                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    raise MessageMediaError(
                        f"Twilio media provider returned HTTP {response.status_code}."
                    ) from exc

                response_content_type = response.headers.get("content-type", "")
                try:
                    normalized_response_type = ensure_allowed_media_type(response_content_type)
                except MessageMediaError as exc:
                    raise MessageMediaError("Twilio media response has an unsupported content type.") from exc
                if media_kind_for_content_type(normalized_response_type) != media_kind_for_content_type(
                    expected_content_type
                ):
                    raise MessageMediaError("Twilio media response type does not match the signed webhook metadata.")

                content_length = response.headers.get("content-length", "").strip()
                if content_length:
                    try:
                        if int(content_length) > byte_limit:
                            raise MessageMediaError("Attachment is too large.")
                    except ValueError:
                        pass

                chunks: list[bytes] = []
                total_bytes = 0
                async for chunk in response.aiter_bytes():
                    total_bytes += len(chunk)
                    if total_bytes > byte_limit:
                        raise MessageMediaError("Attachment is too large.")
                    chunks.append(chunk)
                if total_bytes == 0:
                    raise MessageMediaError("Attachment file is empty.")
                content = b"".join(chunks)
                ensure_media_content_matches_type(normalized_response_type, content)
                return content

    raise MessageMediaError("Twilio media download did not return content.")


def _validate_media_url(
    raw_url: str,
    *,
    require_twilio_api_host: bool,
    expected_account_sid: str = "",
):
    if len(raw_url) > _MAX_MEDIA_URL_LENGTH:
        raise MessageMediaError("Twilio media URL is too long.")
    try:
        parsed = urlparse(raw_url)
        port = parsed.port or 443
    except ValueError as exc:
        raise MessageMediaError("Twilio media URL is invalid.") from exc
    if parsed.scheme.lower() != "https" or not parsed.hostname:
        raise MessageMediaError("Twilio media URL must use HTTPS.")
    if parsed.username is not None or parsed.password is not None:
        raise MessageMediaError("Twilio media URL cannot contain credentials.")
    if port != 443:
        raise MessageMediaError("Twilio media URL must use the standard HTTPS port.")

    host = parsed.hostname.lower().rstrip(".")
    if require_twilio_api_host and not _is_twilio_api_host(host):
        raise MessageMediaError("Twilio media URL host is not trusted.")
    expected_sid = str(expected_account_sid or "").strip()
    if require_twilio_api_host and expected_sid and f"/Accounts/{expected_sid}/" not in parsed.path:
        raise MessageMediaError("Twilio media URL does not belong to the configured account.")
    return parsed


def _is_twilio_api_host(host: str) -> bool:
    normalized = str(host or "").lower().rstrip(".")
    return normalized == "api.twilio.com" or (normalized.startswith("api.") and normalized.endswith(".twilio.com"))


async def _resolve_public_addresses(
    host: str,
    port: int,
) -> tuple[ipaddress.IPv4Address | ipaddress.IPv6Address, ...]:
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None:
        _assert_public_address(literal)
        return (literal,)

    loop = asyncio.get_running_loop()
    try:
        records = await loop.run_in_executor(
            None,
            partial(socket.getaddrinfo, host, port, type=socket.SOCK_STREAM),
        )
    except (socket.gaierror, UnicodeError, OverflowError) as exc:
        raise MessageMediaError("Twilio media host could not be resolved.") from exc

    addresses: set[ipaddress.IPv4Address | ipaddress.IPv6Address] = set()
    for record in records:
        raw_address = str(record[4][0]).split("%", maxsplit=1)[0]
        try:
            address = ipaddress.ip_address(raw_address)
        except ValueError as exc:
            raise MessageMediaError("Twilio media host resolved to an invalid address.") from exc
        _assert_public_address(address)
        addresses.add(address)
    if not addresses:
        raise MessageMediaError("Twilio media host did not resolve to an address.")
    return tuple(sorted(addresses, key=lambda address: (address.version, int(address))))


def _assert_public_address(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> None:
    if not address.is_global:
        raise MessageMediaError("Twilio media redirects to private or non-public networks are not allowed.")


def _pinned_request_target(
    url: str,
    address: ipaddress.IPv4Address | ipaddress.IPv6Address,
) -> tuple[str, dict[str, str], dict[str, str]]:
    parsed = urlparse(url)
    original_host = (parsed.hostname or "").encode("idna").decode("ascii")
    address_host = f"[{address}]" if address.version == 6 else str(address)
    explicit_port = parsed.port
    request_netloc = f"{address_host}:{explicit_port}" if explicit_port is not None else address_host

    host_header = f"[{original_host}]" if ":" in original_host else original_host
    if explicit_port is not None:
        host_header = f"{host_header}:{explicit_port}"
    request_url = urlunparse(
        (
            parsed.scheme,
            request_netloc,
            parsed.path or "/",
            parsed.params,
            parsed.query,
            "",
        )
    )
    extensions = {"sni_hostname": original_host} if parsed.scheme.lower() == "https" else {}
    return request_url, {"Host": host_header}, extensions
