import asyncio
import ipaddress
import stat

import httpx
import pytest

from app.core.config import Settings
from app.services import message_media
from app.services.message_media import (
    MessageMediaError,
    download_twilio_media,
    store_message_media,
)
from app.workers import tasks as worker_tasks


async def _public_test_address(host: str, port: int):
    _ = host, port
    return (ipaddress.ip_address("93.184.216.34"),)


def test_media_download_limits_redirects_and_drops_twilio_auth_on_redirect(monkeypatch):
    requests: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(
            {
                "host": request.headers.get("host", ""),
                "authorization": request.headers.get("authorization", ""),
            }
        )
        if len(requests) == 1:
            return httpx.Response(302, headers={"location": "https://cdn.example/media/ME123"})
        return httpx.Response(200, headers={"content-type": "image/jpeg"}, content=b"\xff\xd8\xffjpeg-data")

    monkeypatch.setattr(message_media, "_resolve_public_addresses", _public_test_address)
    content = asyncio.run(
        download_twilio_media(
            media_url="https://api.twilio.com/2010-04-01/Accounts/AC123/Messages/MM123/Media/ME123",
            content_type="image/jpeg",
            account_sid="AC123",
            auth_token="secret-token",
            timeout_seconds=5,
            max_bytes=1024,
            transport=httpx.MockTransport(handler),
        )
    )

    assert content == b"\xff\xd8\xffjpeg-data"
    assert requests[0]["host"] == "api.twilio.com"
    assert requests[0]["authorization"].startswith("Basic ")
    assert requests[1] == {"host": "cdn.example", "authorization": ""}


def test_media_download_rejects_untrusted_initial_host():
    with pytest.raises(MessageMediaError, match="host is not trusted"):
        asyncio.run(
            download_twilio_media(
                media_url="https://attacker.example/media/ME123",
                content_type="image/jpeg",
                account_sid="AC123",
                auth_token="secret-token",
                timeout_seconds=5,
            )
        )


def test_media_download_rejects_cross_account_api_url():
    with pytest.raises(MessageMediaError, match="configured account"):
        asyncio.run(
            download_twilio_media(
                media_url="https://api.twilio.com/2010-04-01/Accounts/AC-OTHER/Messages/MM123/Media/ME123",
                content_type="image/jpeg",
                account_sid="AC123",
                auth_token="secret-token",
                timeout_seconds=5,
            )
        )


def test_media_download_rejects_private_redirect_destination():
    with pytest.raises(MessageMediaError, match="private or non-public"):
        asyncio.run(message_media._resolve_public_addresses("127.0.0.1", 443))


def test_media_download_rejects_response_mime_mismatch(monkeypatch):
    monkeypatch.setattr(message_media, "_resolve_public_addresses", _public_test_address)
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, headers={"content-type": "video/mp4"}, content=b"video")
    )

    with pytest.raises(MessageMediaError, match="does not match"):
        asyncio.run(
            download_twilio_media(
                media_url="https://api.twilio.com/2010-04-01/Accounts/AC123/Messages/MM123/Media/ME123",
                content_type="image/jpeg",
                account_sid="AC123",
                auth_token="secret-token",
                timeout_seconds=5,
                max_bytes=1024,
                transport=transport,
            )
        )


def test_media_download_enforces_streamed_byte_limit_without_content_length(monkeypatch):
    class ChunkedBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"1234"
            yield b"5678"

    monkeypatch.setattr(message_media, "_resolve_public_addresses", _public_test_address)
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            headers={"content-type": "image/jpeg"},
            stream=ChunkedBody(),
        )
    )

    with pytest.raises(MessageMediaError, match="too large"):
        asyncio.run(
            download_twilio_media(
                media_url="https://api.twilio.com/2010-04-01/Accounts/AC123/Messages/MM123/Media/ME123",
                content_type="image/jpeg",
                account_sid="AC123",
                auth_token="secret-token",
                timeout_seconds=5,
                max_bytes=6,
                transport=transport,
            )
        )


def test_stored_media_uses_private_directory_and_file_modes(tmp_path):
    root = tmp_path / "message_media"
    root.mkdir(mode=0o777)
    root.chmod(0o777)
    (root / "17").mkdir(mode=0o755)
    (root / "17").chmod(0o755)
    settings = Settings(
        message_media_storage_dir=str(root),
        message_media_max_bytes=1024,
    )

    stored = store_message_media(
        settings=settings,
        client_id=17,
        message_id=29,
        filename="photo.jpg",
        content_type="image/jpeg",
        content=b"\xff\xd8\xffprivate-image",
    )

    stored_file = root / stored.storage_path
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE((root / "17").stat().st_mode) == 0o700
    assert stat.S_IMODE((root / "17" / "29").stat().st_mode) == 0o700
    assert stat.S_IMODE(stored_file.stat().st_mode) == 0o600


def test_inbound_media_download_uses_remaining_aggregate_time(monkeypatch):
    async def slow_download(**kwargs):
        _ = kwargs
        await asyncio.sleep(0.2)
        return b"\xff\xd8\xfflate"

    monkeypatch.setattr(worker_tasks, "download_twilio_media", slow_download)

    with pytest.raises(TimeoutError):
        asyncio.run(
            worker_tasks._download_inbound_media_with_timeout(
                aggregate_timeout_seconds=0.01,
                media_url="https://api.twilio.com/media/ME",
                content_type="image/jpeg",
                account_sid="AC123",
                auth_token="secret",
                max_bytes=1024,
                timeout_seconds=1,
            )
        )


@pytest.mark.parametrize(
    "value",
    [
        "http://media.example",
        "https://media.example:8443",
        "https://user:pass@media.example",
        "https://media.example?token=secret",
    ],
)
def test_public_media_base_url_rejects_noncanonical_values(value):
    assert message_media.provider_public_base_url(Settings(public_base_url=value)) == ""


def test_public_media_base_url_normalizes_canonical_https_origin():
    assert (
        message_media.provider_public_base_url(
            Settings(public_base_url="https://MEDIA.EXAMPLE/media/"),
        )
        == "https://media.example/media"
    )
