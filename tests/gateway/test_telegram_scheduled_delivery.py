"""Tests for Telegram's non-chunking scheduled-output policy."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from gateway.platforms.base import SendResult
from gateway.config import Platform
from gateway.platforms.telegram import TelegramAdapter


def _adapter():
    adapter = object.__new__(TelegramAdapter)
    adapter.platform = Platform.TELEGRAM
    return adapter


@pytest.mark.asyncio
async def test_scheduled_output_threshold_uses_existing_text_send():
    adapter = _adapter()
    adapter.send = AsyncMock(return_value=SendResult(success=True, message_id="text-1"))
    adapter.send_document = AsyncMock()

    result = await adapter.send_scheduled_output(
        "x" * TelegramAdapter.SCHEDULED_TEXT_THRESHOLD,
        destination="123",
        run_id="run-1",
    )

    assert result.success is True
    adapter.send.assert_awaited_once_with("123", "x" * 3800, metadata=None)
    adapter.send_document.assert_not_awaited()


@pytest.mark.asyncio
async def test_scheduled_output_oversize_is_one_utf8_document_with_plain_caption(tmp_path):
    adapter = _adapter()
    adapter.send = AsyncMock()
    document_call = {}

    async def capture_document(**kwargs):
        document_call.update(kwargs)
        path = Path(kwargs["file_path"])
        assert path.read_bytes() == ("# résumé 😀\n" + "x" * 4488).encode("utf-8")
        assert kwargs["file_name"] == "scheduled-output-run-unsafe-id.md"
        assert kwargs["caption"].startswith("Nightly *report* — full scheduled output attached")
        assert len(kwargs["caption"]) <= 512
        assert "parse_mode" not in kwargs
        return SendResult(success=True, message_id="document-1")

    adapter.send_document = AsyncMock(side_effect=capture_document)
    payload = "# résumé 😀\n" + "x" * 4488

    result = await adapter.send_scheduled_output(
        payload,
        destination="123",
        run_id="../../run unsafe/id",
        job_name="Nightly *report*",
    )

    assert result.success is True
    assert result.raw_response["delivery_kind"] == "document"
    assert result.raw_response["run_id"] == "../../run unsafe/id"
    adapter.send.assert_not_awaited()
    adapter.send_document.assert_awaited_once()
    assert not Path(document_call["file_path"]).exists()


@pytest.mark.asyncio
async def test_scheduled_output_temp_file_is_removed_when_upload_raises():
    adapter = _adapter()
    adapter.send = AsyncMock()
    paths = []

    async def fail_document(**kwargs):
        paths.append(Path(kwargs["file_path"]))
        assert paths[-1].exists()
        raise RuntimeError("upload failed")

    adapter.send_document = AsyncMock(side_effect=fail_document)

    with pytest.raises(RuntimeError, match="upload failed"):
        await adapter.send_scheduled_output(
            "x" * 4500,
            destination="123",
            run_id="run-2",
        )

    assert paths and not paths[0].exists()
    adapter.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_scheduled_output_above_local_safety_ceiling_is_visible_error():
    adapter = _adapter()
    adapter.send = AsyncMock()
    adapter.send_document = AsyncMock()

    result = await adapter.send_scheduled_output(
        "x" * (TelegramAdapter.SCHEDULED_MAX_BYTES + 1),
        destination="123",
        run_id="run-3",
    )

    assert result.success is False
    assert "10 MiB" in result.error
    adapter.send.assert_not_awaited()
    adapter.send_document.assert_not_awaited()
