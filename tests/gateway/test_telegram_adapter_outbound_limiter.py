"""Behavioral coverage for TelegramAdapter's shared outbound mutation boundary."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.base import SendResult
from gateway.telegram_outbound_contract import TelegramMutation
from gateway.telegram_outbound_limiter import TelegramOutboundLimiter
from plugins.platforms.telegram.adapter import TelegramAdapter


def _adapter(*, extra: dict | None = None) -> TelegramAdapter:
    config = PlatformConfig(
        enabled=True,
        token="test-token",
        typing_indicator=False,
        extra={
            "telegram_outbound_limiter": {
                "global_interval": 0,
                "per_chat_interval": 0,
            },
            **(extra or {}),
        },
    )
    adapter = TelegramAdapter(config)
    adapter._bot = MagicMock()
    adapter._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=101))
    adapter._bot.edit_message_text = AsyncMock(return_value=True)
    adapter._bot.send_chat_action = AsyncMock()
    adapter._rich_send_disabled = True
    adapter._hermes_profile_name = "profile-a"
    return adapter


@pytest.mark.asyncio
async def test_text_and_edit_share_the_profile_chat_route_and_disabled_mode() -> None:
    adapter = _adapter()
    submissions: list[tuple[str, TelegramMutation]] = []
    limiter = adapter._telegram_outbound_limiter
    original_submit = limiter.submit

    async def record_submit(chat_key, callback, *, metadata):
        submissions.append((chat_key, metadata.operation))
        return await original_submit(chat_key, callback, metadata=metadata)

    limiter.submit = record_submit
    try:
        sent = await adapter.send(
            "123",
            "hello",
            metadata={"thread_id": "9"},
        )
        edited = await adapter.edit_message(
            "123",
            "101",
            "hello again",
            metadata={"thread_id": "9"},
        )
    finally:
        await limiter.close()

    assert sent.success and edited.success
    assert submissions == [
        (
            "telegram/profile-a/123/thread=9/dm=-/reply=-",
            TelegramMutation.FINAL_TEXT,
        ),
        (
            "telegram/profile-a/123/thread=9/dm=-/reply=-",
            TelegramMutation.PROGRESSIVE_EDIT,
        ),
    ]
    adapter._bot.send_message.assert_awaited_once()
    adapter._bot.edit_message_text.assert_awaited_once()

    disabled = _adapter(
        extra={
            "telegram_outbound_limiter": {
                "enabled": False,
                "global_interval": 100,
                "per_chat_interval": 100,
            }
        }
    )
    try:
        result = await disabled.send("123", "no queue")
    finally:
        await disabled._telegram_outbound_limiter.close()
    assert result.success
    assert disabled._telegram_outbound_limiter.enabled is False
    disabled._bot.send_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_status_coalesces_cooldown_propagates_and_disconnect_closes() -> None:
    adapter = _adapter()
    limiter = TelegramOutboundLimiter(
        global_interval=0,
        per_chat_interval=0,
        max_queue_size=4,
    )
    await adapter._telegram_outbound_limiter.close()
    adapter._telegram_outbound_limiter = limiter
    adapter._status_message_ids[("123", "lifecycle")] = "101"
    started = asyncio.Event()
    release = asyncio.Event()
    calls: list[str] = []

    async def edit_message_text(**kwargs):
        calls.append(kwargs["text"])
        if len(calls) == 1:
            started.set()
            await release.wait()
        return True

    adapter._bot.edit_message_text = edit_message_text
    first = asyncio.create_task(
        adapter.edit_message(
            "123",
            "101",
            "first",
            metadata={"_telegram_status_key": "lifecycle"},
        )
    )
    await started.wait()
    stale = asyncio.create_task(
        adapter.edit_message(
            "123",
            "101",
            "stale",
            metadata={"_telegram_status_key": "lifecycle"},
        )
    )
    await asyncio.sleep(0)
    newest = asyncio.create_task(
        adapter.edit_message(
            "123",
            "101",
            "newest",
            metadata={"_telegram_status_key": "lifecycle"},
        )
    )
    await asyncio.sleep(0)
    release.set()

    assert (await first).success
    stale_result = await stale
    assert stale_result.success
    assert stale_result.raw_response == {"skipped": True}
    assert (await newest).success
    assert calls == ["first", "newest"]

    await adapter._submit_telegram_mutation(
        TelegramMutation.TYPING,
        "123",
        lambda: SendResult(success=False, retry_after=3.0),
        lane="typing/root",
    )
    assert limiter.cooldown_until >= asyncio.get_running_loop().time() + 3.0

    await adapter.disconnect()
    assert limiter.closed
