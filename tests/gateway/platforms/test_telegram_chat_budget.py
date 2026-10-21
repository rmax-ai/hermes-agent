"""Per-chat Telegram send budget — rmax-ai/system-review#30.

Telegram enforces flood control per CHAT, but every gateway stream used to
pace independently, so N concurrent streams into one chat each spent that
chat's budget as if they were alone: 93 "flood control on send" retries plus
26 MarkdownV2→plaintext downgrades in one 7 h window.

These tests pin the three pieces of the fix:
  * one process-wide token bucket per chat_id (1 call / 3 s, burst 3),
  * a 429 fed back as a chat-wide cooldown that drains the bucket,
  * the stream consumer going final-only (skip intermediate edits, keep
    formatting) while the chat is cooling down.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import asyncio
import pytest

from gateway.config import PlatformConfig
from gateway.platforms.base import Platform
from gateway.platforms.telegram import (
    ChatSendBudget,
    DEFAULT_TELEGRAM_MAX_EDITS_PER_CHAT_PER_MIN,
    TelegramAdapter,
)
from gateway.stream_consumer import GatewayStreamConsumer


class _FakeClock:
    """Deterministic monotonic clock whose sleep advances time instantly."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += max(0.0, float(seconds))


def _budget(per_minute: int = 20, burst: int = 3):
    clock = _FakeClock()
    budget = ChatSendBudget(per_minute=per_minute, burst=burst)
    budget._clock = clock
    budget._sleep = clock.sleep
    return budget, clock


# ── the bucket ───────────────────────────────────────────────────────────


def test_default_rate_is_twenty_per_chat_per_minute():
    budget = ChatSendBudget()
    assert budget.per_minute == DEFAULT_TELEGRAM_MAX_EDITS_PER_CHAT_PER_MIN == 20
    assert budget.interval == pytest.approx(3.0)
    assert budget.burst == 3


@pytest.mark.asyncio
async def test_three_concurrent_streams_share_one_chat_budget():
    """3 streams × 40 updates into one chat: ≤20 calls/min, nothing dropped."""
    budget, clock = _budget()
    admitted: list[float] = []

    async def one_stream() -> None:
        for _ in range(40):
            await budget.acquire("-1001234")
            admitted.append(clock.now)

    await asyncio.gather(*(one_stream() for _ in range(3)))

    # Every update was admitted (paced, never dropped) ...
    assert len(admitted) == 120
    ordered = sorted(admitted)

    # ... at ≤ burst (3) + 20 per rolling 60 s window.  This is exactly the
    # condition that failed before: 3 independent timers each ran ~1 edit/s.
    for index, start in enumerate(ordered):
        in_window = [t for t in ordered[index:] if t < start + 60.0]
        assert len(in_window) <= 23, f"{len(in_window)} calls in 60 s from {start}"

    # Sustained pacing accounts for the rest of the wall time: 117 paced calls
    # at 3 s each.
    assert ordered[-1] - ordered[0] >= (120 - 3) * 3.0 - 1e-6


@pytest.mark.asyncio
async def test_chat_budget_is_independent_per_chat():
    budget, clock = _budget()
    for _ in range(3):
        await budget.acquire("chat-a")
    # chat-a is now empty; chat-b still has its full burst.
    waited_b = await budget.acquire("chat-b")
    assert waited_b == 0.0


@pytest.mark.asyncio
async def test_429_sets_chat_wide_cooldown_and_drains_bucket():
    budget, clock = _budget()

    assert budget.cooldown_remaining("chat") == 0.0
    assert budget.note_flood("chat", 30) == 30.0
    assert budget.cooldown_remaining("chat") == pytest.approx(30.0)

    # Every other writer into that chat is held for the whole cooldown, not
    # just the caller that happened to receive the 429.
    waited = await budget.acquire("chat", max_wait=40.0)
    assert waited >= 30.0
    assert budget.cooldown_remaining("chat") == 0.0

    # After the cooldown the bucket is empty, so the next call is paced.
    assert await budget.acquire("chat", max_wait=10.0) >= 1.0
    # A different chat was never affected.
    assert budget.cooldown_remaining("elsewhere") == 0.0


@pytest.mark.asyncio
async def test_acquire_fails_open_after_max_wait():
    """A pathological rate limit must never wedge delivery."""
    budget, clock = _budget()
    budget.note_flood("chat", 60)
    waited = await budget.acquire("chat", max_wait=2.0)
    assert waited >= 2.0  # returned instead of blocking for the full minute


def test_adapter_reports_and_records_chat_cooldown():
    adapter = object.__new__(TelegramAdapter)
    # ``name`` (used by the debug log) is a property over self.platform.
    adapter.platform = Platform.TELEGRAM
    adapter._chat_send_budget = ChatSendBudget(per_minute=20, burst=3)

    assert adapter.chat_cooldown_remaining("123") == 0.0
    adapter._note_chat_flood("123", 7)
    assert adapter.chat_cooldown_remaining("123") == pytest.approx(7.0, abs=0.5)
    assert adapter.chat_cooldown_remaining("456") == 0.0


# ── config knob ──────────────────────────────────────────────────────────


def test_config_knob_defaults_to_twenty_and_is_overridable():
    assert PlatformConfig().telegram_max_edits_per_chat_per_min == 20
    assert (
        PlatformConfig.from_dict({"telegram_max_edits_per_chat_per_min": 30})
        .telegram_max_edits_per_chat_per_min
        == 30
    )
    assert (
        PlatformConfig.from_dict({"extra": {"max_edits_per_chat_per_min": 12}})
        .telegram_max_edits_per_chat_per_min
        == 12
    )
    # Malformed values fall back instead of raising.
    assert (
        PlatformConfig.from_dict({"telegram_max_edits_per_chat_per_min": "nope"})
        .telegram_max_edits_per_chat_per_min
        == 20
    )


def test_env_var_overrides_config_field(monkeypatch):
    adapter = object.__new__(TelegramAdapter)
    adapter.config = PlatformConfig(telegram_max_edits_per_chat_per_min=20)
    assert adapter._chat_budget_per_minute() == 20
    monkeypatch.setenv("TELEGRAM_MAX_EDITS_PER_CHAT_PER_MIN", "45")
    assert adapter._chat_budget_per_minute() == 45


# ── stream consumer: final-only while the chat is cooling down ───────────


def _stream_adapter(cooldown: float):
    adapter = MagicMock()
    adapter.REQUIRES_EDIT_FINALIZE = True
    adapter.MAX_MESSAGE_LENGTH = 4096
    adapter.chat_cooldown_remaining = MagicMock(return_value=cooldown)
    adapter.send = AsyncMock(
        return_value=SimpleNamespace(success=True, message_id="m1")
    )
    adapter.edit_message = AsyncMock(
        return_value=SimpleNamespace(success=True, message_id="m1")
    )
    return adapter


@pytest.mark.asyncio
async def test_stream_consumer_goes_final_only_during_cooldown():
    adapter = _stream_adapter(cooldown=25.0)
    consumer = GatewayStreamConsumer(adapter, "chat")

    # First frame creates the message normally.
    assert await consumer._send_or_edit("first frame") is True
    assert adapter.send.await_count == 1
    edits_before = adapter.edit_message.await_count

    # Intermediate frame during the cooldown: no API call at all, so there is
    # nothing to 429 and no parse_mode downgrade.
    assert await consumer._send_or_edit("intermediate frame") is False
    assert adapter.edit_message.await_count == edits_before
    assert consumer._fallback_final_send is True
    assert consumer._cooldown_skipped_edits == 1
    assert consumer._last_sent_text == "first frame"

    # Cooldown cleared before the turn ended: the final edit is an EDIT again
    # (formatting preserved) and the fallback flag is cleared so no duplicate
    # tail is sent.
    adapter.chat_cooldown_remaining = MagicMock(return_value=0.0)
    assert await consumer._send_or_edit("final frame", finalize=True) is True
    assert adapter.edit_message.await_count == edits_before + 1
    assert consumer._fallback_final_send is False
    assert consumer._cooldown_fallback is False


@pytest.mark.asyncio
async def test_stream_consumer_without_cooldown_probe_is_unaffected():
    adapter = _stream_adapter(cooldown=0.0)
    del adapter.chat_cooldown_remaining
    consumer = GatewayStreamConsumer(adapter, "chat")

    assert await consumer._send_or_edit("first frame") is True
    assert await consumer._send_or_edit("second frame") is True
    assert adapter.edit_message.await_count == 1
