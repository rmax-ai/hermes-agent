"""Behavioral tests for the runtime-neutral Telegram outbound limiter (issue #3)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from gateway.telegram_outbound_contract import (
    OutboundDurability,
    OutboundPriority,
    RetryAmbiguity,
    TelegramMutation,
    TelegramOutboundMetadata,
)
from gateway.telegram_outbound_limiter import (
    LimiterClosedError,
    MutationDroppedError,
    MutationSupersededError,
    QueueFullError,
    TelegramOutboundLimiter,
)


@dataclass
class FakeClock:
    now: float = 0.0

    def __call__(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.now += delay
        await asyncio.sleep(0)


def _metadata(
    *,
    priority: OutboundPriority,
    durability: OutboundDurability = OutboundDurability.DURABLE,
    coalescing_key: str | None = None,
) -> TelegramOutboundMetadata:
    return TelegramOutboundMetadata(
        operation=TelegramMutation.FINAL_TEXT,
        priority=priority,
        coalescing_key=coalescing_key,
        durability=durability,
        retry_ambiguity=RetryAmbiguity.CONDITIONAL,
    )


@pytest.mark.asyncio
async def test_global_spacing_and_fairness_skip_a_throttled_chat() -> None:
    clock = FakeClock()
    starts: list[tuple[str, float]] = []
    limiter = TelegramOutboundLimiter(
        global_interval=1.0,
        per_chat_interval=10.0,
        clock=clock,
        sleep=clock.sleep,
    )

    async def send(chat: str) -> str:
        starts.append((chat, clock.now))
        return chat

    first = asyncio.create_task(
        limiter.submit(
            "chat-a",
            lambda: send("chat-a"),
            metadata=_metadata(priority=OutboundPriority.NORMAL),
        )
    )
    second = asyncio.create_task(
        limiter.submit(
            "chat-b",
            lambda: send("chat-b"),
            metadata=_metadata(priority=OutboundPriority.NORMAL),
        )
    )
    third = asyncio.create_task(
        limiter.submit(
            "chat-a",
            lambda: send("chat-a"),
            metadata=_metadata(priority=OutboundPriority.NORMAL),
        )
    )
    assert await first == "chat-a"
    assert await second == "chat-b"
    assert await third == "chat-a"
    assert starts == [("chat-a", 0.0), ("chat-b", 1.0), ("chat-a", 10.0)]
    await limiter.close()


@pytest.mark.asyncio
async def test_fifo_is_preserved_within_a_priority() -> None:
    clock = FakeClock()
    order: list[str] = []
    limiter = TelegramOutboundLimiter(
        global_interval=0,
        per_chat_interval=0,
        clock=clock,
        sleep=clock.sleep,
    )

    tasks = [
        asyncio.create_task(
            limiter.submit(
                "chat",
                lambda value=value: order.append(value),
                metadata=_metadata(priority=OutboundPriority.NORMAL),
            )
        )
        for value in ("first", "second", "third")
    ]
    await asyncio.gather(*tasks)
    assert order == ["first", "second", "third"]
    await limiter.close()


@pytest.mark.asyncio
async def test_priority_applies_across_chats_without_reordering_a_chat_head() -> None:
    clock = FakeClock()
    order: list[str] = []
    limiter = TelegramOutboundLimiter(
        global_interval=0,
        per_chat_interval=0,
        clock=clock,
        sleep=clock.sleep,
    )

    low = asyncio.create_task(
        limiter.submit(
            "chat-a",
            lambda: order.append("low"),
            metadata=_metadata(priority=OutboundPriority.LOW),
        )
    )
    high = asyncio.create_task(
        limiter.submit(
            "chat-b",
            lambda: order.append("high"),
            metadata=_metadata(priority=OutboundPriority.HIGH),
        )
    )
    await asyncio.gather(low, high)
    assert order == ["high", "low"]
    await limiter.close()


@pytest.mark.asyncio
async def test_durable_work_can_evict_discardable_work_but_never_durable_work() -> None:
    clock = FakeClock()
    limiter = TelegramOutboundLimiter(
        global_interval=100,
        per_chat_interval=100,
        max_queue_size=1,
        clock=clock,
        sleep=clock.sleep,
    )
    blocker = asyncio.Event()

    async def hold() -> None:
        await blocker.wait()

    running = asyncio.create_task(
        limiter.submit(
            "running", hold, metadata=_metadata(priority=OutboundPriority.HIGH)
        )
    )
    await asyncio.sleep(0)
    best_effort = asyncio.create_task(
        limiter.submit(
            "discard",
            lambda: None,
            metadata=_metadata(
                priority=OutboundPriority.LOW,
                durability=OutboundDurability.BEST_EFFORT,
            ),
        )
    )
    await asyncio.sleep(0)
    durable = asyncio.create_task(
        limiter.submit(
            "durable",
            lambda: None,
            metadata=_metadata(priority=OutboundPriority.URGENT),
        )
    )
    await asyncio.sleep(0)
    with pytest.raises(MutationDroppedError):
        await best_effort
    blocker.set()
    await running
    await durable
    await limiter.close()

    full = TelegramOutboundLimiter(
        global_interval=100,
        per_chat_interval=100,
        max_queue_size=1,
        clock=clock,
        sleep=clock.sleep,
    )
    held = asyncio.create_task(
        full.submit("one", hold, metadata=_metadata(priority=OutboundPriority.NORMAL))
    )
    await asyncio.sleep(0)
    queued = asyncio.create_task(
        full.submit(
            "two", lambda: None, metadata=_metadata(priority=OutboundPriority.NORMAL)
        )
    )
    await asyncio.sleep(0)
    rejected = asyncio.create_task(
        full.submit(
            "three",
            lambda: None,
            metadata=_metadata(priority=OutboundPriority.NORMAL),
        )
    )
    await asyncio.sleep(0)
    with pytest.raises(QueueFullError):
        await rejected
    blocker.set()
    await held
    await queued
    await full.close()


@pytest.mark.asyncio
async def test_replaceable_work_can_displace_best_effort_work() -> None:
    clock = FakeClock()
    limiter = TelegramOutboundLimiter(
        global_interval=100,
        per_chat_interval=100,
        max_queue_size=1,
        clock=clock,
        sleep=clock.sleep,
    )
    gate = asyncio.Event()

    async def hold() -> None:
        await gate.wait()

    running = asyncio.create_task(
        limiter.submit(
            "running", hold, metadata=_metadata(priority=OutboundPriority.HIGH)
        )
    )
    await asyncio.sleep(0)
    best_effort = asyncio.create_task(
        limiter.submit(
            "discard",
            lambda: None,
            metadata=_metadata(
                priority=OutboundPriority.LOW,
                durability=OutboundDurability.BEST_EFFORT,
            ),
        )
    )
    await asyncio.sleep(0)
    replaceable = asyncio.create_task(
        limiter.submit(
            "draft",
            lambda: None,
            metadata=_metadata(
                priority=OutboundPriority.LOW,
                durability=OutboundDurability.REPLACEABLE,
                coalescing_key="draft",
            ),
        )
    )
    await asyncio.sleep(0)
    with pytest.raises(MutationDroppedError):
        await best_effort
    gate.set()
    await running
    await replaceable
    await limiter.close()


@pytest.mark.asyncio
async def test_replaceable_work_keeps_newest_pending_value() -> None:
    clock = FakeClock()
    limiter = TelegramOutboundLimiter(
        global_interval=100,
        per_chat_interval=100,
        max_queue_size=4,
        clock=clock,
        sleep=clock.sleep,
    )
    gate = asyncio.Event()
    started: list[str] = []

    async def hold() -> None:
        await gate.wait()

    running = asyncio.create_task(
        limiter.submit("chat", hold, metadata=_metadata(priority=OutboundPriority.HIGH))
    )
    await asyncio.sleep(0)
    stale = asyncio.create_task(
        limiter.submit(
            "chat",
            lambda: started.append("stale"),
            metadata=_metadata(
                priority=OutboundPriority.LOW,
                durability=OutboundDurability.REPLACEABLE,
                coalescing_key="draft",
            ),
        )
    )
    await asyncio.sleep(0)
    newest = asyncio.create_task(
        limiter.submit(
            "chat",
            lambda: started.append("newest"),
            metadata=_metadata(
                priority=OutboundPriority.LOW,
                durability=OutboundDurability.REPLACEABLE,
                coalescing_key="draft",
            ),
        )
    )
    await asyncio.sleep(0)
    with pytest.raises(MutationSupersededError):
        await stale
    gate.set()
    await running
    # Move the deterministic clock through the shared/per-chat wait.
    await newest
    assert started == ["newest"]
    await limiter.close()


@pytest.mark.asyncio
async def test_retry_after_extends_shared_and_chat_cooldowns() -> None:
    clock = FakeClock()
    starts: list[tuple[str, float]] = []
    limiter = TelegramOutboundLimiter(
        global_interval=0,
        per_chat_interval=0,
        retry_after_buffer=0.25,
        clock=clock,
        sleep=clock.sleep,
    )
    limiter.notify_retry_after(2.0, chat_key="chat-a")

    a = asyncio.create_task(
        limiter.submit("chat-a", lambda: starts.append(("a", clock.now)))
    )
    b = asyncio.create_task(
        limiter.submit("chat-b", lambda: starts.append(("b", clock.now)))
    )
    await asyncio.gather(a, b)
    assert starts == [("a", 2.25), ("b", 2.25)]
    await limiter.close()


@pytest.mark.asyncio
async def test_operation_failure_is_not_retried_but_retry_after_attribute_is_honored() -> (
    None
):
    clock = FakeClock()
    limiter = TelegramOutboundLimiter(
        global_interval=0,
        per_chat_interval=0,
        clock=clock,
        sleep=clock.sleep,
    )

    class FloodWaitError(RuntimeError):
        retry_after = 3.0

    calls = 0

    def fail() -> None:
        nonlocal calls
        calls += 1
        raise FloodWaitError("flood")

    failed = asyncio.create_task(limiter.submit("chat-a", fail))
    await asyncio.sleep(0)
    with pytest.raises(FloodWaitError):
        await failed
    assert calls == 1
    next_call = asyncio.create_task(limiter.submit("chat-b", lambda: clock.now))
    assert await next_call == 3.25
    await limiter.close()


@pytest.mark.asyncio
async def test_disabled_mode_calls_through_without_spacing_or_queueing() -> None:
    clock = FakeClock()
    calls: list[str] = []
    limiter = TelegramOutboundLimiter(
        global_interval=100,
        per_chat_interval=100,
        clock=clock,
        sleep=clock.sleep,
        enabled=False,
    )
    assert (
        await limiter.submit(
            "chat",
            lambda: calls.append("first"),
            metadata=_metadata(priority=OutboundPriority.LOW),
        )
        is None
    )
    assert (
        await limiter.submit(
            "chat",
            lambda: calls.append("second"),
            metadata=_metadata(priority=OutboundPriority.LOW),
        )
        is None
    )
    assert calls == ["first", "second"]
    assert clock.now == 0
    await limiter.close()


@pytest.mark.asyncio
async def test_shutdown_rejects_new_work_and_resolves_pending_work() -> None:
    clock = FakeClock()
    limiter = TelegramOutboundLimiter(
        global_interval=100,
        per_chat_interval=100,
        clock=clock,
        sleep=clock.sleep,
    )
    gate = asyncio.Event()
    await limiter.start()

    running = asyncio.create_task(limiter.submit("running", lambda: gate.wait()))
    await asyncio.sleep(0)
    pending = asyncio.create_task(limiter.submit("pending", lambda: None))
    await asyncio.sleep(0)
    closing = limiter.close()
    with pytest.raises(LimiterClosedError):
        await pending
    with pytest.raises(LimiterClosedError):
        await limiter.submit("new", lambda: None)
    gate.set()
    await running
    assert closing is not None
    await closing
    assert limiter.closed
