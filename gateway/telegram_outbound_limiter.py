"""Runtime-neutral async scheduling for Telegram outbound mutations.

The limiter owns one in-process budget for one Telegram bot credential.  Callers
provide an already-normalized chat/route key and a zero-argument operation.  It
does not know about python-telegram-bot, retries, payloads, or persistence.

An operation is started at most once.  Exceptions, including network timeouts,
are returned to the caller unchanged; the limiter never centrally replays an
ambiguous mutation.  A caller that observes a Telegram ``retry_after`` can
extend the shared cooldown with :meth:`notify_retry_after`.
"""

from __future__ import annotations

import asyncio
import inspect
import math
import time
from collections import deque
from collections.abc import Awaitable, Callable, Hashable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast

from gateway.telegram_outbound_contract import (
    OutboundDurability,
    OutboundPriority,
    RetryAmbiguity,
    TelegramMutation,
    TelegramOutboundMetadata,
)

T = TypeVar("T")

MutationOperation = Callable[[], T | Awaitable[T]]
Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[None] | None]


class TelegramLimiterError(RuntimeError):
    """Base class for admission and lifecycle failures."""


class LimiterClosedError(TelegramLimiterError):
    """A mutation was submitted after the limiter began shutting down."""


class QueueFullError(TelegramLimiterError):
    """A durable mutation could not be admitted without dropping durable work."""


class MutationDroppedError(TelegramLimiterError):
    """Discardable work was rejected by queue pressure."""


class MutationSupersededError(MutationDroppedError):
    """A pending replaceable mutation was replaced by a newer value."""


class LimiterWorkerError(TelegramLimiterError):
    """The scheduler stopped because its clock or sleep dependency failed."""


@dataclass(slots=True)
class _QueuedMutation(Generic[T]):
    chat_key: Hashable
    operation: MutationOperation[T]
    metadata: TelegramOutboundMetadata
    sequence: int
    future: asyncio.Future[T]


class TelegramOutboundLimiter:
    """Shared async spacing limiter for one Telegram bot credential.

    ``chat_key`` is deliberately opaque.  The Telegram adapter will eventually
    supply a key containing the bot/profile/chat/topic route, but this module
    does not derive or persist one.

    ``submit`` is an async method and accepts a zero-argument callable.  The
    callable is invoked only when its mutation is dispatched.  The returned
    value is the callable's value, and its exceptions are propagated without a
    retry.

    Queue capacity counts pending, not already-dispatched, operations.  Pending
    replaceable work is coalesced by ``metadata.coalescing_key``.  When a
    durable mutation arrives at a full queue, discardable pending work may be
    evicted to make room; if no such work exists, ``QueueFullError`` is raised.
    """

    DEFAULT_GLOBAL_INTERVAL = 1.0 / 25.0
    DEFAULT_PER_CHAT_INTERVAL = 1.0
    DEFAULT_RETRY_AFTER_BUFFER = 0.250
    DEFAULT_MAX_QUEUE_SIZE = 512

    def __init__(
        self,
        *,
        global_interval: float = DEFAULT_GLOBAL_INTERVAL,
        per_chat_interval: float = DEFAULT_PER_CHAT_INTERVAL,
        retry_after_buffer: float = DEFAULT_RETRY_AFTER_BUFFER,
        max_queue_size: int = DEFAULT_MAX_QUEUE_SIZE,
        clock: Clock = time.monotonic,
        sleep: Sleep = asyncio.sleep,
        enabled: bool = True,
        disabled: bool | None = None,
    ) -> None:
        if disabled is not None:
            enabled = not disabled
        self._enabled = bool(enabled)
        self._global_interval = self._validate_interval(
            global_interval, "global_interval"
        )
        self._per_chat_interval = self._validate_interval(
            per_chat_interval, "per_chat_interval"
        )
        self._retry_after_buffer = self._validate_interval(
            retry_after_buffer, "retry_after_buffer"
        )
        if not isinstance(max_queue_size, int) or isinstance(max_queue_size, bool):
            raise TypeError("max_queue_size must be an integer")
        if max_queue_size < 1:
            raise ValueError("max_queue_size must be at least 1")
        self._max_queue_size = max_queue_size
        self._clock = clock
        self._sleep = sleep

        self._pending_by_chat: dict[Hashable, deque[_QueuedMutation[Any]]] = {}
        self._coalesced: dict[str, _QueuedMutation[Any]] = {}
        self._pending_count = 0
        self._sequence = 0
        self._active_chats: set[Hashable] = set()
        self._inflight: set[asyncio.Task[Any]] = set()

        # These are the earliest times at which a new request may start.  A
        # retry-after extension uses the same values as normal spacing, so an
        # extension can never accidentally shorten an existing budget.
        self._global_available_at = 0.0
        self._chat_available_at: dict[Hashable, float] = {}

        self._closed = False
        self._wake = asyncio.Event()
        self._worker: asyncio.Task[None] | None = None

    @staticmethod
    def _validate_interval(value: float, name: str) -> float:
        if isinstance(value, bool):
            raise TypeError(f"{name} must be a finite non-negative number")
        try:
            value = float(value)
        except (TypeError, ValueError) as exc:
            raise TypeError(f"{name} must be a finite non-negative number") from exc
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{name} must be a finite non-negative number")
        return value

    @property
    def enabled(self) -> bool:
        """Whether spacing and queueing are active."""

        return self._enabled

    @property
    def closed(self) -> bool:
        """Whether new submissions are rejected."""

        return self._closed

    @property
    def pending_count(self) -> int:
        """Number of admitted operations waiting to be dispatched."""

        return self._pending_count

    @property
    def queue_size(self) -> int:
        """Alias for :attr:`pending_count` used by queue observers."""

        return self._pending_count

    @property
    def cooldown_until(self) -> float:
        """Earliest clock value at which the shared budget may start work."""

        return self._global_available_at

    def start(self) -> Awaitable[None]:
        """Start the scheduler task, if enabled and not already running.

        Starting is idempotent.  ``submit`` starts it lazily as well, which
        keeps the lifecycle explicit without making a forgotten ``start`` call
        strand an admitted mutation.
        """

        if not self._enabled or self._closed:
            return self._completed_awaitable()
        if self._worker is not None and not self._worker.done():
            return self._completed_awaitable()
        loop = asyncio.get_running_loop()
        self._worker = loop.create_task(
            self._run_loop(), name="telegram-outbound-limiter"
        )
        return self._completed_awaitable()

    @staticmethod
    def _completed_awaitable() -> asyncio.Future[None]:
        loop = asyncio.get_running_loop()
        completed: asyncio.Future[None] = loop.create_future()
        completed.set_result(None)
        return completed

    async def submit(
        self,
        chat_key: Hashable | MutationOperation[T] | None = None,
        operation: MutationOperation[T] | None = None,
        *,
        chat_id: Hashable | None = None,
        callback: MutationOperation[T] | None = None,
        metadata: TelegramOutboundMetadata | None = None,
        priority: OutboundPriority | int | None = None,
        durability: OutboundDurability | str | None = None,
        coalescing_key: str | None = None,
        retry_ambiguity: RetryAmbiguity | str = RetryAmbiguity.NONE,
        mutation: TelegramMutation = TelegramMutation.FINAL_TEXT,
    ) -> T:
        """Admit and await one mutation.

        The primary form is ``submit(chat_key, operation, ...)``.  The
        ``chat_id=`` and ``callback=`` spellings are accepted so adapter
        integrations can use their existing vocabulary without making this
        module Telegram-specific.  A callable passed as the first positional
        argument is also accepted when ``chat_id=`` is supplied.
        """

        if operation is None and callback is not None:
            operation = callback
        if operation is None and callable(chat_key) and chat_id is not None:
            operation = cast(MutationOperation[T], chat_key)
            chat_key = chat_id
        elif chat_key is None and chat_id is not None:
            chat_key = chat_id

        if operation is None or not callable(operation):
            raise TypeError("submit requires a callable operation")
        if chat_key is None:
            raise TypeError("submit requires a chat_key")
        try:
            hash(chat_key)
        except TypeError as exc:
            raise TypeError("chat_key must be hashable") from exc

        if self._closed:
            raise LimiterClosedError("Telegram mutation limiter is closed")

        if not self._enabled:
            return await self._invoke(operation)

        request_metadata = self._metadata(
            metadata=metadata,
            priority=priority,
            durability=durability,
            coalescing_key=coalescing_key,
            retry_ambiguity=retry_ambiguity,
            mutation=mutation,
        )
        loop = asyncio.get_running_loop()
        future: asyncio.Future[T] = loop.create_future()
        request = _QueuedMutation(
            chat_key=chat_key,
            operation=operation,
            metadata=request_metadata,
            sequence=self._sequence,
            future=future,
        )
        self._sequence += 1
        self._admit(request)
        self.start()
        self._wake.set()

        try:
            return await future
        except asyncio.CancelledError:
            # A caller withdrawing before dispatch must not leave its mutation
            # in the queue.  Once dispatched, _remove_pending is a no-op and
            # the already-started operation is intentionally not cancelled.
            self._remove_pending(request)
            raise

    @staticmethod
    def _metadata(
        *,
        metadata: TelegramOutboundMetadata | None,
        priority: OutboundPriority | int | None,
        durability: OutboundDurability | str | None,
        coalescing_key: str | None,
        retry_ambiguity: RetryAmbiguity | str,
        mutation: TelegramMutation,
    ) -> TelegramOutboundMetadata:
        resolved_ambiguity = RetryAmbiguity(retry_ambiguity)
        if metadata is not None:
            if (
                any(
                    value is not None
                    for value in (priority, durability, coalescing_key)
                )
                or resolved_ambiguity is not RetryAmbiguity.NONE
            ):
                raise ValueError(
                    "metadata cannot be combined with direct mutation metadata"
                )
            return metadata

        if durability is None:
            resolved_durability = OutboundDurability.DURABLE
        else:
            resolved_durability = OutboundDurability(durability)
        resolved_priority: OutboundPriority | int
        if priority is None:
            resolved_priority = OutboundPriority.NORMAL
        else:
            try:
                resolved_priority = OutboundPriority(priority)
            except (TypeError, ValueError):
                resolved_priority = int(priority)
        return TelegramOutboundMetadata(
            operation=mutation,
            priority=resolved_priority,  # type: ignore[arg-type]
            coalescing_key=coalescing_key,
            durability=resolved_durability,
            retry_ambiguity=resolved_ambiguity,
        )

    def _admit(self, request: _QueuedMutation[Any]) -> None:
        if request.metadata.durability is OutboundDurability.REPLACEABLE:
            key = request.metadata.coalescing_key
            assert key is not None
            prior = self._coalesced.get(key)
            if prior is not None:
                self._remove_pending(prior)
                self._complete_exception(
                    prior,
                    MutationSupersededError(
                        "pending replaceable mutation was superseded"
                    ),
                )

        if self._pending_count >= self._max_queue_size:
            if request.metadata.durability is OutboundDurability.DURABLE:
                evicted = self._evict_discardable()
                if not evicted:
                    raise QueueFullError(
                        "Telegram mutation queue is full of durable work"
                    )
            elif request.metadata.durability is OutboundDurability.REPLACEABLE:
                evicted = self._evict_discardable(best_effort_only=True)
                if not evicted:
                    raise MutationDroppedError(
                        "replaceable Telegram mutation dropped because the queue is full"
                    )
            else:
                raise MutationDroppedError(
                    "discardable Telegram mutation dropped because the queue is full"
                )

        self._pending_by_chat.setdefault(request.chat_key, deque()).append(request)
        self._pending_count += 1
        key = request.metadata.coalescing_key
        if (
            request.metadata.durability is OutboundDurability.REPLACEABLE
            and key is not None
        ):
            self._coalesced[key] = request

    def _evict_discardable(self, *, best_effort_only: bool = False) -> bool:
        candidate: _QueuedMutation[Any] | None = None
        for queue in self._pending_by_chat.values():
            for request in queue:
                if request.metadata.durability is OutboundDurability.DURABLE:
                    continue
                if (
                    best_effort_only
                    and request.metadata.durability
                    is not OutboundDurability.BEST_EFFORT
                ):
                    continue
                if candidate is None or self._eviction_key(
                    request
                ) > self._eviction_key(candidate):
                    candidate = request
        if candidate is None:
            return False
        self._remove_pending(candidate)
        self._complete_exception(
            candidate,
            MutationDroppedError(
                "discardable Telegram mutation evicted for durable work"
            ),
        )
        return True

    @staticmethod
    def _eviction_key(request: _QueuedMutation[Any]) -> tuple[int, int, int]:
        # Best-effort work is less valuable than replaceable work.  Within a
        # durability class, lower-priority and newer work is the easiest to
        # discard under pressure.
        discardability = (
            1 if request.metadata.durability is OutboundDurability.BEST_EFFORT else 0
        )
        return (
            discardability,
            int(request.metadata.priority),
            request.sequence,
        )

    def _remove_pending(self, request: _QueuedMutation[Any]) -> bool:
        queue = self._pending_by_chat.get(request.chat_key)
        if queue is None:
            return False
        for index, candidate in enumerate(queue):
            if candidate is request:
                del queue[index]
                self._pending_count -= 1
                if not queue:
                    self._pending_by_chat.pop(request.chat_key, None)
                key = request.metadata.coalescing_key
                if key is not None and self._coalesced.get(key) is request:
                    self._coalesced.pop(key, None)
                return True
        return False

    @staticmethod
    def _complete_exception(
        request: _QueuedMutation[Any], error: BaseException
    ) -> None:
        if not request.future.done():
            request.future.set_exception(error)

    def notify_retry_after(
        self,
        retry_after_seconds: float,
        chat_key: Hashable | None = None,
        *,
        chat_id: Hashable | None = None,
    ) -> float:
        """Extend the shared cooldown and return its absolute deadline.

        Telegram flood waits are deliberately not retried here.  The caller
        reports the wait after its operation has failed, and the next admitted
        mutation observes the extended budget.  A supplied chat key extends
        that chat's budget as well as the shared bot budget.
        """

        if chat_key is None:
            chat_key = chat_id
        try:
            seconds = float(retry_after_seconds)
        except (TypeError, ValueError) as exc:
            raise TypeError("retry_after_seconds must be a finite number") from exc
        if not math.isfinite(seconds) or seconds < 0:
            raise ValueError("retry_after_seconds must be a finite number")
        if chat_key is not None:
            try:
                hash(chat_key)
            except TypeError as exc:
                raise TypeError("chat_key must be hashable") from exc
        deadline = self._clock() + seconds + self._retry_after_buffer
        self._global_available_at = max(self._global_available_at, deadline)
        if chat_key is not None:
            self._chat_available_at[chat_key] = max(
                self._chat_available_at.get(chat_key, 0.0), deadline
            )
        self._wake.set()
        return deadline

    # Names used by callers that describe the same boundary event differently.
    retry_after = notify_retry_after
    record_retry_after = notify_retry_after
    extend_cooldown = notify_retry_after

    def close(self) -> Awaitable[None]:
        """Reject new work, fail pending futures, and await dispatched work.

        The method changes state synchronously and returns the scheduler task
        when one exists, so both ``limiter.close()`` and
        ``await limiter.close()`` have explicit, useful shutdown semantics.
        Pending durable work receives ``LimiterClosedError`` and is never
        replayed by this in-memory component.  Already-dispatched operations
        are allowed to finish; their result or exception remains authoritative.
        """

        if not self._closed:
            self._closed = True
            self._fail_pending(
                LimiterClosedError("Telegram mutation limiter closed before dispatch")
            )
            self._wake.set()

        if self._worker is not None:
            return self._worker
        return self._completed_awaitable()

    async def __aenter__(self) -> TelegramOutboundLimiter:
        self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    def _fail_pending(self, error: BaseException) -> None:
        for queue in tuple(self._pending_by_chat.values()):
            while queue:
                request = queue.popleft()
                self._pending_count -= 1
                self._complete_exception(request, error)
        self._pending_by_chat.clear()
        self._coalesced.clear()

    def _select_request(
        self,
    ) -> tuple[_QueuedMutation[Any] | None, float | None]:
        now = self._clock()
        selected: _QueuedMutation[Any] | None = None
        next_ready_at: float | None = None

        for chat_key, queue in self._pending_by_chat.items():
            if not queue or chat_key in self._active_chats:
                continue
            # The head is the only eligible mutation for a chat.  Priorities
            # choose among chats, but cannot reorder a chat's mutation stream.
            request = queue[0]
            ready_at = max(
                self._global_available_at,
                self._chat_available_at.get(chat_key, 0.0),
            )
            if ready_at <= now:
                if selected is None or (
                    int(request.metadata.priority),
                    request.sequence,
                ) < (
                    int(selected.metadata.priority),
                    selected.sequence,
                ):
                    selected = request
            elif next_ready_at is None or ready_at < next_ready_at:
                next_ready_at = ready_at

        if selected is not None:
            return selected, 0.0
        if next_ready_at is None:
            return None, None
        return None, max(0.0, next_ready_at - now)

    def _dispatch(self, request: _QueuedMutation[Any]) -> None:
        queue = self._pending_by_chat[request.chat_key]
        assert queue and queue[0] is request
        queue.popleft()
        self._pending_count -= 1
        if not queue:
            self._pending_by_chat.pop(request.chat_key, None)
        key = request.metadata.coalescing_key
        if key is not None and self._coalesced.get(key) is request:
            self._coalesced.pop(key, None)

        now = self._clock()
        self._global_available_at = max(now, self._global_available_at) + (
            self._global_interval
        )
        self._chat_available_at[request.chat_key] = (
            max(now, self._chat_available_at.get(request.chat_key, 0.0))
            + self._per_chat_interval
        )
        self._active_chats.add(request.chat_key)
        task = asyncio.create_task(self._execute(request))
        self._inflight.add(task)

    async def _execute(self, request: _QueuedMutation[Any]) -> None:
        current = asyncio.current_task()
        try:
            result = await self._invoke(request.operation)
        except asyncio.CancelledError:
            if not request.future.done():
                request.future.cancel()
        except Exception as exc:
            retry_after = getattr(exc, "retry_after", None)
            if retry_after is not None:
                try:
                    self.notify_retry_after(retry_after, chat_key=request.chat_key)
                except (TypeError, ValueError):
                    # An operation-specific exception may expose a non-numeric
                    # attribute.  Its original exception remains authoritative.
                    pass
            self._complete_exception(request, exc)
        else:
            if not request.future.done():
                request.future.set_result(result)
        finally:
            self._active_chats.discard(request.chat_key)
            if current is not None:
                self._inflight.discard(current)
            self._wake.set()

    @staticmethod
    async def _invoke(operation: MutationOperation[T]) -> T:
        result = operation()
        if inspect.isawaitable(result):
            return cast(T, await result)
        return cast(T, result)

    async def _run_loop(self) -> None:
        try:
            while True:
                if self._closed:
                    if not self._inflight:
                        return
                    await asyncio.gather(*tuple(self._inflight), return_exceptions=True)
                    continue

                request, delay = self._select_request()
                if request is not None:
                    self._dispatch(request)
                elif delay is None:
                    await self._wait_for_wake()
                else:
                    await self._wait_for_delay_or_wake(delay)
        except asyncio.CancelledError:
            if not self._closed:
                self._fail_pending(
                    LimiterClosedError("Telegram limiter scheduler was cancelled")
                )
            raise
        except Exception as exc:
            error = LimiterWorkerError("Telegram limiter scheduler stopped")
            error.__cause__ = exc
            self._fail_pending(error)

    async def _wait_for_wake(self) -> None:
        if self._wake.is_set():
            self._wake.clear()
            return
        await self._wake.wait()
        self._wake.clear()

    async def _wait_for_delay_or_wake(self, delay: float) -> None:
        sleeper = asyncio.create_task(self._sleep_for(delay))
        wake = asyncio.create_task(self._wake.wait())
        done, pending = await asyncio.wait(
            (sleeper, wake), return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if wake in done:
            self._wake.clear()
        if sleeper in done:
            sleeper.result()

    async def _sleep_for(self, delay: float) -> None:
        result = self._sleep(delay)
        if inspect.isawaitable(result):
            await result


TelegramMutationLimiter = TelegramOutboundLimiter
TelegramRateLimiter = TelegramOutboundLimiter

__all__ = [
    "LimiterClosedError",
    "LimiterWorkerError",
    "MutationDroppedError",
    "MutationSupersededError",
    "QueueFullError",
    "TelegramLimiterError",
    "TelegramMutationLimiter",
    "TelegramOutboundLimiter",
    "TelegramRateLimiter",
]
