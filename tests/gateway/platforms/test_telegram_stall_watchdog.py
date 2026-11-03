"""Polling stall watchdog regression tests (silent Telegram getUpdates wedge).

Observed 2026-09-25/27/28: the getUpdates long-poll stalled for 2-28 minutes
with no error_callback and no log line — no completed ``getUpdates`` call,
so neither the reconnect ladder nor the post-reconnect heartbeat probe
fired.  The stall watchdog converts "no completed getUpdates in
POLL_STALL_THRESHOLD seconds" into a re-entry of the existing reconnect
ladder (``_handle_polling_network_error``).

Contract pinned here, on a stub adapter that bypasses the heavy __init__:

- the watchdog fires exactly when the liveness stamp is stale AND no
  recovery is already in flight AND the updater claims running;
- it stays quiet while a reconnect/conflict ladder is in flight, while the
  updater is stopped, or while the stamp is fresh;
- the liveness hook stamps only on successful completion and is idempotent
  (no double-wrapping across repeated installs).
"""
import asyncio
import time

import pytest

from gateway.platforms.telegram import TelegramAdapter


class _StubAdapter(TelegramAdapter):
    """Instance-free adapter exposing just the watchdog surface."""

    has_fatal_error = False  # shadow the base-class property
    fatal_error_code = None
    name = "tg-stub"  # shadow the base-class property

    def __init__(self):
        # Deliberately skip super().__init__() — heavy config wiring.
        self._reconnects = []

        class _Updater:
            running = True

        class _App:
            pass

        self._app = _App()
        self._app.updater = _Updater()
        self._polling_error_task = None
        self._polling_conflict_count = 0
        self._polling_network_error_count = 0
        self._last_poll_completed_at = 0.0
        self._stall_watchdog_task = None
        self._background_tasks = set()
        # Fast clocks so tests don't wait on real 30s/90s constants.
        self.POLL_STALL_CHECK_INTERVAL = 0.01
        self.POLL_STALL_THRESHOLD = 0.05

    async def _handle_polling_network_error(self, error):  # type: ignore[override]
        self._reconnects.append(error)


async def _run_loop_briefly(obj, seconds=0.06):
    task = asyncio.ensure_future(obj._stall_watchdog_loop())
    await asyncio.sleep(seconds)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_watchdog_fires_on_stale_stamp_and_stays_quiet_when_fresh():
    async def scenario():
        # Stale stamp → must force a reconnect via the existing ladder.
        stale = _StubAdapter()
        stale._last_poll_completed_at = time.monotonic() - 500
        await _run_loop_briefly(stale)
        assert stale._reconnects, "watchdog must fire when the stamp is stale"
        assert isinstance(stale._reconnects[0], TimeoutError)

        # Fresh stamp → must stay quiet.
        fresh = _StubAdapter()
        fresh.POLL_STALL_THRESHOLD = 5.0
        fresh._last_poll_completed_at = time.monotonic()
        await _run_loop_briefly(fresh)
        assert fresh._reconnects == []

    asyncio.run(scenario())


def test_watchdog_skips_while_recovery_in_flight_or_updater_stopped():
    async def scenario():
        # Network ladder already advancing (count > 0) → no extra attempt.
        inflight = _StubAdapter()
        inflight._last_poll_completed_at = time.monotonic() - 500
        inflight._polling_network_error_count = 2
        await _run_loop_briefly(inflight)
        assert inflight._reconnects == []

        # Conflict ladder in flight → no extra attempt.
        conflict = _StubAdapter()
        conflict._last_poll_completed_at = time.monotonic() - 500
        conflict._polling_conflict_count = 1
        await _run_loop_briefly(conflict)
        assert conflict._reconnects == []

        # Updater not running (mid-reconnect) → no extra attempt.
        stopped = _StubAdapter()
        stopped._last_poll_completed_at = time.monotonic() - 500
        stopped._app.updater.running = False
        await _run_loop_briefly(stopped)
        assert stopped._reconnects == []

    asyncio.run(scenario())


def test_liveness_hook_stamps_only_on_success_and_is_idempotent():
    async def scenario():
        obj = _StubAdapter()
        calls = {"n": 0}

        class _Bot:
            async def get_updates(self, **kwargs):
                calls["n"] += 1
                return ["update"]

        obj._app.bot = _Bot()
        obj._install_poll_liveness_hook()
        obj._install_poll_liveness_hook()  # idempotent — must not double-wrap

        obj._last_poll_completed_at = 0.0
        out = await obj._app.bot.get_updates(offset=1)
        assert out == ["update"]
        assert calls["n"] == 1, "hook must not double-wrap the bound method"
        assert obj._last_poll_completed_at > 0.0, "success must stamp"

        class _FailingBot:
            async def get_updates(self, **kwargs):
                raise TimeoutError("hung poll")

        failing = _StubAdapter()
        failing._app.bot = _FailingBot()
        failing._install_poll_liveness_hook()
        failing._last_poll_completed_at = 0.0
        with pytest.raises(TimeoutError):
            await failing._app.bot.get_updates()
        assert failing._last_poll_completed_at == 0.0, "failure must NOT stamp"

    asyncio.run(scenario())


def test_ensure_arms_once_and_refreshes_stamp():
    async def scenario():
        obj = _StubAdapter()
        obj.POLL_STALL_THRESHOLD = 5.0
        obj._last_poll_completed_at = 12.0

        obj._ensure_stall_watchdog()
        first = obj._stall_watchdog_task
        assert first is not None
        assert obj._last_poll_completed_at > 12.0, "arm must refresh the stamp"

        obj._ensure_stall_watchdog()
        assert obj._stall_watchdog_task is first, "ensure must be idempotent"

        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

    asyncio.run(scenario())
