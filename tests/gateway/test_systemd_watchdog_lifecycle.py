"""Gateway lifecycle contract for the opt-in systemd watchdog."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from gateway.config import GatewayConfig
from gateway.run import GatewayRunner
from gateway.run_startup import GatewayStartupMixin


class _FakeWatchdog:
    instances: list["_FakeWatchdog"] = []

    def __init__(self, *, config_enabled: bool = True):
        self.config_enabled = config_enabled
        self.calls: list[str] = []
        self.__class__.instances.append(self)

    def start(self) -> bool:
        self.calls.append("start")
        return self.config_enabled

    def ready(self, status: str) -> bool:
        self.calls.append(f"ready:{status}")
        return True

    async def stop(self) -> None:
        self.calls.append("stop")


def _bare_runner(*, seconds: int, running: bool = True) -> GatewayRunner:
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig(systemd_watchdog_seconds=seconds)
    runner._running = running
    runner._systemd_watchdog = None
    return runner


def test_runner_starts_watchdog_only_after_running(monkeypatch):
    _FakeWatchdog.instances.clear()
    monkeypatch.setattr("gateway.systemd_notify.SystemdWatchdog", _FakeWatchdog)
    runner = _bare_runner(seconds=120, running=True)

    assert runner._start_systemd_watchdog() is True

    watchdog = _FakeWatchdog.instances[-1]
    assert watchdog.config_enabled is True
    assert watchdog.calls == ["start", "ready:Hermes Gateway running"]


def test_systemd_startup_waits_for_scheduler_readiness(monkeypatch):
    from cron.scheduler_provider import InProcessCronScheduler

    monkeypatch.setenv("NOTIFY_SOCKET", "notify-socket")
    monkeypatch.setenv("WATCHDOG_USEC", "1000000")
    notifications = []
    readiness = iter(({"status": "degraded"}, {"status": "ok"}))
    monkeypatch.setattr(
        "gateway.systemd_notify.notify",
        lambda message: notifications.append(message) or True,
    )
    monkeypatch.setattr(
        "gateway.scheduler_recovery.scheduler_readiness",
        lambda home, status: next(readiness),
    )
    monkeypatch.setattr("gateway.status.read_runtime_status", lambda: {})

    async def no_wait(_seconds):
        return None

    monkeypatch.setattr("gateway.run_startup.asyncio.sleep", no_wait)
    runner = SimpleNamespace(
        config=GatewayConfig(systemd_watchdog_seconds=120),
        _running=True,
        should_exit_cleanly=False,
    )

    asyncio.run(
        GatewayStartupMixin._await_scheduler_ready_for_systemd(
            runner,
            InProcessCronScheduler(),
        )
    )

    assert len(notifications) == 1
    assert notifications[0].startswith("EXTEND_TIMEOUT_USEC=")
    assert "READY=1" not in notifications[0]
