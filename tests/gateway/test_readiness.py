from __future__ import annotations

import sqlite3
import time

from gateway.readiness import collect_runtime_readiness
from gateway.scheduler_recovery import SchedulerProgress, current_host_boot_id


def test_collect_runtime_readiness_reports_healthy_local_runtime(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    (home / "config.yaml").write_text(
        "model:\n  provider: openrouter\n  model: test/model\n",
        encoding="utf-8",
    )
    with sqlite3.connect(home / "state.db") as conn:
        conn.execute("CREATE TABLE probe (id INTEGER PRIMARY KEY)")
    now = time.time()
    progress_dir = home / "cron"
    progress_dir.mkdir()
    progress = SchedulerProgress(
        pid=2345,
        start_time=6789,
        host_boot_id=current_host_boot_id(),
        heartbeat_sequence=3,
        tick_sequence=2,
        started_at=now - 120,
        heartbeat_at=now,
        tick_at=now,
    )
    (progress_dir / "ticker_progress.json").write_text(
        progress.model_dump_json(),
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(
        "gateway.readiness.memory_readiness_check", lambda: {"status": "ok"}
    )

    result = collect_runtime_readiness(
        configured_model="test/model",
        runtime_status={
            "gateway_state": "running",
            "pid": 2345,
            "start_time": 6789,
            "gateway_banner": "Gateway running",
            "scheduler_provider": "builtin",
            "platforms": {"telegram": {"state": "connected"}},
            "updated_at": "2026-07-09T00:00:00Z",
        },
        active_api_runs=2,
    )

    assert result["status"] == "ok"
    assert result["checks"]["state_db"]["status"] == "ok"
    assert result["checks"]["session_store"]["status"] == "ok"
    assert result["checks"]["config"]["status"] == "ok"
    assert result["checks"]["model"]["status"] == "ok"
    assert result["checks"]["gateway"]["status"] == "ok"
    assert result["checks"]["scheduler"]["status"] == "ok"
    assert result["checks"]["background_queues"]["active_api_runs"] == 2
    assert result["checks"]["disk"]["status"] in {"ok", "degraded"}


def test_collect_runtime_readiness_degrades_on_invalid_config_and_stopped_gateway(
    tmp_path, monkeypatch
):
    home = tmp_path / ".hermes"
    home.mkdir()
    (home / "config.yaml").write_text("model: [unterminated", encoding="utf-8")
    monkeypatch.setenv("HERMES_HOME", str(home))

    result = collect_runtime_readiness(
        configured_model="",
        runtime_status={"gateway_state": "stopped", "platforms": {}},
    )

    assert result["status"] == "degraded"
    assert result["checks"]["config"]["status"] == "degraded"
    assert result["checks"]["model"]["status"] == "degraded"
    assert result["checks"]["gateway"]["status"] == "degraded"
    # Readiness is diagnostic data, not an exception or a destructive repair.
    assert (home / "config.yaml").read_text(encoding="utf-8") == "model: [unterminated"


def test_readiness_requires_current_banner_and_two_fresh_scheduler_ticks(
    tmp_path,
    monkeypatch,
):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(
        "gateway.readiness.memory_readiness_check", lambda: {"status": "ok"}
    )
    runtime = {
        "gateway_state": "running",
        "pid": 12,
        "start_time": 34,
        "scheduler_provider": "builtin",
        "platforms": {},
    }

    no_banner = collect_runtime_readiness(
        configured_model="test/model", runtime_status=runtime
    )
    assert no_banner["checks"]["scheduler"]["status"] == "degraded"
    assert "banner" in no_banner["checks"]["scheduler"]["detail"]

    runtime["gateway_banner"] = "Gateway running"
    progress_dir = home / "cron"
    progress_dir.mkdir()
    now = time.time()
    one_tick = SchedulerProgress(
        pid=12,
        start_time=34,
        host_boot_id=current_host_boot_id(),
        heartbeat_sequence=2,
        tick_sequence=1,
        started_at=now - 60,
        heartbeat_at=now,
        tick_at=now,
    )
    progress_path = progress_dir / "ticker_progress.json"
    progress_path.write_text(one_tick.model_dump_json(), encoding="utf-8")
    before_two_ticks = collect_runtime_readiness(
        configured_model="test/model",
        runtime_status=runtime,
    )
    assert before_two_ticks["checks"]["scheduler"]["status"] == "degraded"
    assert before_two_ticks["checks"]["scheduler"]["tick_sequence"] == 1

    two_ticks = one_tick.model_copy(update={"tick_sequence": 2})
    progress_path.write_text(two_ticks.model_dump_json(), encoding="utf-8")
    after_two_ticks = collect_runtime_readiness(
        configured_model="test/model",
        runtime_status=runtime,
    )
    assert after_two_ticks["checks"]["scheduler"]["status"] == "ok"


def test_readiness_uses_running_session_store_state_over_independent_probe(
    tmp_path, monkeypatch
):
    home = tmp_path / ".hermes"
    home.mkdir()
    with sqlite3.connect(home / "state.db") as conn:
        conn.execute("CREATE TABLE probe (id INTEGER PRIMARY KEY)")
    monkeypatch.setenv("HERMES_HOME", str(home))

    unavailable = collect_runtime_readiness(
        configured_model="test/model",
        runtime_status={
            "gateway_state": "running",
            "platforms": {},
            "session_store": {"status": "unavailable"},
        },
    )

    assert unavailable["checks"]["state_db"]["status"] == "ok"
    assert unavailable["checks"]["session_store"] == {"status": "unavailable"}
    assert unavailable["status"] == "degraded"

    recovered = collect_runtime_readiness(
        configured_model="test/model",
        runtime_status={
            "gateway_state": "running",
            "platforms": {},
            "session_store": {"status": "ok"},
        },
    )
    assert recovered["checks"]["session_store"] == {"status": "ok"}
