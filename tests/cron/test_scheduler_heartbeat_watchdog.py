"""Scheduler publication and standalone watchdog contracts, with no live gateway."""

import importlib.util
import json
import math
import os
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from cron import scheduler


WATCHDOG = Path("/home/rmax-10/.hermes/scripts/gateway-freeze-watchdog.py")
NOW = 1_793_691_000.0


@pytest.fixture
def scheduler_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(scheduler, "_heartbeat_pid", None)
    monkeypatch.setattr(scheduler, "_heartbeat_seq", 0)
    monkeypatch.setattr(scheduler, "_heartbeat_failures", 0)
    monkeypatch.setattr(scheduler, "_running_job_ids", set())
    monkeypatch.setattr(scheduler, "get_due_jobs", lambda: [])
    monkeypatch.setattr(scheduler, "load_config", lambda: {})
    monkeypatch.setattr(scheduler, "_get_lock_paths",
                        lambda: (tmp_path / "cron", tmp_path / "cron/.tick.lock"))
    monkeypatch.setitem(sys.modules, "tools.mcp_tool", SimpleNamespace(
        _kill_orphaned_mcp_children=lambda: None,
    ))
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux ownership/start identity regression tests")
    start = scheduler._process_start_id(os.getpid())
    (tmp_path / "gateway.pid").write_text(json.dumps({
        "pid": os.getpid(), "start_time": int(start), "kind": "hermes-gateway",
    }))
    return tmp_path


@pytest.mark.parametrize("verbose", [False, True])
def test_idle_completed_pass_publishes_and_sequence_advances(scheduler_home, verbose):
    path = scheduler.heartbeat_path()
    assert path == scheduler_home / "state/scheduler-heartbeat.json"
    for seq in (1, 2):
        assert scheduler.tick(verbose=verbose, sync=False) == 0
        heartbeat = json.loads(path.read_text())
        assert heartbeat == {
            "schema": 1, "ts": heartbeat["ts"], "pid": os.getpid(),
            "process_start_id": scheduler._process_start_id(os.getpid()),
            "tick_seq": seq, "due_count": 0, "in_flight_count": 0,
            "oldest_lease_age_s": None, "provider": "gateway",
        }
        assert math.isfinite(heartbeat["ts"])


def test_exception_does_not_publish_progress(scheduler_home, monkeypatch):
    scheduler.tick(verbose=False, sync=False)
    before = scheduler.heartbeat_path().read_bytes()

    def fail():
        raise RuntimeError("control pass failed")

    monkeypatch.setattr(scheduler, "get_due_jobs", fail)
    with pytest.raises(RuntimeError, match="control pass failed"):
        scheduler.tick(verbose=False, sync=False)
    assert scheduler.heartbeat_path().read_bytes() == before


def test_lock_contention_does_not_publish(scheduler_home, monkeypatch):
    if scheduler.fcntl is None:
        pytest.skip("POSIX lock test")

    def busy(*args):
        raise BlockingIOError("already locked")

    monkeypatch.setattr(scheduler.fcntl, "flock", busy)
    assert scheduler.tick(verbose=False, sync=False) == 0
    assert not scheduler.heartbeat_path().exists()


@pytest.mark.parametrize("provider,sync", [
    ("manual", False), ("test", True), ("", False), (None, True),
])
def test_other_callers_do_not_publish(scheduler_home, provider, sync):
    scheduler.tick(verbose=False, sync=sync, provider=provider)
    assert not scheduler.heartbeat_path().exists()


@pytest.mark.parametrize("record", [
    {"pid": 987654321, "start_time": 1},
    {"pid": str(os.getpid()), "start_time": 1},
    {"pid": os.getpid(), "start_time": -1},
    {"pid": os.getpid()},
    None,
])
def test_gateway_label_cannot_bypass_pid_ownership(scheduler_home, record):
    (scheduler_home / "gateway.pid").write_text(json.dumps(record))
    scheduler.tick(verbose=False, sync=False, provider="gateway")
    assert not scheduler.heartbeat_path().exists()


def test_dispatch_counts_only_after_control_pass(scheduler_home, monkeypatch):
    jobs = [{"id": "a"}, {"id": "b", "workdir": "/unused-test-workdir"}]
    monkeypatch.setattr(scheduler, "get_due_jobs", lambda: jobs)
    monkeypatch.setattr(scheduler, "advance_next_run", lambda job_id: None)

    class Pool:
        def submit(self, worker):
            # Leave the jobs in flight, without invoking any actual job code.
            future = scheduler.concurrent.futures.Future()
            assert not scheduler.heartbeat_path().exists()
            return future

    monkeypatch.setattr(scheduler, "_get_parallel_pool", lambda workers: Pool())
    monkeypatch.setattr(scheduler, "_get_sequential_pool", lambda: Pool())
    assert scheduler.tick(verbose=False, sync=False) == 2
    heartbeat = json.loads(scheduler.heartbeat_path().read_text())
    assert heartbeat["due_count"] == heartbeat["in_flight_count"] == len(jobs)


def test_job_held_lock_cannot_block_heartbeat(scheduler_home):
    # Publication snapshots len(set), not the job-worker lock.
    with scheduler._running_lock:
        scheduler._publish_heartbeat(0, "gateway")
    assert scheduler.heartbeat_path().exists()


def test_write_failures_do_not_abort_tick_and_are_rate_limited(
    scheduler_home, monkeypatch, caplog,
):
    def fail(*args):
        raise OSError("simulated disk full")

    monkeypatch.setattr(scheduler.os, "replace", fail)
    for _ in range(61):
        assert scheduler.tick(verbose=False, sync=False) == 0
    assert not scheduler.heartbeat_path().exists()
    assert not list((scheduler_home / "state").glob(".scheduler-heartbeat-*"))
    assert sum("heartbeat publication failed" in record.message
               for record in caplog.records) == 2


def test_fsync_replace_and_directory_fsync_order(scheduler_home, monkeypatch):
    events = []
    real_fsync, real_replace = scheduler.os.fsync, scheduler.os.replace

    def fsync(fd):
        events.append("fsync")
        real_fsync(fd)

    def replace(source, target):
        # The tempfile is already a complete JSON document before becoming visible.
        assert json.loads(Path(source).read_text())["tick_seq"] == 1
        assert Path(source).parent == Path(target).parent
        events.append("replace")
        real_replace(source, target)

    monkeypatch.setattr(scheduler.os, "fsync", fsync)
    monkeypatch.setattr(scheduler.os, "replace", replace)
    scheduler._publish_heartbeat(0, "gateway")
    assert events == ["fsync", "replace", "fsync"]


def test_atomic_writes_never_expose_partial_json(scheduler_home):
    scheduler._publish_heartbeat(0, "gateway")
    stop = threading.Event()
    started = threading.Event()
    errors, observed = [], []

    def read_loop():
        try:
            while not stop.is_set():
                heartbeat = json.loads(scheduler.heartbeat_path().read_text())
                assert heartbeat["schema"] == 1
                assert heartbeat["due_count"] + 1 == heartbeat["tick_seq"]
                observed.append(heartbeat["tick_seq"])
                started.set()
        except Exception as exc:
            errors.append(exc)
            started.set()

    reader = threading.Thread(target=read_loop)
    reader.start()
    try:
        assert started.wait(timeout=1)
        for count in range(1, 101):
            scheduler._publish_heartbeat(count, "gateway")
    finally:
        stop.set()
        reader.join(timeout=1)
    assert not reader.is_alive()
    assert not errors
    assert observed
    assert json.loads(scheduler.heartbeat_path().read_text())["tick_seq"] == 101


@pytest.fixture
def watchdog(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("heartbeat_watchdog_test", WATCHDOG)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "PROC", tmp_path / "proc")
    monkeypatch.setattr(module, "PID_FILE", tmp_path / "gateway.pid")
    monkeypatch.setattr(module, "STATE", tmp_path / "state/watchdog.json")
    monkeypatch.setattr(module, "HEARTBEAT", scheduler.heartbeat_path())
    monkeypatch.setattr(module, "JOBS", tmp_path / "jobs.json")
    monkeypatch.setattr(module, "LOG", tmp_path / "logs/watchdog.log")
    module.LOG.parent.mkdir(parents=True)
    monkeypatch.setattr(module.time, "time", lambda: NOW)
    monkeypatch.setattr(module, "VERIFY_S", 0)
    module.calls = []
    module.emails = []

    def run(command, **kwargs):
        module.calls.append((command, kwargs))
        assert command[0] == "kill", "unexpected subprocess (no network allowed)"
        assert command == ["kill", "-USR1", str(module.gateway_owner()["pid"])]
        assert kwargs["timeout"] == 5
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(module.subprocess, "run", run)
    monkeypatch.setattr(module, "send_email",
                        lambda subject, body: module.emails.append((subject, body)))
    module.JOBS.write_text('{"jobs": []}')
    install_gateway(module)
    return module


def install_gateway(module, pid=43210, start=1234, argv=None, state="S"):
    module.PID_FILE.write_text(json.dumps({"pid": pid, "start_time": start}))
    process = module.PROC / str(pid)
    process.mkdir(parents=True, exist_ok=True)
    # Field 2 deliberately includes spaces and a closing parenthesis.
    fields = [state] + ["0"] * 18 + [str(start)]
    (process / "stat").write_text(f"{pid} (gateway ) worker) " + " ".join(fields))
    argv = argv if argv is not None else ["python", "-m", "hermes_cli.main", "gateway", "run"]
    (process / "cmdline").write_bytes("\0".join(argv).encode() + b"\0")


def write_heartbeat(module, age=601, **overrides):
    owner = module.gateway_owner()
    heartbeat = {
        "schema": 1, "ts": NOW - age, **owner, "tick_seq": 1,
        "due_count": 0, "in_flight_count": 0, "oldest_lease_age_s": None,
        "provider": "gateway",
    }
    heartbeat.update(overrides)
    module.HEARTBEAT.parent.mkdir(parents=True, exist_ok=True)
    module.HEARTBEAT.write_text(json.dumps(heartbeat))
    return heartbeat


def probe(module, now=NOW, *extra):
    return module.main([
        "--jobs-file", str(module.JOBS), "--state-file", str(module.STATE),
        "--heartbeat-file", str(module.HEARTBEAT), "--now", str(now), *extra,
    ])


@pytest.mark.parametrize("age,signals", [(599, 0), (600, 0), (601, 1)])
def test_strict_heartbeat_age_threshold(watchdog, age, signals, capsys):
    write_heartbeat(watchdog, age=age)
    assert probe(watchdog) == 0
    assert len(watchdog.calls) == signals
    output = capsys.readouterr().out
    assert ("WATCHDOG-ACTION" in output) == bool(signals)
    if signals:
        assert "WATCHDOG-VERIFY recovered=false pid_changed=false heartbeat_advanced=false" in output
        state = json.loads(watchdog.STATE.read_text())
        assert state["signal_submitted"] is True
        assert state["recovered"] is False


def test_incident_counterfactual_and_log_writes_do_not_suppress_action(watchdog, tmp_path):
    def epoch(clock):
        return datetime.fromisoformat(f"2026-11-03T{clock}+00:00").timestamp()

    write_heartbeat(watchdog, ts=epoch("07:08:44"))
    probe(watchdog, epoch("07:18:44"))
    assert not watchdog.calls
    probe(watchdog, epoch("07:19:00"))
    assert len(watchdog.calls) == 1
    log = tmp_path / "logs/gateway.log"
    log.write_text("unrelated warning\n")
    os.utime(log, (epoch("09:44:00"), epoch("09:44:00")))
    probe(watchdog, epoch("09:48:00"))
    assert len(watchdog.calls) == 2


@pytest.mark.parametrize("invalid,reason", [
    ("missing", "missing"), ("corrupt", "unreadable"),
    ("future", "future_ts"), ("wrong_pid", "owner_mismatch"),
    ("wrong_start", "owner_mismatch"), ("null_start", "owner_mismatch"),
])
def test_invalid_heartbeat_grace_is_from_first_observation(watchdog, invalid, reason, capsys):
    heartbeat = write_heartbeat(watchdog)
    if invalid == "missing":
        watchdog.HEARTBEAT.unlink()
    elif invalid == "corrupt":
        watchdog.HEARTBEAT.write_text("{malformed")
    else:
        heartbeat.update({
            "future": {"ts": NOW + 61},
            "wrong_pid": {"pid": 99999},
            "wrong_start": {"process_start_id": "99999"},
            "null_start": {"process_start_id": None},
        }[invalid])
        watchdog.HEARTBEAT.write_text(json.dumps(heartbeat))
    probe(watchdog)
    assert json.loads(watchdog.STATE.read_text())["invalid_since"] == NOW
    for delta in (300, 599):
        if invalid == "future":
            heartbeat["ts"] = NOW + delta + 61
            watchdog.HEARTBEAT.write_text(json.dumps(heartbeat))
        probe(watchdog, NOW + delta)
        assert not watchdog.calls
        assert json.loads(watchdog.STATE.read_text())["invalid_since"] == NOW
    if invalid == "future":
        heartbeat["ts"] = NOW + 661
        watchdog.HEARTBEAT.write_text(json.dumps(heartbeat))
    probe(watchdog, NOW + 600)
    assert len(watchdog.calls) == 1
    assert f"WATCHDOG-INVALID reason={reason}" in capsys.readouterr().out


@pytest.mark.parametrize("value,reason", [
    ([], "not_object"), ({}, "fields"),
    ({"schema": True}, "schema"), ({"schema": 2}, "schema"),
    ({"ts": True}, "ts"), ({"ts": "123"}, "ts"),
    ({"ts": float("nan")}, "ts"), ({"ts": float("inf")}, "ts"),
    ({"ts": 10 ** 400}, "ts"),
    ({"pid": True}, "pid"), ({"pid": "43210"}, "pid"),
    ({"tick_seq": 0}, "tick_seq"), ({"tick_seq": 1.5}, "tick_seq"),
    ({"tick_seq": True}, "tick_seq"), ({"due_count": -1}, "due_count"),
    ({"in_flight_count": "0"}, "in_flight_count"),
    ({"process_start_id": 1234}, "process_start_id"),
    ({"oldest_lease_age_s": 0}, "oldest_lease_age_s"),
    ({"provider": "manual"}, "provider"), ({"extra": 1}, "fields"),
])
def test_strict_boundary_validation(watchdog, value, reason):
    heartbeat = write_heartbeat(watchdog, age=0)
    if isinstance(value, dict) and value:
        heartbeat.update(value)
    else:
        heartbeat = value
    watchdog.HEARTBEAT.write_text(json.dumps(heartbeat))
    parsed, error = watchdog.read_heartbeat(watchdog.HEARTBEAT, NOW, watchdog.gateway_owner())
    assert parsed is None
    assert error == reason


def test_future_skew_boundary(watchdog):
    write_heartbeat(watchdog, ts=NOW + 60)
    probe(watchdog)
    assert not watchdog.calls
    assert "invalid_since" not in json.loads(watchdog.STATE.read_text())


def test_scheduler_payload_and_watchdog_contract_agree(scheduler_home, watchdog):
    # Real writer + real reader boundaries, with only process probes redirected.
    install_gateway(watchdog, pid=os.getpid(),
                    start=int(scheduler._process_start_id(os.getpid())))
    scheduler.tick(verbose=False, sync=False)
    heartbeat, reason = watchdog.read_heartbeat(
        scheduler.heartbeat_path(), NOW, watchdog.gateway_owner(),
    )
    assert reason is None
    assert heartbeat["provider"] == "gateway"
    assert heartbeat["tick_seq"] == 1


def test_bad_input_changes_do_not_reset_grace(watchdog):
    watchdog.HEARTBEAT.parent.mkdir(parents=True, exist_ok=True)
    probe(watchdog)
    watchdog.HEARTBEAT.write_text("[]")
    probe(watchdog, NOW + 300)
    watchdog.HEARTBEAT.write_text("{}")
    probe(watchdog, NOW + 600)
    assert len(watchdog.calls) == 1


def test_valid_progress_clears_invalid_grace(watchdog):
    probe(watchdog)
    write_heartbeat(watchdog, ts=NOW + 300)
    probe(watchdog, NOW + 300)
    assert "invalid_since" not in json.loads(watchdog.STATE.read_text())
    watchdog.HEARTBEAT.write_text("{}")
    probe(watchdog, NOW + 400)
    assert json.loads(watchdog.STATE.read_text())["invalid_since"] == NOW + 400


@pytest.mark.parametrize("next_seq", [1, 2])
def test_stuck_or_regressed_sequence_cannot_refresh_liveness(watchdog, next_seq, capsys):
    write_heartbeat(watchdog, age=0, tick_seq=2)
    probe(watchdog)
    write_heartbeat(watchdog, ts=NOW + 300, tick_seq=next_seq)
    probe(watchdog, NOW + 300)
    assert not watchdog.calls
    write_heartbeat(watchdog, ts=NOW + 601, tick_seq=next_seq)
    probe(watchdog, NOW + 601)
    assert len(watchdog.calls) == 1
    assert "WATCHDOG-INVALID reason=tick_seq_not_advancing" in capsys.readouterr().out


def test_advancing_sequence_and_new_process_reset_sequence(watchdog):
    write_heartbeat(watchdog, age=0, tick_seq=5)
    probe(watchdog)
    write_heartbeat(watchdog, ts=NOW + 601, tick_seq=6)
    probe(watchdog, NOW + 601)
    install_gateway(watchdog, pid=43211, start=2345)
    write_heartbeat(watchdog, ts=NOW + 1202, tick_seq=1)
    probe(watchdog, NOW + 1202)
    assert not watchdog.calls


@pytest.mark.parametrize("elapsed,signals", [(1199, 0), (1200, 1)])
def test_cooldown_boundary(watchdog, elapsed, signals):
    write_heartbeat(watchdog)
    watchdog.STATE.parent.mkdir(parents=True)
    watchdog.STATE.write_text(json.dumps({"restarts": [NOW - elapsed]}))
    probe(watchdog)
    assert len(watchdog.calls) == signals


def test_rolling_day_cap_and_expiry(watchdog):
    write_heartbeat(watchdog)
    watchdog.STATE.parent.mkdir(parents=True)
    watchdog.STATE.write_text(json.dumps({
        "restarts": [NOW - 2000 * (index + 1) for index in range(6)],
    }))
    probe(watchdog)
    probe(watchdog, NOW + 1)
    assert not watchdog.calls
    assert len(watchdog.emails) == 1
    probe(watchdog, NOW + 86400)
    assert len(watchdog.calls) == 1


@pytest.mark.parametrize("mode", ["missing", "stale", "cap"])
def test_dry_run_never_signals_or_mutates_state(watchdog, mode):
    if mode != "missing":
        write_heartbeat(watchdog)
        watchdog.STATE.parent.mkdir(parents=True)
        restarts = [NOW - 2000 * (i + 1) for i in range(6)] if mode == "cap" else []
        watchdog.STATE.write_text(json.dumps({"restarts": restarts}))
    before = watchdog.STATE.read_bytes() if watchdog.STATE.exists() else None
    probe(watchdog, NOW, "--dry-run", "--quiet-override", "0")
    assert not watchdog.calls
    assert not watchdog.emails
    after = watchdog.STATE.read_bytes() if watchdog.STATE.exists() else None
    assert after == before
    assert not watchdog.LOG.exists()


@pytest.mark.parametrize("jobs", ["missing", "{broken", "[]", "null"])
def test_unreadable_or_empty_jobs_never_suppress_action(watchdog, jobs):
    write_heartbeat(watchdog)
    if jobs == "missing":
        watchdog.JOBS.unlink()
    else:
        watchdog.JOBS.write_text(jobs)
    probe(watchdog, NOW, "--quiet-override", "0")
    assert len(watchdog.calls) == 1


def test_large_jobs_file_still_reports_context(watchdog, capsys):
    write_heartbeat(watchdog)
    watchdog.JOBS.write_text(json.dumps({"jobs": [{
        "enabled": True, "name": "overdue-job", "prompt": "x" * 70000,
        "next_run_at": datetime.fromtimestamp(NOW - 3600).isoformat(),
    }]}))
    probe(watchdog)
    assert len(watchdog.calls) == 1
    assert "overdue=1" in capsys.readouterr().out


@pytest.mark.parametrize("raw", ["[" * 2000, "x" * 65537])
def test_nested_or_oversized_json_is_unhealthy(watchdog, raw):
    watchdog.HEARTBEAT.parent.mkdir(parents=True, exist_ok=True)
    watchdog.HEARTBEAT.write_text(raw)
    probe(watchdog)
    assert not watchdog.calls
    probe(watchdog, NOW + 600)
    assert len(watchdog.calls) == 1


@pytest.mark.parametrize("invalid", ["missing", "dead", "start", "cmdline", "zombie", "legacy"])
def test_absent_or_invalid_gateway_never_signals_or_consumes_cap(watchdog, invalid, capsys):
    write_heartbeat(watchdog)
    if invalid == "missing":
        watchdog.PID_FILE.unlink()
    elif invalid == "legacy":
        watchdog.PID_FILE.write_text("43210")
    elif invalid == "dead":
        watchdog.PID_FILE.write_text('{"pid": 99999, "start_time": 1234}')
    elif invalid == "start":
        watchdog.PID_FILE.write_text('{"pid": 43210, "start_time": 99999}')
    elif invalid == "cmdline":
        install_gateway(watchdog, argv=["python", "-m", "hermes_cli.main", "gateway", "status"])
    else:
        install_gateway(watchdog, state="Z")
    probe(watchdog)
    probe(watchdog, NOW + 601)
    assert not watchdog.calls
    assert not json.loads(watchdog.STATE.read_text()).get("restarts")
    assert "gateway_absent" in capsys.readouterr().out


def test_owner_revalidated_immediately_before_signal(watchdog, monkeypatch):
    write_heartbeat(watchdog)
    owner = watchdog.gateway_owner()
    replies = iter([owner, None])
    monkeypatch.setattr(watchdog, "gateway_owner", lambda: next(replies))
    probe(watchdog)
    assert not watchdog.calls
    assert not json.loads(watchdog.STATE.read_text()).get("restarts")


@pytest.mark.parametrize("turnover,fresh,recovered", [
    (False, False, False), (True, False, False), (False, True, False), (True, True, True),
])
def test_recovery_requires_turnover_and_fresh_owned_heartbeat(
    watchdog, monkeypatch, capsys, turnover, fresh, recovered,
):
    write_heartbeat(watchdog)
    original_run = watchdog.subprocess.run

    def signal(command, **kwargs):
        result = original_run(command, **kwargs)
        if turnover:
            install_gateway(watchdog, pid=43211, start=2345)
        if fresh:
            write_heartbeat(watchdog, age=0, tick_seq=2)
        return result

    monkeypatch.setattr(watchdog.subprocess, "run", signal)
    probe(watchdog)
    state = json.loads(watchdog.STATE.read_text())
    assert state["signal_submitted"] is True
    assert state["recovered"] is recovered
    assert f"WATCHDOG-VERIFY recovered={str(recovered).lower()}" in capsys.readouterr().out


def test_verification_wait_is_bounded_without_real_sleep(watchdog, monkeypatch):
    previous = write_heartbeat(watchdog)
    clock = [0]
    monkeypatch.setattr(watchdog, "VERIFY_S", 45)
    monkeypatch.setattr(watchdog.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(watchdog.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    assert not watchdog.verify_recovery(watchdog.gateway_owner(), previous, watchdog.HEARTBEAT, NOW)
    assert clock[0] == 45


def test_later_healthy_probe_preserves_last_action_result(watchdog):
    write_heartbeat(watchdog)
    probe(watchdog)
    write_heartbeat(watchdog, age=0, tick_seq=2)
    probe(watchdog)
    state = json.loads(watchdog.STATE.read_text())
    assert state["last_restart_ts"] == NOW
    assert state["signal_submitted"] is True
    assert state["recovered"] is False
    assert len(watchdog.calls) == 1


@pytest.mark.parametrize("failure", ["returncode", "timeout"])
def test_failed_submission_does_not_claim_recovery(watchdog, monkeypatch, failure):
    write_heartbeat(watchdog)

    def fail(command, **kwargs):
        if failure == "timeout":
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr(watchdog.subprocess, "run", fail)
    probe(watchdog)
    state = json.loads(watchdog.STATE.read_text())
    assert state["signal_submitted"] is False
    assert state["recovered"] is False
    assert not state.get("restarts")


def test_email_alert_is_capped_at_two_kib(watchdog, monkeypatch):
    # Exercise the actual email adapter, replacing only its subprocess.
    spec = importlib.util.spec_from_file_location("watchdog_email_test", WATCHDOG)
    fresh = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fresh)
    messages = []

    def send(command, **kwargs):
        assert command == [fresh.HIMALAYA, "message", "send"]
        messages.append(kwargs["input"].encode())
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(fresh.subprocess, "run", send)
    assert fresh.send_email("test", "é" * 4000)
    assert len(messages[0]) <= 2048
    fresh.DRY = True
    assert not fresh.send_email("test", "not sent")
    assert len(messages) == 1
