"""Regression contracts for out-of-process scheduler recovery and readiness witnesses."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from gateway import scheduler_recovery as recovery


def _identity(pid: int = 101, start_time: int = 202, boot_id: str = "boot-a"):
    return recovery.ProcessIdentity(
        pid=pid, start_time=start_time, host_boot_id=boot_id
    )


def _progress(
    identity=None,
    *,
    started_at=500.0,
    heartbeat_at=999.0,
    tick_at=600.0,
    heartbeat_sequence=4,
    tick_sequence=1,
):
    identity = identity or _identity()
    return recovery.SchedulerProgress(
        pid=identity.pid,
        start_time=identity.start_time,
        host_boot_id=identity.host_boot_id,
        heartbeat_sequence=heartbeat_sequence,
        tick_sequence=tick_sequence,
        started_at=started_at,
        heartbeat_at=heartbeat_at,
        tick_at=tick_at,
    )


def _observation(
    *,
    identity=None,
    progress=None,
    loop_heartbeat_fresh=True,
    intentional_shutdown=False,
    memory=None,
):
    return recovery.RecoveryObservation(
        unit_active=True,
        serving=True,
        banner=True,
        intentional_shutdown=intentional_shutdown,
        identity=identity or _identity(),
        progress=progress or _progress(identity),
        loop_heartbeat_fresh=loop_heartbeat_fresh,
        memory=memory or recovery.MemoryDiagnostics(),
    )


def _missing_progress_observation(identity=None):
    return recovery.RecoveryObservation(
        unit_active=True,
        serving=True,
        banner=True,
        intentional_shutdown=False,
        identity=identity or _identity(),
        progress=None,
        progress_missing=True,
        loop_heartbeat_fresh=True,
    )


def test_frozen_scheduler_requires_two_stale_observations_and_only_one_restart():
    identity = _identity()
    first = recovery.advance_recovery(
        recovery.RecoveryState(),
        _observation(
            identity=identity,
            progress=_progress(identity, heartbeat_at=999.0, tick_at=600.0),
            loop_heartbeat_fresh=True,
        ),
        now=1_000.0,
    )
    assert first.request_restart is False
    assert first.state.stale is not None
    assert first.state.stale.observations == 1

    # The scheduler thread heartbeat and the gateway's loop heartbeat advance, but the
    # successful-tick witness stays frozen. They cannot verify scheduler recovery.
    second = recovery.advance_recovery(
        first.state,
        _observation(
            identity=identity,
            progress=_progress(
                identity,
                heartbeat_at=1_029.0,
                tick_at=600.0,
                heartbeat_sequence=5,
            ),
            loop_heartbeat_fresh=True,
        ),
        now=1_030.0,
    )
    assert second.request_restart is True
    assert second.state.pending is not None
    assert len(second.state.automatic_actions) == 1
    assert [event.event for event in second.audit_events].count(
        "restart_requested"
    ) == 1

    third = recovery.advance_recovery(
        second.state,
        _observation(
            identity=identity,
            progress=_progress(
                identity,
                heartbeat_at=1_059.0,
                tick_at=600.0,
                heartbeat_sequence=6,
            ),
            loop_heartbeat_fresh=True,
        ),
        now=1_060.0,
    )
    assert third.request_restart is False
    assert len(third.state.automatic_actions) == 1


def test_missing_progress_gets_a_grace_period_then_two_observations():
    identity = _identity()
    first = recovery.advance_recovery(
        recovery.RecoveryState(),
        _missing_progress_observation(identity),
        now=0.0,
    )
    within_grace = recovery.advance_recovery(
        first.state,
        _missing_progress_observation(identity),
        now=recovery.DEFAULT_STALE_AFTER_SECONDS - 1,
    )
    assert within_grace.request_restart is False
    assert within_grace.state.stale is not None
    assert within_grace.state.stale.observations == 0

    first_stale = recovery.advance_recovery(
        within_grace.state,
        _missing_progress_observation(identity),
        now=recovery.DEFAULT_STALE_AFTER_SECONDS,
    )
    assert first_stale.request_restart is False
    assert first_stale.state.stale is not None
    assert first_stale.state.stale.observations == 1

    second_stale = recovery.advance_recovery(
        first_stale.state,
        _missing_progress_observation(identity),
        now=recovery.DEFAULT_STALE_AFTER_SECONDS
        + recovery.DEFAULT_OBSERVATION_SPACING_SECONDS,
    )
    assert second_stale.request_restart is True


def test_stale_progress_from_a_different_process_is_not_restart_evidence():
    identity = _identity()
    foreign_progress = _progress(
        _identity(pid=identity.pid + 1, start_time=identity.start_time + 1),
        tick_at=600.0,
    )

    decision = recovery.advance_recovery(
        recovery.RecoveryState(),
        _observation(identity=identity, progress=foreign_progress),
        now=1_000.0,
    )

    assert decision.request_restart is False
    assert decision.state.stale is None


def test_restart_is_not_verified_until_new_process_has_two_ticks():
    previous_identity = _identity()
    restarted_identity = _identity(pid=303, start_time=404)
    state = recovery.RecoveryState(
        pending=recovery.PendingRestart(
            request_id="request-a",
            requested_at=1_000.0,
            identity=previous_identity,
            heartbeat_sequence=8,
            tick_sequence=3,
        ),
        automatic_actions=[1_000.0],
    )

    one_tick = recovery.advance_recovery(
        state,
        _observation(
            identity=restarted_identity,
            progress=_progress(
                restarted_identity,
                started_at=2_000.0,
                heartbeat_at=2_000.0,
                tick_at=2_000.0,
                heartbeat_sequence=2,
                tick_sequence=1,
            ),
            loop_heartbeat_fresh=True,
        ),
        now=2_000.0,
    )
    assert one_tick.state.pending is not None
    assert one_tick.state.recovered is False
    assert not any(
        event.event == "recovery_verified" for event in one_tick.audit_events
    )

    two_ticks = recovery.advance_recovery(
        one_tick.state,
        _observation(
            identity=restarted_identity,
            progress=_progress(
                restarted_identity,
                started_at=2_000.0,
                heartbeat_at=2_010.0,
                tick_at=2_010.0,
                heartbeat_sequence=3,
                tick_sequence=2,
            ),
            loop_heartbeat_fresh=True,
        ),
        now=2_010.0,
    )
    assert two_ticks.state.pending is None
    assert two_ticks.state.recovered is True
    assert any(event.event == "recovery_verified" for event in two_ticks.audit_events)


def test_five_automatic_restarts_preserve_operator_reserve():
    now = 1_000.0
    identity = _identity()
    state = recovery.RecoveryState(
        automatic_actions=[now - 100.0 + float(index) for index in range(5)],
    )
    sample = _observation(
        identity=identity, progress=_progress(identity, tick_at=600.0)
    )
    first = recovery.advance_recovery(state, sample, now=now)
    capped = recovery.advance_recovery(
        first.state,
        _observation(identity=identity, progress=_progress(identity, tick_at=600.0)),
        now=now + 30,
    )

    assert capped.request_restart is False
    assert len(capped.state.automatic_actions) == 5
    cap_event = next(
        event for event in capped.audit_events if event.event == "restart_cap_reached"
    )
    assert cap_event.automatic_actions_24h == 5
    assert cap_event.operator_reserved_slots == 1


def test_healthy_epoch_reset_keeps_actions_and_appends_audit(tmp_path):
    home = tmp_path
    state_dir = home / "state"
    state_dir.mkdir()
    audit_path = recovery.recovery_audit_path(home)
    original_line = '{"event":"earlier"}\n'
    audit_path.write_text(original_line, encoding="utf-8")
    now = 10_000.0
    identity = _identity()
    state = recovery.RecoveryState(
        epoch=2,
        automatic_actions=[9_000.0],
        recovered=True,
        cap_alert=True,
        healthy_since=now - recovery.HEALTHY_EPOCH_RESET_SECONDS - 1,
        healthy_identity=identity,
        healthy_tick_sequence=2,
    )
    decision = recovery.advance_recovery(
        state,
        _observation(
            identity=identity,
            progress=_progress(
                identity,
                started_at=9_000.0,
                heartbeat_at=now,
                tick_at=now,
                heartbeat_sequence=8,
                tick_sequence=3,
            ),
        ),
        now=now,
    )
    recovery._append_audit(home, decision.audit_events)

    assert decision.state.epoch == 3
    assert decision.state.recovered is False
    assert decision.state.cap_alert is False
    assert decision.state.automatic_actions == [9_000.0]
    lines = audit_path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == original_line.rstrip("\n")
    assert json.loads(lines[1])["event"] == "recovery_epoch_reset"


def test_memory_pressure_is_reported_and_inhibits_restart_without_tuning_limits(
    tmp_path,
):
    cgroup = tmp_path / "gateway.slice"
    cgroup.mkdir()
    current_file = cgroup / "memory.current"
    high_file = cgroup / "memory.high"
    events_file = cgroup / "memory.events"
    current_file.write_text("1200\n", encoding="ascii")
    high_file.write_text("1000\n", encoding="ascii")
    events_file.write_text(
        "low 0\nhigh 4\nmax 0\noom 0\noom_kill 0\n", encoding="ascii"
    )

    memory = recovery.sample_cgroup_memory(
        "/gateway.slice",
        root=tmp_path,
        previous_events={"low": 0, "high": 3, "max": 0, "oom": 0, "oom_kill": 0},
        now=1_000.0,
    )
    assert memory.available is True
    assert memory.current_bytes == 1200
    assert memory.high_bytes == 1000
    assert memory.event_deltas["high"] == 1
    assert memory.pressure is True

    decision = recovery.advance_recovery(
        recovery.RecoveryState(),
        _observation(
            progress=_progress(tick_at=600.0),
            memory=memory,
        ),
        now=1_000.0,
    )
    assert decision.request_restart is False
    assert decision.state.pressure_inhibited is True
    assert current_file.read_text(encoding="ascii") == "1200\n"
    assert high_file.read_text(encoding="ascii") == "1000\n"
    assert events_file.read_text(encoding="ascii").startswith("low 0\n")


@pytest.mark.linux_only
def test_cgroup_memory_probe_rejects_paths_outside_its_root(tmp_path):
    root = tmp_path / "cgroup_root"
    root.mkdir()
    outside = tmp_path / "outside-cgroup"
    outside.mkdir()
    (outside / "memory.current").write_text("1200\n", encoding="ascii")
    (outside / "memory.high").write_text("1000\n", encoding="ascii")
    (root / "escaped").symlink_to(outside, target_is_directory=True)

    traversal = recovery.sample_cgroup_memory("../outside-cgroup", root=root)
    symlink_escape = recovery.sample_cgroup_memory("/escaped", root=root)

    assert traversal.available is False
    assert symlink_escape.available is False


def test_systemd_target_accepts_safe_service_names_only():
    assert recovery.SystemdTarget(unit="hermes-gateway.service").unit
    with pytest.raises(recovery.ValidationError):
        recovery.SystemdTarget(unit="-help.service")
    with pytest.raises(recovery.ValidationError):
        recovery.SystemdTarget(unit=r"hermes\gateway.service")


def test_progress_reader_rejects_malformed_and_coercible_json(tmp_path):
    path = tmp_path / "ticker_progress.json"
    valid = json.loads(_progress().model_dump_json())
    invalid_payloads = (
        "{malformed",
        json.dumps({**valid, "pid": True}),
        json.dumps({**valid, "unexpected": "field"}),
    )

    for payload in invalid_payloads:
        path.write_text(payload, encoding="utf-8")
        progress, error = recovery.read_scheduler_progress(path)
        assert progress is None
        assert error == "input.invalid"


def test_loop_heartbeat_wall_time_is_not_compared_to_process_fingerprint(
    tmp_path, monkeypatch
):
    identity = _identity(pid=909, start_time=808)
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "gateway.heartbeat").write_text(
        json.dumps({
            "pid": identity.pid,
            "start_time": 1_700_000_000.0,
            "monotonic": 999.0,
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(recovery.time, "monotonic", lambda: 1_004.0)
    monkeypatch.setattr(recovery, "current_host_boot_id", lambda: identity.host_boot_id)

    assert recovery._loop_heartbeat_is_fresh(tmp_path, identity) is True


@pytest.mark.parametrize(
    ("restart_result", "expected_error", "completion_event"),
    [
        (0, None, "restart_command_completed"),
        (1, "systemd.restart.failed", "restart_command_failed"),
    ],
)
@pytest.mark.linux_only
def test_run_once_persists_one_restart_request_after_two_observations(
    tmp_path, monkeypatch, restart_result, expected_error, completion_event
):
    home = tmp_path / "home"
    state_dir = home / "state"
    cron_dir = home / "cron"
    state_dir.mkdir(parents=True)
    cron_dir.mkdir()
    approval = tmp_path / "approved"
    approval.touch()
    now = 1_000.0
    identity = _identity(pid=909, start_time=808)
    runtime = {
        "gateway_state": "running",
        "pid": identity.pid,
        "start_time": identity.start_time,
        "restart_requested": False,
        "gateway_banner": "Gateway running",
        "scheduler_provider": "builtin",
        "platforms": {},
    }
    (home / "gateway_state.json").write_text(json.dumps(runtime), encoding="utf-8")
    (cron_dir / "ticker_progress.json").write_text(
        _progress(
            identity,
            started_at=500.0,
            heartbeat_at=999.0,
            tick_at=600.0,
        ).model_dump_json(),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        recovery,
        "_show_unit",
        lambda target: recovery.SystemdUnitStatus(
            active_state="active",
            sub_state="running",
            main_pid=identity.pid,
        ),
    )
    restart = Mock(return_value=restart_result)
    monkeypatch.setattr(recovery, "_restart_unit", restart)
    monkeypatch.setattr(recovery, "process_start_time", lambda pid: identity.start_time)
    monkeypatch.setattr(recovery, "current_host_boot_id", lambda: identity.host_boot_id)
    monkeypatch.setattr(recovery, "_planned_stop_active", lambda home, current: False)
    monkeypatch.setattr(
        recovery, "_loop_heartbeat_is_fresh", lambda home, current: True
    )
    monkeypatch.setattr("gateway.drain_control.drain_requested", lambda home: False)

    first = recovery.run_once(
        home=home,
        unit="hermes-gateway.service",
        approval_file=approval,
        now=now,
    )
    second = recovery.run_once(
        home=home,
        unit="hermes-gateway.service",
        approval_file=approval,
        now=now + 30,
    )
    third = recovery.run_once(
        home=home,
        unit="hermes-gateway.service",
        approval_file=approval,
        now=now + 60,
    )

    assert first["restart_requested"] is False
    assert second["restart_requested"] is True
    assert second["restart_error"] == expected_error
    assert third["restart_requested"] is False
    assert restart.call_count == 1
    persisted, error = recovery.load_recovery_state(home)
    assert error is None
    assert persisted is not None and persisted.pending is not None
    assert len(persisted.automatic_actions) == 1
    audit_events = [
        json.loads(line)["event"]
        for line in recovery
        .recovery_audit_path(home)
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert audit_events.count("restart_requested") == 1
    assert audit_events.count(completion_event) == 1


def test_run_once_is_disabled_without_approval_marker(monkeypatch, tmp_path):
    show = Mock()
    monkeypatch.setattr(recovery, "_show_unit", show)

    result = recovery.run_once(
        home=tmp_path,
        unit="hermes-gateway.service",
        approval_file=None,
    )

    assert result["status"] == "disabled"
    show.assert_not_called()


@pytest.mark.linux_only
def test_run_once_does_not_restart_when_audit_persistence_fails(tmp_path, monkeypatch):
    home = tmp_path / "home"
    state_dir = home / "state"
    cron_dir = home / "cron"
    state_dir.mkdir(parents=True)
    cron_dir.mkdir()
    approval = tmp_path / "approved"
    approval.touch()
    identity = _identity(pid=909, start_time=808)
    runtime = {
        "gateway_state": "running",
        "pid": identity.pid,
        "start_time": identity.start_time,
        "gateway_banner": "Gateway running",
        "scheduler_provider": "builtin",
    }
    (home / "gateway_state.json").write_text(json.dumps(runtime), encoding="utf-8")
    progress = _progress(identity, heartbeat_at=999.0, tick_at=600.0)
    (cron_dir / "ticker_progress.json").write_text(
        progress.model_dump_json(),
        encoding="utf-8",
    )
    recovery._save_state(
        home,
        recovery.RecoveryState(
            stale=recovery.StaleEvidence(
                identity=identity,
                heartbeat_sequence=progress.heartbeat_sequence,
                tick_sequence=progress.tick_sequence,
                tick_at=progress.tick_at,
                first_observed_at=970.0,
                last_observed_at=970.0,
                observations=1,
            )
        ),
    )
    monkeypatch.setattr(
        recovery,
        "_show_unit",
        lambda target: recovery.SystemdUnitStatus(
            active_state="active",
            sub_state="running",
            main_pid=identity.pid,
        ),
    )
    restart = Mock(return_value=0)
    append_audit = Mock(side_effect=OSError("audit filesystem unavailable"))
    monkeypatch.setattr(recovery, "_restart_unit", restart)
    monkeypatch.setattr(recovery, "process_start_time", lambda pid: identity.start_time)
    monkeypatch.setattr(recovery, "current_host_boot_id", lambda: identity.host_boot_id)
    monkeypatch.setattr(recovery, "_planned_stop_active", lambda home, current: False)
    monkeypatch.setattr(
        recovery,
        "_append_audit",
        append_audit,
    )
    monkeypatch.setattr("gateway.drain_control.drain_requested", lambda home: False)

    result = recovery.run_once(
        home=home,
        unit="hermes-gateway.service",
        approval_file=approval,
        now=1_000.0,
    )

    assert result["error_code"] == "recovery.persistence.failed"
    append_audit.assert_called_once()
    restart.assert_not_called()


def test_main_exit_status_reports_observer_errors_not_gateway_health(
    monkeypatch, tmp_path, capsys
):
    monkeypatch.setattr(
        recovery,
        "run_once",
        lambda **kwargs: {
            "status": "degraded",
            "restart_requested": False,
            "loop_heartbeat_fresh": True,
        },
    )
    healthy_observation = recovery.main([
        "--home",
        str(tmp_path),
        "--unit",
        "hermes-gateway.service",
    ])
    assert healthy_observation == 0
    capsys.readouterr()

    monkeypatch.setattr(
        recovery,
        "run_once",
        lambda **kwargs: {"status": "degraded", "error_code": "systemd.unavailable"},
    )
    observer_failure = recovery.main([
        "--home",
        str(tmp_path),
        "--unit",
        "hermes-gateway.service",
    ])
    assert observer_failure == 1


def test_cron_heartbeat_persists_separate_heartbeat_and_successful_tick_sequences(
    tmp_path,
    monkeypatch,
):
    from cron import jobs

    monkeypatch.setattr(
        jobs,
        "_current_cron_store",
        lambda: SimpleNamespace(cron_dir=tmp_path),
    )
    monkeypatch.setattr(jobs, "_write_marker", lambda *args, **kwargs: None)
    monkeypatch.setattr(recovery, "process_start_time", lambda pid: 808)
    monkeypatch.setattr(recovery, "current_host_boot_id", lambda: "boot-a")

    jobs.record_ticker_heartbeat()
    jobs.record_ticker_heartbeat(success=True)
    jobs.record_ticker_heartbeat(success=True)

    progress, error = recovery.read_scheduler_progress(
        tmp_path / "ticker_progress.json"
    )
    assert error is None
    assert progress is not None
    assert progress.heartbeat_sequence == 3
    assert progress.tick_sequence == 2
