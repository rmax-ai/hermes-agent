"""Lease recovery must prove ownership, preserve cadence, and count suppressed slots."""

import concurrent.futures
import threading
import time
from unittest.mock import patch

import pytest

import cron.scheduler as sched
import cron.scheduler_inflight as inflight


@pytest.fixture(autouse=True)
def _clean_inflight():
    """Reset lease and finding state so tests are isolated."""
    sched._running_job_ids.clear()
    sched._running_since.clear()
    sched._running_futures.clear()
    sched._running_leases.clear()
    sched.reset_inflight_guard_stats()
    inflight._current_lease_ids.set({})
    yield
    sched._running_job_ids.clear()
    sched._running_since.clear()
    sched._running_futures.clear()
    sched._running_leases.clear()
    inflight._current_lease_ids.set({})


def _job(job_id="wedged", minutes=60, kind="interval", cron_expr=None,
         repeat=None):
    """Build a job row using the PERSISTED schedule dict shape."""
    if kind == "interval":
        schedule = {
            "kind": "interval",
            "minutes": minutes,
            "display": f"every {minutes}m",
        }
    elif kind == "cron":
        expr = cron_expr or "0 9 * * 1"
        schedule = {"kind": "cron", "expr": expr, "display": expr}
    else:
        schedule = {
            "kind": "once",
            "run_at": "2030-01-01T00:00:00",
            "display": "once at 2030-01-01 00:00",
        }
    job = {
        "id": job_id,
        "name": f"board-pm-triage-{job_id}",
        "schedule": schedule,
    }
    if repeat is not None:
        job["repeat"] = repeat
    return job


def _dead_thread() -> threading.Thread:
    thread = threading.Thread(target=lambda: None)
    thread.start()
    thread.join()
    return thread


def _inject_stale_claim(job_id: str, *, age_seconds: float = 6 * 60 * 60) -> str:
    """Create a leaked pre-run lease whose exact owner thread has ended."""
    assert sched.try_register_running_job(job_id)
    lease = sched._running_leases[job_id]
    lease.started_at = time.time() - age_seconds
    lease.started_monotonic = time.monotonic() - age_seconds
    lease.last_progress_at = lease.started_at
    lease.last_progress_monotonic = lease.started_monotonic
    lease.owner_thread = _dead_thread()
    lease.owner_thread_ident = lease.owner_thread.ident
    lease.owner_thread_name = lease.owner_thread.name
    return lease.lease_id


class TestStaleInflightLeak:
    def test_stale_claim_is_force_released_and_reported_after_tick(
        self, tmp_path, caplog
    ):
        """The regression: a leaked in-flight claim must be force-released by
        the next tick, surface as ``last_error``, and emit a countable
        signal — instead of silently skipping every fire until the gateway
        process restarts."""
        job = _job(job_id="board-pm-triage-wedged", minutes=60)
        job_id = job["id"]
        _inject_stale_claim(job_id)

        with caplog.at_level("WARNING"), \
             patch.object(sched, "_get_hermes_home", return_value=tmp_path), \
             patch("cron.jobs.load_jobs", return_value=[job]), \
             patch.object(sched, "get_due_jobs", return_value=[]), \
             patch.object(sched, "mark_job_run") as mark:
            sched.tick(verbose=False)

        assert job_id not in sched.get_running_job_ids()
        mark.assert_not_called()

        # Recovery is visible without using jobs.json as lease authority.
        assert any(
            "cron.inflight.forced_release" in r.message for r in caplog.records
        )
        stats = sched.get_inflight_guard_stats()
        assert stats["forced_releases"] == 1
        assert stats["oldest_lease_age_seconds"] is None

    def test_young_inflight_claim_is_not_force_released(self, tmp_path):
        """A live owner remains registered before its age allowance."""
        job = _job(job_id="young", minutes=60)
        job_id = job["id"]
        assert sched.try_register_running_job(job_id)

        with patch.object(sched, "_get_hermes_home", return_value=tmp_path), \
             patch("cron.jobs.load_jobs", return_value=[job]), \
             patch.object(sched, "get_due_jobs", return_value=[]), \
             patch.object(sched, "mark_job_run") as mark:
            sched.tick(verbose=False)

        assert job_id in sched.get_running_job_ids()
        mark.assert_not_called()


def test_only_the_registered_owner_thread_can_normally_release():
    job_id = "owner-thread-release"
    assert sched.try_register_running_job(job_id)
    lease_id = sched.get_running_job_lease_id(job_id)
    released = []
    worker = threading.Thread(
        target=lambda: released.append(
            sched.release_running_job(job_id, lease_id=lease_id)
        )
    )
    worker.start()
    worker.join()

    assert released == [False]
    assert job_id in sched.get_running_job_ids()
    assert sched.release_running_job(job_id, lease_id=lease_id)


class TestJobIntervalMinutes:
    """Allowance inputs come from the PERSISTED dict schedule shape, not the
    string form parse_schedule consumes (review Blocker 1)."""

    def test_reads_persisted_interval_dict(self):
        job = _job(minutes=4320)
        assert sched._job_interval_minutes(job) == 4320.0

    def test_reads_persisted_cron_dict(self):
        # */15 every 15 minutes → cadence 15m.
        job = _job(kind="cron", cron_expr="*/15 * * * *")
        assert sched._job_interval_minutes(job) == 15.0

    def test_reads_persisted_weekly_cron_dict(self):
        # 0 9 * * 1 fires weekly → cadence 7*24*60 = 10080m.
        job = _job(kind="cron", cron_expr="0 9 * * 1")
        assert sched._job_interval_minutes(job) == 7 * 24 * 60

    def test_string_fallback_still_works(self):
        # Defensive fallback for programmatic callers.
        job = {"id": "x", "schedule": "every 60m"}
        assert sched._job_interval_minutes(job) == 60.0

    def test_oneshot_has_no_interval(self):
        job = _job(kind="once")
        assert sched._job_interval_minutes(job) is None

    def test_garbage_returns_none(self):
        job = {"id": "x", "schedule": {"kind": "bogus"}}
        assert sched._job_interval_minutes(job) is None


class TestStaleInflightSweep:
    """Unit-level bound checks on sweep_stale_inflight itself."""

    def test_allowance_is_at_least_two_intervals(self, tmp_path):
        """A slow-but-healthy 6h job is not clipped by the 30m floor."""
        job = _job(minutes=360)
        _inject_stale_claim(job["id"], age_seconds=4 * 60 * 60)  # 4h < 12h

        with patch.object(sched, "_get_hermes_home", return_value=tmp_path):
            assert sched.sweep_stale_inflight([job]) == []
        assert job["id"] in sched.get_running_job_ids()

    def test_allowance_honors_persisted_4320m_row(self, tmp_path):
        """The real guide-curator row (4320m) gets a 144h allowance, not the
        30m floor — the Blocker-1 regression against the live store shape."""
        job = _job(job_id="guide-curator", minutes=4320)
        _inject_stale_claim(job["id"], age_seconds=4 * 60 * 60)  # 4h ≪ 144h

        with patch.object(sched, "_get_hermes_home", return_value=tmp_path):
            assert sched.sweep_stale_inflight([job]) == []
        assert job["id"] in sched.get_running_job_ids()

    def test_cron_allowance_not_clipped_to_floor(self, tmp_path):
        """A weekly cron job (cadence 10080m) is not clipped at 30m: a 24h
        claim is still healthy (allowance 20160m)."""
        job = _job(job_id="weekly", kind="cron", cron_expr="0 9 * * 1")
        _inject_stale_claim(job["id"], age_seconds=24 * 60 * 60)

        with patch.object(sched, "_get_hermes_home", return_value=tmp_path):
            assert sched.sweep_stale_inflight([job]) == []
        assert job["id"] in sched.get_running_job_ids()

    def test_live_future_is_never_released(self, tmp_path):
        """A live future is retained; its terminal state releases the exact lease."""
        job = _job()
        fut: concurrent.futures.Future = concurrent.futures.Future()
        assert sched.try_register_running_job(job["id"], dispatching=True)
        lease = sched._running_leases[job["id"]]
        lease.started_monotonic -= 10 * 60 * 60
        lease.started_at -= 10 * 60 * 60
        assert sched.bind_running_job_future(job["id"], lease.lease_id, fut)

        with patch.object(sched, "_get_hermes_home", return_value=tmp_path):
            assert sched.sweep_stale_inflight([job]) == []
        assert job["id"] in sched.get_running_job_ids()
        fut.set_result(True)

        # A terminal future is exact owner evidence, not an age-based guess.
        with patch.object(sched, "_get_hermes_home", return_value=tmp_path):
            assert sched.sweep_stale_inflight([job]) == [job["id"]]
        assert sched.get_inflight_guard_stats()["forced_releases"] == 1

    def test_old_pending_sentinel_is_not_released_by_age(self, tmp_path):
        """A stale pending submit with a live owner is retained and reported."""
        job = _job(job_id="pending-old")
        assert sched.try_register_running_job(job["id"], dispatching=True)
        lease = sched._running_leases[job["id"]]
        lease.started_monotonic -= 5 * 60 * 60
        lease.started_at -= 5 * 60 * 60

        with patch.object(sched, "_get_hermes_home", return_value=tmp_path):
            assert sched.sweep_stale_inflight([job]) == []
        assert job["id"] in sched.get_running_job_ids()
        finding = sched.get_inflight_guard_stats()["recent_findings"][-1]
        assert finding["owner_state"] == "active"
        assert finding["decision"] == "retain"

    def test_pending_sentinel_young_claim_is_not_released(self, tmp_path):
        """A young pending claim (submit still in flight) is safe."""
        job = _job()
        assert sched.try_register_running_job(job["id"], dispatching=True)

        with patch.object(sched, "mark_job_run") as mark, \
             patch.object(sched, "_get_hermes_home", return_value=tmp_path):
            assert sched.sweep_stale_inflight([job]) == []
        assert job["id"] in sched.get_running_job_ids()
        mark.assert_not_called()

    def test_finite_repeat_job_released_without_mark_job_run(self, tmp_path):
        """Recovery does not write job configuration or consume repeat budget."""
        job = _job(repeat={"times": 1, "completed": 0})
        _inject_stale_claim(job["id"], age_seconds=5 * 60 * 60)

        with patch.object(sched, "mark_job_run") as mark, \
             patch.object(sched, "_get_hermes_home", return_value=tmp_path):
            assert sched.sweep_stale_inflight([job]) == [job["id"]]
        assert job["id"] not in sched.get_running_job_ids()
        mark.assert_not_called()
        assert sched.get_inflight_guard_stats()["forced_releases"] == 1

    def test_legacy_claim_without_owner_is_reported_not_released(self, tmp_path):
        """A legacy id without owner identity fails closed even when old."""
        job = _job()
        sched._running_job_ids.add(job["id"])
        sched._running_since[job["id"]] = time.time() - 5 * 60 * 60

        with patch.object(sched, "_get_hermes_home", return_value=tmp_path):
            assert sched.sweep_stale_inflight([job]) == []
        assert job["id"] in sched.get_running_job_ids()
        findings = sched.get_inflight_guard_stats()["recent_findings"]
        assert findings[-1]["reason"] == "owner-identity-unavailable"

    def test_forced_release_logs_a_warning(self, tmp_path, caplog):
        job = _job()
        _inject_stale_claim(job["id"], age_seconds=5 * 60 * 60)

        with caplog.at_level("WARNING"), \
             patch.object(sched, "_get_hermes_home", return_value=tmp_path):
            sched.sweep_stale_inflight([job])

        assert any("cron.inflight.forced_release" in r.message for r in caplog.records)


class TestWedgedJobRefiresWithoutRestart:
    def test_tick_sweeps_then_dispatches_the_previously_wedged_job(self, tmp_path):
        """A proven pre-run owner loss releases before the next due dispatch."""
        job = dict(_job(), enabled=True, next_run_at="2020-01-01T00:00:00",
                   deliver="local")
        _inject_stale_claim(job["id"])

        with patch.object(sched, "_get_hermes_home", return_value=tmp_path), \
             patch.object(sched, "get_due_jobs", return_value=[job]), \
             patch("cron.jobs.load_jobs", return_value=[job]), \
             patch.object(sched, "advance_next_runs"), \
             patch.object(sched, "mark_job_run"), \
             patch.object(sched, "create_execution", return_value={"id": "exec-1"}), \
             patch.object(sched, "finish_execution"), \
             patch.object(sched, "run_one_job", return_value=True):
            n = sched.tick(verbose=False)

        assert n == 1, "wedged job must fire again without a gateway restart"
        assert job["id"] not in sched.get_running_job_ids()
        assert sched.get_inflight_guard_stats()["forced_releases"] == 1


class TestLedgerTerminalReconciliation:
    def _inject_claim(
        self,
        job_id: str,
        *,
        run_id: str = "exec-current",
        age_seconds: float = 60,
    ):
        assert sched.try_register_running_job(job_id)
        lease = sched._running_leases[job_id]
        lease.run_id = run_id
        lease.owner_task_id = run_id
        lease.started_at = time.time() - age_seconds
        lease.started_monotonic = time.monotonic() - age_seconds
        lease.owner_thread = _dead_thread()
        lease.owner_thread_ident = lease.owner_thread.ident
        lease.owner_thread_name = lease.owner_thread.name
        return lease

    @staticmethod
    def _row(lease, status: str) -> dict:
        return {
            "id": lease.run_id,
            "job_id": lease.job_id,
            "process_id": lease.owner_process_id,
            "pid": lease.owner_pid,
            "process_started_at": lease.owner_process_started_at,
            "status": status,
            "handoff_pending": 0,
            "scheduled_instant": lease.scheduled_occurrence,
        }

    def test_exact_terminal_run_is_released_once_by_cas(self, tmp_path):
        job = _job(job_id="ledger-terminal", minutes=10)
        lease = self._inject_claim(job["id"], age_seconds=60)
        row = self._row(lease, "failed")

        with patch.object(sched, "_get_hermes_home", return_value=tmp_path), \
             patch("cron.executions.get_execution", return_value=row):
            assert sched.sweep_stale_inflight([job]) == [job["id"]]
            assert sched.sweep_stale_inflight([job]) == []

        assert job["id"] not in sched.get_running_job_ids()
        assert sched.get_inflight_guard_stats()["forced_releases"] == 1
        assert sched.try_register_running_job(job["id"])
        new_lease_id = sched.get_running_job_lease_id(job["id"])
        assert new_lease_id != lease.lease_id
        assert not sched.release_running_job(job["id"], lease_id=lease.lease_id)
        assert sched.get_running_job_lease_id(job["id"]) == new_lease_id

    def test_previous_run_terminal_row_does_not_release_new_lease(self, tmp_path):
        job = _job(job_id="prev-run-terminal", minutes=10)
        lease = self._inject_claim(job["id"])
        row = self._row(lease, "completed")
        row["id"] = "exec-previous"

        with patch.object(sched, "_get_hermes_home", return_value=tmp_path), \
             patch("cron.executions.get_execution", return_value=row):
            assert sched.sweep_stale_inflight([job]) == []
        assert job["id"] in sched.get_running_job_ids()

    def test_terminal_row_from_different_owner_is_not_released(self, tmp_path):
        job = _job(job_id="different-owner", minutes=10)
        lease = self._inject_claim(job["id"], age_seconds=5 * 60 * 60)
        row = self._row(lease, "completed")
        row["process_id"] = "different-process"

        with patch.object(sched, "_get_hermes_home", return_value=tmp_path), \
             patch("cron.executions.get_execution", return_value=row):
            assert sched.sweep_stale_inflight([job]) == []
        assert job["id"] in sched.get_running_job_ids()
        assert sched.get_inflight_guard_stats()["recent_findings"][-1]["reason"] == (
            "execution-owner-mismatch"
        )

    def test_adopted_worker_owner_is_bound_before_terminal_recovery(self, tmp_path):
        job = _job(job_id="adopted-owner", minutes=10)
        assert sched.try_register_running_job(job["id"])
        lease_id = sched.get_running_job_lease_id(job["id"])
        lease = sched._running_leases[job["id"]]
        run_id = "exec-adopted"
        assert sched.bind_running_job_execution(job["id"], run_id)
        adopted = self._row(lease, "running")
        adopted.update({
            "process_id": "external-worker-process",
            "pid": 4242,
            "process_started_at": 9876,
        })
        assert inflight.bind_running_job_execution_owner(
            job["id"], lease_id, run_id, adopted, expected_pid=4242)
        assert lease.owner_process_id == "external-worker-process"
        assert lease.owner_pid == 4242

        lease.owner_thread = _dead_thread()
        lease.owner_thread_ident = lease.owner_thread.ident
        terminal = {**adopted, "status": "completed"}
        with patch.object(sched, "_get_hermes_home", return_value=tmp_path), \
             patch("cron.executions.get_execution", return_value=terminal):
            assert sched.sweep_stale_inflight([job]) == [job["id"]]


    def test_malformed_database_row_fails_closed(self, tmp_path):
        job = _job(job_id="malformed-row", minutes=10)
        lease = self._inject_claim(job["id"], age_seconds=5 * 60 * 60)
        malformed = self._row(lease, "completed")
        malformed.pop("process_id")

        with patch.object(sched, "_get_hermes_home", return_value=tmp_path), \
             patch("cron.executions.get_execution", return_value=malformed):
            assert sched.sweep_stale_inflight([job]) == []
        assert job["id"] in sched.get_running_job_ids()
        assert sched.get_inflight_guard_stats()["recent_findings"][-1]["reason"] == (
            "execution-row-invalid"
        )

    def test_missing_and_nonterminal_rows_fail_closed(self, tmp_path):
        job = _job(job_id="uncertain-row", minutes=10)
        lease = self._inject_claim(job["id"], age_seconds=5 * 60 * 60)

        with patch.object(sched, "_get_hermes_home", return_value=tmp_path), \
             patch("cron.executions.get_execution", return_value=None):
            assert sched.sweep_stale_inflight([job]) == []
        assert job["id"] in sched.get_running_job_ids()

        row = self._row(lease, "running")
        with patch.object(sched, "_get_hermes_home", return_value=tmp_path), \
             patch("cron.executions.get_execution", return_value=row):
            assert sched.sweep_stale_inflight([job]) == []
        assert job["id"] in sched.get_running_job_ids()

    def test_unstarted_absent_owner_can_be_recovered_without_job_store_write(self, tmp_path):
        job = _job(job_id="owner-absent", minutes=10)
        lease = self._inject_claim(job["id"], age_seconds=5 * 60 * 60)
        row = self._row(lease, "claimed")
        terminal = {**row, "status": "failed"}

        with patch.object(sched, "_get_hermes_home", return_value=tmp_path), \
             patch("cron.executions.get_execution", return_value=row), \
             patch(
                 "cron.executions.finish_unstarted_execution",
                 return_value=terminal,
             ) as finish, \
             patch.object(sched, "mark_job_run") as mark:
            assert sched.sweep_stale_inflight([job]) == [job["id"]]
        finish.assert_called_once()
        mark.assert_not_called()

    def test_lost_unstarted_execution_cas_does_not_release_lease(self, tmp_path):
        job = _job(job_id="handoff-race", minutes=10)
        lease = self._inject_claim(job["id"], age_seconds=5 * 60 * 60)
        claimed = self._row(lease, "claimed")
        handed_off = {**claimed, "handoff_pending": 1}

        with patch.object(sched, "_get_hermes_home", return_value=tmp_path), \
             patch(
                 "cron.executions.get_execution",
                 side_effect=[claimed, handed_off],
             ), \
             patch(
                 "cron.executions.finish_unstarted_execution",
                 return_value=None,
             ) as finish:
            assert sched.sweep_stale_inflight([job]) == []

        finish.assert_called_once()
        assert job["id"] in sched.get_running_job_ids()
        assert sched.get_inflight_guard_stats()["recent_findings"][-1]["reason"] == (
            "execution-terminal-cas-lost"
        )


def test_two_suppressed_occurrences_are_cadence_normalized_and_bounded():
    job_id = "suppressed-job"
    job = _job(job_id=job_id, minutes=10)
    assert sched.try_register_running_job(
        job_id,
        scheduled_occurrence="2026-11-10T10:00:00+00:00",
        cadence_seconds=600,
    )
    first = {**job, "_scheduled_instant": "2026-11-10T10:00:00+00:00"}
    second = {**job, "_scheduled_instant": "2026-11-10T10:10:00+00:00"}

    assert sched.note_already_running_skip(first) == 1
    assert sched.note_already_running_skip(first) == 1
    assert sched.note_already_running_skip(second) == 2
    stats = sched.get_inflight_guard_stats()
    assert stats["suppressed_occurrences_by_job"][job_id] == 2
    assert stats["leases"][0]["scheduled_occurrence"] == (
        "2026-11-10T10:00:00+00:00"
    )
    assert stats["oldest_lease_job_id"] == job_id
    assert stats["oldest_lease_age_seconds"] >= 0
    assert stats["leases"][0]["owner_thread_id"] == threading.current_thread().ident
    finding = stats["recent_findings"][-1]
    assert finding["event"] == "repeated_suppression"
    assert finding["normalized_suppression_count"] == 2
    assert finding["cadence_seconds"] == 600


def test_finding_history_is_bounded():
    for index in range(inflight._MAX_RECENT_FINDINGS + 3):
        job_id = f"bounded-{index}"
        assert sched.try_register_running_job(job_id, cadence_seconds=60)
        job = _job(job_id=job_id, minutes=1)
        sched.note_already_running_skip(
            {**job, "_scheduled_instant": "2026-11-10T10:00:00+00:00"})
        sched.note_already_running_skip(
            {**job, "_scheduled_instant": "2026-11-10T10:01:00+00:00"})
        assert sched.release_running_job(job_id)

    assert len(sched.get_inflight_guard_stats()["recent_findings"]) == (
        inflight._MAX_RECENT_FINDINGS
    )
