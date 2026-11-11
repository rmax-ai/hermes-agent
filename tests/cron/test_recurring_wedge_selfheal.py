"""Exercise lease recovery and completion accounting through the real scheduler tick."""
from __future__ import annotations

import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def _inject_absent_pre_run_owner(scheduler, job_id: str) -> None:
    assert scheduler.try_register_running_job(job_id)
    lease = scheduler._running_leases[job_id]
    lease.started_at -= 6 * 60 * 60
    lease.started_monotonic -= 6 * 60 * 60
    lease.owner_thread = threading.Thread(target=lambda: None)
    lease.owner_thread.start()
    lease.owner_thread.join()
    lease.owner_thread_ident = lease.owner_thread.ident
    lease.owner_thread_name = lease.owner_thread.name


@pytest.fixture
def cron_env(tmp_path, monkeypatch):
    """Isolated cron env + a recurring no_agent interval job, due NOW."""
    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    (hermes_home / "cron").mkdir()
    (hermes_home / "cron" / "output").mkdir()
    (hermes_home / "scripts").mkdir()
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))

    import cron.jobs as jobs_mod
    monkeypatch.setattr(jobs_mod, "HERMES_DIR", hermes_home)
    monkeypatch.setattr(jobs_mod, "CRON_DIR", hermes_home / "cron")
    monkeypatch.setattr(jobs_mod, "JOBS_FILE", hermes_home / "cron" / "jobs.json")
    monkeypatch.setattr(jobs_mod, "OUTPUT_DIR", hermes_home / "cron" / "output")

    job = jobs_mod.create_job(
        prompt="probe",
        schedule="every 10m",
        no_agent=True,
        script="probe.py",
    )
    now = datetime.now(timezone.utc)
    jobs_mod.update_job(job["id"], {"next_run_at": (now - timedelta(minutes=1)).isoformat()})

    script = hermes_home / "scripts" / "probe.py"
    script.write_text("print('ok')\n")

    return {"home": hermes_home, "job_id": job["id"]}


@pytest.fixture(autouse=True)
def _clear_scheduler_leases():
    from cron import scheduler
    import cron.scheduler_inflight as inflight

    scheduler._running_job_ids.clear()
    scheduler._running_since.clear()
    scheduler._running_futures.clear()
    scheduler._running_leases.clear()
    scheduler.reset_inflight_guard_stats()
    inflight._current_lease_ids.set({})
    yield
    scheduler._running_job_ids.clear()
    scheduler._running_since.clear()
    scheduler._running_futures.clear()
    scheduler._running_leases.clear()
    inflight._current_lease_ids.set({})


class TestStaleInflightSelfHeal:
    def _setup(self, cron_env, monkeypatch):
        from cron import scheduler as S
        from cron import executions as E

        env = cron_env
        monkeypatch.setattr(E, "EXECUTIONS_FILE", env["home"] / "cron" / "executions.db")
        monkeypatch.setattr(S, "_hermes_home", env["home"])
        return S, E, env

    def test_stale_claim_self_heals_and_redispatches(self, cron_env, monkeypatch):
        """A proven absent pre-run owner is recovered, then the due job completes."""
        S, E, env = self._setup(cron_env, monkeypatch)
        job_id = env["job_id"]
        import cron.jobs as J

        _inject_absent_pre_run_owner(S, job_id)

        # get_due_jobs is called inside tick before the sweep; it will release
        # only after the recorded owner has been shown to be absent.
        job = J.get_job(job_id)
        with mock.patch("cron.jobs.load_jobs", return_value=[job]):
            n = S.tick(verbose=False, sync=True)

        latest = E.latest_execution(job_id)
        assert job_id not in S.get_running_job_ids(), "stale claim must be released"
        assert latest is not None, "wedged job must create an execution"
        assert latest["status"] == "completed", (
            "wedged job must fire again without force-run"
        )

    def test_two_consecutive_auto_fires_after_guard(self, cron_env, monkeypatch):
        """Normal completion releases one run before a distinct slot fires."""
        S, E, env = self._setup(cron_env, monkeypatch)
        job_id = env["job_id"]
        import cron.jobs as J

        _inject_absent_pre_run_owner(S, job_id)

        job = J.get_job(job_id)
        with mock.patch("cron.jobs.load_jobs", return_value=[job]):
            n1 = S.tick(verbose=False, sync=True)
        latest1 = E.latest_execution(job_id)
        assert latest1["status"] == "completed"

        # Re-arm due and tick again: fire #2.
        now = datetime.now(timezone.utc)
        J.update_job(job_id, {"next_run_at": (now - timedelta(minutes=1)).isoformat()})
        n2 = S.tick(verbose=False, sync=True)
        latest2 = E.latest_execution(job_id)
        assert latest2["status"] == "completed"
        assert latest2["id"] != latest1["id"], "two distinct executions"

    def test_guard_stats_reported(self, cron_env, monkeypatch):
        """The guard must surface a countable forced-release signal."""
        S, E, env = self._setup(cron_env, monkeypatch)
        import cron.jobs as J
        _inject_absent_pre_run_owner(S, env["job_id"])
        S.sweep_stale_inflight([J.get_job(env["job_id"])])
        stats = S.get_inflight_guard_stats()
        assert stats["forced_releases"] >= 1
        assert env["job_id"] not in S.get_running_job_ids()


class TestEAGAINDispatchRecovery:
    """Separate safe pre-submit failures from ambiguous executor handoffs."""

    def test_create_execution_failure_releases_claim(self, cron_env, monkeypatch, tmp_path):
        from cron import scheduler as S
        from cron import executions as E
        import cron.jobs as J

        env = cron_env
        monkeypatch.setattr(E, "EXECUTIONS_FILE", env["home"] / "cron" / "executions.db")
        monkeypatch.setattr(S, "_hermes_home", env["home"])
        job_id = env["job_id"]
        job = J.get_job(job_id)

        S._running_job_ids.clear()
        S._running_since.clear()
        S._running_futures.clear()

        # Simulate EAGAIN during create_execution (substrate thread exhaustion
        # at 12:50): the in-flight claim was taken but execution creation fails.
        def boom(*a, **k):
            raise OSError(11, "Resource temporarily unavailable")
        monkeypatch.setattr(S, "create_execution", boom)

        with mock.patch("cron.jobs.load_jobs", return_value=[job]):
            # The failure is contained per-job (#86482 follow-up): the tick
            # logs an ERROR, skips this fire, and moves on to the remaining
            # due jobs instead of aborting the whole dispatch loop.
            S.tick(verbose=False, sync=True)

        # The claim must be released (not leaked) so the NEXT tick can retry.
        assert job_id not in S.get_running_job_ids(), (
            "claim must be released when execution creation fails, so the "
            "next tick re-dispatches instead of wedging on 'already running'"
        )

    def test_pool_submit_eagain_retains_ambiguous_claim_until_run_finishes(
        self, cron_env, monkeypatch, tmp_path,
    ):
        from cron import scheduler as S
        from cron import executions as E
        import cron.jobs as J

        env = cron_env
        S._shutdown_parallel_pool()
        monkeypatch.setattr(E, "EXECUTIONS_FILE", env["home"] / "cron" / "executions.db")
        monkeypatch.setattr(S, "_hermes_home", env["home"])
        job_id = env["job_id"]
        job = J.get_job(job_id)

        S._running_job_ids.clear()
        S._running_since.clear()
        S._running_futures.clear()

        # submit() may enqueue the worker before thread startup raises EAGAIN.
        # Model that boundary by retaining the callable, then raising.
        real_submit = S.concurrent.futures.ThreadPoolExecutor.submit
        state = {"n": 0, "queued": None}

        def flaky(self, callable_, *args, **kwargs):
            state["n"] += 1
            if state["n"] == 1:
                state["queued"] = (callable_, args, kwargs)
                raise OSError(11, "Resource temporarily unavailable")
            return real_submit(self, callable_, *args, **kwargs)

        monkeypatch.setattr(S.concurrent.futures.ThreadPoolExecutor, "submit", flaky)

        with mock.patch("cron.jobs.load_jobs", return_value=[job]):
            S.tick(verbose=False, sync=True)

        attempt = E.latest_execution(job_id)
        assert attempt is not None and attempt["status"] == "claimed"
        assert state["queued"] is not None
        assert job_id in S.get_running_job_ids()

        # A later dispatch attempt cannot race the still-possible queued run.
        assert S._submit_with_guard(job, S._parallel_pool, lambda _job: True) is None
        assert state["n"] == 1

        callable_, args, kwargs = state["queued"]
        try:
            callable_(*args, **kwargs)
        finally:
            S._shutdown_parallel_pool()

        completed = E.get_execution(attempt["id"])
        assert completed is not None and completed["status"] == "completed"
        assert len(E.list_executions(job_id=job_id)) == 1
        assert job_id not in S.get_running_job_ids()
