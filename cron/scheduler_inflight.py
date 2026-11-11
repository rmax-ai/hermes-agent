"""Process-local cron dispatch leases and bounded recovery diagnostics."""

from __future__ import annotations

import contextvars
import logging
import math
import os
import sys
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, replace
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, ValidationError

from cron.occurrences import scheduled_instant as _scheduled_instant

logger = logging.getLogger(__name__)

_MAX_RECENT_FINDINGS = 20
_MAX_LEASES_PER_SWEEP = 100
_MAX_REPORTED_LEGACY_LEASES = 256
_INFLIGHT_MIN_ALLOWANCE_MINUTES = 30.0

_running_lock = threading.Lock()
_running_job_ids: set[str] = set()
_running_since: dict[str, float] = {}
_running_futures: dict[str, Any] = {}
_running_leases: dict[str, "_InFlightLease"] = {}
_FUTURE_PENDING = object()
_current_lease_ids: contextvars.ContextVar[dict[str, str]] = contextvars.ContextVar(
    "cron_inflight_lease_ids", default={})

_forced_release_count = 0
_forced_releases: list[dict[str, Any]] = []
_inflight_findings: list[dict[str, Any]] = []
_reported_legacy_leases: OrderedDict[str, None] = OrderedDict()
_cron_interval_cache: dict[str, Optional[float]] = {}


class _ExecutionLeaseRow(BaseModel):
    """Validated execution-ledger fields used to prove lease ownership or completion."""

    model_config = ConfigDict(strict=True, extra="ignore")

    id: str
    job_id: str
    process_id: str
    pid: int
    process_started_at: Optional[int]
    status: Literal["claimed", "running", "completed", "failed", "unknown"]
    handoff_pending: int
    scheduled_instant: Optional[str]


@dataclass
class _InFlightLease:
    job_id: str
    lease_id: str
    scheduled_occurrence: Optional[str]
    run_id: Optional[str]
    owner_task_id: str
    owner_process_id: str
    owner_pid: int
    owner_process_started_at: Optional[int]
    owner_thread: Optional[threading.Thread]
    owner_thread_ident: Optional[int]
    owner_thread_name: Optional[str]
    started_at: float
    started_monotonic: float
    last_progress_at: float
    last_progress_monotonic: float
    cadence_seconds: Optional[float]
    phase: str
    future: Any
    suppressed_occurrences: int = 0
    last_suppressed_occurrence: Optional[str] = None
    suppression_finding_recorded: bool = False
    stale_finding_recorded: bool = False


def _execution_row(lease: _InFlightLease) -> tuple[Optional[_ExecutionLeaseRow], str]:
    if not lease.run_id:
        return None, "run-not-created"
    try:
        from cron.executions import get_execution

        raw = get_execution(lease.run_id)
    except Exception:
        return None, "execution-read-failed"
    if raw is None:
        return None, "execution-missing"
    return _parse_execution_row(lease, raw)


def _parse_execution_row(
    lease: _InFlightLease, raw: Any,
) -> tuple[Optional[_ExecutionLeaseRow], str]:
    try:
        row = _ExecutionLeaseRow.model_validate(raw)
    except ValidationError:
        return None, "execution-row-invalid"
    if row.id != lease.run_id or row.job_id != lease.job_id:
        return None, "execution-identity-mismatch"
    if row.scheduled_instant != lease.scheduled_occurrence:
        return None, "execution-occurrence-mismatch"
    return row, ""


def _same_owner_process(lease: _InFlightLease, row: _ExecutionLeaseRow) -> bool:
    return (
        row.process_id == lease.owner_process_id
        and row.pid == lease.owner_pid
        and row.process_started_at == lease.owner_process_started_at
    )


def _owner_thread_is_alive(lease: _InFlightLease) -> bool:
    return lease.owner_thread is not None and lease.owner_thread.is_alive()


def _assess_owner(lease: _InFlightLease) -> tuple[str, str, Optional[_ExecutionLeaseRow]]:
    """Classify only exact owner/run evidence; unknown state stays fail-closed."""
    future = lease.future
    future_done = False
    if future is not None and future is not _FUTURE_PENDING:
        if not callable(getattr(future, "done", None)):
            return "ambiguous", "future-state-unavailable", None
        if not future.done():
            return "active", "future-running", None
        future_done = True

    if not future_done and _owner_thread_is_alive(lease):
        return "active", "owner-thread-running", None

    if future is _FUTURE_PENDING and lease.phase == "submitting":
        # submit() may have enqueued work before its caller disappeared. The future is not
        # available yet, so neither a missing thread nor age proves the submitted task is absent.
        return "ambiguous", "submit-result-unknown", None

    if lease.run_id is None:
        if future_done:
            return "terminal", "future-terminal-before-run-record", None
        if lease.phase == "direct":
            return "absent", "owner-ended-before-run-record", None
        return "ambiguous", "run-identity-unavailable", None

    row, row_error = _execution_row(lease)
    if row is None:
        return "ambiguous", row_error, None
    if row.status in {"completed", "failed", "unknown"}:
        if not _same_owner_process(lease, row):
            return "ambiguous", "execution-owner-mismatch", row
        return "terminal", "execution-terminal", row
    if (
        row.status == "claimed"
        and row.handoff_pending == 0
        and _same_owner_process(lease, row)
        and lease.phase in {"direct", "worker", "future"}
    ):
        # The run has not crossed the claimed -> running gate or handed off to a child.
        return "absent", "owner-ended-before-run-start", row
    return "ambiguous", f"execution-{row.status}-owner-uncertain", row


def _same_lease_state(current: _InFlightLease, expected: _InFlightLease) -> bool:
    return (
        current.lease_id == expected.lease_id
        and current.run_id == expected.run_id
        and current.scheduled_occurrence == expected.scheduled_occurrence
        and current.owner_task_id == expected.owner_task_id
        and current.owner_process_id == expected.owner_process_id
        and current.owner_pid == expected.owner_pid
        and current.owner_process_started_at == expected.owner_process_started_at
        and current.phase == expected.phase
        and current.future is expected.future
        and current.owner_thread is expected.owner_thread
        and current.owner_thread_ident == expected.owner_thread_ident
        and current.started_monotonic == expected.started_monotonic
        and current.last_progress_monotonic == expected.last_progress_monotonic
    )


def _can_release_from_evidence(
    current: _InFlightLease,
    expected: _InFlightLease,
    row: Optional[_ExecutionLeaseRow],
    owner_state: str,
) -> bool:
    if not _same_lease_state(current, expected):
        return False
    future_done = False
    if current.future is not None and current.future is not _FUTURE_PENDING:
        if not callable(getattr(current.future, "done", None)):
            return False
        future_done = current.future.done()
        if not future_done:
            return False
    if not future_done and _owner_thread_is_alive(current):
        return False
    if current.future is _FUTURE_PENDING and current.phase == "submitting":
        return False
    if current.run_id is None:
        return (
            owner_state == "terminal" and future_done
        ) or (
            owner_state == "absent" and current.phase == "direct"
        )
    if row is None:
        return False
    if (
        row.id != current.run_id
        or row.job_id != current.job_id
        or row.scheduled_instant != current.scheduled_occurrence
    ):
        return False
    if row.status in {"completed", "failed", "unknown"}:
        return owner_state == "terminal" and _same_owner_process(current, row)
    return (
        owner_state == "absent"
        and row.status == "claimed"
        and row.handoff_pending == 0
        and _same_owner_process(current, row)
        and current.phase in {"direct", "worker", "future"}
    )


def _remove_lease_locked(job_id: str, lease: _InFlightLease) -> None:
    _running_job_ids.discard(job_id)
    _running_since.pop(job_id, None)
    _running_futures.pop(job_id, None)
    _running_leases.pop(job_id, None)
    scheduler = sys.modules.get("cron.scheduler")
    if scheduler is not None:
        scheduler._running_worker_pids.pop(job_id, None)


def _forget_context_lease(job_id: str, lease_id: str) -> None:
    tokens = _current_lease_ids.get()
    if tokens.get(job_id) == lease_id:
        updated = dict(tokens)
        updated.pop(job_id, None)
        _current_lease_ids.set(updated)


def try_register_running_job(
    job_id: str,
    *,
    scheduled_occurrence: Optional[str] = None,
    cadence_seconds: Optional[float] = None,
    dispatching: bool = False,
) -> bool:
    """Acquire the single per-job lease and bind it to this run attempt."""
    from hermes_cli.backend_retirement import retirement

    job_id = str(job_id)
    occurrence = _scheduled_instant(scheduled_occurrence)
    try:
        cadence = float(cadence_seconds) if cadence_seconds is not None else None
        if cadence is not None and (not math.isfinite(cadence) or cadence <= 0):
            cadence = None
    except (TypeError, ValueError):
        cadence = None

    with retirement.work() as admitted:
        if not admitted:
            return False
        from cron.executions import _PROCESS_ID, _process_start_time

        owner_pid = os.getpid()
        owner_started_at = _process_start_time(owner_pid)
        now_wall = time.time()
        now_mono = time.monotonic()
        owner_thread = threading.current_thread()
        lease_id = uuid.uuid4().hex
        with _running_lock:
            if job_id in _running_job_ids:
                return False

            lease = _InFlightLease(
                job_id=job_id,
                lease_id=lease_id,
                scheduled_occurrence=occurrence,
                run_id=None,
                owner_task_id=lease_id,
                owner_process_id=_PROCESS_ID,
                owner_pid=owner_pid,
                owner_process_started_at=owner_started_at,
                owner_thread=owner_thread,
                owner_thread_ident=owner_thread.ident,
                owner_thread_name=owner_thread.name[:80],
                started_at=now_wall,
                started_monotonic=now_mono,
                last_progress_at=now_wall,
                last_progress_monotonic=now_mono,
                cadence_seconds=cadence,
                phase="submitting" if dispatching else "direct",
                future=_FUTURE_PENDING if dispatching else None,
            )
            _running_job_ids.add(job_id)
            _running_since[job_id] = now_wall
            _running_futures[job_id] = lease.future
            _running_leases[job_id] = lease

    tokens = dict(_current_lease_ids.get())
    tokens[job_id] = lease_id
    _current_lease_ids.set(tokens)
    return True


def get_running_job_lease_id(job_id: str) -> Optional[str]:
    with _running_lock:
        lease = _running_leases.get(str(job_id))
        return lease.lease_id if lease is not None else None


def bind_running_job_execution(
    job_id: str,
    run_id: str,
    *,
    scheduled_occurrence: Optional[str] = None,
    cadence_seconds: Optional[float] = None,
    lease_id: Optional[str] = None,
) -> bool:
    """Bind the durable execution row to the lease before any worker side effects."""
    job_id = str(job_id)
    run_id = str(run_id)
    if lease_id is None:
        lease_id = _current_lease_ids.get().get(job_id)
    occurrence = _scheduled_instant(scheduled_occurrence)
    with _running_lock:
        lease = _running_leases.get(job_id)
        if (
            lease is None
            or not lease_id
            or lease.lease_id != lease_id
            or lease.owner_thread is not threading.current_thread()
            or (lease.run_id is not None and lease.run_id != run_id)
            or (
                lease.scheduled_occurrence is not None
                and lease.scheduled_occurrence != occurrence
            )
        ):
            return False
        lease.run_id = run_id
        lease.owner_task_id = run_id
        if occurrence is not None:
            lease.scheduled_occurrence = occurrence
        try:
            cadence = float(cadence_seconds) if cadence_seconds is not None else None
            if cadence is not None and math.isfinite(cadence) and cadence > 0:
                lease.cadence_seconds = cadence
        except (TypeError, ValueError):
            pass
        lease.last_progress_at = time.time()
        lease.last_progress_monotonic = time.monotonic()
        return True


def bind_running_job_worker(job_id: str, lease_id: str, run_id: str) -> bool:
    """Transfer task ownership to the executor thread before invoking job code."""
    job_id = str(job_id)
    with _running_lock:
        lease = _running_leases.get(job_id)
        if lease is None or lease.lease_id != lease_id or lease.run_id != str(run_id):
            return False
        owner_thread = threading.current_thread()
        lease.owner_thread = owner_thread
        lease.owner_thread_ident = owner_thread.ident
        lease.owner_thread_name = owner_thread.name[:80]
        lease.owner_task_id = str(run_id)
        lease.phase = "worker"
        lease.last_progress_at = time.time()
        lease.last_progress_monotonic = time.monotonic()
        return True


def bind_running_job_execution_owner(
    job_id: str,
    lease_id: str,
    run_id: str,
    raw_row: Any,
    *,
    expected_pid: int,
) -> bool:
    """Track the exact durable owner after a restart-safe worker adopts the run."""
    job_id = str(job_id)
    run_id = str(run_id)
    with _running_lock:
        lease = _running_leases.get(job_id)
        if (
            lease is None
            or lease.lease_id != lease_id
            or lease.run_id != run_id
            or lease.owner_thread is not threading.current_thread()
        ):
            return False
        snapshot = replace(lease)
    row, _error = _parse_execution_row(snapshot, raw_row)
    if (
        row is None
        or row.pid != expected_pid
        or row.status not in {"running", "completed", "failed", "unknown"}
    ):
        return False
    with _running_lock:
        current = _running_leases.get(job_id)
        if (
            current is None
            or current.lease_id != lease_id
            or not _same_lease_state(current, snapshot)
            or current.owner_thread is not threading.current_thread()
        ):
            return False
        current.owner_process_id = row.process_id
        current.owner_pid = row.pid
        current.owner_process_started_at = row.process_started_at
        current.last_progress_at = time.time()
        current.last_progress_monotonic = time.monotonic()
        return True


def bind_running_job_future(job_id: str, lease_id: str, future: Any) -> bool:
    """Publish the executor owner unless its terminal finally already released the lease."""
    job_id = str(job_id)
    with _running_lock:
        lease = _running_leases.get(job_id)
        if lease is None or lease.lease_id != lease_id:
            return False
        lease.future = future
        if lease.phase == "submitting":
            lease.phase = "future"
        _running_futures[job_id] = future
        lease.last_progress_at = time.time()
        lease.last_progress_monotonic = time.monotonic()
        return True


def touch_running_job(job_id: str, run_id: str) -> bool:
    """Refresh diagnostic progress only for the execution that owns this lease."""
    with _running_lock:
        lease = _running_leases.get(str(job_id))
        if lease is None or lease.run_id != str(run_id):
            return False
        lease.last_progress_at = time.time()
        lease.last_progress_monotonic = time.monotonic()
        return True


def release_running_job(
    job_id: str,
    *,
    lease_id: Optional[str] = None,
    _expected: Optional[_InFlightLease] = None,
    _execution_row: Optional[_ExecutionLeaseRow] = None,
    _owner_state: Optional[str] = None,
) -> bool:
    """CAS-release one lease from its owner finally or from proven terminal recovery."""
    job_id = str(job_id)
    if lease_id is None:
        lease_id = _current_lease_ids.get().get(job_id)
    if not lease_id:
        return False

    with _running_lock:
        lease = _running_leases.get(job_id)
        if lease is None or lease.lease_id != lease_id:
            return False
        if _expected is None and lease.owner_thread is not threading.current_thread():
            return False
        if _expected is not None:
            if not _can_release_from_evidence(
                lease, _expected, _execution_row, _owner_state or "ambiguous"
            ):
                return False
        _remove_lease_locked(job_id, lease)
    _forget_context_lease(job_id, lease_id)
    return True


def _job_interval_minutes(job: dict) -> Optional[float]:
    """Best-effort cadence in minutes from a persisted schedule snapshot."""
    try:
        schedule = job.get("schedule")
        if isinstance(schedule, str) and schedule.strip():
            from cron.jobs import parse_schedule

            schedule = parse_schedule(schedule) or {}
        if not isinstance(schedule, dict):
            return None
        kind = schedule.get("kind")
        if kind == "interval":
            minutes = schedule.get("minutes")
            return float(minutes) if minutes else None
        if kind != "cron":
            return None
        expr = str(schedule.get("expr") or "")
        if expr in _cron_interval_cache:
            return _cron_interval_cache[expr]
        result = None
        from cron.jobs import _ensure_croniter

        if _ensure_croniter():
            from cron.jobs import croniter
            from datetime import datetime

            iterator = croniter(expr, datetime.now())
            first = iterator.get_next(datetime)
            second = iterator.get_next(datetime)
            gap = (second - first).total_seconds() / 60.0
            result = gap if gap > 0 else None
        _cron_interval_cache[expr] = result
        return result
    except Exception:
        return None


def _inflight_min_allowance_minutes() -> float:
    """Resolve the existing stale-diagnostic floor without adding a new setting."""
    from cron.env_settings import cron_env_setting

    scheduler = __import__("sys").modules.get("cron.scheduler")
    if scheduler is not None:
        try:
            config = scheduler.load_config() or {}
            cron_config = config.get("cron", {}) if isinstance(config, dict) else {}
            configured = cron_config.get("inflight_max_minutes")
            if configured is not None:
                value = float(configured)
                if value > 0 and math.isfinite(value):
                    return value
        except Exception:
            pass
    raw = cron_env_setting("HERMES_CRON_INFLIGHT_MAX_MINUTES").strip()
    if raw:
        try:
            value = float(raw)
            if value > 0 and math.isfinite(value):
                return value
        except (TypeError, ValueError):
            if scheduler is not None:
                scheduler.logger.warning(
                    "Invalid HERMES_CRON_INFLIGHT_MAX_MINUTES=%r; using default %s",
                    raw, _INFLIGHT_MIN_ALLOWANCE_MINUTES)
    return _INFLIGHT_MIN_ALLOWANCE_MINUTES


def _age_seconds(lease: _InFlightLease, now: float) -> float:
    return max(0.0, now - lease.started_monotonic)


def _finding_entry(
    lease: _InFlightLease,
    *,
    event: str,
    age_seconds: float,
    owner_state: str,
    decision: str,
    reason: str,
) -> dict[str, Any]:
    now = time.monotonic()
    return {
        "event": event[:40],
        "job_id": lease.job_id[:128],
        "lease_id": lease.lease_id[:64],
        "run_id": lease.run_id[:64] if lease.run_id else None,
        "scheduled_occurrence": (
            lease.scheduled_occurrence[:64] if lease.scheduled_occurrence else None
        ),
        "age_seconds": round(max(0.0, age_seconds), 1),
        "last_progress_age_seconds": round(
            max(0.0, now - lease.last_progress_monotonic), 1),
        "cadence_seconds": (
            round(lease.cadence_seconds, 1) if lease.cadence_seconds else None
        ),
        "suppressed_occurrences": lease.suppressed_occurrences,
        "normalized_suppression_count": lease.suppressed_occurrences,
        "owner_state": owner_state[:32],
        "decision": decision[:32],
        "reason": reason[:64],
    }


def _record_finding(entry: dict[str, Any]) -> None:
    with _running_lock:
        _inflight_findings.append(entry)
        del _inflight_findings[:-_MAX_RECENT_FINDINGS]
    logger.warning(
        "cron.inflight.%s event=%s job=%s run=%s age=%.0fs owner=%s "
        "decision=%s reason=%s suppressions=%s cadence=%s",
        entry["event"], entry["event"], entry["job_id"], entry["run_id"] or "-",
        entry["age_seconds"], entry["owner_state"], entry["decision"], entry["reason"],
        entry["suppressed_occurrences"], entry["cadence_seconds"] or "-")


def note_already_running_skip(job: dict) -> int:
    """Count unique scheduled slots suppressed by this lease, independent of wall-clock cadence."""
    job_id = str(job.get("id") or "")
    occurrence = _scheduled_instant(job.get("_scheduled_instant"))
    if not job_id or occurrence is None:
        return 0
    cadence = _job_interval_minutes(job)
    cadence_seconds = cadence * 60.0 if cadence else None
    entry = None
    with _running_lock:
        lease = _running_leases.get(job_id)
        if lease is None or job_id not in _running_job_ids:
            return 0
        if lease.last_suppressed_occurrence == occurrence:
            return lease.suppressed_occurrences
        lease.last_suppressed_occurrence = occurrence
        lease.suppressed_occurrences += 1
        if lease.cadence_seconds is None and cadence_seconds is not None:
            lease.cadence_seconds = cadence_seconds
        if lease.suppressed_occurrences >= 2 and not lease.suppression_finding_recorded:
            lease.suppression_finding_recorded = True
            entry = _finding_entry(
                lease,
                event="repeated_suppression",
                age_seconds=_age_seconds(lease, time.monotonic()),
                owner_state="active",
                decision="skip",
                reason="two-scheduled-occurrences",
            )
        count = lease.suppressed_occurrences
    if entry is not None:
        _record_finding(entry)
    return count


def _snapshot_leases() -> tuple[list[_InFlightLease], list[str]]:
    with _running_lock:
        known = [
            replace(lease)
            for job_id, lease in _running_leases.items()
            if job_id in _running_job_ids
        ]
        legacy = sorted(_running_job_ids - _running_leases.keys())
    known.sort(key=lambda lease: lease.started_monotonic)
    known = known[:_MAX_LEASES_PER_SWEEP]
    if len(known) < _MAX_LEASES_PER_SWEEP:
        legacy = legacy[:_MAX_LEASES_PER_SWEEP - len(known)]
    else:
        legacy = []
    return known, legacy


def _record_stale_owner_finding(
    snapshot: _InFlightLease,
    *,
    age: float,
    owner_state: str,
    reason: str,
) -> None:
    with _running_lock:
        current = _running_leases.get(snapshot.job_id)
        if current is None or not _same_lease_state(current, snapshot):
            return
        if current.stale_finding_recorded:
            return
        current.stale_finding_recorded = True
        entry = _finding_entry(
            snapshot,
            event="stale_owner",
            age_seconds=age,
            owner_state=owner_state,
            decision="retain",
            reason=reason,
        )
    _record_finding(entry)


def note_running_job_submission_ambiguous(job_id: str, lease_id: str) -> bool:
    """Keep an unreturned executor submission fenced until its owner is known."""
    job_id = str(job_id)
    with _running_lock:
        lease = _running_leases.get(job_id)
        if (
            lease is None
            or lease.lease_id != lease_id
            or lease.phase != "submitting"
            or lease.future is not _FUTURE_PENDING
        ):
            return False
        # submit() may have queued the worker before raising. The dispatcher
        # thread no longer owns the run, but no Future is available to prove
        # whether the task is still queued or already terminal.
        lease.owner_thread = None
        lease.owner_thread_ident = None
        lease.owner_thread_name = None
        snapshot = replace(lease)
        age = _age_seconds(snapshot, time.monotonic())
    _record_stale_owner_finding(
        snapshot,
        age=age,
        owner_state="ambiguous",
        reason="submit-result-unknown",
    )
    return True


def _record_legacy_owner_finding(job_id: str, age: float) -> None:
    with _running_lock:
        if job_id in _reported_legacy_leases:
            return
        _reported_legacy_leases[job_id] = None
        if len(_reported_legacy_leases) > _MAX_REPORTED_LEGACY_LEASES:
            _reported_legacy_leases.popitem(last=False)
        entry = {
            "event": "stale_owner",
            "job_id": job_id[:128],
            "lease_id": None,
            "run_id": None,
            "scheduled_occurrence": None,
            "age_seconds": round(max(0.0, age), 1),
            "last_progress_age_seconds": None,
            "cadence_seconds": None,
            "suppressed_occurrences": 0,
            "normalized_suppression_count": 0,
            "owner_state": "ambiguous",
            "decision": "retain",
            "reason": "owner-identity-unavailable",
        }
    _record_finding(entry)


def _release_from_sweep(
    snapshot: _InFlightLease,
    *,
    owner_state: str,
    row: Optional[_ExecutionLeaseRow],
    reason: str,
    age: float,
    allowance: float,
) -> bool:
    final_owner_state = owner_state
    final_reason = reason
    if row is not None and row.status == "claimed":
        try:
            from cron.executions import finish_unstarted_execution, get_execution

            terminal = finish_unstarted_execution(
                row.id,
                job_id=snapshot.job_id,
                process_id=snapshot.owner_process_id,
                pid=snapshot.owner_pid,
                process_started_at=snapshot.owner_process_started_at,
                scheduled_instant=snapshot.scheduled_occurrence,
                error="Cron lease owner ended before execution began.",
            )
            if terminal is None:
                terminal = get_execution(row.id)
        except Exception:
            logger.warning(
                "Could not close unstarted cron execution %s after lease recovery",
                row.id,
                exc_info=True,
            )
            return False
        row, _ = _parse_execution_row(snapshot, terminal)
        if (
            row is None
            or row.status not in {"completed", "failed", "unknown"}
            or not _same_owner_process(snapshot, row)
        ):
            _record_stale_owner_finding(
                snapshot,
                age=age,
                owner_state="ambiguous",
                reason="execution-terminal-cas-lost",
            )
            return False
        final_owner_state = "terminal"
        final_reason = "unstarted-execution-terminalized"

    if not release_running_job(
        snapshot.job_id,
        lease_id=snapshot.lease_id,
        _expected=snapshot,
        _execution_row=row,
        _owner_state=final_owner_state,
    ):
        return False

    global _forced_release_count
    entry = _finding_entry(
        snapshot,
        event="forced_release",
        age_seconds=age,
        owner_state=final_owner_state,
        decision="released",
        reason=final_reason,
    )
    entry["allowance_seconds"] = round(allowance, 1)
    with _running_lock:
        _forced_release_count += 1
        _forced_releases.append(entry)
        del _forced_releases[:-_MAX_RECENT_FINDINGS]
    _record_finding(entry)
    return True


def sweep_stale_inflight(due_jobs: Optional[list] = None) -> list[str]:
    """Recover only terminal or provably absent owners; age alone never releases a lease."""
    by_id = {
        str(job.get("id")): job
        for job in (due_jobs or [])
        if isinstance(job, dict) and job.get("id") is not None
    }
    floor_seconds = _inflight_min_allowance_minutes() * 60.0
    now_mono = time.monotonic()
    now_wall = time.time()
    leases, legacy_ids = _snapshot_leases()
    released: list[str] = []

    for snapshot in leases:
        age = _age_seconds(snapshot, now_mono)
        interval_minutes = _job_interval_minutes(by_id.get(snapshot.job_id) or {})
        cadence = (
            interval_minutes * 60.0
            if interval_minutes and math.isfinite(interval_minutes) and interval_minutes > 0
            else snapshot.cadence_seconds
        )
        allowance = max(floor_seconds, 2.0 * cadence if cadence else 0.0)
        owner_state, reason, row = _assess_owner(snapshot)
        if owner_state == "terminal":
            if _release_from_sweep(
                snapshot, owner_state=owner_state, row=row, reason=reason,
                age=age, allowance=allowance,
            ):
                released.append(snapshot.job_id)
            else:
                _record_stale_owner_finding(
                    snapshot,
                    age=age,
                    owner_state="ambiguous",
                    reason="lease-state-changed-before-release",
                )
            continue
        if age < allowance:
            continue
        if owner_state == "absent":
            if _release_from_sweep(
                snapshot, owner_state=owner_state, row=row, reason=reason,
                age=age, allowance=allowance,
            ):
                released.append(snapshot.job_id)
            else:
                _record_stale_owner_finding(
                    snapshot,
                    age=age,
                    owner_state="ambiguous",
                    reason="lease-state-changed-before-release",
                )
            continue
        _record_stale_owner_finding(
            snapshot, age=age, owner_state=owner_state, reason=reason)

    # State written before this lease registry was introduced has no task/run identity. Its
    # wall-clock age is diagnostic only: it can never authorize recovery.
    with _running_lock:
        legacy_since = {
            job_id: _running_since.get(job_id)
            for job_id in legacy_ids
            if job_id in _running_job_ids and job_id not in _running_leases
        }
    for job_id, started in legacy_since.items():
        if started is None:
            continue
        interval = _job_interval_minutes(by_id.get(job_id) or {})
        allowance = max(
            floor_seconds,
            2.0 * interval * 60.0 if interval and interval > 0 else 0.0,
        )
        age = max(0.0, now_wall - started)
        if age >= allowance:
            _record_legacy_owner_finding(job_id, age)
    return released


def get_running_job_ids() -> frozenset[str]:
    """Thread-safe snapshot used by the gateway shutdown drain."""
    scheduler = sys.modules.get("cron.scheduler")
    with _running_lock:
        running = set(_running_job_ids)
        if scheduler is not None:
            running.update(scheduler._running_fire_owners.keys())
        return frozenset(running)


def get_running_job_details() -> list[dict[str, Any]]:
    now = time.monotonic()
    scheduler = sys.modules.get("cron.scheduler")
    worker_pids = scheduler._running_worker_pids if scheduler is not None else {}
    with _running_lock:
        job_ids = set(_running_job_ids)
        if scheduler is not None:
            job_ids.update(scheduler._running_fire_owners.keys())
        details = []
        for job_id in sorted(job_ids):
            lease = _running_leases.get(job_id)
            elapsed = (
                round(_age_seconds(lease, now), 1)
                if lease is not None
                else (
                    round(max(0.0, time.time() - _running_since[job_id]), 1)
                    if job_id in _running_since else None
                )
            )
            details.append({
                "job_id": job_id,
                "elapsed_s": elapsed,
                "worker_pid": worker_pids.get(job_id),
            })
        return details


def get_inflight_guard_stats() -> dict[str, Any]:
    now_mono = time.monotonic()
    with _running_lock:
        leases = [
            lease for job_id, lease in _running_leases.items()
            if job_id in _running_job_ids
        ]
        ages = {
            job_id: round(_age_seconds(lease, now_mono), 1)
            for job_id, lease in _running_leases.items()
            if job_id in _running_job_ids
        }
        for job_id in _running_job_ids - ages.keys():
            started = _running_since.get(job_id)
            if started is not None:
                ages[job_id] = round(max(0.0, time.time() - started), 1)
        leases.sort(key=lambda lease: lease.started_monotonic)
        oldest = max(ages.items(), key=lambda pair: pair[1]) if ages else None
        lease_details = [
            {
                "job_id": lease.job_id,
                "lease_id": lease.lease_id,
                "run_id": lease.run_id,
                "scheduled_occurrence": lease.scheduled_occurrence,
                "owner_task_id": lease.owner_task_id,
                "owner_process_id": lease.owner_process_id,
                "owner_pid": lease.owner_pid,
                "owner_thread_id": lease.owner_thread_ident,
                "started_at": lease.started_at,
                "last_progress_at": lease.last_progress_at,
                "age_seconds": ages.get(lease.job_id),
                "last_progress_age_seconds": round(
                    max(0.0, now_mono - lease.last_progress_monotonic), 1),
                "cadence_seconds": lease.cadence_seconds,
                "suppressed_occurrences": lease.suppressed_occurrences,
            }
            for lease in leases[-_MAX_RECENT_FINDINGS:]
        ]
        return {
            "running": sorted(_running_job_ids),
            "running_ages_seconds": ages,
            "oldest_lease_age_seconds": oldest[1] if oldest else None,
            "oldest_lease_job_id": oldest[0] if oldest else None,
            "suppressed_occurrences_by_job": {
                lease.job_id: lease.suppressed_occurrences
                for lease in leases
                if lease.suppressed_occurrences
            },
            "lease_count": len(_running_job_ids),
            "leases": lease_details,
            "forced_releases": _forced_release_count,
            "recent_forced_releases": list(_forced_releases),
            "recent_findings": list(_inflight_findings),
        }


def reset_inflight_guard_stats() -> None:
    """Reset diagnostic counters for isolated scheduler tests."""
    global _forced_release_count
    with _running_lock:
        _forced_release_count = 0
        _forced_releases.clear()
        _inflight_findings.clear()
        _reported_legacy_leases.clear()
