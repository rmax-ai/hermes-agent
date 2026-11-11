"""Out-of-process scheduler recovery policy and readiness witnesses.

The cron thread writes ``cron/ticker_progress.json`` independently of the asyncio
loop. A separately scheduled systemd service may observe that record and request
one bounded restart; it never treats ``systemctl is-active`` or an in-process
loop heartbeat as proof that the scheduler recovered.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Mapping, TypeVar

try:
    import fcntl
except ImportError:  # pragma: no cover - systemd recovery itself is Linux-only
    fcntl = None  # type: ignore[assignment]

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from hermes_constants import get_hermes_home
from utils import atomic_json_write

logger = logging.getLogger(__name__)

TICKER_PROGRESS_FILENAME = "ticker_progress.json"
RECOVERY_STATE_FILENAME = "scheduler_recovery.json"
RECOVERY_AUDIT_FILENAME = "scheduler_recovery_audit.jsonl"
RECOVERY_LOCK_FILENAME = "scheduler_recovery.lock"

TICK_SECONDS = 60
DEFAULT_STALE_AFTER_SECONDS = 3 * TICK_SECONDS
DEFAULT_OBSERVATION_SPACING_SECONDS = 30
RESTART_BUDGET_WINDOW_SECONDS = 24 * 60 * 60
MAX_AUTOMATIC_RESTARTS_24H = 5
OPERATOR_RESERVED_RESTARTS = 1
HEALTHY_EPOCH_RESET_SECONDS = 15 * 60
SYSTEMD_RESTART_TIMEOUT_SECONDS = 60
_CGROUP_ROOT = Path("/sys/fs/cgroup")
_MEMORY_EVENT_NAMES = frozenset({
    "low",
    "high",
    "max",
    "oom",
    "oom_kill",
    "oom_group_kill",
})
_SYSTEMD_UNIT_NAME_RE = re.compile(r"^[A-Za-z0-9_@][A-Za-z0-9_.@-]*\.service$")


class _StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class SchedulerProgress(_StrictModel):
    """Validated progress record written by the scheduler thread."""

    version: Literal[1] = 1
    pid: int = Field(gt=0)
    start_time: int | None = Field(default=None, gt=0)
    host_boot_id: str | None = None
    heartbeat_sequence: int = Field(ge=0)
    tick_sequence: int = Field(ge=0)
    started_at: float = Field(ge=0)
    heartbeat_at: float = Field(ge=0)
    tick_at: float | None = Field(default=None, ge=0)

    @field_validator("started_at", "heartbeat_at", "tick_at")
    @classmethod
    def _finite_time(cls, value: float | None) -> float | None:
        if value is not None and not math.isfinite(value):
            raise ValueError("time must be finite")
        return value


class GatewayRuntimeStatus(BaseModel):
    """The fields the recovery observer consumes from gateway_state.json."""

    model_config = ConfigDict(strict=True, extra="ignore")

    gateway_state: str | None = None
    pid: int | None = Field(default=None, gt=0)
    start_time: int | None = Field(default=None, gt=0)
    restart_requested: bool = False
    desired_state: str | None = None
    gateway_banner: str | None = None
    scheduler_provider: str | None = None


class ProcessIdentity(_StrictModel):
    pid: int = Field(gt=0)
    start_time: int = Field(gt=0)
    host_boot_id: str = Field(min_length=1)


class MemoryDiagnostics(_StrictModel):
    """Read-only cgroup v2 memory sample; all byte fields are kernel values."""

    available: bool = False
    current_bytes: int | None = Field(default=None, ge=0)
    high_bytes: int | None = Field(default=None, ge=0)
    events: dict[str, int] = Field(default_factory=dict)
    event_deltas: dict[str, int] = Field(default_factory=dict)
    pressure: bool = False
    sampled_at: float = Field(default=0.0, ge=0)

    @field_validator("sampled_at")
    @classmethod
    def _finite_sample_time(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("time must be finite")
        return value

    @field_validator("events", "event_deltas")
    @classmethod
    def _known_event_counters(cls, value: dict[str, int]) -> dict[str, int]:
        if any(
            key not in _MEMORY_EVENT_NAMES or count < 0 for key, count in value.items()
        ):
            raise ValueError("memory event counters are invalid")
        return value


class SystemdTarget(_StrictModel):
    unit: str = Field(pattern=_SYSTEMD_UNIT_NAME_RE.pattern)
    scope: Literal["user", "system"] = "user"


class SystemdUnitStatus(_StrictModel):
    active_state: str
    sub_state: str
    main_pid: int = Field(ge=0)
    control_group: str | None = None


class StaleEvidence(_StrictModel):
    identity: ProcessIdentity
    progress_missing: bool = False
    heartbeat_sequence: int = Field(ge=0)
    tick_sequence: int = Field(ge=0)
    tick_at: float | None = Field(default=None, ge=0)
    first_observed_at: float = Field(ge=0)
    last_observed_at: float = Field(ge=0)
    observations: int = Field(ge=0)

    @field_validator("tick_at", "first_observed_at", "last_observed_at")
    @classmethod
    def _finite_observation_time(cls, value: float | None) -> float | None:
        if value is not None and not math.isfinite(value):
            raise ValueError("time must be finite")
        return value


class PendingRestart(_StrictModel):
    request_id: str = Field(min_length=1)
    requested_at: float = Field(ge=0)
    identity: ProcessIdentity
    heartbeat_sequence: int = Field(ge=0)
    tick_sequence: int = Field(ge=0)

    @field_validator("requested_at")
    @classmethod
    def _finite_request_time(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("time must be finite")
        return value


class RecoveryState(_StrictModel):
    version: Literal[1] = 1
    epoch: int = Field(default=0, ge=0)
    automatic_actions: list[float] = Field(default_factory=list)
    stale: StaleEvidence | None = None
    pending: PendingRestart | None = None
    recovered: bool = False
    cap_alert: bool = False
    healthy_since: float | None = Field(default=None, ge=0)
    healthy_identity: ProcessIdentity | None = None
    healthy_tick_sequence: int | None = Field(default=None, ge=0)
    pressure_inhibited: bool = False
    memory: MemoryDiagnostics = Field(default_factory=MemoryDiagnostics)
    memory_events: dict[str, int] = Field(default_factory=dict)
    memory_host_boot_id: str | None = None

    @field_validator("automatic_actions")
    @classmethod
    def _finite_action_times(cls, value: list[float]) -> list[float]:
        if any(not math.isfinite(timestamp) or timestamp < 0 for timestamp in value):
            raise ValueError("action timestamps must be finite and non-negative")
        return value

    @field_validator("healthy_since")
    @classmethod
    def _finite_healthy_time(cls, value: float | None) -> float | None:
        if value is not None and not math.isfinite(value):
            raise ValueError("time must be finite")
        return value

    @field_validator("memory_events")
    @classmethod
    def _known_memory_baseline(cls, value: dict[str, int]) -> dict[str, int]:
        if any(
            key not in _MEMORY_EVENT_NAMES or count < 0 for key, count in value.items()
        ):
            raise ValueError("memory event counters are invalid")
        return value


class RecoveryObservation(_StrictModel):
    """One external sample after systemd, runtime status, and progress agree."""

    unit_active: bool
    serving: bool
    banner: bool
    intentional_shutdown: bool
    identity: ProcessIdentity | None = None
    progress: SchedulerProgress | None = None
    progress_missing: bool = False
    loop_heartbeat_fresh: bool = False
    memory: MemoryDiagnostics = Field(default_factory=MemoryDiagnostics)


class RecoveryAuditEvent(_StrictModel):
    version: Literal[1] = 1
    at: float = Field(ge=0)
    event: Literal[
        "stale_observed",
        "restart_requested",
        "restart_cap_reached",
        "restart_command_completed",
        "restart_command_failed",
        "recovery_verified",
        "recovery_epoch_reset",
        "memory_pressure_started",
        "memory_pressure_cleared",
    ]
    epoch: int = Field(ge=0)
    reason: str | None = None
    pid: int | None = Field(default=None, gt=0)
    start_time: int | None = Field(default=None, gt=0)
    host_boot_id: str | None = None
    heartbeat_sequence: int | None = Field(default=None, ge=0)
    tick_sequence: int | None = Field(default=None, ge=0)
    automatic_actions_24h: int = Field(ge=0)
    automatic_limit: Literal[5] = MAX_AUTOMATIC_RESTARTS_24H
    operator_reserved_slots: Literal[1] = OPERATOR_RESERVED_RESTARTS
    memory: MemoryDiagnostics | None = None

    @field_validator("at")
    @classmethod
    def _finite_audit_time(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("time must be finite")
        return value


@dataclass(frozen=True)
class RecoveryDecision:
    state: RecoveryState
    audit_events: tuple[RecoveryAuditEvent, ...] = ()
    request_restart: bool = False


_JsonModel = TypeVar("_JsonModel", bound=BaseModel)
_readiness_memory_lock = threading.Lock()
_readiness_memory_baselines: dict[
    tuple[int, int | None, str | None], dict[str, int]
] = {}


def scheduler_progress_path(home: Path) -> Path:
    return Path(home) / "cron" / TICKER_PROGRESS_FILENAME


def recovery_state_path(home: Path) -> Path:
    return Path(home) / "state" / RECOVERY_STATE_FILENAME


def recovery_audit_path(home: Path) -> Path:
    return Path(home) / "state" / RECOVERY_AUDIT_FILENAME


def recovery_lock_path(home: Path) -> Path:
    return Path(home) / "state" / RECOVERY_LOCK_FILENAME


def current_host_boot_id() -> str | None:
    """Linux boot identity, absent on platforms that do not expose the procfs value."""
    try:
        value = (
            Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
        )
    except OSError:
        return None
    return value or None


def process_start_time(pid: int) -> int | None:
    """Reuse the gateway's PID-reuse fingerprint instead of inventing another format."""
    try:
        from gateway.status import get_process_start_time

        return get_process_start_time(pid)
    except Exception:
        return None


def _read_json_model(
    path: Path, model: type[_JsonModel]
) -> tuple[_JsonModel | None, str | None]:
    try:
        payload = path.read_bytes()
    except FileNotFoundError:
        return None, "input.missing"
    except OSError:
        return None, "input.unavailable"
    try:
        return model.model_validate_json(payload), None
    except (ValidationError, ValueError, TypeError):
        return None, "input.invalid"


def read_scheduler_progress(
    path: Path | None = None,
    *,
    home: Path | None = None,
) -> tuple[SchedulerProgress | None, str | None]:
    target = (
        Path(path)
        if path is not None
        else scheduler_progress_path(
            Path(home) if home is not None else get_hermes_home()
        )
    )
    return _read_json_model(target, SchedulerProgress)


def read_gateway_runtime_status(
    home: Path,
) -> tuple[GatewayRuntimeStatus | None, str | None]:
    return _read_json_model(Path(home) / "gateway_state.json", GatewayRuntimeStatus)


def load_recovery_state(home: Path) -> tuple[RecoveryState | None, str | None]:
    target = recovery_state_path(home)
    state, error = _read_json_model(target, RecoveryState)
    if error == "input.missing":
        return RecoveryState(), None
    if error:
        return (
            None,
            "state.invalid" if error == "input.invalid" else "state.unavailable",
        )
    return state, None


def record_scheduler_heartbeat(path: Path, *, success: bool) -> None:
    """Advance the per-process sequence from the cron thread, not the gateway loop."""
    target = Path(path)
    try:
        # Cron directory creation is owned by ensure_dirs(); a stale multiplex
        # ticker must not recreate a named profile that was deleted meanwhile.
        if not target.parent.is_dir():
            return
        pid = os.getpid()
        start_time = process_start_time(pid)
        boot_id = current_host_boot_id()
        now = time.time()
        previous, _ = read_scheduler_progress(target)
        same_process = bool(
            previous
            and previous.pid == pid
            and previous.start_time == start_time
            and previous.host_boot_id == boot_id
        )
        heartbeat_sequence = previous.heartbeat_sequence + 1 if same_process else 1
        tick_sequence = previous.tick_sequence if same_process else 0
        started_at = previous.started_at if same_process else now
        tick_at = previous.tick_at if same_process else None
        if success:
            tick_sequence += 1
            tick_at = now
        record = SchedulerProgress(
            pid=pid,
            start_time=start_time,
            host_boot_id=boot_id,
            heartbeat_sequence=heartbeat_sequence,
            tick_sequence=tick_sequence,
            started_at=started_at,
            heartbeat_at=now,
            tick_at=tick_at,
        )
        atomic_json_write(
            target, record.model_dump(mode="json"), mode=0o600, fsync_dir=True
        )
    except Exception:
        logger.debug("Failed to write cron scheduler progress", exc_info=True)


def _read_proc_cgroup_path(pid: int) -> Path | None:
    if not sys.platform.startswith("linux"):
        return None
    try:
        rows = Path(f"/proc/{pid}/cgroup").read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for row in rows:
        hierarchy, separator, remainder = row.partition(":")
        controllers, separator2, cgroup = remainder.partition(":")
        if not separator or not separator2:
            continue
        if hierarchy == "0" and not controllers and cgroup.startswith("/"):
            return Path(cgroup)
    return None


def _resolve_cgroup_path(value: Path | str | None, *, root: Path) -> Path | None:
    if value is None:
        return None
    raw = Path(value)
    if ".." in raw.parts:
        return None
    resolved_root = root.resolve()
    try:
        already_rooted = raw.is_absolute() and raw.resolve(strict=False).is_relative_to(
            resolved_root
        )
        candidate = raw if already_rooted else root / str(raw).lstrip("/")
        resolved = candidate.resolve(strict=False)
        if not resolved.is_relative_to(resolved_root):
            return None
        return resolved
    except (OSError, RuntimeError):
        return None


def sample_cgroup_memory(
    cgroup_path: Path | str | None,
    *,
    root: Path = _CGROUP_ROOT,
    previous_events: Mapping[str, int] | None = None,
    now: float | None = None,
) -> MemoryDiagnostics:
    """Read cgroup v2 memory diagnostics without changing memory limits or counters."""
    sampled_at = time.time() if now is None else float(now)
    cgroup = _resolve_cgroup_path(cgroup_path, root=Path(root))
    if cgroup is None or not cgroup.is_dir():
        return MemoryDiagnostics(sampled_at=sampled_at)
    try:
        current = int((cgroup / "memory.current").read_text(encoding="ascii").strip())
        high_text = (cgroup / "memory.high").read_text(encoding="ascii").strip()
        high = None if high_text == "max" else int(high_text)
        if current < 0 or (high is not None and high < 0):
            raise ValueError("negative cgroup memory value")
    except (OSError, ValueError):
        return MemoryDiagnostics(sampled_at=sampled_at)
    events: dict[str, int] = {}
    try:
        for line in (cgroup / "memory.events").read_text(encoding="ascii").splitlines():
            fields = line.split()
            if len(fields) != 2:
                continue
            key, raw_count = fields
            if key not in _MEMORY_EVENT_NAMES:
                continue
            count = int(raw_count)
            if count >= 0:
                events[key] = count
    except (OSError, ValueError):
        events = {}
    deltas = {
        key: max(0, value - int(previous_events.get(key, 0)))
        for key, value in events.items()
        if previous_events is not None
    }
    pressure = bool(
        (high is not None and current >= high)
        or any(
            deltas.get(key, 0) > 0
            for key in ("high", "max", "oom", "oom_kill", "oom_group_kill")
        )
    )
    return MemoryDiagnostics(
        available=True,
        current_bytes=current,
        high_bytes=high,
        events=events,
        event_deltas=deltas,
        pressure=pressure,
        sampled_at=sampled_at,
    )


def memory_readiness_check() -> dict[str, Any]:
    pid = os.getpid()
    start_time = process_start_time(pid)
    boot_id = current_host_boot_id()
    key = (pid, start_time, boot_id)
    cgroup_path = _read_proc_cgroup_path(pid)
    with _readiness_memory_lock:
        previous = _readiness_memory_baselines.get(key)
        sample = sample_cgroup_memory(cgroup_path, previous_events=previous)
        if sample.events:
            _readiness_memory_baselines[key] = sample.events
    return {
        "status": "degraded" if sample.pressure else "ok",
        "memory_current_bytes": sample.current_bytes,
        "memory_high_bytes": sample.high_bytes,
        "memory_events": sample.events,
        "memory_event_deltas": sample.event_deltas,
        "pressure": sample.pressure,
        **(
            {"detail": "cgroup v2 memory metrics unavailable"}
            if not sample.available
            else {}
        ),
    }


def scheduler_readiness(
    home: Path,
    runtime_status: Mapping[str, Any] | None,
    *,
    now: float | None = None,
) -> dict[str, Any]:
    """Boot-readiness contract: banner, current PID identity, and two fresh scheduler ticks."""
    try:
        runtime = GatewayRuntimeStatus.model_validate(runtime_status or {})
    except ValidationError:
        return {"status": "degraded", "detail": "gateway runtime status is invalid"}
    provider = (runtime.scheduler_provider or "builtin").strip().lower()
    if runtime.gateway_state == "draining":
        return {"status": "ok", "detail": "gateway is intentionally draining"}
    if runtime.gateway_state not in {"running", "degraded"}:
        return {"status": "degraded", "detail": "gateway is not serving"}
    if not (runtime.gateway_banner and runtime.gateway_banner.strip()):
        return {
            "status": "degraded",
            "detail": "gateway startup banner is not recorded",
        }
    if provider not in {"builtin", "in-process", "inprocess"}:
        return {
            "status": "ok",
            "detail": "external scheduler provider",
            "provider": provider,
        }
    if runtime.pid is None or runtime.start_time is None:
        return {
            "status": "degraded",
            "detail": "gateway process identity is unavailable",
        }
    progress, error = read_scheduler_progress(home=Path(home))
    if progress is None:
        return {
            "status": "degraded",
            "detail": "scheduler tick progress is unavailable",
            "error_code": error or "input.invalid",
        }
    if (progress.pid, progress.start_time) != (runtime.pid, runtime.start_time):
        return {
            "status": "degraded",
            "detail": "scheduler progress belongs to another gateway process",
        }
    host_boot_id = current_host_boot_id()
    if host_boot_id and progress.host_boot_id != host_boot_id:
        return {
            "status": "degraded",
            "detail": "scheduler progress belongs to another host boot",
        }
    moment = time.time() if now is None else float(now)
    if progress.tick_sequence < 2 or progress.tick_at is None:
        return {
            "status": "degraded",
            "detail": "waiting for two successful scheduler ticks",
            "tick_sequence": progress.tick_sequence,
        }
    heartbeat_age = moment - progress.heartbeat_at
    tick_age = moment - progress.tick_at
    if (
        heartbeat_age < -5
        or tick_age < -5
        or max(heartbeat_age, tick_age) > DEFAULT_STALE_AFTER_SECONDS
    ):
        return {
            "status": "degraded",
            "detail": "scheduler tick progress is stale",
            "tick_sequence": progress.tick_sequence,
        }
    return {
        "status": "ok",
        "pid": progress.pid,
        "start_time": progress.start_time,
        "tick_sequence": progress.tick_sequence,
        "heartbeat_sequence": progress.heartbeat_sequence,
        "tick_age_seconds": round(max(0.0, tick_age), 1),
    }


def _progress_matches(observation: RecoveryObservation) -> bool:
    identity, progress = observation.identity, observation.progress
    return bool(
        identity
        and progress
        and progress.pid == identity.pid
        and progress.start_time == identity.start_time
        and progress.host_boot_id == identity.host_boot_id
    )


def _healthy(observation: RecoveryObservation, now: float, stale_after: float) -> bool:
    progress = observation.progress
    if (
        not observation.unit_active
        or not observation.serving
        or not observation.banner
        or observation.intentional_shutdown
        or observation.memory.pressure
        or not _progress_matches(observation)
        or progress is None
        or progress.tick_sequence < 2
        or progress.tick_at is None
    ):
        return False
    ages = (now - progress.heartbeat_at, now - progress.tick_at)
    return all(-5 <= age <= stale_after for age in ages)


def _stale(observation: RecoveryObservation, now: float, stale_after: float) -> bool:
    progress = observation.progress
    if progress is None:
        return bool(
            observation.unit_active
            and observation.serving
            and not observation.intentional_shutdown
            and not observation.memory.pressure
            and observation.identity is not None
            and observation.progress_missing
        )
    last_tick_progress = (
        progress.tick_at if progress.tick_at is not None else progress.started_at
    )
    return bool(
        observation.unit_active
        and observation.serving
        and not observation.intentional_shutdown
        and not observation.memory.pressure
        and _progress_matches(observation)
        and last_tick_progress is not None
        and now - last_tick_progress >= stale_after
    )


def _audit(
    event: str,
    *,
    state: RecoveryState,
    observation: RecoveryObservation,
    now: float,
    reason: str | None = None,
    memory: bool = False,
) -> RecoveryAuditEvent:
    identity, progress = observation.identity, observation.progress
    return RecoveryAuditEvent(
        at=now,
        event=event,  # type: ignore[arg-type]
        epoch=state.epoch,
        reason=reason,
        pid=identity.pid if identity else None,
        start_time=identity.start_time if identity else None,
        host_boot_id=identity.host_boot_id if identity else None,
        heartbeat_sequence=progress.heartbeat_sequence if progress else None,
        tick_sequence=progress.tick_sequence if progress else None,
        automatic_actions_24h=len(state.automatic_actions),
        memory=observation.memory if memory else None,
    )


def advance_recovery(
    previous: RecoveryState,
    observation: RecoveryObservation,
    *,
    now: float,
    stale_after: float = DEFAULT_STALE_AFTER_SECONDS,
    observation_spacing: float = DEFAULT_OBSERVATION_SPACING_SECONDS,
) -> RecoveryDecision:
    """Apply one validated sample to the bounded restart state machine."""
    if not math.isfinite(now):
        raise ValueError("now must be finite")
    state = previous.model_copy(deep=True)
    state.automatic_actions = sorted(
        ts for ts in state.automatic_actions if now - ts < RESTART_BUDGET_WINDOW_SECONDS
    )
    events: list[RecoveryAuditEvent] = []

    if observation.memory.pressure != state.pressure_inhibited:
        events.append(
            _audit(
                "memory_pressure_started"
                if observation.memory.pressure
                else "memory_pressure_cleared",
                state=state,
                observation=observation,
                now=now,
                memory=True,
            )
        )
    state.pressure_inhibited = observation.memory.pressure
    state.memory = observation.memory
    state.memory_events = observation.memory.events
    if observation.identity is not None:
        state.memory_host_boot_id = observation.identity.host_boot_id

    if state.pending is not None:
        if (
            observation.identity is not None
            and observation.identity != state.pending.identity
            and _healthy(observation, now, stale_after)
        ):
            state.pending = None
            state.stale = None
            state.recovered = True
            state.healthy_since = now
            state.healthy_identity = observation.identity
            state.healthy_tick_sequence = (
                observation.progress.tick_sequence if observation.progress else None
            )
            events.append(
                _audit(
                    "recovery_verified", state=state, observation=observation, now=now
                )
            )
        else:
            state.healthy_since = None
            state.healthy_identity = None
            state.healthy_tick_sequence = None
        return RecoveryDecision(state=state, audit_events=tuple(events))

    if _healthy(observation, now, stale_after):
        assert observation.identity is not None and observation.progress is not None
        if (
            state.healthy_identity != observation.identity
            or state.healthy_since is None
        ):
            state.healthy_identity = observation.identity
            state.healthy_since = now
            state.healthy_tick_sequence = observation.progress.tick_sequence
        elif state.healthy_tick_sequence is not None:
            if observation.progress.tick_sequence < state.healthy_tick_sequence:
                state.healthy_since = now
            state.healthy_tick_sequence = observation.progress.tick_sequence
        state.stale = None
        if now - state.healthy_since >= HEALTHY_EPOCH_RESET_SECONDS and (
            state.recovered or state.cap_alert
        ):
            state.epoch += 1
            state.recovered = False
            state.cap_alert = False
            state.healthy_since = now
            state.healthy_tick_sequence = observation.progress.tick_sequence
            events.append(
                _audit(
                    "recovery_epoch_reset",
                    state=state,
                    observation=observation,
                    now=now,
                )
            )
        return RecoveryDecision(state=state, audit_events=tuple(events))

    state.healthy_since = None
    state.healthy_identity = None
    state.healthy_tick_sequence = None
    if not _stale(observation, now, stale_after):
        state.stale = None
        return RecoveryDecision(state=state, audit_events=tuple(events))

    assert observation.identity is not None
    progress = observation.progress
    heartbeat_sequence = progress.heartbeat_sequence if progress else 0
    tick_sequence = progress.tick_sequence if progress else 0
    tick_at = progress.tick_at if progress else None
    same_evidence = bool(
        state.stale
        and state.stale.identity == observation.identity
        and state.stale.progress_missing == observation.progress_missing
        and state.stale.tick_sequence == tick_sequence
        and state.stale.tick_at == tick_at
    )
    if same_evidence:
        assert state.stale is not None
        if (
            observation.progress_missing
            and state.stale.observations == 0
            and now - state.stale.first_observed_at < stale_after
        ):
            return RecoveryDecision(state=state, audit_events=tuple(events))
        if now - state.stale.last_observed_at < observation_spacing:
            if state.stale.observations > 0:
                return RecoveryDecision(state=state, audit_events=tuple(events))
        state.stale = state.stale.model_copy(
            update={
                "last_observed_at": now,
                "observations": state.stale.observations + 1,
            }
        )
    else:
        state.stale = StaleEvidence(
            identity=observation.identity,
            progress_missing=observation.progress_missing,
            heartbeat_sequence=heartbeat_sequence,
            tick_sequence=tick_sequence,
            tick_at=tick_at,
            first_observed_at=now,
            last_observed_at=now,
            observations=0 if observation.progress_missing else 1,
        )
        if observation.progress_missing:
            return RecoveryDecision(state=state, audit_events=tuple(events))
    events.append(
        _audit("stale_observed", state=state, observation=observation, now=now)
    )
    if state.stale.observations < 2:
        return RecoveryDecision(state=state, audit_events=tuple(events))

    if len(state.automatic_actions) >= MAX_AUTOMATIC_RESTARTS_24H:
        if not state.cap_alert:
            state.cap_alert = True
            events.append(
                _audit(
                    "restart_cap_reached",
                    state=state,
                    observation=observation,
                    now=now,
                    reason="automatic restart budget exhausted",
                )
            )
        return RecoveryDecision(state=state, audit_events=tuple(events))

    state.automatic_actions.append(now)
    state.pending = PendingRestart(
        request_id=uuid.uuid4().hex,
        requested_at=now,
        identity=observation.identity,
        heartbeat_sequence=heartbeat_sequence,
        tick_sequence=tick_sequence,
    )
    state.recovered = False
    state.stale = None
    state.healthy_since = None
    state.healthy_identity = None
    state.healthy_tick_sequence = None
    events.append(
        _audit(
            "restart_requested",
            state=state,
            observation=observation,
            now=now,
            reason="two stale scheduler observations with unchanged process and tick evidence",
        )
    )
    return RecoveryDecision(
        state=state, audit_events=tuple(events), request_restart=True
    )


def parse_systemd_unit_status(text: str) -> SystemdUnitStatus:
    values: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        if separator and key in {"ActiveState", "SubState", "MainPID", "ControlGroup"}:
            values[key] = value
    try:
        main_pid = int(values.get("MainPID", ""))
    except ValueError as exc:
        raise ValueError("systemd.status.invalid") from exc
    try:
        return SystemdUnitStatus.model_validate({
            "active_state": values["ActiveState"],
            "sub_state": values["SubState"],
            "main_pid": main_pid,
            "control_group": values.get("ControlGroup") or None,
        })
    except (KeyError, ValidationError) as exc:
        raise ValueError("systemd.status.invalid") from exc


def _systemctl_command(scope: str) -> list[str]:
    systemctl = shutil.which("systemctl")
    if not systemctl:
        raise OSError("systemd.unavailable")
    return [systemctl, *(["--user"] if scope == "user" else [])]


def _show_unit(target: SystemdTarget) -> SystemdUnitStatus:
    command = [
        *_systemctl_command(target.scope),
        "show",
        "--no-pager",
        "--property=MainPID,ActiveState,SubState,ControlGroup",
        target.unit,
    ]
    result = subprocess.run(
        command, capture_output=True, text=True, timeout=5, check=False
    )
    if result.returncode != 0:
        raise OSError("systemd.status.unavailable")
    return parse_systemd_unit_status(result.stdout)


def _restart_unit(target: SystemdTarget) -> int:
    command = [*_systemctl_command(target.scope), "restart", target.unit]
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=SYSTEMD_RESTART_TIMEOUT_SECONDS,
        check=False,
    )
    return result.returncode


def _loop_heartbeat_is_fresh(home: Path, identity: ProcessIdentity | None) -> bool:
    if identity is None:
        return False
    heartbeat_path = Path(home) / "state" / "gateway.heartbeat"
    payload, _ = _read_json_model(heartbeat_path, GatewayRuntimeHeartbeat)
    if payload is None or payload.pid != identity.pid:
        return False
    # The loop heartbeat stores the gateway's wall-clock initialization time. The
    # process fingerprint is verified separately from systemd MainPID and runtime
    # status in _load_observation, so these values must not be compared.
    host_boot = current_host_boot_id()
    age = time.monotonic() - payload.monotonic
    return (
        not host_boot or host_boot == identity.host_boot_id
    ) and 0 <= age <= DEFAULT_STALE_AFTER_SECONDS


class GatewayRuntimeHeartbeat(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")

    pid: int = Field(gt=0)
    monotonic: float = Field(ge=0)
    start_time: float | None = None

    @field_validator("monotonic", "start_time")
    @classmethod
    def _finite_heartbeat_value(cls, value: float | None) -> float | None:
        if value is not None and not math.isfinite(value):
            raise ValueError("heartbeat value must be finite")
        return value


def _planned_stop_active(home: Path, identity: ProcessIdentity | None) -> bool:
    if identity is None:
        return True
    try:
        from gateway.status import planned_stop_marker_targets_process

        return planned_stop_marker_targets_process(
            identity.pid,
            identity.start_time,
            hermes_home=home,
        )
    except Exception:
        return True


def _load_observation(
    home: Path,
    target: SystemdTarget,
    state: RecoveryState,
    *,
    now: float,
) -> tuple[RecoveryObservation | None, str | None, RecoveryState]:
    try:
        unit_status = _show_unit(target)
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return None, "systemd.status.unavailable", state
    runtime, runtime_error = read_gateway_runtime_status(home)
    if runtime is None:
        return None, runtime_error or "runtime.invalid", state
    host_boot = current_host_boot_id()
    actual_start = (
        process_start_time(unit_status.main_pid) if unit_status.main_pid else None
    )
    identity: ProcessIdentity | None = None
    if (
        host_boot
        and unit_status.main_pid > 0
        and runtime.pid == unit_status.main_pid
        and runtime.start_time is not None
        and actual_start == runtime.start_time
    ):
        identity = ProcessIdentity(
            pid=unit_status.main_pid,
            start_time=runtime.start_time,
            host_boot_id=host_boot,
        )
    progress, progress_error = read_scheduler_progress(home=home)
    try:
        from gateway.drain_control import drain_requested

        drain_active = drain_requested(home=home)
    except Exception:
        drain_active = True
    intentional = bool(
        runtime.restart_requested
        or runtime.desired_state == "stopped"
        or runtime.gateway_state
        in {"draining", "stopped", "stopping", "startup_failed"}
        or drain_active
        or _planned_stop_active(home, identity)
    )
    same_memory_boot = state.memory_host_boot_id == host_boot
    memory = sample_cgroup_memory(
        unit_status.control_group,
        previous_events=state.memory_events if same_memory_boot else None,
        now=now,
    )
    state.memory = memory
    state.memory_events = memory.events
    state.memory_host_boot_id = host_boot
    provider = (runtime.scheduler_provider or "builtin").strip().lower()
    observation = RecoveryObservation(
        unit_active=(
            unit_status.active_state == "active" and unit_status.sub_state == "running"
        )
        or (unit_status.active_state == "activating" and unit_status.main_pid > 0),
        serving=runtime.gateway_state in {"running", "degraded"}
        and provider
        in {
            "builtin",
            "in-process",
            "inprocess",
        },
        banner=bool(runtime.gateway_banner and runtime.gateway_banner.strip()),
        intentional_shutdown=intentional,
        identity=identity,
        progress=progress,
        progress_missing=progress_error == "input.missing",
        loop_heartbeat_fresh=_loop_heartbeat_is_fresh(home, identity),
        memory=memory,
    )
    return observation, None, state


def _state_lock(path: Path):
    if fcntl is None:
        raise OSError("recovery.lock.unavailable")
    if not path.parent.is_dir():
        raise OSError("recovery.state_directory.unavailable")
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(descriptor)
        raise
    return descriptor


def _save_state(home: Path, state: RecoveryState) -> None:
    target = recovery_state_path(home)
    if not target.parent.is_dir():
        raise OSError("recovery.state_directory.unavailable")
    atomic_json_write(
        target, state.model_dump(mode="json"), indent=2, mode=0o600, fsync_dir=True
    )


def _append_audit(
    home: Path, events: tuple[RecoveryAuditEvent, ...] | list[RecoveryAuditEvent]
) -> None:
    if not events:
        return
    target = recovery_audit_path(home)
    if not target.parent.is_dir():
        raise OSError("recovery.audit_directory.unavailable")
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        with os.fdopen(descriptor, "a", encoding="utf-8", closefd=False) as stream:
            for event in events:
                stream.write(event.model_dump_json() + "\n")
            stream.flush()
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


def run_once(
    *,
    home: Path,
    unit: str,
    scope: str = "user",
    approval_file: Path | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """Observe once and request at most one approved, bounded systemd restart."""
    if approval_file is None or not Path(approval_file).is_file():
        return {
            "status": "disabled",
            "detail": "activation approval marker is absent",
            "automatic_limit_24h": MAX_AUTOMATIC_RESTARTS_24H,
            "operator_reserved_slots": OPERATOR_RESERVED_RESTARTS,
        }
    try:
        target = SystemdTarget.model_validate({"unit": unit, "scope": scope})
    except ValidationError:
        return {"status": "degraded", "error_code": "systemd.target.invalid"}
    root = Path(home)
    if not root.is_dir():
        return {"status": "degraded", "error_code": "home.unavailable"}
    moment = time.time() if now is None else float(now)
    lock_fd = None
    try:
        lock_fd = _state_lock(recovery_lock_path(root))
        state, state_error = load_recovery_state(root)
        if state is None:
            return {"status": "degraded", "error_code": state_error or "state.invalid"}
        observation, observation_error, state = _load_observation(
            root, target, state, now=moment
        )
        if observation is None:
            return {
                "status": "degraded",
                "error_code": observation_error or "observation.invalid",
            }
        decision = advance_recovery(state, observation, now=moment)
        try:
            _save_state(root, decision.state)
            _append_audit(root, decision.audit_events)
        except OSError:
            # No restart without a durable budget/pending marker and its audit record.
            return {"status": "degraded", "error_code": "recovery.persistence.failed"}
        restart_result: int | None = None
        restart_error: str | None = None
        if decision.request_restart:
            try:
                restart_result = _restart_unit(target)
                if restart_result != 0:
                    restart_error = "systemd.restart.failed"
            except (OSError, subprocess.TimeoutExpired):
                restart_error = "systemd.restart.failed"
            completion = RecoveryAuditEvent(
                at=moment,
                event="restart_command_failed"
                if restart_error or restart_result
                else "restart_command_completed",
                epoch=decision.state.epoch,
                reason=restart_error,
                pid=observation.identity.pid if observation.identity else None,
                start_time=observation.identity.start_time
                if observation.identity
                else None,
                host_boot_id=observation.identity.host_boot_id
                if observation.identity
                else None,
                heartbeat_sequence=observation.progress.heartbeat_sequence
                if observation.progress
                else None,
                tick_sequence=observation.progress.tick_sequence
                if observation.progress
                else None,
                automatic_actions_24h=len(decision.state.automatic_actions),
            )
            try:
                _append_audit(root, [completion])
            except OSError:
                logger.error(
                    "Scheduler recovery command result could not be appended to its audit"
                )
        healthy = _healthy(observation, moment, DEFAULT_STALE_AFTER_SECONDS)
        return {
            "status": "ok" if healthy else "degraded",
            "restart_requested": decision.request_restart,
            "restart_result": restart_result,
            "restart_error": restart_error,
            "automatic_actions_24h": len(decision.state.automatic_actions),
            "automatic_limit_24h": MAX_AUTOMATIC_RESTARTS_24H,
            "operator_reserved_slots": OPERATOR_RESERVED_RESTARTS,
            "loop_heartbeat_fresh": observation.loop_heartbeat_fresh,
            "memory": decision.state.memory.model_dump(mode="json"),
        }
    except (OSError, ValueError):
        return {"status": "degraded", "error_code": "recovery.observation.failed"}
    finally:
        if lock_fd is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Observe and recover a frozen Hermes scheduler"
    )
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--unit", required=True)
    parser.add_argument("--scope", choices=("user", "system"), default="user")
    parser.add_argument("--approval-file", type=Path)
    args = parser.parse_args(argv)
    result = run_once(
        home=args.home,
        unit=args.unit,
        scope=args.scope,
        approval_file=args.approval_file,
    )
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    return (
        1
        if (
            result.get("error_code")
            or result.get("restart_error")
            or result.get("restart_result") not in (None, 0)
        )
        else 0
    )


if __name__ == "__main__":
    raise SystemExit(main())
