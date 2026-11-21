"""Regression coverage for the per-file pytest process watchdog."""

from __future__ import annotations

import importlib.util
import json
import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
_RUNNER_PATH = _REPO_ROOT / "scripts" / "run_tests_parallel.py"


def _load_runner():
    spec = importlib.util.spec_from_file_location("run_tests_parallel", _RUNNER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _linux_start_ticks(pid: int) -> str | None:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except OSError:
        return None
    comm_end = stat.rfind(")")
    if comm_end < 0:
        return None
    fields = stat[comm_end + 2 :].split()
    return fields[19] if len(fields) > 19 else None


def _pid_is_running(pid: int, start_ticks: str | None = None) -> bool:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except OSError:
        return False
    comm_end = stat.rfind(")")
    if comm_end < 0:
        return False
    fields = stat[comm_end + 2 :].split()
    if len(fields) <= 19 or (start_ticks is not None and fields[19] != start_ticks):
        return False
    return fields[0] != "Z"


def _collect_probe_identities(
    marker: Path,
    identities: dict[int, str],
) -> dict[str, int | str]:
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    for key in ("pytest_pid", "owned_pid"):
        if key not in payload:
            continue
        pid = int(payload[key])
        start_ticks = payload.get(key.removesuffix("_pid") + "_start_ticks")
        if start_ticks is None:
            start_ticks = _linux_start_ticks(pid)
        if start_ticks is not None:
            identities[pid] = str(start_ticks)
    return payload


def _kill_if_same_group(pid: int, start_ticks: str | None) -> None:
    if start_ticks is None or _linux_start_ticks(pid) != start_ticks:
        return
    try:
        if os.getpgid(pid) != pid or _linux_start_ticks(pid) != start_ticks:
            return
        os.killpg(pid, signal.SIGKILL)
    except OSError:
        pass


def _wait_for_processes_to_stop(
    pids: list[int],
    identities: dict[int, str],
    timeout: float = 5.0,
) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(not _pid_is_running(pid, identities.get(pid)) for pid in pids):
            return True
        time.sleep(0.05)
    return all(not _pid_is_running(pid, identities.get(pid)) for pid in pids)


@pytest.mark.linux_only
def test_silent_collection_timeout_only_kills_its_owned_process_group(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "silent-child-pids.json"
    probe = tmp_path / "test_silent_collection.py"
    probe.write_text(
        textwrap.dedent(
            """
            import json, os, subprocess, sys, time
            from pathlib import Path

            def process_start_ticks(pid):
                stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
                return stat[stat.rfind(")") + 2 :].split()[19]

            owned = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
            )
            Path(os.environ["HERMES_WATCHDOG_MARKER"]).write_text(
                json.dumps({
                    "pytest_pid": os.getpid(),
                    "pytest_start_ticks": process_start_ticks(os.getpid()),
                    "owned_pid": owned.pid,
                    "owned_start_ticks": process_start_ticks(owned.pid),
                }),
                encoding="utf-8",
            )
            time.sleep(60)

            def test_never_collected():
                assert True
            """
        ),
        encoding="utf-8",
    )
    env = os.environ.copy()
    env["HERMES_WATCHDOG_MARKER"] = str(marker)
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"

    unrelated = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    identities: dict[int, str] = {}
    helper = textwrap.dedent(
        """
        import importlib.util
        import json
        import os
        import subprocess
        from pathlib import Path

        class WaitingCommunicate:
            def __init__(self, *args, **kwargs):
                self.child = real_popen(*args, **kwargs)
                stat = Path(f"/proc/{self.child.pid}/stat").read_text(encoding="ascii")
                ticks = stat[stat.rfind(")") + 2 :].split()[19]
                Path(os.environ["HERMES_WATCHDOG_MARKER"]).write_text(
                    json.dumps({
                        "pytest_pid": self.child.pid,
                        "pytest_start_ticks": ticks,
                    }),
                    encoding="utf-8",
                )

            @property
            def pid(self):
                return self.child.pid

            @property
            def returncode(self):
                return self.child.returncode

            def communicate(self, timeout=None):
                self.child.wait()
                return "", None

            def kill(self):
                self.child.kill()

        real_popen = subprocess.Popen
        spec = importlib.util.spec_from_file_location(
            "run_tests_parallel", os.environ["HERMES_WATCHDOG_RUNNER"]
        )
        runner = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(runner)
        subprocess.Popen = WaitingCommunicate
        result = runner._run_one_file_once(
            Path(os.environ["HERMES_WATCHDOG_PROBE"]),
            [],
            Path(os.environ["HERMES_WATCHDOG_REPO"]),
            5,
        )
        print(json.dumps({
            "rc": result[1],
            "output": result[2],
            "wall": result[4],
        }))
        """
    )
    env["HERMES_WATCHDOG_RUNNER"] = str(_RUNNER_PATH)
    env["HERMES_WATCHDOG_PROBE"] = str(probe)
    env["HERMES_WATCHDOG_REPO"] = str(_REPO_ROOT)
    started = time.monotonic()
    try:
        helper_result = subprocess.run(
            [sys.executable, "-c", helper],
            cwd=_REPO_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
        )
        assert helper_result.returncode == 0, helper_result.stdout
        assert time.monotonic() - started < 20
        helper_outcome = json.loads(helper_result.stdout.strip().splitlines()[-1])
        assert helper_outcome["rc"] == 124, helper_result.stdout
        assert "5s exceeded; process tree SIGKILL'd" in helper_outcome["output"]
        assert helper_outcome["wall"] < 15
        helper_payload = _collect_probe_identities(marker, identities)
        assert "owned_pid" in helper_payload, helper_result.stdout
        helper_pids = [
            int(helper_payload["pytest_pid"]),
            int(helper_payload["owned_pid"]),
        ]
        assert _wait_for_processes_to_stop(helper_pids, identities), (
            f"watchdog left live child-group processes: {helper_pids}"
        )
        assert unrelated.poll() is None, "runner terminated a process outside its group"
    finally:
        payload = _collect_probe_identities(marker, identities)
        for key in ("pytest_pid", "owned_pid"):
            if key in payload:
                pid = int(payload[key])
                _kill_if_same_group(pid, identities.get(pid))
        if unrelated.poll() is None:
            unrelated.kill()
            unrelated.wait(timeout=3)


@pytest.mark.linux_only
def test_stale_identity_is_not_signalled_and_results_keep_summary_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    runner = _load_runner()
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        pgid = os.getpgid(child.pid)
        identity = runner._capture_process_identity(child)
        assert identity is not None
        with monkeypatch.context() as patch:
            patch.setattr(
                runner,
                "_process_start_identity",
                lambda _pid: ("linux-proc-stat", "reused-pid"),
            )
            runner._kill_tree(
                child,
                pgid=pgid,
                expected_identity=identity,
                revalidate_identity=True,
            )
        assert child.poll() is None, "stale PID identity must not signal the child group"
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=3)

    passing = tmp_path / "test_output_pass.py"
    failing = tmp_path / "test_output_fail.py"
    passing.write_text(
        "def test_pass():\n"
        "    print('pass-output-marker')\n"
        "    assert True\n",
        encoding="utf-8",
    )
    failing.write_text(
        "def test_fail():\n"
        "    print('fail-output-marker')\n"
        "    assert False, 'expected failure'\n",
        encoding="utf-8",
    )

    direct_result = runner._run_one_file_once(
        passing,
        ["-s"],
        _REPO_ROOT,
        15,
    )
    assert direct_result[1] == 0
    assert "pass-output-marker" in direct_result[2]
    assert direct_result[3].get("passed") == 1

    env = os.environ.copy()
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    result = subprocess.run(
        [
            sys.executable,
            str(_RUNNER_PATH),
            "--files",
            os.pathsep.join((str(passing), str(failing))),
            "--file-timeout",
            "15",
            "--file-retries",
            "0",
            "-j",
            "2",
            "-s",
        ],
        cwd=_REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=45,
    )
    assert result.returncode == 1, result.stdout
    assert "=== Summary: 2 files, 1 tests passed, 1 failed" in result.stdout
    assert "fail-output-marker" in result.stdout
