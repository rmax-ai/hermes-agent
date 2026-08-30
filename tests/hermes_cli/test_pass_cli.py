"""Tests for ``hermes_cli.pass_cli`` — the ``hermes pass`` subcommand.

Covers the two bugs fixed when wiring the subcommand into ``main()``:

1. Piped ``pass insert`` used pass's default noecho branch, which re-reads
   stdin for a "Retype password" prompt; on a pipe the second read hits EOF
   and pass exits 1 silently (``read ... || exit 1``). ``cmd_insert`` now
   appends ``--multiline`` when stdin is not a TTY, which reads stdin until
   EOF with no retype prompt.
2. ``hermes pass`` printed the top-level usage banner because the subcommand
   was never registered — ``pass`` was missing from ``_BUILTIN_SUBCOMMANDS``
   and no parser was attached in ``main()``.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile

import pytest


class _FakeStdin:
    """Stand-in for sys.stdin: records isatty() and feeds canned data."""

    def __init__(self, data: str = "", tty: bool = False):
        self._data = data
        self._tty = tty

    def isatty(self) -> bool:
        return self._tty

    def read(self) -> str:
        return self._data


@pytest.fixture()
def isolated_pass_home(monkeypatch):
    """Fresh HERMES_HOME so get_hermes_home() never touches the real store."""
    test_home = tempfile.mkdtemp(prefix="pass_cli_")
    monkeypatch.setenv("HERMES_HOME", test_home)
    for mod in list(sys.modules.keys()):
        if mod.startswith("hermes_cli") or mod == "hermes_constants":
            del sys.modules[mod]
    yield test_home


@pytest.fixture()
def recording_pass(monkeypatch):
    """Replace subprocess.run inside pass_cli with a recorder.

    Stands in for the real pass binary: every invocation succeeds with no
    output, and the full argv + kwargs of each call are captured.
    """
    calls: list[dict] = []

    def fake_run(cmd, **kwargs):
        calls.append({"cmd": list(cmd), "kwargs": kwargs})
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr("hermes_cli.pass_cli.subprocess.run", fake_run)
    return calls


def _insert_calls(recording_pass):
    return [c for c in recording_pass if c["cmd"][:2] == ["pass", "insert"]]


def test_piped_insert_uses_multiline_and_passes_stdin(
    isolated_pass_home, recording_pass, monkeypatch
):
    from hermes_cli.pass_cli import cmd_insert

    monkeypatch.setattr(sys, "stdin", _FakeStdin("my-secret\n"))
    args = argparse.Namespace(path="hermes/foo/api-key", force=False)

    assert cmd_insert(args) == 0

    insert = _insert_calls(recording_pass)
    assert len(insert) == 1
    call = insert[0]
    assert "--multiline" in call["cmd"]
    assert call["cmd"][-1] == "hermes/foo/api-key"
    assert call["kwargs"]["input"] == "my-secret\n"


def test_piped_insert_with_empty_stdin_errors_without_calling_pass(
    isolated_pass_home, recording_pass, monkeypatch
):
    from hermes_cli.pass_cli import cmd_insert

    monkeypatch.setattr(sys, "stdin", _FakeStdin("   \n"))
    args = argparse.Namespace(path="hermes/foo/api-key", force=False)

    assert cmd_insert(args) == 1
    assert _insert_calls(recording_pass) == []


def test_tty_insert_skips_multiline_and_forwards_force(
    isolated_pass_home, recording_pass, monkeypatch
):
    from hermes_cli.pass_cli import cmd_insert

    monkeypatch.setattr(sys, "stdin", _FakeStdin(tty=True))
    args = argparse.Namespace(path="hermes/foo/api-key", force=True)

    assert cmd_insert(args) == 0

    insert = _insert_calls(recording_pass)
    assert len(insert) == 1
    call = insert[0]
    assert "--multiline" not in call["cmd"]
    assert "--force" in call["cmd"]
    assert call["cmd"][-1] == "hermes/foo/api-key"
    # Interactive mode inherits the terminal: no capture, no piped input.
    assert call["kwargs"]["capture_output"] is False
    assert call["kwargs"]["input"] is None


def test_pass_is_registered_as_builtin_subcommand():
    from hermes_cli.main import _BUILTIN_SUBCOMMANDS

    assert "pass" in _BUILTIN_SUBCOMMANDS
