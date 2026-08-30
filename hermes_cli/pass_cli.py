"""CLI handlers for ``hermes pass ...``."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys

from hermes_constants import get_hermes_home


def register_cli(parent_parser: argparse.ArgumentParser) -> None:
    """Attach subcommands to the parent ``pass`` parser."""
    sub = parent_parser.add_subparsers(dest="pass_command")

    ls_parser = sub.add_parser("ls", help="List all hermes/* entries in the store")
    ls_parser.set_defaults(func=cmd_ls)

    show_parser = sub.add_parser("show", help="Decrypt and show a credential")
    show_parser.add_argument(
        "path", help="Pass path (e.g. hermes/deepseek/api-key)"
    )
    show_parser.add_argument(
        "--clip",
        "-c",
        action="store_true",
        help="Copy to clipboard instead of printing",
    )
    show_parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt (non-interactive use)",
    )
    show_parser.set_defaults(func=cmd_show)

    insert_parser = sub.add_parser("insert", help="Insert a new credential")
    insert_parser.add_argument(
        "path", help="Pass path (e.g. hermes/deepseek/api-key)"
    )
    insert_parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Overwrite existing entry",
    )
    insert_parser.set_defaults(func=cmd_insert)

    otp_parser = sub.add_parser("otp", help="Generate a TOTP code")
    otp_parser.add_argument("path", help="Pass path for the OTP secret")
    otp_parser.add_argument(
        "--clip",
        "-c",
        action="store_true",
        help="Copy to clipboard",
    )
    otp_parser.set_defaults(func=cmd_otp)

    init_parser = sub.add_parser("init", help="Verify pass setup and store")
    init_parser.set_defaults(func=cmd_init)

    status_parser = sub.add_parser("status", help="Show store health")
    status_parser.set_defaults(func=cmd_status)


def _pass_env() -> dict[str, str]:
    """Return env with ``PASSWORD_STORE_DIR`` set for the Hermes store."""
    env = os.environ.copy()
    env["PASSWORD_STORE_DIR"] = str(get_hermes_home() / ".password-store")
    return env


def _check_pass_binary() -> bool:
    """Return True if the ``pass`` binary is available."""
    try:
        subprocess.run(
            ["pass", "ls"],
            capture_output=True,
            env=_pass_env(),
            timeout=5,
            check=False,
        )
        return True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _run_pass(
    cmd: list[str],
    *,
    capture_output: bool = True,
    text: bool = True,
    timeout: int = 10,
    input: str | None = None,
) -> subprocess.CompletedProcess:
    """Run a ``pass`` command against the Hermes password store."""
    return subprocess.run(
        cmd,
        capture_output=capture_output,
        text=text,
        env=_pass_env(),
        timeout=timeout,
        check=False,
        input=input,
    )


def _iter_store_entries(store_root: str) -> list[str]:
    """Return relative entry names from the Hermes password store."""
    store = get_hermes_home() / ".password-store"
    root = store / store_root
    if not root.exists():
        return []
    entries: list[str] = []
    for gpg_file in sorted(root.rglob("*.gpg")):
        rel = gpg_file.relative_to(store).with_suffix("")
        entries.append(rel.as_posix())
    return entries


def cmd_ls(args: argparse.Namespace) -> int:
    """List all ``hermes/*`` entries."""
    del args
    if not _check_pass_binary():
        print("Error: pass is not installed or not working.", file=sys.stderr)
        return 1
    try:
        result = _run_pass(["pass", "ls"])
        if result.returncode != 0:
            print(f"Error: {result.stderr.strip()}", file=sys.stderr)
            return 1
        entries = _iter_store_entries("hermes")
        if not entries:
            print("No credentials in the store.")
            return 0
        print("\n".join(entries))
        return 0
    except subprocess.TimeoutExpired:
        print("Error: pass ls timed out.", file=sys.stderr)
        return 1


def cmd_show(args: argparse.Namespace) -> int:
    """Decrypt and display a credential."""
    if not _check_pass_binary():
        print("Error: pass is not installed or not working.", file=sys.stderr)
        return 1
    try:
        if args.clip:
            result = _run_pass(
                ["pass", "show", "--clip", args.path],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode != 0:
                print(f"Error: {result.stderr.strip()}", file=sys.stderr)
                return 1
            print(f"Copied {args.path} to clipboard.")
            return 0

        if not args.yes:
            print(f"WARNING: About to display credential '{args.path}' in plaintext.")
            print("This may be visible to others with access to your terminal.")
            confirm = input("Are you sure? (y/N): ").strip().lower()
            if confirm not in {"y", "yes"}:
                print("Aborted.")
                return 0

        result = _run_pass(["pass", "show", args.path])
        if result.returncode != 0:
            print(f"Error: {result.stderr.strip()}", file=sys.stderr)
            return 1
        first_line = result.stdout.strip().splitlines()[0] if result.stdout.strip() else ""
        print(first_line)
        return 0
    except subprocess.TimeoutExpired:
        print(f"Error: pass show timed out for {args.path}.", file=sys.stderr)
        return 1


def cmd_insert(args: argparse.Namespace) -> int:
    """Insert a credential. Interactive double-prompt in a TTY; stores stdin
    verbatim when piped (via ``--multiline``).

    Why ``--multiline`` when piped: pass's default noecho branch re-reads
    stdin for a "Retype password" prompt; on a pipe the second read hits EOF
    and pass exits 1 with no message (``read ... || exit 1``). Multiline
    mode reads stdin until EOF with no retype prompt, which is the correct
    behavior for non-interactive insert.
    """
    if not _check_pass_binary():
        print("Error: pass is not installed or not working.", file=sys.stderr)
        return 1
    cmd = ["pass", "insert"]
    data: str | None = None
    if not sys.stdin.isatty():
        data = sys.stdin.read()
        if not data.strip():
            print("Error: no input provided on stdin.", file=sys.stderr)
            return 1
        cmd.append("--multiline")
    if args.force:
        cmd.append("--force")
    cmd.append(args.path)
    try:
        if data is not None:
            result = _run_pass(cmd, input=data)
        else:
            result = _run_pass(cmd, capture_output=False, text=False, timeout=30)
        if result.returncode != 0:
            err = result.stderr
            if err:
                print(f"Error: {err.strip()}", file=sys.stderr)
            return 1
        print(f"Stored {args.path}.")
        return 0
    except subprocess.TimeoutExpired:
        print(f"Error: pass insert timed out for {args.path}.", file=sys.stderr)
        return 1


def cmd_otp(args: argparse.Namespace) -> int:
    """Generate a TOTP code."""
    if not _check_pass_binary():
        print("Error: pass is not installed or not working.", file=sys.stderr)
        return 1
    cmd = ["pass", "otp"]
    if args.clip:
        cmd.append("--clip")
    cmd.append(args.path)
    try:
        result = _run_pass(cmd)
        if result.returncode != 0:
            print(f"Error: {result.stderr.strip()}", file=sys.stderr)
            return 1
        if result.stdout.strip():
            print(result.stdout.strip())
        return 0
    except subprocess.TimeoutExpired:
        print(f"Error: pass otp timed out for {args.path}.", file=sys.stderr)
        return 1


def cmd_init(args: argparse.Namespace) -> int:
    """Verify pass setup and store health."""
    del args
    try:
        result = _run_pass(["pass", "ls"], capture_output=True, text=True, timeout=5)
        if result.returncode != 0:
            print(f"pass binary found but returned error: {result.stderr.strip()}")
            return 1
        print("pass binary: available")
    except FileNotFoundError:
        print("pass binary: not found. Install with your system package manager.")
        return 1
    except subprocess.TimeoutExpired:
        print("pass binary: timed out while checking availability.")
        return 1

    store = get_hermes_home() / ".password-store"
    if store.exists():
        print(f"store: {store}")
    else:
        print(f"store: {store} does not exist")
        print("Run: PASSWORD_STORE_DIR=~/.hermes/.password-store pass init <GPG-KEY-ID>")
        return 1

    gpg_id_file = store / ".gpg-id"
    if gpg_id_file.exists():
        key_id = gpg_id_file.read_text(encoding="utf-8").strip()
        print(f"GPG key: {key_id}")
    else:
        print("no .gpg-id file found")
        return 1

    entries = _iter_store_entries("hermes")
    print(f"entries: {len(entries)} hermes/* item(s)")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Show store health."""
    return cmd_init(args)
