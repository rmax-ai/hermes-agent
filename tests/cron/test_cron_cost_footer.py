"""Tests for the cron cost-footer local patch v2 (cron/scheduler.py).

The footer must:
- key on the EXACT session id captured from the invocation, never a
  job-id-prefix/time guess (decoy rows with the same prefix are ignored),
- aggregate the full session lineage via parent_session_id (delegated
  children and compression continuations at any depth),
- degrade to explicit static lines ("no LLM calls", "unavailable") instead
  of omitting the footer or breaking delivery,
- fall back to the in-memory capture when the store is unreadable,
- mark unpriced / subscription / partially-priced runs honestly.
"""
import sqlite3
import time

import cron.scheduler as s
from hermes_constants import get_hermes_home

RUN_ID = "cron_j1_20260912_120000"


def _seed_state_db(rows):
    """Create a minimal sessions table in the per-test HERMES_HOME."""
    home = get_hermes_home()
    home.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(home / "state.db"))
    con.execute(
        "CREATE TABLE IF NOT EXISTS sessions ("
        "id TEXT PRIMARY KEY, model TEXT, input_tokens INTEGER, output_tokens INTEGER, "
        "estimated_cost_usd REAL, cost_status TEXT, cost_source TEXT, "
        "api_call_count INTEGER, started_at REAL, parent_session_id TEXT)"
    )
    for r in rows:
        con.execute("INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?)", r)
    con.commit()
    con.close()


def _std_capture(**over):
    """A capture dict shaped like run_job's success-path fill."""
    c = {
        "session_id": RUN_ID,
        "model": "deepseek-v4-pro",
        "cost_usd": 0.1856,
        "cost_status": "estimated",
        "cost_source": "official_docs_snapshot",
        "input_tokens": 1000,
        "output_tokens": 500,
        "api_calls": 7,
    }
    c.update(over)
    return c


def _patch_pipeline(monkeypatch, *, success=True, output="out", final="final response",
                    error=None, silent_marker_in=None, capture=None):
    """Patch pipeline primitives. The fake run_job honors the cost_capture
    out-param contract exactly like the real one (fills the dict it gets)."""
    calls = []

    def fake_run_job(job, cost_capture=None):
        calls.append(("run_job", job["id"]))
        if cost_capture is not None and capture is not None:
            cost_capture.update(capture)
        fr = final if silent_marker_in is None else silent_marker_in
        return (success, output, fr, error)

    def fake_save(jid, out):
        calls.append(("save", jid, out))
        return f"/tmp/{jid}.txt"

    def fake_deliver(job, content, adapters=None, loop=None):
        calls.append(("deliver", job["id"], content))
        return None

    def fake_mark(jid, ok, err=None, delivery_error=None):
        calls.append(("mark", jid, ok))

    monkeypatch.setattr(s, "run_job", fake_run_job)
    monkeypatch.setattr(s, "save_job_output", fake_save)
    monkeypatch.setattr(s, "_deliver_result", fake_deliver)
    monkeypatch.setattr(s, "mark_job_run", fake_mark)
    return calls


def _delivered(calls):
    hit = [c for c in calls if c[0] == "deliver"]
    assert hit, "expected a delivery call"
    return hit[0][2]


def _saved(calls):
    hit = [c for c in calls if c[0] == "save"]
    assert hit, "expected a save call"
    return hit[0][2]


def test_exact_id_wins_over_same_prefix_decoy(monkeypatch):
    """A newer row sharing the job-id prefix must never be selected."""
    calls = _patch_pipeline(monkeypatch, capture=_std_capture(cost_usd=0.9999))
    now = time.time()
    _seed_state_db([
        # decoy: same job prefix, different (later) run, much higher cost
        ("cron_j1_99990101_000000", "decoy", 0, 0, 9.99,
         "estimated", "x", 1, now + 10, None),
        (RUN_ID, "deepseek-v4-pro", 1000, 500, 0.1856,
         "estimated", "official_docs_snapshot", 7, now, None),
    ])

    s.run_one_job({"id": "j1", "name": "t"})

    delivered = _delivered(calls)
    assert "💰 Cost: $0.1856 · deepseek-v4-pro · 7 calls" in delivered
    assert "9.99" not in delivered


def test_lineage_aggregates_children_and_grandchildren(monkeypatch):
    """Children AND grandchildren (delegation / compression cascade) count."""
    calls = _patch_pipeline(monkeypatch, capture=_std_capture(cost_usd=0.5))
    now = time.time()
    _seed_state_db([
        (RUN_ID, "deepseek-v4-pro", 1000, 500, 0.1000,
         "estimated", "official_docs_snapshot", 7, now, None),
        # delegated child of the root
        ("sub_a", "deepseek-v4-pro", 100, 50, 0.0500,
         "estimated", "official_docs_snapshot", 2, now, RUN_ID),
        # compression continuation (non-cron id) of the root
        ("20260912_121000_abc123", "deepseek-v4-pro", 10, 5, 0.0100,
         "estimated", "official_docs_snapshot", 1, now, RUN_ID),
        # grandchild: child of the continuation
        ("sub_b", "deepseek-v4-pro", 1, 1, 0.0050,
         "estimated", "official_docs_snapshot", 1, now, "20260912_121000_abc123"),
    ])

    s.run_one_job({"id": "j1", "name": "t"})

    delivered = _delivered(calls)
    assert "💰 Cost: $0.1650" in delivered        # 0.1 + 0.05 + 0.01 + 0.005
    assert "· 11 calls" in delivered              # 7 + 2 + 1 + 1
    saved = _saved(calls)
    assert "1,111 in / 556 out tokens" in saved
    assert "pricing: official_docs_snapshot" in saved


def test_partially_unpriced_marker(monkeypatch):
    calls = _patch_pipeline(monkeypatch, capture=_std_capture())
    now = time.time()
    _seed_state_db([
        (RUN_ID, "deepseek-v4-pro", 1000, 500, 0.1000,
         "estimated", "official_docs_snapshot", 7, now, None),
        ("sub_unpriced", "deepseek-flash", 100, 50, 0.0,
         "unknown", "none", 2, now, RUN_ID),
    ])

    s.run_one_job({"id": "j1", "name": "t"})

    delivered = _delivered(calls)
    assert "💰 Cost: $0.1000 (partially unpriced)" in delivered


def test_early_exit_gets_no_llm_calls_line(monkeypatch):
    """capture empty (wake-gate / blocked / pre-agent crash): explicit line."""
    calls = _patch_pipeline(monkeypatch)  # capture stays empty

    s.run_one_job({"id": "j2", "name": "t"})

    assert "💰 Cost: $0.0000 (no LLM calls)" in _delivered(calls)
    assert "no LLM calls — agent did not run" in _saved(calls)


def test_missing_store_falls_back_to_capture(monkeypatch):
    """No state.db at all — but the in-memory capture still reports."""
    calls = _patch_pipeline(monkeypatch, capture=_std_capture())

    s.run_one_job({"id": "j3", "name": "t"})

    assert "💰 Cost: $0.1856 · deepseek-v4-pro · 7 calls" in _delivered(calls)


def test_corrupt_store_falls_back_to_capture(monkeypatch):
    home = get_hermes_home()
    home.mkdir(parents=True, exist_ok=True)
    (home / "state.db").write_bytes(b"not a sqlite database")
    calls = _patch_pipeline(monkeypatch, capture=_std_capture())

    s.run_one_job({"id": "j4", "name": "t"})

    assert "💰 Cost: $0.1856" in _delivered(calls)


def test_corrupt_store_without_capture_numbers_is_unavailable(monkeypatch):
    home = get_hermes_home()
    home.mkdir(parents=True, exist_ok=True)
    (home / "state.db").write_bytes(b"not a sqlite database")
    calls = _patch_pipeline(monkeypatch, capture={"session_id": RUN_ID})

    s.run_one_job({"id": "j5", "name": "t"})

    assert "💰 Cost: unavailable" in _delivered(calls)
    assert "Unavailable" in _saved(calls)


def test_no_agent_job_gets_script_only_line(monkeypatch):
    calls = _patch_pipeline(monkeypatch)

    s.run_one_job({"id": "j6", "name": "t", "no_agent": True})

    assert "💰 Cost: $0.0000 (script-only)" in _delivered(calls)
    assert "script-only" in _saved(calls)


def test_unpriced_zero_cost_run_is_marked(monkeypatch):
    calls = _patch_pipeline(monkeypatch, capture=_std_capture())
    _seed_state_db([
        (RUN_ID, "deepseek-flash", 100, 50, 0.0,
         "unknown", "none", 3, time.time(), None),
    ])

    s.run_one_job({"id": "j7", "name": "t"})

    assert "💰 Cost: $0.0000 (unpriced) · deepseek-flash · 3 calls" in _delivered(calls)


def test_subscription_zero_cost_run_is_marked(monkeypatch):
    calls = _patch_pipeline(monkeypatch, capture=_std_capture())
    _seed_state_db([
        (RUN_ID, "some-max-model", 100, 50, 0.0,
         "included", "none", 2, time.time(), None),
    ])

    s.run_one_job({"id": "j8", "name": "t"})

    assert "💰 Cost: $0.0000 (subscription)" in _delivered(calls)


def test_priced_zero_cost_run_has_no_marker(monkeypatch):
    calls = _patch_pipeline(monkeypatch, capture=_std_capture())
    _seed_state_db([
        (RUN_ID, "free-model", 100, 50, 0.0,
         "estimated", "official_docs_snapshot", 2, time.time(), None),
    ])

    s.run_one_job({"id": "j9", "name": "t"})

    delivered = _delivered(calls)
    assert "💰 Cost: $0.0000 · free-model · 2 calls" in delivered
    assert "unpriced" not in delivered and "subscription" not in delivered


def test_failure_delivery_also_carries_cost(monkeypatch):
    calls = _patch_pipeline(
        monkeypatch, success=False, final="", error="boom",
        capture=_std_capture(cost_usd=0.1, api_calls=1),
    )
    _seed_state_db([
        (RUN_ID, "deepseek-v4-pro", 100, 50, 0.1,
         "estimated", "official_docs_snapshot", 1, time.time(), None),
    ])

    s.run_one_job({"id": "j10", "name": "t"})

    assert "💰 Cost: $0.1000" in _delivered(calls)


def test_silent_run_keeps_archive_cost_without_delivery(monkeypatch):
    calls = _patch_pipeline(monkeypatch, silent_marker_in="[SILENT]",
                            capture=_std_capture())
    _seed_state_db([
        (RUN_ID, "deepseek-flash", 10, 2, 0.0,
         "unknown", "none", 1, time.time(), None),
    ])

    s.run_one_job({"id": "j11", "name": "t"})

    assert not [c for c in calls if c[0] == "deliver"]
    assert "## Cost" in _saved(calls)
