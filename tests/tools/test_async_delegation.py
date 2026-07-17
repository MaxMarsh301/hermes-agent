"""Tests for async (background) delegation — tools/async_delegation.py.

Covers the dispatch handle, non-blocking behavior, completion-event delivery
onto the shared process_registry.completion_queue, the rich re-injection block
formatting, capacity rejection, and crash handling.
"""

import hashlib
import json
import os
import queue
import sqlite3
import subprocess
import sys
import threading
import time

import pytest

from tools import async_delegation as ad
from tools.process_registry import process_registry, format_process_notification


@pytest.fixture(autouse=True)
def _clean_state():
    ad._reset_for_tests()
    while not process_registry.completion_queue.empty():
        process_registry.completion_queue.get_nowait()
    yield
    # Give just-released workers a beat to finalize BEFORE draining, so their
    # completion events land now instead of leaking into the next test's
    # queue (worker threads push events asynchronously; a drain that races an
    # in-flight _finalize misses it).
    deadline = time.monotonic() + 2.0
    while ad.active_count() and time.monotonic() < deadline:
        time.sleep(0.02)
    ad._reset_for_tests()
    while not process_registry.completion_queue.empty():
        process_registry.completion_queue.get_nowait()


def _drain_one(timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not process_registry.completion_queue.empty():
            return process_registry.completion_queue.get_nowait()
        time.sleep(0.02)
    return None


def _drain_for(delegation_id, timeout=5.0):
    """Drain until the event for *delegation_id* appears (discarding others).

    Completion events are pushed asynchronously by worker threads, so a
    straggler from a PREVIOUS test can land after that test's teardown drain
    and leak into the current test's queue. Matching on delegation_id makes
    the assertion immune to that cross-test leak.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not process_registry.completion_queue.empty():
            evt = process_registry.completion_queue.get_nowait()
            if evt.get("delegation_id") == delegation_id:
                return evt
            continue
        time.sleep(0.02)
    return None


def test_dispatch_returns_immediately_without_blocking():
    gate = threading.Event()

    def runner():
        gate.wait(timeout=60)
        return {"status": "completed", "summary": "done", "api_calls": 1,
                "duration_seconds": 0.1, "model": "m"}

    t0 = time.monotonic()
    res = ad.dispatch_async_delegation(
        goal="g", context=None, toolsets=None, role="leaf", model="m",
        session_key="", runner=runner, max_async_children=3,
    )
    elapsed = time.monotonic() - t0

    assert res["status"] == "dispatched"
    assert res["delegation_id"].startswith("deleg_")
    # Non-blocking invariant: dispatch returned while the runner is still
    # gated (active), so it cannot have waited on the gate. The active_count
    # check is the environment-independent proof; the generous wall-clock
    # bound is a loose sanity backstop, not the primary assertion (a loaded
    # CI runner can be slow but never anywhere near the runner's 5s gate).
    assert ad.active_count() == 1
    assert elapsed < 4.0, f"dispatch blocked {elapsed:.2f}s (gate is 5s)"
    gate.set()


def test_async_executor_workers_are_daemon_threads():
    gate = threading.Event()

    def runner():
        gate.wait(timeout=60)
        return {"status": "completed", "summary": "done"}

    res = ad.dispatch_async_delegation(
        goal="daemon check", context=None, toolsets=None, role="leaf", model="m",
        session_key="", runner=runner, max_async_children=1,
    )
    assert res["status"] == "dispatched"

    deadline = time.monotonic() + 2
    worker = None
    while time.monotonic() < deadline:
        worker = next(
            (t for t in threading.enumerate() if t.name.startswith("async-delegate")),
            None,
        )
        if worker is not None:
            break
        time.sleep(0.02)
    assert worker is not None
    assert worker.daemon is True
    gate.set()
    assert _drain_one() is not None


def test_completion_event_lands_on_shared_queue_with_session_key():
    def runner():
        return {"status": "completed", "summary": "the result",
                "api_calls": 3, "duration_seconds": 2.0, "model": "test-model"}

    res = ad.dispatch_async_delegation(
        goal="compute X", context="some context", toolsets=["web", "file"],
        role="leaf", model="test-model", session_key="agent:main:cli:dm:local",
        parent_session_id="20260703_parent_sid",
        runner=runner, max_async_children=3,
    )
    assert res["status"] == "dispatched"

    evt = _drain_one()
    assert evt is not None
    assert evt["type"] == "async_delegation"
    assert evt["summary"] == "the result"
    assert evt["session_key"] == "agent:main:cli:dm:local"
    assert evt["parent_session_id"] == "20260703_parent_sid"
    assert evt["delegation_id"] == res["delegation_id"]


def test_rich_reinjection_block_is_self_contained():
    def runner():
        return {"status": "completed", "summary": "The answer is 42.",
                "api_calls": 7, "duration_seconds": 3.5, "model": "test-model"}

    ad.dispatch_async_delegation(
        goal="Compute the meaning of life",
        context="User is a philosopher. Respond tersely.",
        toolsets=["web"], role="leaf", model="test-model",
        session_key="", runner=runner, max_async_children=3,
    )
    evt = _drain_one()
    assert evt is not None
    text = format_process_notification(evt)
    assert text is not None
    for needle in [
        "ASYNC DELEGATION COMPLETE",
        "Compute the meaning of life",
        "User is a philosopher",
        "Toolsets: web",
        "The answer is 42.",
        "Status: completed",
        "API calls: 7",
    ]:
        assert needle in text, f"missing {needle!r}"


def test_dispatch_rejected_at_capacity():
    ev = threading.Event()

    def blocker():
        ev.wait(timeout=60)
        return {"status": "completed", "summary": "x"}

    for i in range(2):
        r = ad.dispatch_async_delegation(
            goal=f"task{i}", context=None, toolsets=None, role="leaf",
            model="m", session_key="", runner=blocker, max_async_children=2,
        )
        assert r["status"] == "dispatched"

    r3 = ad.dispatch_async_delegation(
        goal="task3", context=None, toolsets=None, role="leaf", model="m",
        session_key="", runner=blocker, max_async_children=2,
    )
    assert r3["status"] == "rejected"
    assert "capacity reached" in r3["error"]
    ev.set()


def test_interrupt_all_signals_running_children():
    ev = threading.Event()
    interrupted = {"count": 0}
    # No short internal timeout: the blocker holds until interrupt_fn fires.
    # The old ev.wait(timeout=5) made this test a change-detector for CI
    # worker load — on a CPU-starved runner the 5s expired before
    # interrupt_all() ran, the record finalized, and interrupt_all() found
    # nothing running (n == 0). The pytest-level timeout is the real
    # runaway guard.

    def blocker():
        ev.wait(timeout=60)
        return {"status": "interrupted", "summary": None,
                "error": "cancelled"}

    def interrupt_fn():
        interrupted["count"] += 1
        ev.set()

    r = ad.dispatch_async_delegation(
        goal="long task", context=None, toolsets=None, role="leaf",
        model="m", session_key="", runner=blocker,
        interrupt_fn=interrupt_fn, max_async_children=3,
    )
    n = ad.interrupt_all(reason="test")
    assert n == 1
    assert interrupted["count"] == 1
    # child still emits a completion event after interrupt. Match on THIS
    # delegation's id — straggler 'completed' events from a previous test's
    # workers can finalize after that test's teardown drain and leak into
    # this queue (observed on loaded CI workers).
    evt = _drain_for(r["delegation_id"])
    assert evt is not None
    assert evt["status"] == "interrupted"


def test_completed_records_pruned_to_cap():
    # Run more than the retention cap quickly; ensure list doesn't grow forever.
    for i in range(ad._MAX_RETAINED_COMPLETED + 10):
        ad.dispatch_async_delegation(
            goal=f"t{i}", context=None, toolsets=None, role="leaf", model="m",
            session_key="", runner=lambda: {"status": "completed", "summary": "ok"},
            max_async_children=ad._MAX_RETAINED_COMPLETED + 20,
        )
    # let workers finish
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and ad.active_count() > 0:
        time.sleep(0.05)
    assert len(ad.list_async_delegations()) <= ad._MAX_RETAINED_COMPLETED


def test_completion_is_persisted_and_delivery_can_be_acknowledged(tmp_path, monkeypatch):
    """A finished child remains pending on disk until its queue consumer acks it."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    dispatched = ad.dispatch_async_delegation(
        goal="durable", context="ctx", toolsets=["terminal"], role="leaf",
        model="m", session_key="owner", parent_session_id="parent",
        runner=lambda: {"status": "completed", "summary": "survived"},
    )
    assert _drain_one() is not None

    restored = queue.Queue()
    assert ad.restore_undelivered_completions(restored) == 1
    row = ad.get_durable_delegation(dispatched["delegation_id"])
    assert row["origin_session"] == "owner"
    assert row["state"] == "completed"
    assert row["result"]["summary"] == "survived"
    assert row["delivery_state"] == "pending"
    # Queue publication/restoration is not a destination delivery attempt.
    assert row["delivery_attempts"] == 0

    assert ad.mark_completion_delivered(dispatched["delegation_id"])
    assert ad.restore_undelivered_completions(queue.Queue()) == 0
    assert ad.get_durable_delegation(dispatched["delegation_id"])["delivery_state"] == "delivered"


def test_real_process_restart_restores_owned_completion_once(tmp_path):
    """Real-import E2E: a fresh interpreter restores a prior process's result."""
    repo = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    env = {**os.environ, "HERMES_HOME": str(tmp_path), "PYTHONPATH": repo}
    producer = r'''
import time
from tools import async_delegation as ad
r = ad.dispatch_async_delegation(
    goal="restart", context=None, toolsets=None, role="leaf", model="m",
    session_key="owner-session", parent_session_id="durable-parent",
    runner=lambda: {"status": "completed", "summary": "after restart"},
)
deadline = time.time() + 5
while ad.active_count() and time.time() < deadline:
    time.sleep(.01)
print(r["delegation_id"])
'''
    first = subprocess.run(
        [sys.executable, "-c", producer], cwd=repo, env=env,
        text=True, capture_output=True, timeout=15, check=True,
    )
    delegation_id = first.stdout.strip().splitlines()[-1]

    consumer = r'''
import json
from tools.process_registry import process_registry
evt = process_registry.completion_queue.get_nowait()
print(json.dumps(evt, sort_keys=True))
'''
    second = subprocess.run(
        [sys.executable, "-c", consumer], cwd=repo, env=env,
        text=True, capture_output=True, timeout=15, check=True,
    )
    evt = json.loads(second.stdout.strip().splitlines()[-1])
    assert evt["delegation_id"] == delegation_id
    assert evt["session_key"] == "owner-session"
    assert evt["parent_session_id"] == "durable-parent"
    assert evt["summary"] == "after restart"

    acker = f'''
from tools import async_delegation as ad
assert ad.mark_completion_delivered({delegation_id!r})
'''
    subprocess.run(
        [sys.executable, "-c", acker], cwd=repo, env=env,
        text=True, capture_output=True, timeout=15, check=True,
    )
    probe = subprocess.run(
        [sys.executable, "-c", "from tools.process_registry import process_registry; print(process_registry.completion_queue.qsize())"],
        cwd=repo, env=env, text=True, capture_output=True, timeout=15, check=True,
    )
    assert probe.stdout.strip().splitlines()[-1] == "0"


def test_submit_failure_removes_durable_running_record(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))

    class _BrokenExecutor:
        def submit(self, *_args, **_kwargs):
            raise RuntimeError("submit failed")

    monkeypatch.setattr(ad, "_get_executor", lambda _max_workers: _BrokenExecutor())
    result = ad.dispatch_async_delegation(
        goal="never ran", context=None, toolsets=None, role="leaf", model="m",
        session_key="owner", runner=lambda: {},
    )

    assert result["status"] == "rejected"
    with ad._DB_LOCK, ad._connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM async_delegations").fetchone()[0] == 0


def test_pending_retention_prunes_delivered_before_undelivered(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(ad, "_MAX_RETAINED_COMPLETED", 2)
    for index, delivery_state in enumerate(("pending", "delivered", "pending")):
        delegation_id = f"deleg_{index}"
        record = {
            "delegation_id": delegation_id,
            "session_key": "owner",
            "origin_ui_session_id": "",
            "parent_session_id": None,
            "dispatched_at": float(index + 1),
        }
        ad._persist_dispatch(record)
        ad._persist_completion(
            {
                "delegation_id": delegation_id,
                "status": "completed",
                "completed_at": float(index + 1),
            },
            {"status": "completed", "summary": delegation_id},
        )
        if delivery_state == "delivered":
            ad.mark_completion_delivered(delegation_id)

    ad._prune_durable_records()

    assert ad.get_durable_delegation("deleg_0") is not None
    assert ad.get_durable_delegation("deleg_1") is None
    assert ad.get_durable_delegation("deleg_2") is not None


def test_recover_marks_abandoned_running_record_unknown(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    record = {
        "delegation_id": "deleg_abandoned",
        "session_key": "owner",
        "origin_ui_session_id": "",
        "parent_session_id": None,
        "dispatched_at": 1.0,
    }
    ad._persist_dispatch(record)
    with ad._DB_LOCK, ad._connect() as conn:
        conn.execute(
            "UPDATE async_delegations SET owner_pid=?, owner_started_at=NULL WHERE delegation_id=?",
            (99999999, "deleg_abandoned"),
        )

    assert ad.recover_abandoned_delegations() == 1
    durable = ad.get_durable_delegation("deleg_abandoned")
    assert durable["state"] == "unknown"
    assert durable["delivery_state"] == "pending"
    restored = queue.Queue()
    assert ad.restore_undelivered_completions(restored) == 1
    assert restored.get_nowait()["status"] == "unknown"


def test_durable_delivery_claim_is_exclusive_and_retryable(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    record = {
        "delegation_id": "deleg_claim", "session_key": "owner",
        "origin_ui_session_id": "", "parent_session_id": None,
        "dispatched_at": 1.0,
    }
    ad._persist_dispatch(record)
    ad._persist_completion(
        {"delegation_id": "deleg_claim", "status": "completed", "completed_at": 2.0},
        {"status": "completed", "summary": "done"},
    )

    assert ad.claim_completion_delivery("deleg_claim", "consumer-a")
    assert not ad.claim_completion_delivery("deleg_claim", "consumer-b")
    assert ad.release_completion_delivery("deleg_claim", "consumer-a")
    assert ad.claim_completion_delivery("deleg_claim", "consumer-b")
    assert ad.complete_completion_delivery("deleg_claim", "consumer-b")
    assert not ad.claim_completion_delivery("deleg_claim", "consumer-c")
    assert ad.get_durable_delegation("deleg_claim")["delivery_state"] == "delivered"


# ---------------------------------------------------------------------------
# Integration: delegate_task(background=True) routing
# ---------------------------------------------------------------------------

def test_delegate_task_background_routes_async_and_does_not_block(monkeypatch):
    """delegate_task(background=True) returns a handle without running the
    child synchronously, and the child completes on the background thread.
    A single task is dispatched as a one-item background batch unit."""
    from unittest.mock import MagicMock, patch
    import tools.delegate_tool as dt

    parent = MagicMock()
    parent._delegate_depth = 0
    parent.session_id = "sess"
    parent._interrupt_requested = False
    parent._active_children = []
    parent._active_children_lock = None
    fake_child = MagicMock()
    fake_child._delegate_role = "leaf"
    fake_child._subagent_id = "s1"

    gate = threading.Event()

    def slow_child(task_index, goal, child=None, parent_agent=None, **kw):
        gate.wait(timeout=60)  # a sync impl would hang delegate_task here
        return {
            "task_index": 0, "status": "completed", "summary": f"done: {goal}",
            "api_calls": 1, "duration_seconds": 0.1, "model": "m",
            "exit_reason": "completed",
        }

    creds = {
        "model": "m", "provider": None, "base_url": None, "api_key": None,
        "api_mode": None, "command": None, "args": None,
    }
    # monkeypatch (not `with`) so patches outlive delegate_task's return and
    # remain active while the background worker runs.
    monkeypatch.setattr(dt, "_build_child_agent", lambda **kw: fake_child)
    monkeypatch.setattr(dt, "_run_single_child", slow_child)
    monkeypatch.setattr(dt, "_resolve_delegation_credentials", lambda *a, **k: creds)
    out = dt.delegate_task(
        goal="the real task", context="ctx",
        background=True, parent_agent=parent,
    )

    import json
    parsed = json.loads(out)
    assert parsed["status"] == "dispatched"
    assert parsed["mode"] == "background"
    assert parsed["delegation_id"].startswith("deleg_")
    # Non-blocking invariant: delegate_task returned while the child is STILL
    # blocked on the closed gate, so no completion event exists yet.
    assert process_registry.completion_queue.empty()
    assert ad.active_count() == 1  # one background batch unit, not finished

    gate.set()
    evt = _drain_one()
    assert evt is not None
    assert evt["type"] == "async_delegation"
    # Single task rides the batch path → carries a 1-item results list.
    assert evt.get("is_batch") is True
    assert len(evt["results"]) == 1
    assert evt["results"][0]["summary"] == "done: the real task"
    text = format_process_notification(evt)
    assert text is not None
    assert "the real task" in text


def test_delegate_task_background_uses_live_tui_agent_session_id(monkeypatch):
    """TUI async delegation must route to the live/compressed agent id.

    Regression: delegate_task captured the stale approval/session context key
    after compression rotated parent_agent.session_id. The resulting completion
    was orphaned and could be consumed by an unrelated desktop session poller.
    """
    import json
    from unittest.mock import MagicMock
    import tools.delegate_tool as dt
    from gateway.session_context import clear_session_vars, set_session_vars
    from tools.approval import reset_current_session_key, set_current_session_key

    parent = MagicMock()
    parent._delegate_depth = 0
    parent.session_id = "post-compress-tip"
    parent._interrupt_requested = False
    parent._active_children = []
    parent._active_children_lock = None
    fake_child = MagicMock()
    fake_child._delegate_role = "leaf"

    creds = {
        "model": "m", "provider": None, "base_url": None, "api_key": None,
        "api_mode": None, "command": None, "args": None,
    }
    monkeypatch.setattr(dt, "_build_child_agent", lambda **kw: fake_child)
    monkeypatch.setattr(dt, "_resolve_delegation_credentials", lambda *a, **k: creds)
    monkeypatch.setattr(
        dt,
        "_run_single_child",
        lambda *a, **k: {
            "task_index": 0,
            "status": "completed",
            "summary": "done",
            "api_calls": 1,
            "duration_seconds": 0.1,
            "model": "m",
            "exit_reason": "completed",
        },
    )

    approval_token = set_current_session_key("pre-compress-parent")
    session_tokens = set_session_vars(
        source="tui",
        session_key="pre-compress-parent",
        ui_session_id="origin-tab",
    )
    try:
        out = dt.delegate_task(goal="bg task", background=True, parent_agent=parent)
        assert json.loads(out)["status"] == "dispatched"
        evt = _drain_one()
    finally:
        reset_current_session_key(approval_token)
        clear_session_vars(session_tokens)

    assert evt is not None
    assert evt["type"] == "async_delegation"
    assert evt["session_key"] == "post-compress-tip"
    assert evt["origin_ui_session_id"] == "origin-tab"


def test_delegate_task_background_batch_runs_as_one_unit(monkeypatch):
    """A multi-item batch with background=True dispatches the WHOLE fan-out as
    ONE background unit (one handle, one async slot). The children run in
    parallel and join; the consolidated results come back as a single
    completion event when ALL of them finish."""
    import json
    from unittest.mock import MagicMock, patch
    import tools.delegate_tool as dt

    parent = MagicMock()
    parent._delegate_depth = 0
    parent.session_id = "sess"
    parent._interrupt_requested = False
    parent._active_children = []
    parent._active_children_lock = None

    fake_child = MagicMock()
    fake_child._delegate_role = "leaf"

    gate = threading.Event()

    def _blocking_child(task_index, goal, child=None, parent_agent=None, **kw):
        gate.wait(timeout=60)
        return {
            "task_index": task_index, "status": "completed",
            "summary": f"done: {goal}", "api_calls": 1,
            "duration_seconds": 0.1, "model": "m", "exit_reason": "completed",
        }

    creds = {
        "model": "m", "provider": None, "base_url": None, "api_key": None,
        "api_mode": None, "command": None, "args": None,
    }

    # Use monkeypatch (not a `with` block) so the patches stay active while the
    # background worker thread runs _execute_and_aggregate AFTER delegate_task
    # has already returned.
    monkeypatch.setattr(dt, "_build_child_agent", lambda **kw: fake_child)
    monkeypatch.setattr(dt, "_run_single_child", _blocking_child)
    monkeypatch.setattr(dt, "_resolve_delegation_credentials", lambda *a, **k: creds)
    out = dt.delegate_task(
        tasks=[{"goal": "a"}, {"goal": "b"}, {"goal": "c"}],
        background=True,
        parent_agent=parent,
    )

    parsed = json.loads(out)
    assert parsed["status"] == "dispatched"
    assert parsed["mode"] == "background"
    assert parsed["count"] == 3
    assert parsed["delegation_id"].startswith("deleg_")
    assert parsed["goals"] == ["a", "b", "c"]
    # ONE background unit for the whole fan-out (not three), and the call
    # returned while all children are still blocked → chat not blocked.
    assert process_registry.completion_queue.empty()
    assert ad.active_count() == 1

    # Release the children; the whole batch joins and emits ONE event.
    gate.set()
    evt = _drain_one()
    assert evt is not None
    assert evt["type"] == "async_delegation"
    assert evt.get("is_batch") is True
    assert len(evt["results"]) == 3
    summaries = sorted(r["summary"] for r in evt["results"])
    assert summaries == ["done: a", "done: b", "done: c"]
    # The consolidated notification names all three tasks in one block.
    text = format_process_notification(evt)
    assert text is not None
    assert "TASK 1/3" in text and "TASK 2/3" in text and "TASK 3/3" in text
    assert "done: a" in text and "done: b" in text and "done: c" in text
    # No more events — it's a single combined completion, not N of them.
    assert _drain_one() is None


def test_model_dispatch_forces_background():
    """The MODEL-facing dispatch path forces background=True for any top-level
    delegation (single task OR batch), and keeps it off for an orchestrator
    subagent (depth > 0). Direct delegate_task() callers are unaffected (they
    keep the synchronous default)."""
    import tools.delegate_tool as dt
    from unittest.mock import MagicMock

    top = MagicMock()
    top._delegate_depth = 0
    sub = MagicMock()
    sub._delegate_depth = 1

    # Registry-fallback helper: top-level always background, regardless of
    # single vs batch; subagent never.
    assert dt._model_background_value({"goal": "x"}, top) is True
    assert dt._model_background_value(
        {"tasks": [{"goal": "a"}, {"goal": "b"}]}, top
    ) is True
    assert dt._model_background_value({"tasks": [{"goal": "a"}]}, top) is True
    assert dt._model_background_value({"goal": "x"}, sub) is False
    assert dt._model_background_value(
        {"tasks": [{"goal": "a"}, {"goal": "b"}]}, sub
    ) is False


def test_run_agent_dispatch_forces_background():
    """run_agent._dispatch_delegate_task — the live model path — forces
    background on for any top-level delegation (single OR batch) and off for a
    subagent."""
    from unittest.mock import patch
    import run_agent

    class _FakeAgent:
        _delegate_depth = 0

    captured = {}

    def _fake_delegate(**kwargs):
        captured.update(kwargs)
        return "{}"

    with patch("tools.delegate_tool.delegate_task", _fake_delegate):
        agent = _FakeAgent()
        run_agent.AIAgent._dispatch_delegate_task(agent, {"goal": "x"})
        assert captured["background"] is True

        run_agent.AIAgent._dispatch_delegate_task(
            agent, {"tasks": [{"goal": "a"}, {"goal": "b"}]}
        )
        assert captured["background"] is True

        sub = _FakeAgent()
        sub._delegate_depth = 1
        run_agent.AIAgent._dispatch_delegate_task(sub, {"goal": "x"})
        assert captured["background"] is False


def test_dispatch_never_forwards_model_toolsets():
    """The model has no toolsets argument — subagents always inherit the
    parent's toolsets. Even if a model smuggles a `toolsets` key into the
    tool-call args, the live dispatch path must NOT forward it to
    delegate_task (which no longer accepts it) and must not crash."""
    from unittest.mock import patch
    import run_agent

    class _FakeAgent:
        _delegate_depth = 0

    captured = {}

    def _fake_delegate(**kwargs):
        captured.update(kwargs)
        return "{}"

    with patch("tools.delegate_tool.delegate_task", _fake_delegate):
        run_agent.AIAgent._dispatch_delegate_task(
            _FakeAgent(), {"goal": "x", "toolsets": ["web", "terminal"]}
        )
    assert "toolsets" not in captured


def test_delegate_task_background_detaches_child_from_parent(monkeypatch):
    """A background child must NOT remain in parent._active_children —
    otherwise parent-turn interrupts / cache evicts / session close would
    kill the detached subagent mid-run."""
    from unittest.mock import MagicMock, patch
    import tools.delegate_tool as dt

    parent = MagicMock()
    parent._delegate_depth = 0
    parent.session_id = "sess"
    parent._active_children = []
    parent._active_children_lock = threading.Lock()
    fake_child = MagicMock()
    fake_child._delegate_role = "leaf"
    fake_child._subagent_id = "s1"

    gate = threading.Event()

    def slow_child(task_index, goal, child=None, parent_agent=None, **kw):
        gate.wait(timeout=60)
        return {"task_index": 0, "status": "completed", "summary": "ok"}

    def build_and_register(**kw):
        # Mirror what the real _build_child_agent does: register the child
        # for interrupt propagation.
        parent._active_children.append(fake_child)
        return fake_child

    creds = {
        "model": "m", "provider": None, "base_url": None, "api_key": None,
        "api_mode": None, "command": None, "args": None,
    }
    with patch.object(dt, "_build_child_agent", side_effect=build_and_register), \
         patch.object(dt, "_run_single_child", side_effect=slow_child), \
         patch.object(dt, "_resolve_delegation_credentials", return_value=creds):
        out = dt.delegate_task(goal="bg task", background=True, parent_agent=parent)

    import json
    assert json.loads(out)["status"] == "dispatched"
    # Child detached immediately at dispatch, while it is still running.
    assert fake_child not in parent._active_children
    gate.set()
    assert _drain_one() is not None


def test_concurrent_dispatch_respects_capacity():
    """Two threads racing dispatch with cap=1 must yield exactly one accept
    (capacity check and record insert are atomic under the records lock)."""
    gate = threading.Event()

    def blocker():
        gate.wait(timeout=60)
        return {"status": "completed", "summary": "x"}

    results = []
    barrier = threading.Barrier(2)

    def racer():
        barrier.wait(timeout=5)
        results.append(
            ad.dispatch_async_delegation(
                goal="race", context=None, toolsets=None, role="leaf",
                model="m", session_key="", runner=blocker,
                max_async_children=1,
            )
        )

    threads = [threading.Thread(target=racer) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    statuses = sorted(r["status"] for r in results)
    assert statuses == ["dispatched", "rejected"]
    gate.set()


# ---------------------------------------------------------------------------
# Gateway routing: session_key -> platform/chat_id, rich formatting, injection
# ---------------------------------------------------------------------------

def _make_async_evt(**over):
    evt = {
        "type": "async_delegation",
        "delegation_id": "deleg_x1",
        "session_key": "agent:main:telegram:dm:12345:678",
        "goal": "Investigate flaky test",
        "context": "repo /tmp/p",
        "toolsets": ["terminal"],
        "role": "leaf",
        "model": "m",
        "status": "completed",
        "summary": "Found the bug in test_foo",
        "api_calls": 4,
        "duration_seconds": 12.0,
        "dispatched_at": 1000.0,
        "completed_at": 1012.0,
    }
    evt.update(over)
    return evt


def test_gateway_enriches_routing_from_session_key():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    evt = _make_async_evt()
    runner._enrich_async_delegation_routing(evt)
    assert evt["platform"] == "telegram"
    assert evt["chat_id"] == "12345"
    assert evt["thread_id"] == "678"


def test_gateway_formatter_renders_async_block():
    from gateway.run import _format_gateway_process_notification

    txt = _format_gateway_process_notification(_make_async_evt())
    assert txt is not None
    assert "ASYNC DELEGATION COMPLETE" in txt
    assert "Found the bug in test_foo" in txt
    assert "Investigate flaky test" in txt


def test_gateway_watch_drain_requeues_async_without_looping():
    from gateway.run import _drain_gateway_watch_events

    q = queue.Queue()
    async_evt = _make_async_evt()
    watch_evt = {
        "type": "watch_match",
        "session_id": "proc_1",
        "command": "pytest",
        "pattern": "READY",
        "output": "READY",
    }
    q.put(async_evt)
    q.put(watch_evt)

    watch_events = _drain_gateway_watch_events(q)

    assert watch_events == [watch_evt]
    assert q.qsize() == 1
    assert q.get_nowait() == async_evt


def test_gateway_builds_routable_source_from_enriched_event():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    evt = _make_async_evt()
    runner._enrich_async_delegation_routing(evt)
    src = runner._build_process_event_source(evt)
    assert src is not None
    assert src.platform.value == "telegram"
    assert src.chat_id == "12345"


def test_gateway_cli_origin_event_left_unrouted():
    """An empty session_key (CLI origin) is left without routing fields."""
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    evt = _make_async_evt(session_key="")
    runner._enrich_async_delegation_routing(evt)
    assert "platform" not in evt


# ---------------------------------------------------------------------------
# Durable ANTON detached-completion handoff (Slice B)
# ---------------------------------------------------------------------------


def _anton_route():
    conversation = "conversation_0123456789abcdef0123456789abcdef"
    return {
        "protocol": "anton.delegation.v1",
        "originConversationId": conversation,
        "deliveryTarget": f"anton:{conversation}",
        "parentRunId": "run_0123456789abcdef0123456789abcdef",
        "parentSessionId": "anton-chat-0123456789abcdef0123456789abcdef",
    }


def _persist_anton_terminal(tmp_path, monkeypatch, delegation_id="deleg_test"):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    record = {
        "delegation_id": delegation_id, "session_key": "private-session",
        "origin_ui_session_id": "", "parent_session_id": "private-parent",
        "dispatched_at": 1.0, "delivery_kind": "anton", "anton_route": _anton_route(),
        "delivery_id": "delivery_" + hashlib.sha256(delegation_id.encode()).hexdigest()[:32],
    }
    ad._persist_dispatch(record)
    ad._persist_completion(
        {"delegation_id": delegation_id, "status": "completed", "completed_at": 2.0},
        {"status": "completed", "summary": "safe result"}, record,
    )
    return record


def test_anton_persists_before_process_registry_import_and_never_queues(tmp_path, monkeypatch):
    record = _persist_anton_terminal(tmp_path, monkeypatch, "deleg_persist")
    # Recreate a running row because the helper intentionally proves canonical
    # terminal serialization separately; this path must not import registry.
    record["delegation_id"] = "deleg_noimport"
    record["delivery_id"] = "delivery_" + "b" * 32
    ad._persist_dispatch(record)
    import builtins
    original_import = builtins.__import__

    def deny_registry(name, *args, **kwargs):
        if name == "tools.process_registry":
            raise ImportError("registry unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", deny_registry)
    ad._push_completion_event(record, {"status": "completed", "summary": "exact result"}, "completed")
    durable = ad.get_durable_delegation("deleg_noimport")
    assert durable["handoff_state"] == "pending"
    assert durable["anton_body_sha256"] == hashlib.sha256(durable["anton_body_json"].encode()).hexdigest()
    assert process_registry.completion_queue.empty()


def test_anton_route_is_immutable_and_ids_are_full_entropy(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    route = _anton_route()
    monkeypatch.setattr(ad, "_trusted_anton_route", lambda: route)
    gate = threading.Event()
    def runner():
        gate.wait(2)
        return {"status": "completed", "summary": "ok"}

    dispatched = ad.dispatch_async_delegation(
        goal="private goal", context="private context", toolsets=None, role="leaf", model="m",
        session_key="private-session", runner=runner,
    )
    assert dispatched["delegation_id"].startswith("deleg_")
    assert len(dispatched["delegation_id"].removeprefix("deleg_")) == 32
    route["parentRunId"] = "run_" + "f" * 32
    route["deliveryTarget"] = "anton:conversation_" + "f" * 32
    gate.set()
    deadline = time.monotonic() + 3
    while ad.active_count() and time.monotonic() < deadline:
        time.sleep(.01)
    durable = ad.get_durable_delegation(dispatched["delegation_id"])
    body = json.loads(durable["anton_body_json"])
    assert durable["delivery_id"].startswith("delivery_") and len(durable["delivery_id"].removeprefix("delivery_")) == 32
    assert body["parentRunId"] == _anton_route()["parentRunId"]
    assert body["deliveryTarget"] == _anton_route()["deliveryTarget"]


def test_anton_canonical_single_and_mixed_batch_callback_body_redacts(tmp_path, monkeypatch):
    record = _persist_anton_terminal(tmp_path, monkeypatch, "deleg_body")
    single = ad.get_durable_delegation("deleg_body")
    body = json.loads(single["anton_body_json"])
    assert list(body) == sorted(body)
    assert set(body) == {"version", "deliveryId", "delegationId", "parentRunId", "parentSessionId", "deliveryTarget", "status", "occurredAt", "completion"}
    assert body["occurredAt"] == "1970-01-01T00:00:02.000Z"
    assert body["status"] == "completed" and body["completion"]["completedChildren"] == 1
    single_summary = ad._safe_summary({"summary": "single token=sk_abcdefghijklmnopqrstuvwxyz https://u:p@example.test/x /private/a [private]hide[/private] Traceback (most recent call last): boom " + "界" * 1000})
    for forbidden in ("sk_", "example.test", "/private/a", "hide", "Traceback"):
        assert forbidden not in single_summary
    assert len(single_summary.encode("utf-8")) <= 2048
    sensitive = "first token=sk_abcdefghijklmnopqrstuvwxyz https://u:p@example.test/x /private/a [private]hide[/private]"
    batch = ad._anton_body(record, {"results": [
        {"status": "completed", "summary": sensitive},
        {"status": "error", "summary": "second Traceback (most recent call last): boom"},
        {"status": "interrupted", "error": "raw child error must not appear"},
    ]}, "error", 3.0)
    parsed = json.loads(batch)
    assert parsed["status"] == "failed"
    assert [parsed["completion"][key] for key in ("completedChildren", "failedChildren", "interruptedChildren", "unknownChildren")] == [1, 1, 1, 0]
    summary = parsed["completion"]["summary"]
    for forbidden in ("sk_", "example.test", "/private/a", "hide", "Traceback", "raw child error"):
        assert forbidden not in summary
    assert "first" in summary and "second" in summary
    assert len(batch.encode("utf-8")) <= 65536 and len(summary.encode("utf-8")) <= 2048


def test_anton_sensitive_dispatch_and_crash_logs_are_safe(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(ad, "_trusted_anton_route", _anton_route)
    caplog.set_level("INFO", logger="tools.async_delegation")

    def crash():
        raise RuntimeError("token=sk_abcdefghijklmnopqrstuvwxyz /leak/path private-session secret result")

    result = ad.dispatch_async_delegation(
        goal="private goal token=sk_abcdefghijklmnopqrstuvwxyz", context="private context",
        toolsets=None, role="leaf", model="m", session_key="private-session", runner=crash,
    )
    deadline = time.monotonic() + 3
    while ad.active_count() and time.monotonic() < deadline:
        time.sleep(.01)
    logs = caplog.text
    for forbidden in ("private goal", "private context", "private-session", "sk_", "/leak/path", "secret result"):
        assert forbidden not in logs
    assert "Dispatched ANTON async delegation" in logs
    assert ad.get_durable_delegation(result["delegation_id"])["handoff_state"] == "pending"


def test_anton_invalid_trusted_route_fails_before_runner_or_persistence(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(ad, "_trusted_anton_route", lambda: (_ for _ in ()).throw(ValueError("invalid route")))
    called = False

    def runner():
        nonlocal called
        called = True
        return {}

    with pytest.raises(ValueError, match="invalid route"):
        ad.dispatch_async_delegation(goal="g", context=None, toolsets=None, role="leaf", model="m", session_key="s", runner=runner)
    assert not called
    with ad._DB_LOCK, ad._connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM async_delegations").fetchone()[0] == 0


def test_anton_claims_are_fenced_hash_checked_and_corruption_fails_closed(tmp_path, monkeypatch):
    _persist_anton_terminal(tmp_path, monkeypatch, "deleg_claimone")
    claims = []
    barrier = threading.Barrier(2)

    def claimant():
        barrier.wait()
        claims.append(ad.claim_anton_handoffs(limit=1, lease_seconds=1))

    threads = [threading.Thread(target=claimant) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(len(claim) for claim in claims) == [0, 1]
    first = next(claim[0] for claim in claims if claim)
    assert hashlib.sha256(first["body_json"].encode()).hexdigest() == first["body_sha256"]
    assert ad.claim_anton_handoffs(limit=0) == []
    assert not ad.release_anton_handoff(first["delegation_id"], "wrong", first["claim_version"])
    with ad._DB_LOCK, ad._connect() as conn:
        conn.execute("UPDATE async_delegations SET handoff_claim_expires_at=0 WHERE delegation_id=?", (first["delegation_id"],))
    second = ad.claim_anton_handoffs(limit=1)[0]
    assert second["claim_version"] == first["claim_version"] + 1
    assert not ad.release_anton_handoff(first["delegation_id"], first["claim_token"], first["claim_version"])
    assert not ad.mark_anton_handoff_completed(first["delegation_id"], first["claim_token"], first["claim_version"])
    assert ad.release_anton_handoff(second["delegation_id"], second["claim_token"], second["claim_version"])
    corrupt = _persist_anton_terminal(tmp_path, monkeypatch, "deleg_corrupt")
    with ad._DB_LOCK, ad._connect() as conn:
        conn.execute("UPDATE async_delegations SET anton_body_sha256='bad' WHERE delegation_id=?", (corrupt["delegation_id"],))
    assert ad.claim_anton_handoffs(limit=2) == []
    assert ad.get_durable_delegation(corrupt["delegation_id"])["handoff_state"] == "pending"


def test_anton_handoffs_cannot_enter_generic_session_delivery_rail(tmp_path, monkeypatch):
    record = _persist_anton_terminal(tmp_path, monkeypatch, "deleg_anton_isolated")

    assert not ad.claim_completion_delivery(record["delegation_id"], "malformed-session-claim")
    assert not ad.release_completion_delivery(record["delegation_id"], "malformed-session-claim")
    assert not ad.mark_completion_delivered(record["delegation_id"])
    assert not ad.complete_completion_delivery(record["delegation_id"], "malformed-session-claim")
    durable = ad.get_durable_delegation(record["delegation_id"])
    assert durable is not None
    assert durable["delivery_state"] == "pending"
    assert durable["handoff_state"] == "pending"

    claimed = ad.claim_anton_handoffs(limit=1)
    assert [handoff["delegation_id"] for handoff in claimed] == [record["delegation_id"]]


def test_anton_owner_loss_is_unknown_once_without_session_queue(tmp_path, monkeypatch):
    record = _persist_anton_terminal(tmp_path, monkeypatch, "deleg_owner")
    # Restore it to a running ANTON row, simulating a process crash before finalization.
    with ad._DB_LOCK, ad._connect() as conn:
        conn.execute("UPDATE async_delegations SET state='running', completed_at=NULL, handoff_state=NULL, owner_pid=99999999, owner_started_at=NULL WHERE delegation_id=?", (record["delegation_id"],))
    assert ad.recover_abandoned_delegations() == 1
    assert ad.recover_abandoned_delegations() == 0
    durable = ad.get_durable_delegation(record["delegation_id"])
    assert durable["state"] == "unknown" and durable["handoff_state"] == "pending"
    assert json.loads(durable["anton_body_json"])["status"] == "unknown"
    assert ad.restore_undelivered_completions(queue.Queue()) == 0


def test_anton_pruning_excludes_protected_handoffs_but_allows_completed(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(ad, "_MAX_RETAINED_COMPLETED", 10)
    generic = {"delegation_id": "deleg_generic", "session_key": "s", "origin_ui_session_id": "", "parent_session_id": None, "dispatched_at": 1.0}
    ad._persist_dispatch(generic)
    ad._persist_completion({"delegation_id": "deleg_generic", "status": "completed", "completed_at": 1.0}, {"summary": "generic"})
    protected = _persist_anton_terminal(tmp_path, monkeypatch, "deleg_protected")
    monkeypatch.setattr(ad, "_MAX_RETAINED_COMPLETED", 1)
    ad._prune_durable_records()
    assert ad.get_durable_delegation("deleg_generic") is not None
    assert ad.get_durable_delegation(protected["delegation_id"]) is not None
    with ad._DB_LOCK, ad._connect() as conn:
        conn.execute("UPDATE async_delegations SET handoff_state='completed' WHERE delegation_id=?", (protected["delegation_id"],))
    monkeypatch.setattr(ad, "_MAX_RETAINED_COMPLETED", 0)
    ad._prune_durable_records()
    assert ad.get_durable_delegation(protected["delegation_id"]) is None


def test_legacy_schema_migrates_preserves_row_and_adds_handoff_indexes(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    db = tmp_path / "state.db"
    with sqlite3.connect(db) as conn:
        conn.execute("""CREATE TABLE async_delegations (
            delegation_id TEXT PRIMARY KEY, origin_session TEXT NOT NULL,
            origin_ui_session_id TEXT NOT NULL DEFAULT '', parent_session_id TEXT,
            state TEXT NOT NULL, dispatched_at REAL NOT NULL, completed_at REAL,
            updated_at REAL NOT NULL, event_json TEXT, result_json TEXT,
            delivery_state TEXT NOT NULL DEFAULT 'pending', delivery_attempts INTEGER NOT NULL DEFAULT 0,
            delivered_at REAL)""")
        conn.execute("INSERT INTO async_delegations (delegation_id, origin_session, state, dispatched_at, updated_at) VALUES ('deleg_legacy', 'legacy', 'completed', 1, 1)")
    with ad._DB_LOCK, ad._connect() as conn:
        row = conn.execute("SELECT origin_session, delivery_kind, handoff_claim_version FROM async_delegations WHERE delegation_id='deleg_legacy'").fetchone()
        indexes = {item[1] for item in conn.execute("PRAGMA index_list(async_delegations)")}
    assert row == ("legacy", "session", 0)
    assert {"idx_async_delegations_delivery_id", "idx_async_delegations_anton_handoff_due", "idx_async_delegations_anton_terminal"} <= indexes


