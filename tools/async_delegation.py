#!/usr/bin/env python3
"""
Async (background) delegation registry.

Backs ``delegate_task(background=true)``: the parent agent dispatches a
subagent that runs on a module-level daemon executor and returns a handle
immediately, so the user and the model can keep working while the child runs.

When the child finishes, a completion event is pushed onto the SHARED
``process_registry.completion_queue`` with ``type="async_delegation"``. The
CLI (``cli.py`` process_loop) and gateway (``_run_process_watcher`` /
``completion_queue`` drain) already poll that queue while the agent is idle
and forge a fresh user/internal turn from each event. We deliberately reuse
that rail rather than reaching into a running agent loop:

  - completions surface as a NEW turn when the agent is idle, never spliced
    between a tool result and an assistant message. That keeps strict
    message-role alternation legal and the prompt cache intact (hard
    invariant: never mutate past context).
  - we inherit the queue's de-dup, crash-recovery checkpoint, and the
    existing CLI + gateway drain wiring for free — no new drain loops in the
    two largest files in the repo.

The completion payload carries a RICH, self-contained task-source block (the
original goal, the context the parent supplied, toolsets, model, dispatch
time, status, and the full result summary). When the result re-enters the
conversation the parent may be deep in unrelated context and won't remember
why the subagent existed; the block lets it either use the result or
re-dispatch if the world has moved on.

This module owns ONLY the async lifecycle. The actual child build + run is
delegated back to ``delegate_tool._run_single_child`` via an injected
runner, so all the credential leasing, heartbeat, timeout, and result-shaping
logic stays in one place.
"""

from __future__ import annotations

import json
import logging
import hashlib
import re
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional

from hermes_constants import get_hermes_home
from tools.daemon_pool import DaemonThreadPoolExecutor
from tools.thread_context import propagate_context_to_thread

logger = logging.getLogger(__name__)

# Back-compat alias — the daemon executor now lives in tools.daemon_pool so
# other subsystems (tool_executor, memory_manager, delegate_tool, skills_hub)
# can share it. Existing imports of ``_DaemonThreadPoolExecutor`` keep working.
_DaemonThreadPoolExecutor = DaemonThreadPoolExecutor


# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
# A persistent daemon executor (NOT a `with ThreadPoolExecutor()` block, which
# would join on exit and defeat the whole point of async). Workers are daemon
# threads so a hard process exit doesn't hang on an in-flight child.
_executor: Optional[ThreadPoolExecutor] = None
_executor_lock = threading.Lock()
_executor_max_workers: int = 0

_records_lock = threading.Lock()
# delegation_id -> record dict. Kept for the lifetime of the run plus a short
# tail after completion so `list_async_delegations()` can show recent results.
_records: Dict[str, Dict[str, Any]] = {}

_DEFAULT_MAX_ASYNC_CHILDREN = 3
# How many completed records to retain for status queries before pruning.
_MAX_RETAINED_COMPLETED = 50
_DURABLE_RETENTION_SECONDS = 7 * 24 * 60 * 60
_MAX_DURABLE_PENDING = 1000
_DB_LOCK = threading.Lock()
_CONVERSATION_RE = re.compile(r"conversation_[0-9a-f]{32}\Z")
_RUN_RE = re.compile(r"run_[0-9a-f]{32}\Z")
_SESSION_RE = re.compile(r"anton-chat-[0-9a-f]{32}\Z")
_TARGET_RE = re.compile(r"anton:conversation_[0-9a-f]{32}\Z")
_DELIVERY_RE = re.compile(r"delivery_[0-9a-f]{32}\Z")
_PRIVATE_RE = re.compile(r"\[private\].*?\[/private\]", re.I | re.S)
_URL_RE = re.compile(r"(?:https?|ftp)://\S+|\b\S+://\S+", re.I)
_ABSOLUTE_PATH_RE = re.compile(r"(?<!\w)/(?:[^\s/]+(?:/[^\s/]+)*)")
_DELEGATION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\Z")
_SECRET_RE = re.compile(r"(?i)\b(?:sk|token|secret|password|api[_-]?key)[=_:-]?[A-Za-z0-9_./+=-]{8,}\b")
_TRACEBACK_RE = re.compile(r"(?is)traceback\s*\(most recent call last\):.*")
_MAX_HANDOFF_CLAIM_LIMIT = 100


def _db_path():
    return get_hermes_home() / "state.db"


def _connect() -> sqlite3.Connection:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS async_delegations (
            delegation_id TEXT PRIMARY KEY,
            origin_session TEXT NOT NULL,
            origin_ui_session_id TEXT NOT NULL DEFAULT '',
            parent_session_id TEXT,
            state TEXT NOT NULL,
            dispatched_at REAL NOT NULL,
            completed_at REAL,
            updated_at REAL NOT NULL,
            event_json TEXT,
            result_json TEXT,
            delivery_state TEXT NOT NULL DEFAULT 'pending',
            delivery_attempts INTEGER NOT NULL DEFAULT 0,
            delivered_at REAL,
            owner_pid INTEGER,
            owner_started_at INTEGER,
            task_json TEXT,
            delivery_claim TEXT,
            delivery_claimed_at REAL,
            delivery_kind TEXT NOT NULL DEFAULT 'session',
            anton_route_json TEXT,
            delivery_id TEXT,
            anton_body_json TEXT,
            anton_body_sha256 TEXT,
            handoff_state TEXT,
            handoff_claim_token TEXT,
            handoff_claim_version INTEGER NOT NULL DEFAULT 0,
            handoff_claim_expires_at REAL,
            handoff_attempts INTEGER NOT NULL DEFAULT 0,
            handoff_updated_at REAL
        )"""
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(async_delegations)")}
    for name, sql_type in (
        ("owner_pid", "INTEGER"),
        ("owner_started_at", "INTEGER"),
        ("task_json", "TEXT"),
        ("delivery_claim", "TEXT"),
        ("delivery_claimed_at", "REAL"),
        ("delivery_kind", "TEXT NOT NULL DEFAULT 'session'"),
        ("anton_route_json", "TEXT"),
        ("delivery_id", "TEXT"),
        ("anton_body_json", "TEXT"),
        ("anton_body_sha256", "TEXT"),
        ("handoff_state", "TEXT"),
        ("handoff_claim_token", "TEXT"),
        ("handoff_claim_version", "INTEGER NOT NULL DEFAULT 0"),
        ("handoff_claim_expires_at", "REAL"),
        ("handoff_attempts", "INTEGER NOT NULL DEFAULT 0"),
        ("handoff_updated_at", "REAL"),
    ):
        if name not in columns:
            conn.execute(f"ALTER TABLE async_delegations ADD COLUMN {name} {sql_type}")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_async_delegations_delivery_id ON async_delegations(delivery_id) WHERE delivery_id IS NOT NULL")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_async_delegations_anton_handoff_due ON async_delegations(handoff_state, handoff_claim_expires_at, completed_at) WHERE delivery_kind='anton' AND handoff_state IN ('pending','claimed')")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_async_delegations_anton_terminal ON async_delegations(completed_at) WHERE delivery_kind='anton'")
    return conn


def _trusted_anton_route() -> Optional[Dict[str, str]]:
    """Capture only the authenticated, request-scoped v2 route at dispatch."""
    try:
        from hermes_plugins.anton_gateway.origin_context import get
        origin = get()
    except Exception:
        return None
    if origin is None:
        return None
    value = lambda snake, camel: getattr(origin, snake, None) if not isinstance(origin, dict) else origin.get(camel, origin.get(snake))
    protocol = value("protocol", "protocol")
    mode = value("mode", "mode")
    # A trusted v2-shaped origin must never silently degrade to a session route.
    is_v2_claim = protocol == "anton.delegation.v1" or mode == "parent_continuation"
    if not is_v2_claim:
        return None
    conversation = value("origin_conversation_id", "originConversationId")
    target = value("delivery_target", "deliveryTarget")
    parent_run = value("parent_run_id", "parentRunId")
    parent_session = value("hermes_session_id", "parentSessionId")
    if not (
        protocol == "anton.delegation.v1" and mode == "parent_continuation"
        and isinstance(conversation, str) and _CONVERSATION_RE.fullmatch(conversation)
        and isinstance(target, str) and _TARGET_RE.fullmatch(target)
        and target == f"anton:{conversation}"
        and isinstance(parent_run, str) and _RUN_RE.fullmatch(parent_run)
        and isinstance(parent_session, str) and _SESSION_RE.fullmatch(parent_session)
    ):
        raise ValueError("authenticated ANTON delegation origin is incomplete or invalid")
    return {"protocol": protocol, "deliveryTarget": target, "originConversationId": conversation,
            "parentRunId": parent_run, "parentSessionId": parent_session}


def _canonical_json(value: Dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _safe_summary(result: Dict[str, Any]) -> str:
    """Return a bounded forced-redacted terminal projection, never raw errors."""
    raw = result.get("summary")
    if not isinstance(raw, str) and isinstance(result.get("results"), list):
        # Combined batch objects intentionally have no top-level summary. Keep
        # child order deterministic and never substitute raw child errors.
        raw = "; ".join(
            child["summary"] for child in result["results"][:32]
            if isinstance(child, dict) and isinstance(child.get("summary"), str)
        )
    if not isinstance(raw, str) or not raw:
        return "Delegated work reached a terminal state."
    try:
        from agent.redact import redact_sensitive_text
        text = redact_sensitive_text(raw, force=True)
        # The generic redactor intentionally preserves URLs and paths. This
        # private callback projection is stricter by contract.
        text = _PRIVATE_RE.sub("[redacted]", text)
        text = _TRACEBACK_RE.sub("[redacted]", text)
        text = _SECRET_RE.sub("[redacted]", text)
        text = _URL_RE.sub("[redacted]", text)
        text = _ABSOLUTE_PATH_RE.sub("[redacted]", text)
        text = " ".join(text.split())
        if not text or "traceback" in text.lower():
            raise ValueError("unsafe summary")
    except Exception:
        return "Delegated work reached a terminal state."
    encoded = text.encode("utf-8")[:2048]
    while encoded:
        try:
            return encoded.decode("utf-8")
        except UnicodeDecodeError:
            encoded = encoded[:-1]
    return "Delegated work reached a terminal state."


def _valid_rfc3339_millis(value: Any) -> bool:
    if not isinstance(value, str) or not _TIMESTAMP_RE.fullmatch(value):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return False
    return True


def _valid_anton_handoff(route_json: Any, delivery_id: Any, body: Any, digest: Any) -> bool:
    """Validate persisted terminal material before it can be leased.

    A corrupt oldest candidate fails the whole ordered claim batch closed; this
    avoids silently reordering a mixed good/corrupt durable handoff queue.
    """
    if not isinstance(route_json, str) or not isinstance(delivery_id, str) or not _DELIVERY_RE.fullmatch(delivery_id):
        return False
    if not isinstance(body, str) or not isinstance(digest, str):
        return False
    encoded = body.encode("utf-8")
    if len(encoded) > 65536 or hashlib.sha256(encoded).hexdigest() != digest:
        return False
    try:
        route, parsed = json.loads(route_json), json.loads(body)
    except (TypeError, ValueError):
        return False
    required_route = {"protocol", "deliveryTarget", "originConversationId", "parentRunId", "parentSessionId"}
    required_body = {"version", "deliveryId", "delegationId", "parentRunId", "parentSessionId", "deliveryTarget", "status", "occurredAt", "completion"}
    required_completion = {"summary", "completedChildren", "failedChildren", "interruptedChildren", "unknownChildren"}
    if not isinstance(route, dict) or not isinstance(parsed, dict) or set(route) != required_route or set(parsed) != required_body:
        return False
    completion = parsed.get("completion")
    if not isinstance(completion, dict) or set(completion) != required_completion:
        return False
    counts = ("completedChildren", "failedChildren", "interruptedChildren", "unknownChildren")
    return (
        route["protocol"] == "anton.delegation.v1"
        and isinstance(route["originConversationId"], str) and _CONVERSATION_RE.fullmatch(route["originConversationId"])
        and isinstance(route["deliveryTarget"], str) and _TARGET_RE.fullmatch(route["deliveryTarget"])
        and route["deliveryTarget"] == f"anton:{route['originConversationId']}"
        and isinstance(route["parentRunId"], str) and _RUN_RE.fullmatch(route["parentRunId"])
        and isinstance(route["parentSessionId"], str) and _SESSION_RE.fullmatch(route["parentSessionId"])
        and parsed["version"] == "hermes-delegation-completion-v1"
        and isinstance(parsed["delegationId"], str) and _DELEGATION_RE.fullmatch(parsed["delegationId"])
        and parsed["deliveryId"] == delivery_id
        and parsed["parentRunId"] == route["parentRunId"]
        and parsed["parentSessionId"] == route["parentSessionId"]
        and parsed["deliveryTarget"] == route["deliveryTarget"]
        and _valid_rfc3339_millis(parsed["occurredAt"])
        and parsed["status"] in {"completed", "failed", "interrupted", "unknown"}
        and isinstance(completion["summary"], str) and len(completion["summary"].encode("utf-8")) <= 2048
        and all(isinstance(completion[key], int) and 0 <= completion[key] <= 10000 for key in counts)
        and 1 <= sum(completion[key] for key in counts) <= 10000
    )


def _anton_body(record: Dict[str, Any], result: Dict[str, Any], status: str, occurred_at: float) -> str:
    results = result.get("results") if isinstance(result.get("results"), list) else [result]
    counts = {"completed": 0, "failed": 0, "interrupted": 0, "unknown": 0}
    for child in results:
        child_status = str((child or {}).get("status") or status).lower()
        key = "completed" if child_status in {"completed", "success"} else child_status
        counts[key if key in counts else "failed"] += 1
    if sum(counts.values()) < 1:
        counts["failed"] = 1
    route = record["anton_route"]
    body = {
        "version": "hermes-delegation-completion-v1", "deliveryId": record["delivery_id"],
        "delegationId": record["delegation_id"], "parentRunId": route["parentRunId"],
        "parentSessionId": route["parentSessionId"], "deliveryTarget": route["deliveryTarget"],
        "status": status if status in counts else "failed",
        "occurredAt": datetime.fromtimestamp(occurred_at, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "completion": {"summary": _safe_summary(result), "completedChildren": counts["completed"],
                       "failedChildren": counts["failed"], "interruptedChildren": counts["interrupted"],
                       "unknownChildren": counts["unknown"]},
    }
    return _canonical_json(body)


def _persist_dispatch(record: Dict[str, Any]) -> None:
    now = time.time()
    try:
        from gateway.status import get_process_start_time
        owner_started_at = get_process_start_time(__import__("os").getpid())
    except Exception:
        owner_started_at = None
    task_payload = {
        key: record.get(key)
        for key in ("goal", "goals", "context", "toolsets", "role", "model", "is_batch")
        if key in record
    }
    with _DB_LOCK, _connect() as conn:
        conn.execute(
            """INSERT INTO async_delegations
               (delegation_id, origin_session, origin_ui_session_id,
                parent_session_id, state, dispatched_at, updated_at,
                delivery_state, delivery_attempts, owner_pid, owner_started_at,
                task_json, delivery_kind, anton_route_json, delivery_id,
                handoff_state, handoff_updated_at)
               VALUES (?, ?, ?, ?, 'running', ?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (record["delegation_id"], record.get("session_key", ""),
             record.get("origin_ui_session_id", ""), record.get("parent_session_id"),
             record["dispatched_at"], now, __import__("os").getpid(), owner_started_at,
             json.dumps(task_payload), record.get("delivery_kind", "session"),
             _canonical_json(record["anton_route"]) if record.get("anton_route") else None,
             record.get("delivery_id"), None, now),
        )
    _prune_durable_records()


def _delete_durable_delegation(delegation_id: str) -> None:
    with _DB_LOCK, _connect() as conn:
        conn.execute("DELETE FROM async_delegations WHERE delegation_id=?", (delegation_id,))


def _prune_durable_records() -> None:
    """Bound terminal history, preferring delivered records for deletion."""
    now = time.time()
    cutoff = now - _DURABLE_RETENTION_SECONDS
    with _DB_LOCK, _connect() as conn:
        conn.execute(
            "DELETE FROM async_delegations WHERE delivery_state='delivered' AND updated_at < ? AND (delivery_kind!='anton' OR handoff_state='completed')",
            (cutoff,),
        )
        terminal_count = conn.execute(
            """SELECT COUNT(*) FROM async_delegations
               WHERE state NOT IN ('running','finalizing')
                 AND (delivery_kind!='anton' OR handoff_state='completed')"""
        ).fetchone()[0]
        excess = max(0, terminal_count - _MAX_RETAINED_COMPLETED)
        if excess:
            conn.execute(
                """DELETE FROM async_delegations WHERE delegation_id IN (
                     SELECT delegation_id FROM async_delegations
                     WHERE state NOT IN ('running','finalizing')
                       AND (delivery_kind!='anton' OR handoff_state='completed')
                     ORDER BY CASE delivery_state WHEN 'delivered' THEN 0 ELSE 1 END,
                              updated_at ASC LIMIT ?
                   )""",
                (excess,),
            )
        pending_count = conn.execute(
            """SELECT COUNT(*) FROM async_delegations
               WHERE state NOT IN ('running','finalizing') AND delivery_state='pending'
                 AND delivery_kind!='anton'"""
        ).fetchone()[0]
        overflow = max(0, pending_count - _MAX_DURABLE_PENDING)
        if overflow:
            conn.execute(
                """DELETE FROM async_delegations WHERE delegation_id IN (
                     SELECT delegation_id FROM async_delegations
                     WHERE state NOT IN ('running','finalizing') AND delivery_state='pending'
                       AND delivery_kind!='anton'
                     ORDER BY updated_at ASC LIMIT ?
                   )""",
                (overflow,),
            )


def _persist_completion(event: Dict[str, Any], result: Dict[str, Any], record: Optional[Dict[str, Any]] = None) -> None:
    now = time.time()
    anton = bool(record and record.get("delivery_kind") == "anton")
    terminal_status = event.get("status", "completed")
    if anton and terminal_status not in {"completed", "failed", "interrupted", "unknown"}:
        terminal_status = "failed"
        event = dict(event)
        event["status"] = terminal_status
    body = _anton_body(record or {}, result, terminal_status, event.get("completed_at", now)) if anton else None
    with _DB_LOCK, _connect() as conn:
        conn.execute(
            """UPDATE async_delegations SET state=?, completed_at=?, updated_at=?,
               event_json=?, result_json=?, delivery_state='pending', anton_body_json=?,
               anton_body_sha256=?, handoff_state=?, handoff_updated_at=?
 WHERE delegation_id=? AND state IN ('running','finalizing')""",
            (event.get("status", "completed"), event.get("completed_at", now), now,
             json.dumps(event), json.dumps(result), body,
             hashlib.sha256(body.encode("utf-8")).hexdigest() if body else None,
             "pending" if anton else None, now if anton else None, event["delegation_id"]),
        )


def _note_delivery_attempt(delegation_id: str) -> None:
    with _DB_LOCK, _connect() as conn:
        conn.execute(
            "UPDATE async_delegations SET delivery_attempts=delivery_attempts+1, updated_at=? WHERE delegation_id=?",
            (time.time(), delegation_id),
        )


def recover_abandoned_delegations() -> int:
    """Classify records whose owning process disappeared as outcome unknown."""
    try:
        from gateway.status import _pid_exists, get_process_start_time
    except Exception:
        return 0
    now = time.time()
    recovered = 0
    with _DB_LOCK, _connect() as conn:
        rows = conn.execute(
            """SELECT delegation_id, origin_session, origin_ui_session_id,
                      parent_session_id, dispatched_at, owner_pid,
                      owner_started_at, task_json, delivery_kind, anton_route_json, delivery_id
               FROM async_delegations WHERE state IN ('running','finalizing')"""
        ).fetchall()
        for row in rows:
            delegation_id, session_key, origin_ui, parent_id, dispatched_at, pid, started, task_json, delivery_kind, route_json, delivery_id = row
            live = False
            if pid:
                live = _pid_exists(int(pid))
                if live and started is not None:
                    live = get_process_start_time(int(pid)) == int(started)
            if live:
                continue
            task = json.loads(task_json or "{}")
            event = {
                "type": "async_delegation", "delegation_id": delegation_id,
                "session_key": session_key, "origin_ui_session_id": origin_ui,
                "parent_session_id": parent_id, "goal": task.get("goal", ""),
                "goals": task.get("goals"), "context": task.get("context"),
                "toolsets": task.get("toolsets"), "role": task.get("role"),
                "model": task.get("model"), "is_batch": bool(task.get("is_batch")),
                "status": "unknown", "summary": None,
                "error": "Delegation owner exited before recording a terminal result; outcome unknown.",
                "dispatched_at": dispatched_at, "completed_at": now,
            }
            result = {"status": "unknown", "summary": None, "error": event["error"]}
            if delivery_kind == "anton":
                route = json.loads(route_json)
                record = {"delegation_id": delegation_id, "delivery_kind": "anton", "anton_route": route, "delivery_id": delivery_id}
                body = _anton_body(record, result, "unknown", now)
                conn.execute(
                    """UPDATE async_delegations SET state='unknown', completed_at=?, updated_at=?,
                       event_json=?, result_json=?, delivery_state='pending', anton_body_json=?, anton_body_sha256=?,
                       handoff_state='pending', handoff_updated_at=? WHERE delegation_id=?""",
                    (now, now, json.dumps(event), json.dumps(result), body, hashlib.sha256(body.encode("utf-8")).hexdigest(), now, delegation_id),
                )
            else:
                conn.execute(
                """UPDATE async_delegations SET state='unknown', completed_at=?,
                   updated_at=?, event_json=?, result_json=?, delivery_state='pending'
                   WHERE delegation_id=?""",
                (now, now, json.dumps(event), json.dumps(result), delegation_id),
                )
            recovered += 1
    return recovered


def restore_undelivered_completions(target_queue) -> int:
    """Enqueue durable pending completions as fresh turns after process start.

    Every restored event is stamped ``restored=True`` (in-memory only — the
    stamp is added after the durable payload is deserialized and is never
    persisted). Restored events originate from a *previous* process, so no
    consumer in THIS process implicitly owns them: drain paths that run
    without an ownership filter (the legacy single-session behavior) must
    leave them queued for a consumer that can positively prove ownership,
    otherwise a brand-new session adopts a dead session's delegation
    results seconds after boot (#64484).
    """
    recover_abandoned_delegations()
    with _DB_LOCK, _connect() as conn:
        rows = conn.execute(
            """SELECT delegation_id, event_json FROM async_delegations
               WHERE state != 'running' AND delivery_state='pending' AND event_json IS NOT NULL
                 AND delivery_kind!='anton'
               ORDER BY completed_at, delegation_id"""
        ).fetchall()
        for _delegation_id, payload in rows:
            evt = json.loads(payload)
            if isinstance(evt, dict):
                evt["restored"] = True
            target_queue.put(evt)
    return len(rows)


def claim_anton_handoffs(limit: int = 1, lease_seconds: float = 60.0) -> List[Dict[str, Any]]:
    """Fence due, structurally valid ANTON terminal projections for handoff.

    The transaction only mutates after every selected row validates. A corrupt
    ordered candidate therefore returns ``[]`` and leaves all candidates
    pending, rather than handing out a surprising partial batch.
    """
    try:
        requested = int(limit)
    except (TypeError, ValueError):
        return []
    if requested <= 0:
        return []
    requested = min(requested, _MAX_HANDOFF_CLAIM_LIMIT)
    now = time.time()
    claimed: List[Dict[str, Any]] = []
    with _DB_LOCK, _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            """SELECT delegation_id, anton_route_json, delivery_id, anton_body_json,
                      anton_body_sha256, handoff_claim_version
               FROM async_delegations WHERE delivery_kind='anton'
                 AND state NOT IN ('running','finalizing')
                 AND (handoff_state='pending' OR (handoff_state='claimed' AND handoff_claim_expires_at <= ?))
               ORDER BY completed_at, delegation_id LIMIT ?""", (now, requested)
        ).fetchall()
        if any(not _valid_anton_handoff(route, delivery_id, body, digest) for _, route, delivery_id, body, digest, _ in rows):
            conn.rollback()
            return []
        for delegation_id, route, delivery_id, body, digest, version in rows:
            token = uuid.uuid4().hex
            next_version = int(version or 0) + 1
            cur = conn.execute(
                """UPDATE async_delegations SET handoff_state='claimed', handoff_claim_token=?,
                   handoff_claim_version=?, handoff_claim_expires_at=?, handoff_attempts=handoff_attempts+1,
                   handoff_updated_at=? WHERE delegation_id=? AND delivery_kind='anton'
                   AND (handoff_state='pending' OR (handoff_state='claimed' AND handoff_claim_expires_at <= ?))""",
                (token, next_version, now + max(1.0, float(lease_seconds)), now, delegation_id, now),
            )
            if cur.rowcount != 1:
                conn.rollback()
                return []
            claimed.append({"delegation_id": delegation_id, "route": json.loads(route),
                            "delivery_id": delivery_id, "body_json": body, "body_sha256": digest,
                            "claim_token": token, "claim_version": next_version})
    return claimed


def release_anton_handoff(delegation_id: str, claim_token: str, claim_version: int) -> bool:
    with _DB_LOCK, _connect() as conn:
        cur = conn.execute("""UPDATE async_delegations SET handoff_state='pending', handoff_claim_token=NULL,
            handoff_claim_expires_at=NULL, handoff_updated_at=? WHERE delegation_id=? AND handoff_state='claimed'
            AND handoff_claim_token=? AND handoff_claim_version=?""", (time.time(), delegation_id, claim_token, claim_version))
        return cur.rowcount == 1


def mark_anton_handoff_completed(delegation_id: str, claim_token: str, claim_version: int) -> bool:
    """Acknowledge durable outbox copy, not network delivery."""
    with _DB_LOCK, _connect() as conn:
        now = time.time()
        cur = conn.execute("""UPDATE async_delegations SET handoff_state='completed', handoff_claim_token=NULL,
            handoff_claim_expires_at=NULL, handoff_updated_at=? WHERE delegation_id=? AND handoff_state='claimed'
            AND handoff_claim_token=? AND handoff_claim_version=?""", (now, delegation_id, claim_token, claim_version))
        return cur.rowcount == 1


def mark_completion_delivered(delegation_id: str) -> bool:
    """Atomically acknowledge a generic session delivery, never an ANTON handoff."""
    now = time.time()
    with _DB_LOCK, _connect() as conn:
        cur = conn.execute(
            """UPDATE async_delegations SET delivery_state='delivered', delivered_at=?, updated_at=?
               WHERE delegation_id=? AND delivery_kind!='anton' AND delivery_state!='delivered'""",
            (now, now, delegation_id),
        )
        return cur.rowcount == 1


def claim_completion_delivery(delegation_id: str, claim_id: str) -> bool:
    """Claim one pending completion across competing consumers/processes."""
    now = time.time()
    with _DB_LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT delivery_state, delivery_kind FROM async_delegations WHERE delegation_id=?",
            (delegation_id,),
        ).fetchone()
        if row is None:
            return True  # legacy event created before durable dispatch
        if row[1] == "anton":
            return False
        cur = conn.execute(
            """UPDATE async_delegations SET delivery_claim=?, delivery_claimed_at=?,
                      delivery_attempts=delivery_attempts+1, updated_at=?
               WHERE delegation_id=? AND delivery_state='pending'
                 AND (delivery_claim IS NULL OR delivery_claimed_at < ?)""",
            (claim_id, now, now, delegation_id, now - 300),
        )
        return cur.rowcount == 1


def claim_event_delivery(evt: Dict[str, Any], consumer: str) -> Optional[str]:
    """Claim a durable delegation event; non-durable events need no token."""
    if evt.get("type") != "async_delegation":
        return ""
    delegation_id = str(evt.get("delegation_id") or "")
    if not delegation_id:
        return ""
    claim_id = f"{consumer}:{__import__('os').getpid()}:{uuid.uuid4().hex}"
    return claim_id if claim_completion_delivery(delegation_id, claim_id) else None


def release_completion_delivery(delegation_id: str, claim_id: str) -> bool:
    """Release a failed delivery claim so another consumer may retry."""
    with _DB_LOCK, _connect() as conn:
        cur = conn.execute(
            """UPDATE async_delegations SET delivery_claim=NULL,
                      delivery_claimed_at=NULL, updated_at=?
               WHERE delegation_id=? AND delivery_kind!='anton' AND delivery_state='pending'
                 AND delivery_claim=?""",
            (time.time(), delegation_id, claim_id),
        )
        return cur.rowcount == 1


def complete_completion_delivery(delegation_id: str, claim_id: str) -> bool:
    """Acknowledge a generic session consumer claim, never an ANTON handoff."""
    now = time.time()
    with _DB_LOCK, _connect() as conn:
        cur = conn.execute(
            """UPDATE async_delegations SET delivery_state='delivered',
                      delivered_at=?, updated_at=?, delivery_claim=NULL,
                      delivery_claimed_at=NULL
               WHERE delegation_id=? AND delivery_kind!='anton' AND delivery_state='pending'
                 AND delivery_claim=?""",
            (now, now, delegation_id, claim_id),
        )
        return cur.rowcount == 1


def complete_event_delivery(evt: Dict[str, Any], claim_id: str) -> None:
    if claim_id and evt.get("type") == "async_delegation":
        complete_completion_delivery(str(evt.get("delegation_id") or ""), claim_id)


def release_event_delivery(evt: Dict[str, Any], claim_id: str) -> None:
    if claim_id and evt.get("type") == "async_delegation":
        release_completion_delivery(str(evt.get("delegation_id") or ""), claim_id)


def get_durable_delegation(delegation_id: str) -> Optional[Dict[str, Any]]:
    with _DB_LOCK, _connect() as conn:
        row = conn.execute(
            """SELECT origin_session, state, dispatched_at, completed_at,
                      result_json, delivery_state, delivery_attempts, delivery_kind,
                      anton_route_json, delivery_id, anton_body_json, anton_body_sha256,
                      handoff_state, handoff_claim_version, handoff_attempts
               FROM async_delegations WHERE delegation_id=?""", (delegation_id,),
        ).fetchone()
    if row is None:
        return None
    return {
        "delegation_id": delegation_id, "origin_session": row[0], "state": row[1],
        "dispatched_at": row[2], "completed_at": row[3],
        "result": json.loads(row[4]) if row[4] else None,
        "delivery_state": row[5], "delivery_attempts": row[6],
        "delivery_kind": row[7], "anton_route": json.loads(row[8]) if row[8] else None,
        "delivery_id": row[9], "anton_body_json": row[10], "anton_body_sha256": row[11],
        "handoff_state": row[12], "handoff_claim_version": row[13], "handoff_attempts": row[14],
    }


def _get_executor(max_workers: int) -> ThreadPoolExecutor:
    """Lazily create (or grow) the shared daemon executor.

    We never shrink — ThreadPoolExecutor can't resize — but if the configured
    cap grows between calls we rebuild a larger pool. Existing in-flight
    futures keep running on the old pool until it's garbage collected.
    """
    global _executor, _executor_max_workers
    with _executor_lock:
        if _executor is None or max_workers > _executor_max_workers:
            # Daemon threads: thread_name_prefix aids debugging in stack dumps.
            _executor = _DaemonThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="async-delegate",
            )
            _executor_max_workers = max_workers
        return _executor


def active_count() -> int:
    """Number of async delegations currently running."""
    with _records_lock:
        return sum(1 for r in _records.values() if r.get("status") in {"running", "finalizing"})


def _new_delegation_id() -> str:
    # Full entropy avoids accidental collisions across durable process restarts.
    return f"deleg_{uuid.uuid4().hex}"


def _prune_completed_locked() -> None:
    """Drop the oldest completed records beyond the retention cap.

    Caller must hold ``_records_lock``.
    """
    completed = [
        (rid, r)
        for rid, r in _records.items()
        if r.get("status") != "running"
    ]
    if len(completed) <= _MAX_RETAINED_COMPLETED:
        return
    # Oldest-first by completion time (fall back to dispatch time).
    completed.sort(key=lambda kv: kv[1].get("completed_at") or kv[1].get("dispatched_at") or 0)
    for rid, _ in completed[: len(completed) - _MAX_RETAINED_COMPLETED]:
        _records.pop(rid, None)


def dispatch_async_delegation(
    *,
    goal: str,
    context: Optional[str],
    toolsets: Optional[List[str]],
    role: str,
    model: Optional[str],
    session_key: str,
    parent_session_id: Optional[str] = None,
    runner: Callable[[], Dict[str, Any]],
    origin_ui_session_id: str = "",
    interrupt_fn: Optional[Callable[[], None]] = None,
    max_async_children: int = _DEFAULT_MAX_ASYNC_CHILDREN,
) -> Dict[str, Any]:
    """Spawn ``runner`` on the daemon executor and return a handle immediately.

    Parameters
    ----------
    goal, context, toolsets, role, model
        The dispatch-time task spec, captured verbatim for the rich
        completion block.
    session_key
        The gateway session_key (from ``tools.approval.get_current_session_key``)
        captured on the parent thread BEFORE dispatch, because the daemon
        worker thread won't carry the contextvar. Used to route the
        completion back to the originating session.
    parent_session_id
        The durable ``state.db`` session id of the parent agent that spawned
        the delegation. Carried on the completion event so the gateway can
        pin routing to the spawning session instead of recovering the latest
        ``ended_at IS NULL`` row for the peer tuple (#57498).
    runner
        Zero-arg callable that builds + runs the child and returns the same
        result dict ``_run_single_child`` produces. Runs on the worker thread.
    interrupt_fn
        Optional callable to signal the child to stop (used on shutdown /
        explicit cancel).
    max_async_children
        Concurrency cap. When at capacity the dispatch is REJECTED (the caller
        should fall back to sync or tell the user) rather than queued, so a
        runaway model can't pile up unbounded background work.

    Returns
    -------
    dict
        ``{"status": "dispatched", "delegation_id": ...}`` on success, or
        ``{"status": "rejected", "error": ...}`` when at capacity.
    """
    anton_route = _trusted_anton_route()
    # Decouple durable routing from a mutable request context/dataclass.
    anton_route = dict(anton_route) if anton_route else None
    delegation_id = _new_delegation_id()
    dispatched_at = time.time()
    record: Dict[str, Any] = {
        "delegation_id": delegation_id,
        "goal": goal,
        "context": context,
        "toolsets": list(toolsets) if toolsets else None,
        "role": role,
        "model": model,
        "session_key": session_key,
        "origin_ui_session_id": origin_ui_session_id,
        "parent_session_id": parent_session_id,
        "status": "running",
        "dispatched_at": dispatched_at,
        "completed_at": None,
        "interrupt_fn": interrupt_fn,
        "delivery_kind": "anton" if anton_route else "session",
        "anton_route": anton_route,
        "delivery_id": f"delivery_{uuid.uuid4().hex}" if anton_route else None,
    }
    # Capacity check and record insert under ONE lock hold — checking
    # active_count() separately would let two concurrent dispatches (e.g.
    # from different gateway sessions) both pass the check and exceed the cap.
    with _records_lock:
        running = sum(
            1 for r in _records.values() if r.get("status") == "running"
        )
        if running >= max_async_children:
            return {
                "status": "rejected",
                "error": (
                    f"Async delegation capacity reached ({max_async_children} "
                    f"running). Wait for one to finish (its result will re-enter "
                    f"the chat), or run this task synchronously "
                    f"(background=false). Raise delegation.max_concurrent_children in "
                    f"config.yaml to allow more concurrent background subagents."
                ),
            }
        _records[delegation_id] = record

    _persist_dispatch(record)
    executor = _get_executor(max_async_children)

    def _worker() -> None:
        result: Dict[str, Any] = {}
        status = "error"
        try:
            result = runner() or {}
            status = result.get("status") or "completed"
        except Exception as exc:  # noqa: BLE001 — must never crash the worker
            if record.get("delivery_kind") == "anton":
                logger.error("ANTON async delegation %s crashed", delegation_id)
            else:
                logger.exception("Async delegation %s crashed", delegation_id)
            result = {
                "status": "error",
                "summary": None,
                "error": f"{type(exc).__name__}: {exc}",
                "api_calls": 0,
                "duration_seconds": round(time.time() - dispatched_at, 2),
            }
            status = "error"
        finally:
            _finalize(delegation_id, result, status)

    try:
        # Propagate the dispatching profile so the detached child resolves
        # get_hermes_home() under the right profile.
        executor.submit(propagate_context_to_thread(_worker))
    except Exception as exc:  # pragma: no cover — pool submit failure is rare
        with _records_lock:
            _records.pop(delegation_id, None)
        _delete_durable_delegation(delegation_id)
        return {
            "status": "rejected",
            "error": f"Failed to schedule async delegation: {exc}",
        }

    if anton_route:
        logger.info("Dispatched ANTON async delegation %s", delegation_id)
    else:
        logger.info(
            "Dispatched async delegation %s (session_key=%s): %s",
            delegation_id, session_key or "<cli>", (goal or "")[:80],
        )
    return {"status": "dispatched", "delegation_id": delegation_id}


def _finalize(delegation_id: str, result: Dict[str, Any], status: str) -> None:
    """Mark a record complete and push the completion event onto the queue."""
    with _records_lock:
        record = _records.get(delegation_id)
        if record is None:
            return
        # Stay active until durable persistence and queue publication finish;
        # otherwise process shutdown can kill this daemon worker in the narrow
        # gap after status flips but before SQLite is committed.
        record["status"] = "finalizing"
        record["completed_at"] = time.time()
        record["interrupt_fn"] = None  # drop the closure; child is done
        event_record = dict(record)

    _push_completion_event(event_record, result, status)
    with _records_lock:
        record = _records.get(delegation_id)
        if record is not None:
            record["status"] = status
        _prune_completed_locked()


def _push_completion_event(
    record: Dict[str, Any], result: Dict[str, Any], status: str
) -> None:
    """Push a type='async_delegation' event onto the shared completion queue.

    Best-effort: a failure here must not crash the worker, but it WOULD mean a
    silently-lost result, so we log loudly.
    """
    summary = result.get("summary")
    error = result.get("error")
    dispatched_at = record.get("dispatched_at") or time.time()
    completed_at = record.get("completed_at") or time.time()

    evt = {
        "type": "async_delegation",
        "delegation_id": record.get("delegation_id"),
        # session_key routes the completion back to the originating gateway
        # session; empty string => CLI (single-session) path.
        "session_key": record.get("session_key", ""),
        "origin_ui_session_id": record.get("origin_ui_session_id", ""),
        "parent_session_id": record.get("parent_session_id"),
        "goal": record.get("goal", ""),
        "context": record.get("context"),
        "toolsets": record.get("toolsets"),
        "role": record.get("role"),
        "model": result.get("model") or record.get("model"),
        "status": status,
        "summary": summary,
        "error": error,
        "api_calls": result.get("api_calls", 0),
        "duration_seconds": result.get(
            "duration_seconds", round(completed_at - dispatched_at, 2)
        ),
        "dispatched_at": dispatched_at,
        "completed_at": completed_at,
        "exit_reason": result.get("exit_reason"),
    }
    _persist_completion(evt, result, record)
    if record.get("delivery_kind") == "anton":
        return
    try:
        from tools.process_registry import process_registry
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Async delegation %s finished but process_registry import failed; "
            "result lost: %s",
            record.get("delegation_id"), exc,
        )
        return
    try:
        process_registry.completion_queue.put(evt)
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Async delegation %s: failed to enqueue completion event; "
            "result lost: %s",
            record.get("delegation_id"), exc,
        )


def dispatch_async_delegation_batch(
    *,
    goals: List[str],
    context: Optional[str],
    toolsets: Optional[List[str]],
    role: str,
    model: Optional[str],
    session_key: str,
    parent_session_id: Optional[str] = None,
    runner: Callable[[], Dict[str, Any]],
    origin_ui_session_id: str = "",
    interrupt_fn: Optional[Callable[[], None]] = None,
    max_async_children: int = _DEFAULT_MAX_ASYNC_CHILDREN,
) -> Dict[str, Any]:
    """Dispatch a WHOLE fan-out batch as ONE background unit.

    Unlike ``dispatch_async_delegation`` (which backs a single subagent),
    ``runner`` here runs the entire batch — it builds and joins on every child
    in parallel and returns the combined ``{"results": [...],
    "total_duration_seconds": N}`` dict that the synchronous path would have
    returned. We occupy ONE async slot for the whole batch (the in-batch
    parallelism is bounded separately by ``max_concurrent_children``), so a
    single ``delegate_task`` fan-out never exhausts the async pool by itself.

    When the batch finishes, a SINGLE completion event is pushed onto the
    shared ``process_registry.completion_queue`` carrying the full per-task
    ``results`` list, so the consolidated summaries re-enter the conversation
    as one message once every child is done — the chat is never blocked while
    they run.

    Returns ``{"status": "dispatched", "delegation_id": ...}`` on success or
    ``{"status": "rejected", "error": ...}`` when the async pool is at
    capacity.
    """
    anton_route = _trusted_anton_route()
    # Decouple durable routing from a mutable request context/dataclass.
    anton_route = dict(anton_route) if anton_route else None
    delegation_id = _new_delegation_id()
    dispatched_at = time.time()
    n = len(goals)
    # A combined goal label for status listings / the completion header.
    combined_goal = (
        goals[0] if n == 1 else f"{n} parallel subagents: " + "; ".join(g[:40] for g in goals)
    )
    record: Dict[str, Any] = {
        "delegation_id": delegation_id,
        "goal": combined_goal,
        "goals": list(goals),
        "context": context,
        "toolsets": list(toolsets) if toolsets else None,
        "role": role,
        "model": model,
        "session_key": session_key,
        "origin_ui_session_id": origin_ui_session_id,
        "parent_session_id": parent_session_id,
        "status": "running",
        "dispatched_at": dispatched_at,
        "completed_at": None,
        "interrupt_fn": interrupt_fn,
        "is_batch": True,
        "delivery_kind": "anton" if anton_route else "session",
        "anton_route": anton_route,
        "delivery_id": f"delivery_{uuid.uuid4().hex}" if anton_route else None,
    }
    with _records_lock:
        running = sum(
            1 for r in _records.values() if r.get("status") == "running"
        )
        if running >= max_async_children:
            return {
                "status": "rejected",
                "error": (
                    f"Async delegation capacity reached ({max_async_children} "
                    f"running). Wait for one to finish (its result will re-enter "
                    f"the chat), or raise delegation.max_concurrent_children in "
                    f"config.yaml to allow more concurrent background units."
                ),
            }
        _records[delegation_id] = record

    _persist_dispatch(record)
    executor = _get_executor(max_async_children)

    def _worker() -> None:
        combined: Dict[str, Any] = {}
        status = "error"
        try:
            combined = runner() or {}
            # Batch status: completed unless every child errored/was interrupted.
            child_results = combined.get("results") or []
            if child_results and all(
                (r.get("status") not in ("completed", "success"))
                for r in child_results
            ):
                status = "error"
            else:
                status = "completed"
        except Exception as exc:  # noqa: BLE001 — must never crash the worker
            if record.get("delivery_kind") == "anton":
                logger.error("ANTON async delegation batch %s crashed", delegation_id)
            else:
                logger.exception("Async delegation batch %s crashed", delegation_id)
            combined = {
                "results": [],
                "error": f"{type(exc).__name__}: {exc}",
                "total_duration_seconds": round(time.time() - dispatched_at, 2),
            }
            status = "error"
        finally:
            _finalize_batch(delegation_id, combined, status)

    try:
        # Propagate the dispatching profile to the detached batch children.
        executor.submit(propagate_context_to_thread(_worker))
    except Exception as exc:  # pragma: no cover
        with _records_lock:
            _records.pop(delegation_id, None)
        _delete_durable_delegation(delegation_id)
        return {
            "status": "rejected",
            "error": f"Failed to schedule async delegation batch: {exc}",
        }

    if anton_route:
        logger.info("Dispatched ANTON async delegation batch %s", delegation_id)
    else:
        logger.info(
            "Dispatched async delegation batch %s (%d task(s), session_key=%s)",
            delegation_id, n, session_key or "<cli>",
        )
    return {"status": "dispatched", "delegation_id": delegation_id}


def _finalize_batch(
    delegation_id: str, combined: Dict[str, Any], status: str
) -> None:
    """Mark a batch record complete and push ONE combined completion event."""
    with _records_lock:
        record = _records.get(delegation_id)
        if record is None:
            return
        record["status"] = "finalizing"
        record["completed_at"] = time.time()
        record["interrupt_fn"] = None
        event_record = dict(record)

    dispatched_at = event_record.get("dispatched_at") or time.time()
    completed_at = event_record.get("completed_at") or time.time()
    evt = {
        "type": "async_delegation",
        "delegation_id": delegation_id,
        "session_key": event_record.get("session_key", ""),
        "origin_ui_session_id": event_record.get("origin_ui_session_id", ""),
        "parent_session_id": event_record.get("parent_session_id"),
        "goal": event_record.get("goal", ""),
        "goals": event_record.get("goals"),
        "context": event_record.get("context"),
        "toolsets": event_record.get("toolsets"),
        "role": event_record.get("role"),
        "model": event_record.get("model"),
        "status": status,
        "is_batch": True,
        # The full per-task results list — the formatter renders a
        # consolidated multi-task block from this.
        "results": combined.get("results") or [],
        "error": combined.get("error"),
        "total_duration_seconds": combined.get("total_duration_seconds"),
        "dispatched_at": dispatched_at,
        "completed_at": completed_at,
    }
    _persist_completion(evt, combined, event_record)
    if event_record.get("delivery_kind") == "anton":
        with _records_lock:
            record = _records.get(delegation_id)
            if record is not None:
                record["status"] = status
            _prune_completed_locked()
        return
    try:
        from tools.process_registry import process_registry
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Async delegation batch %s finished but process_registry import failed; result lost: %s",
            delegation_id, exc,
        )
        return
    try:
        process_registry.completion_queue.put(evt)
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Async delegation batch %s: failed to enqueue completion event; "
            "result lost: %s",
            delegation_id, exc,
        )
    finally:
        with _records_lock:
            record = _records.get(delegation_id)
            if record is not None:
                record["status"] = status
            _prune_completed_locked()


def list_async_delegations() -> List[Dict[str, Any]]:
    """Snapshot of async delegations (running + recently completed).

    Safe to call from any thread. Excludes the non-serialisable interrupt_fn.
    """
    with _records_lock:
        return [
            {k: v for k, v in r.items() if k != "interrupt_fn"}
            for r in _records.values()
        ]


def interrupt_all(reason: str = "shutdown") -> int:
    """Signal every running async delegation to stop. Returns how many.

    Used on ``/stop`` and gateway shutdown so a dangling background subagent
    can't keep burning tokens with no one listening. The child still emits a
    completion event (status='interrupted') via the normal finalize path.
    """
    count = 0
    with _records_lock:
        targets = [
            r for r in _records.values() if r.get("status") == "running"
        ]
    for r in targets:
        fn = r.get("interrupt_fn")
        if callable(fn):
            try:
                fn()
                count += 1
            except Exception as exc:
                logger.debug(
                    "interrupt_all: %s interrupt failed: %s",
                    r.get("delegation_id"), exc,
                )
    if count:
        logger.info("Interrupted %d async delegation(s) (%s)", count, reason)
    return count


def interrupt_for_session(
    session_key: str = "",
    origin_ui_session_id: str = "",
    parent_session_id: str = "",
    reason: str = "session_end",
) -> int:
    """Signal running async delegations owned by ONE session to stop.

    A delegation's lifecycle is bound to the session that spawned it: when
    that session ends, its in-flight background subagents must end with it —
    a completed orphan would otherwise sit on the shared completion queue
    with no live owner, either leaking into another chat or burning tokens
    with no one listening (#55578).

    Selectors (any matching field claims the record):
    - ``origin_ui_session_id``: the live TUI tab/window that commissioned it.
    - ``session_key``: the durable routing key captured at dispatch.
    - ``parent_session_id``: the spawning agent's durable session-db id —
      the right selector for gateway chats, whose ``session_key`` (the
      platform conversation key) SURVIVES a ``/new`` reset while the
      session id rotates.

    Returns how many were interrupted.
    """
    if not session_key and not origin_ui_session_id and not parent_session_id:
        return 0
    count = 0
    with _records_lock:
        targets = [
            r for r in _records.values()
            if r.get("status") == "running"
            and (
                (origin_ui_session_id and str(r.get("origin_ui_session_id") or "") == origin_ui_session_id)
                or (session_key and str(r.get("session_key") or "") == session_key)
                or (parent_session_id and str(r.get("parent_session_id") or "") == parent_session_id)
            )
        ]
    for r in targets:
        fn = r.get("interrupt_fn")
        if callable(fn):
            try:
                fn()
                count += 1
            except Exception as exc:
                logger.debug(
                    "interrupt_for_session: %s interrupt failed: %s",
                    r.get("delegation_id"), exc,
                )
    if count:
        logger.info(
            "Interrupted %d async delegation(s) for ending session (%s)",
            count, reason,
        )
    return count


def _reset_for_tests() -> None:
    """Test-only: clear all state and tear down the executor."""
    global _executor, _executor_max_workers
    with _executor_lock:
        if _executor is not None:
            _executor.shutdown(wait=False)
        _executor = None
        _executor_max_workers = 0
    with _records_lock:
        _records.clear()
