from __future__ import annotations
import fcntl
import json
import os
import tempfile
import uuid
import hashlib
import sqlite3
import time
from typing import Any
from datetime import datetime, timedelta, timezone
from pathlib import Path

from hermes_constants import get_hermes_home


class DelegationOutboxIntegrityError(RuntimeError):
    """A delivery ID was reused with a materially different immutable record."""


class DelegationOutbox:
    """SQLite-backed, fenced outbox for detached delegation completions.

    This intentionally does not share the cron JSON outbox: its records are
    protocol bytes, not cron messages, and its lease state must survive process
    races independently of scheduler delivery.
    """

    MIN_LEASE_SECONDS = 180.0
    MAX_ATTEMPTS = 20
    MAX_CLAIM_LIMIT = 50

    def __init__(self, path: Path | None = None, *, clock=None, random_fn=None):
        self.path = path or get_hermes_home() / "state.db"
        self.clock = clock or time.time
        self.random = random_fn or __import__("random").random
        self._migrate()

    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _migrate(self):
        with self._connect() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS anton_delegation_outbox (
                delivery_id TEXT PRIMARY KEY,
                route_json TEXT NOT NULL,
                body BLOB NOT NULL,
                body_sha256 TEXT NOT NULL,
                signing_key_id TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL CHECK(state IN ('pending','claimed','delivered','dead_letter')),
                lease_token TEXT, lease_version INTEGER NOT NULL DEFAULT 0,
                lease_expires_at REAL, attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL, error_code TEXT, error_status INTEGER,
                ack_json TEXT, outcome TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL,
                delivered_at REAL
            )""")
            # Additive migration for databases created before key generations
            # were pinned. Empty legacy values deliberately fail closed on send.
            columns = {row[1] for row in conn.execute("PRAGMA table_info(anton_delegation_outbox)")}
            if "signing_key_id" not in columns:
                conn.execute("ALTER TABLE anton_delegation_outbox ADD COLUMN signing_key_id TEXT NOT NULL DEFAULT ''")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_anton_delegation_outbox_due ON anton_delegation_outbox(state, next_attempt_at) WHERE state='pending'")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_anton_delegation_outbox_lease ON anton_delegation_outbox(state, lease_expires_at) WHERE state='claimed'")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_anton_delegation_outbox_retention ON anton_delegation_outbox(state, delivered_at) WHERE state IN ('delivered','dead_letter')")

    @staticmethod
    def _route(route: Any) -> str:
        return json.dumps(route, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    @staticmethod
    def _row(row):
        if row is None:
            return None
        keys = ("deliveryId","routeJson","body","bodySha256","signingKeyId","state","leaseToken","leaseVersion","leaseExpiresAt","attempts","nextAttemptAt","errorCode","errorStatus","ackJson","outcome","createdAt","updatedAt","deliveredAt")
        return dict(zip(keys, row))

    def create_or_confirm(self, delivery_id: str, route: Any, body: bytes | str, body_sha256: str, signing_key_id: str = "") -> dict:
        if isinstance(body, str): body = body.encode("utf-8")
        if not isinstance(body, bytes) or len(body) > 65536 or hashlib.sha256(body).hexdigest() != body_sha256:
            raise DelegationOutboxIntegrityError("invalid immutable delegation record")
        route_json, now = self._route(route), float(self.clock())
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT route_json, body, body_sha256, signing_key_id FROM anton_delegation_outbox WHERE delivery_id=?", (delivery_id,)).fetchone()
            if row is None:
                conn.execute("INSERT INTO anton_delegation_outbox (delivery_id,route_json,body,body_sha256,signing_key_id,state,next_attempt_at,created_at,updated_at) VALUES (?,?,?,?,?, 'pending',?,?,?)", (delivery_id, route_json, body, body_sha256, signing_key_id, now, now, now))
            # The signing generation is immutable once selected.  A retry
            # after rotation confirms the prior saved generation instead of
            # conflicting or silently changing the signature identity.
            elif row[:3] != (route_json, body, body_sha256):
                conn.rollback()
                raise DelegationOutboxIntegrityError("immutable delegation delivery conflict")
            saved = conn.execute("SELECT delivery_id,route_json,body,body_sha256,signing_key_id,state,lease_token,lease_version,lease_expires_at,attempts,next_attempt_at,error_code,error_status,ack_json,outcome,created_at,updated_at,delivered_at FROM anton_delegation_outbox WHERE delivery_id=?", (delivery_id,)).fetchone()
            return self._row(saved)

    def due(self, limit: int = 10, lease_seconds: float = MIN_LEASE_SECONDS, *, signing_key_ids=None) -> list[dict]:
        try: limit = min(max(0, int(limit)), self.MAX_CLAIM_LIMIT)
        except (TypeError, ValueError): return []
        if not limit: return []
        # Callback key readiness is a pre-claim boundary.  In particular, a
        # retired/legacy generation must remain pending without consuming an
        # attempt or being dead-lettered merely because it cannot be signed.
        key_ids = None
        if signing_key_ids is not None:
            key_ids = tuple(sorted({key_id for key_id in signing_key_ids if isinstance(key_id, str) and key_id}))
            if not key_ids:
                return []
        now, lease_seconds = float(self.clock()), max(float(lease_seconds), self.MIN_LEASE_SECONDS)
        result = []
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            key_filter = ""
            key_params: tuple = ()
            if key_ids is not None:
                key_filter = " AND signing_key_id IN (" + ",".join("?" for _ in key_ids) + ")"
                key_params = key_ids
            # Consume exhausted due work before selecting ownership.  This fences
            # a crashed twentieth sender: once its lease expires it cannot cause
            # a twenty-first HTTP attempt.
            conn.execute(
                "UPDATE anton_delegation_outbox SET state='dead_letter', lease_token=NULL, "
                "lease_expires_at=NULL, error_code='attempts_exhausted', updated_at=? "
                "WHERE attempts>=? AND ((state='pending' AND next_attempt_at<=?) "
                "OR (state='claimed' AND lease_expires_at<=?))" + key_filter,
                (now, self.MAX_ATTEMPTS, now, now) + key_params,
            )
            rows = conn.execute(
                "SELECT delivery_id FROM anton_delegation_outbox WHERE attempts<? AND "
                "((state='pending' AND next_attempt_at<=?) OR (state='claimed' AND lease_expires_at<=?)) "
                + key_filter + " ORDER BY next_attempt_at, created_at LIMIT ?",
                (self.MAX_ATTEMPTS, now, now) + key_params + (limit,),
            ).fetchall()
            for (delivery_id,) in rows:
                token = uuid.uuid4().hex
                conn.execute("UPDATE anton_delegation_outbox SET state='claimed', lease_token=?, lease_version=lease_version+1, lease_expires_at=?, attempts=attempts+1, updated_at=? WHERE delivery_id=? AND attempts<? AND (state='pending' OR (state='claimed' AND lease_expires_at<=?))", (token, now + lease_seconds, now, delivery_id, self.MAX_ATTEMPTS, now))
                row = conn.execute("SELECT delivery_id,route_json,body,body_sha256,signing_key_id,state,lease_token,lease_version,lease_expires_at,attempts,next_attempt_at,error_code,error_status,ack_json,outcome,created_at,updated_at,delivered_at FROM anton_delegation_outbox WHERE delivery_id=?", (delivery_id,)).fetchone()
                result.append(self._row(row))
        return result

    def _settle(self, delivery_id, token, version, *, state, error_code=None, error_status=None, ack=None, outcome=None, next_attempt_at=None):
        now = float(self.clock())
        with self._connect() as conn:
            cur = conn.execute("UPDATE anton_delegation_outbox SET state=?, lease_token=NULL, lease_expires_at=NULL, error_code=?, error_status=?, ack_json=?, outcome=?, next_attempt_at=COALESCE(?,next_attempt_at), updated_at=?, delivered_at=CASE WHEN ?='delivered' THEN ? ELSE delivered_at END WHERE delivery_id=? AND state='claimed' AND lease_token=? AND lease_version=?", (state, error_code, error_status, json.dumps(ack, sort_keys=True, separators=(",", ":")) if ack else None, outcome, next_attempt_at, now, state, now, delivery_id, token, version))
            return cur.rowcount == 1

    def delivered(self, delivery_id, token, version, ack, outcome):
        return self._settle(delivery_id, token, version, state="delivered", ack=ack, outcome=outcome)

    def dead_letter(self, delivery_id, token, version, code, status=None):
        return self._settle(delivery_id, token, version, state="dead_letter", error_code=code, error_status=status)

    def retry(self, delivery_id, token, version, code, status=None, retry_after=None):
        # attempts was consumed at claim time, exactly once per HTTP ownership.
        row = self.get(delivery_id)
        if row is None or row["attempts"] >= self.MAX_ATTEMPTS:
            return self.dead_letter(delivery_id, token, version, code, status)
        cap = min(900.0, 5.0 * (2 ** max(0, row["attempts"] - 1)))
        if isinstance(retry_after, (int, float)) and not isinstance(retry_after, bool) and retry_after >= 0:
            delay = min(900.0, float(retry_after))
        else:
            # Full bounded jitter with a nonzero five-second floor.  Attempt one
            # has cap=floor and is therefore exactly five seconds.
            jitter = min(1.0, max(0.0, float(self.random())))
            delay = 5.0 + jitter * (cap - 5.0)
        return self._settle(delivery_id, token, version, state="pending", error_code=code, error_status=status, next_attempt_at=float(self.clock()) + delay)

    def get(self, delivery_id: str):
        with self._connect() as conn:
            return self._row(conn.execute("SELECT delivery_id,route_json,body,body_sha256,signing_key_id,state,lease_token,lease_version,lease_expires_at,attempts,next_attempt_at,error_code,error_status,ack_json,outcome,created_at,updated_at,delivered_at FROM anton_delegation_outbox WHERE delivery_id=?", (delivery_id,)).fetchone())

    def ingest_handoffs(self, limit: int = 10) -> int:
        """Materialize Slice B handoffs before acknowledging their ledger claim."""
        from tools.async_delegation import claim_anton_handoffs, release_anton_handoff, mark_anton_handoff_completed
        created = 0
        for handoff in claim_anton_handoffs(limit=limit, lease_seconds=self.MIN_LEASE_SECONDS):
            try:
                from .config import AntonConfig
                key_id = AntonConfig.delegation_from_env().delegation_delivery_key_id
                self.create_or_confirm(handoff["delivery_id"], handoff["route"], handoff["body_json"], handoff["body_sha256"], key_id)
                if not mark_anton_handoff_completed(handoff["delegation_id"], handoff["claim_token"], handoff["claim_version"]):
                    raise RuntimeError("ledger acknowledgement lost")
                created += 1
            except Exception:
                release_anton_handoff(handoff["delegation_id"], handoff["claim_token"], handoff["claim_version"])
        return created


class AntonOutbox:
    """Durable ANTON delivery records with fenced, leased ownership.

    A lease token is a fencing token: only the claimant that received it can
    settle its in-flight attempt.  This prevents a stale sender from changing a
    record after another worker recovered and re-claimed it.
    """

    MIN_LEASE_SECONDS = 180  # longer than the 120s maximum transport timeout

    def __init__(self, path: Path | None = None):
        self.path = path or get_hermes_home() / "cron" / "anton_delivery_outbox.json"
        self.lock_path = self.path.with_suffix(".lock")

    def _locked(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock = open(self.lock_path, "a+")
        fcntl.flock(lock, fcntl.LOCK_EX)
        return lock

    def _read(self):
        try:
            data = json.loads(self.path.read_text())
            return data if isinstance(data, list) else []
        except FileNotFoundError:
            return []

    def _write(self, records):
        fd, name = tempfile.mkstemp(dir=self.path.parent, prefix=".anton-outbox-")
        try:
            with os.fdopen(fd, "w") as stream:
                json.dump(records, stream, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, self.path)
            # Persist the rename too; a power loss must not turn a claimed
            # record back into a pre-claim record.
            directory_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if os.path.exists(name):
                os.unlink(name)

    @staticmethod
    def _iso(now: datetime) -> str:
        return now.isoformat()

    def _claim(self, row: dict, now: datetime, lease_seconds: int) -> dict:
        lease_seconds = max(int(lease_seconds), self.MIN_LEASE_SECONDS)
        row["state"] = "in_flight"
        row["leaseVersion"] = int(row.get("leaseVersion", 0)) + 1
        row["leaseToken"] = uuid.uuid4().hex
        row["leaseExpiresAt"] = self._iso(now + timedelta(seconds=lease_seconds))
        return dict(row)

    def create(self, record: dict) -> dict:
        """Persist a pending record (used only when no immediate send follows)."""
        required = {"deliveryId", "scheduleId", "executionId", "deliveryTarget", "payload"}
        if required - record.keys():
            raise ValueError("incomplete ANTON outbox record")
        with self._locked():
            rows = self._read()
            if any(x.get("deliveryId") == record["deliveryId"] for x in rows):
                raise ValueError("duplicate deliveryId")
            item = {
                **record,
                "state": "pending",
                "attempts": 0,
                "nextAttemptAt": self._iso(datetime.now(timezone.utc)),
                "errorCode": None,
                "leaseExpiresAt": None,
                "leaseToken": None,
                "leaseVersion": 0,
            }
            rows.append(item)
            self._write(rows)
            return dict(item)

    def create_and_claim(self, record: dict, now: datetime | None = None, lease_seconds: int = MIN_LEASE_SECONDS) -> dict:
        """Atomically persist an initial delivery and fence its first send."""
        required = {"deliveryId", "scheduleId", "executionId", "deliveryTarget", "payload"}
        if required - record.keys():
            raise ValueError("incomplete ANTON outbox record")
        now = now or datetime.now(timezone.utc)
        with self._locked():
            rows = self._read()
            if any(x.get("deliveryId") == record["deliveryId"] for x in rows):
                raise ValueError("duplicate deliveryId")
            item = {
                **record,
                "state": "pending",
                "attempts": 0,
                "nextAttemptAt": self._iso(now),
                "errorCode": None,
                "leaseExpiresAt": None,
                "leaseToken": None,
                "leaseVersion": 0,
            }
            claimed = self._claim(item, now, lease_seconds)
            rows.append(item)
            self._write(rows)
            return claimed

    def transition(self, delivery_id: str, *, expected_token: str, **changes) -> dict | None:
        """Settle an owned in-flight record, never a delivered/newer lease."""
        with self._locked():
            rows = self._read()
            for row in rows:
                if row.get("deliveryId") != delivery_id:
                    continue
                if row.get("state") != "in_flight" or row.get("leaseToken") != expected_token:
                    return None
                # Payload and durable IDs are intentionally never transitionable.
                forbidden = {"payload", "deliveryId", "scheduleId", "executionId", "deliveryTarget"}
                if forbidden & changes.keys():
                    raise ValueError("immutable ANTON outbox fields cannot transition")
                row.update(changes)
                self._write(rows)
                return dict(row)
        raise KeyError(delivery_id)

    def due(self, now=None, lease_seconds=MIN_LEASE_SECONDS, limit=10):
        now = now or datetime.now(timezone.utc)
        with self._locked():
            rows, result, changed = self._read(), [], False
            for row in rows:
                if len(result) >= limit:
                    break
                lease = row.get("leaseExpiresAt")
                if row.get("state") == "in_flight" and lease:
                    try:
                        expired = datetime.fromisoformat(lease) <= now
                    except (TypeError, ValueError):
                        expired = True
                    if expired:
                        # Do not settle the old lease.  Claim directly with a
                        # new token so an old sender is fenced immediately.
                        self._claim(row, now, lease_seconds)
                        result.append(dict(row))
                        changed = True
                        continue
                if (len(result) < limit and row.get("state") == "pending"
                        and datetime.fromisoformat(row["nextAttemptAt"]) <= now):
                    result.append(self._claim(row, now, lease_seconds))
                    changed = True
            if changed:
                self._write(rows)
            return result[:limit]

    def retry_or_dead_letter(self, delivery_id: str, expected_token: str, error_code: str,
                             retryable: bool, max_attempts=5) -> dict | None:
        """Release/dead-letter only the matching current lease."""
        with self._locked():
            rows = self._read()
            for row in rows:
                if row.get("deliveryId") != delivery_id:
                    continue
                if row.get("state") != "in_flight" or row.get("leaseToken") != expected_token:
                    return None
                attempts = int(row.get("attempts", 0)) + 1
                row.update(attempts=attempts, errorCode=error_code, leaseExpiresAt=None, leaseToken=None)
                if not retryable or attempts >= max_attempts:
                    row["state"] = "dead_letter"
                else:
                    row["state"] = "pending"
                    row["nextAttemptAt"] = self._iso(
                        datetime.now(timezone.utc) + timedelta(seconds=min(300, 2 ** attempts))
                    )
                self._write(rows)
                return dict(row)
        raise KeyError(delivery_id)
