from __future__ import annotations
import fcntl
import hashlib
import json
import os
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from hermes_constants import get_hermes_home


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

    def create_or_confirm_same(self, record: dict) -> dict:
        """Crash-safe immutable handoff; duplicate IDs must carry identical bytes."""
        if not isinstance(record.get("deliveryId"), str) or not isinstance(record.get("payload"), dict):
            raise ValueError("incomplete ANTON outbox record")
        # Hash the exact canonical bytes the sender later signs and transmits.
        from .client import canonical_json
        payload_bytes = canonical_json(record["payload"])
        payload_hash = hashlib.sha256(payload_bytes).hexdigest()
        with self._locked():
            rows = self._read()
            for row in rows:
                if row.get("deliveryId") == record["deliveryId"]:
                    if row.get("payloadSha256") != payload_hash or row.get("payload") != record["payload"]:
                        raise ValueError("payload hash conflict")
                    return dict(row)
            item = {**record, "payloadSha256": payload_hash, "state": "pending", "attempts": 0,
                    "nextAttemptAt": self._iso(datetime.now(timezone.utc)), "errorCode": None,
                    "leaseExpiresAt": None, "leaseToken": None, "leaseVersion": 0}
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

    def transition(self, delivery_id: str, *, expected_token: str, expected_version: int | None = None, **changes) -> dict | None:
        """Settle an owned in-flight record, never a delivered/newer lease."""
        with self._locked():
            rows = self._read()
            for row in rows:
                if row.get("deliveryId") != delivery_id:
                    continue
                if row.get("state") != "in_flight" or row.get("leaseToken") != expected_token or (expected_version is not None and row.get("leaseVersion") != expected_version):
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
                             retryable: bool, max_attempts=5,
                             expected_version: int | None = None) -> dict | None:
        """Release/dead-letter only the matching current lease."""
        with self._locked():
            rows = self._read()
            for row in rows:
                if row.get("deliveryId") != delivery_id:
                    continue
                if (row.get("state") != "in_flight" or row.get("leaseToken") != expected_token
                        or (expected_version is not None and row.get("leaseVersion") != expected_version)):
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
