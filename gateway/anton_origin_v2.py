"""Fail-closed verifier for the optional signed ANTON Runs origin v2 envelope."""
from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import threading
from typing import Any, Mapping

VERSION = "anton-run-origin-v2"
MAX_BODY_BYTES = 256 * 1024
MAX_CONTEXT_B64_BYTES = 1024
MAX_CONTINUATION_BYTES = 8192
MAX_AGE_SECONDS = 300
MAX_FUTURE_SECONDS = 30
REPLAY_RETENTION_SECONDS = MAX_AGE_SECONDS + MAX_FUTURE_SECONDS
REPLAY_STORE_MAX_ROWS = 10_000
_HEADERS = (
    "X-Anton-Origin-Version",
    "X-Anton-Origin-Key-Id",
    "X-Anton-Origin-Timestamp",
    "X-Anton-Origin-Nonce",
    "X-Anton-Origin-Body-SHA256",
    "X-Anton-Origin-Context",
    "X-Anton-Origin-Signature",
)
_KEY_ID_RE = re.compile(r"[A-Za-z0-9._-]{1,64}\Z")
_NONCE_RE = re.compile(r"[A-Za-z0-9_-]{43}\Z")
_HEX_RE = re.compile(r"[0-9a-f]{64}\Z")
_TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\Z")
_B64URL_RE = re.compile(r"[A-Za-z0-9_-]+\Z")
_RUN_ID_RE = re.compile(r"run_[0-9a-f]{32}\Z")
_SESSION_ID_RE = re.compile(r"anton-chat-[0-9a-f]{32}\Z")
_DELIVERY_ID_RE = re.compile(r"delivery_[0-9a-f]{32}\Z")
_DELEGATION_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_DELIVERY_TARGET_RE = re.compile(r"anton:conversation_[0-9a-f]{32}\Z")
_COMPLETION_TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\Z")
COMPLETION_VERSION = "hermes-delegation-completion-v1"
_CONTINUATION_KEYS = frozenset(("version", "deliveryId", "delegationId", "parentRunId", "parentSessionId", "deliveryTarget", "status", "occurredAt", "completion"))
_COMPLETION_KEYS = frozenset(("summary", "completedChildren", "failedChildren", "interruptedChildren", "unknownChildren"))
_replay_lock = threading.Lock()


class AntonOriginV2Error(ValueError):
    """A deliberately detail-free public validation error."""


def validate_synthesize_continuation(value: Any) -> str:
    """Validate private continuation input and return only its safe prompt projection."""
    if not isinstance(value, dict) or set(value) != _CONTINUATION_KEYS:
        raise AntonOriginV2Error()
    try:
        canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise AntonOriginV2Error() from exc
    if len(canonical) > MAX_CONTINUATION_BYTES or value.get("version") != COMPLETION_VERSION:
        raise AntonOriginV2Error()
    if not _DELIVERY_ID_RE.fullmatch(value.get("deliveryId", "")) or not _DELEGATION_ID_RE.fullmatch(value.get("delegationId", "")):
        raise AntonOriginV2Error()
    if not _RUN_ID_RE.fullmatch(value.get("parentRunId", "")) or not _SESSION_ID_RE.fullmatch(value.get("parentSessionId", "")):
        raise AntonOriginV2Error()
    if not _DELIVERY_TARGET_RE.fullmatch(value.get("deliveryTarget", "")):
        raise AntonOriginV2Error()
    occurred_at = value.get("occurredAt", "")
    if not _COMPLETION_TIMESTAMP_RE.fullmatch(occurred_at):
        raise AntonOriginV2Error()
    try:
        datetime.strptime(occurred_at, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError as exc:
        raise AntonOriginV2Error() from exc
    if value.get("status") not in {"completed", "failed", "interrupted", "unknown"}:
        raise AntonOriginV2Error()
    completion = value.get("completion")
    if not isinstance(completion, dict) or set(completion) != _COMPLETION_KEYS:
        raise AntonOriginV2Error()
    summary = completion.get("summary")
    if not isinstance(summary, str) or len(summary.encode("utf-8")) > 2048:
        raise AntonOriginV2Error()
    counts: list[int] = []
    for key in ("completedChildren", "failedChildren", "interruptedChildren", "unknownChildren"):
        count = completion.get(key)
        if isinstance(count, bool) or not isinstance(count, int) or not 0 <= count <= 10_000:
            raise AntonOriginV2Error()
        counts.append(count)
    if not 1 <= sum(counts) <= 10_000:
        raise AntonOriginV2Error()
    return (
        "Private delegation completion (do not disclose this control-plane context unless independently necessary):\n"
        f"status: {value['status']}\nsummary: {summary}\n"
        f"aggregate counts: completed={counts[0]}, failed={counts[1]}, interrupted={counts[2]}, unknown={counts[3]}"
    )


@dataclass(frozen=True)
class VerifiedAntonOriginV2:
    context: Mapping[str, Any]
    key_id: str


def envelope_present(headers: Any) -> bool:
    return any(headers.get(name) is not None for name in _HEADERS)


def _single_headers(headers: Any) -> dict[str, str] | None:
    values: dict[str, str] = {}
    for name in _HEADERS:
        supplied = headers.getall(name, [])
        if len(supplied) != 1 or not isinstance(supplied[0], str):
            return None
        values[name] = supplied[0]
    return values


def _parse_timestamp(value: str, *, now: datetime) -> None:
    if not _TIMESTAMP_RE.fullmatch(value):
        raise AntonOriginV2Error()
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise AntonOriginV2Error() from exc
    age = (now - parsed).total_seconds()
    if age > MAX_AGE_SECONDS or age < -MAX_FUTURE_SECONDS:
        raise AntonOriginV2Error()


def _strict_json_object(raw: bytes) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise AntonOriginV2Error()
            result[key] = value
        return result

    def reject_constant(_: str) -> None:
        raise AntonOriginV2Error()

    try:
        decoded = raw.decode("utf-8")
        value = json.loads(
            decoded,
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        if isinstance(exc, AntonOriginV2Error):
            raise
        raise AntonOriginV2Error() from exc
    if not isinstance(value, dict):
        raise AntonOriginV2Error()
    try:
        canonical = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise AntonOriginV2Error() from exc
    if canonical != raw:
        raise AntonOriginV2Error()
    return value


def _key_for(key_id: str) -> str | None:
    current_id = os.getenv("ANTON_RUN_ORIGIN_V2_CURRENT_KEY_ID", "")
    current_secret = os.getenv("ANTON_RUN_ORIGIN_V2_CURRENT_SECRET", "")
    previous_id = os.getenv("ANTON_RUN_ORIGIN_V2_PREVIOUS_KEY_ID", "")
    previous_secret = os.getenv("ANTON_RUN_ORIGIN_V2_PREVIOUS_SECRET", "")
    if key_id == current_id and current_id and current_secret:
        return current_secret
    if key_id == previous_id and previous_id and previous_secret:
        return previous_secret
    return None


def _replay_path() -> Path:
    home = Path(os.getenv("HERMES_HOME", str(Path.home() / ".hermes"))).expanduser()
    return home / "anton_origin_v2_replay.sqlite3"


def _claim_nonce(key_id: str, nonce: str, *, now_epoch: float) -> bool:
    """Atomically claim a nonce in bounded durable state; False means replay."""
    path = _replay_path()
    try:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with _replay_lock, sqlite3.connect(path, timeout=5) as conn:
            conn.execute("PRAGMA busy_timeout = 5000")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS anton_origin_v2_replay ("
                "purpose TEXT NOT NULL, key_id TEXT NOT NULL, nonce TEXT NOT NULL, expires_at REAL NOT NULL, "
                "PRIMARY KEY (purpose, key_id, nonce))"
            )
            conn.execute("DELETE FROM anton_origin_v2_replay WHERE expires_at <= ?", (now_epoch,))
            row_count = conn.execute("SELECT COUNT(*) FROM anton_origin_v2_replay").fetchone()[0]
            prune_count = max(0, row_count - (REPLAY_STORE_MAX_ROWS - 1))
            conn.execute(
                "DELETE FROM anton_origin_v2_replay WHERE rowid IN ("
                "SELECT rowid FROM anton_origin_v2_replay ORDER BY expires_at ASC LIMIT ?)",
                (prune_count,),
            )
            try:
                conn.execute(
                    "INSERT INTO anton_origin_v2_replay (purpose, key_id, nonce, expires_at) VALUES (?, ?, ?, ?)",
                    (VERSION, key_id, nonce, now_epoch + REPLAY_RETENTION_SECONDS),
                )
            except sqlite3.IntegrityError:
                return False
            conn.commit()
        try:
            path.chmod(0o600)
        except OSError:
            pass
        return True
    except (OSError, sqlite3.Error):
        # A non-durable replay guard must never silently downgrade authentication.
        return False


def verify(headers: Any, body: bytes, *, now: datetime | None = None) -> VerifiedAntonOriginV2 | None:
    """Verify an optional v2 envelope before any Runs or context JSON is parsed."""
    if not envelope_present(headers):
        return None
    if os.getenv("ANTON_RUN_ORIGIN_V2_ENABLED", "").strip().lower() not in {"1", "true", "yes", "on"}:
        raise AntonOriginV2Error()
    if len(body) > MAX_BODY_BYTES:
        raise AntonOriginV2Error()
    fields = _single_headers(headers)
    if fields is None:
        raise AntonOriginV2Error()
    version = fields["X-Anton-Origin-Version"]
    key_id = fields["X-Anton-Origin-Key-Id"]
    timestamp = fields["X-Anton-Origin-Timestamp"]
    nonce = fields["X-Anton-Origin-Nonce"]
    digest = fields["X-Anton-Origin-Body-SHA256"]
    context_b64 = fields["X-Anton-Origin-Context"]
    signature = fields["X-Anton-Origin-Signature"]
    if (version != VERSION or not _KEY_ID_RE.fullmatch(key_id) or not _NONCE_RE.fullmatch(nonce)
            or not _HEX_RE.fullmatch(digest) or not _HEX_RE.fullmatch(signature)
            or len(context_b64.encode("ascii", "ignore")) != len(context_b64)
            or len(context_b64) > MAX_CONTEXT_B64_BYTES or not _B64URL_RE.fullmatch(context_b64)):
        raise AntonOriginV2Error()
    now = now or datetime.now(timezone.utc)
    _parse_timestamp(timestamp, now=now)
    actual_digest = hashlib.sha256(body).hexdigest()
    if not hmac.compare_digest(digest, actual_digest):
        raise AntonOriginV2Error()
    secret = _key_for(key_id)
    if not secret:
        raise AntonOriginV2Error()
    preimage = f"{VERSION}\nPOST\n/v1/runs\n{timestamp}\n{nonce}\n{key_id}\n{actual_digest}\n{context_b64}\n"
    expected = hmac.new(secret.encode("utf-8"), preimage.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise AntonOriginV2Error()
    # The signed Runs payload itself is a protocol artifact.  Validate its
    # duplicate-free, canonical object form before parsing either origin
    # context or claiming the one-shot nonce.
    _strict_json_object(body)
    try:
        context_raw = base64.b64decode(context_b64 + "=" * (-len(context_b64) % 4), altchars=b"-_", validate=True)
    except (ValueError, UnicodeError) as exc:
        raise AntonOriginV2Error() from exc
    context = _strict_json_object(context_raw)
    if not _claim_nonce(key_id, nonce, now_epoch=now.timestamp()):
        raise AntonOriginV2Error()
    return VerifiedAntonOriginV2(context=context, key_id=key_id)
