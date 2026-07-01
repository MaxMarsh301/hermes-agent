"""Matrix presence publishing tools.

Small, self-registering module for publishing a public-safe Matrix presence
status and keeping a local SQLite audit log under HERMES_HOME.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from hermes_constants import get_hermes_home
from tools.registry import registry

MATRIX_STATUS_DB = "matrix_presence_status.db"
MATRIX_LOG_ROOM_ENV_KEYS = (
    "MATRIX_PRESENCE_LOG_ROOM_ID",
    "MATRIX_PRESENCE_LOG_ROOM",
    "MATRIX_LOG_ROOM_ID",
)
VALID_STATES = {"online", "offline", "unavailable"}
MAX_STATUS_CHARS = 160


PRESENCE_PUBLISH_SCHEMA = {
    "name": "matrix_publish_presence_status",
    "description": (
        "Publish a concise, public-safe Matrix presence status and log the "
        "attempt locally. Set dry_run=true to validate and log without Matrix calls."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "state": {
                "type": "string",
                "enum": sorted(VALID_STATES),
                "description": "Matrix presence state.",
            },
            "status_msg": {
                "type": "string",
                "description": "One-line public status text; must not contain private data.",
            },
            "thoughts": {
                "type": "string",
                "description": "Private note stored only in the local log.",
                "default": "",
            },
            "context": {
                "type": "string",
                "description": "Private local context stored only in the local log.",
                "default": "",
            },
            "dry_run": {
                "type": "boolean",
                "description": "Validate and log locally without calling Matrix.",
                "default": False,
            },
        },
        "required": ["state", "status_msg"],
    },
}

PRESENCE_HISTORY_SCHEMA = {
    "name": "matrix_presence_status_history",
    "description": "Return recent Matrix presence status log rows from the local SQLite database.",
    "parameters": {
        "type": "object",
        "properties": {
            "limit": {
                "type": "integer",
                "description": "Maximum number of recent rows to return (1-100).",
                "default": 20,
            }
        },
        "required": [],
    },
}

RECENT_WORK_SESSIONS_SCHEMA = {
    "name": "matrix_recent_work_sessions",
    "description": (
        "Read the real Hermes session database and return compact, redacted "
        "recent-session context plus status history for Matrix presence generation."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "limit": {
                "type": "integer",
                "description": "Maximum number of recent work sessions to read (1-50).",
                "default": 10,
            },
            "lookback_hours": {
                "type": "integer",
                "description": "How far back to read sessions from state.db (1 hour to 30 days).",
                "default": 168,
            },
        },
        "required": [],
    },
}


_PRIVATE_PATTERNS = [
    # Local filesystem paths, including the explicit /home/anton case from tests.
    re.compile(r"/home/[A-Za-z0-9._-]+(?:/[^\s]*)?"),
    re.compile(r"/(?:Users|var|tmp|etc|opt|root)/[^\s]+"),
    # Matrix room IDs and MXIDs.
    re.compile(r"![A-Za-z0-9._=\-/]+:[A-Za-z0-9.-]+"),
    re.compile(r"@[A-Za-z0-9._=\-/]+:[A-Za-z0-9.-]+"),
    # URLs and obvious IP addresses.
    re.compile(r"https?://[^\s]+", re.IGNORECASE),
    re.compile(r"www\.[^\s]+", re.IGNORECASE),
    re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)(?::\d{1,5})?\b"),
    # Key/value secrets.
    re.compile(
        r"(?i)\b(?:api[_-]?key|access[_-]?token|auth[_-]?token|token|secret|password|passwd|bearer)\b\s*[:=]?\s*[^\s,;]+"
    ),
    # Common token prefixes and long high-entropy-looking strings.
    re.compile(r"\b(?:sk-[A-Za-z0-9_-]{12,}|gh[opsu]_[A-Za-z0-9_]{12,}|xox[baprs]-[A-Za-z0-9-]{12,})\b"),
    re.compile(r"\b[A-Za-z0-9_-]{32,}\b"),
]


def _json(data: dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False)


def _db_path() -> Path:
    return get_hermes_home() / MATRIX_STATUS_DB


def _connect() -> sqlite3.Connection:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS status_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at REAL NOT NULL,
            state TEXT NOT NULL,
            status_msg TEXT NOT NULL,
            thoughts TEXT NOT NULL DEFAULT '',
            context TEXT NOT NULL DEFAULT '',
            dry_run INTEGER NOT NULL DEFAULT 0,
            success INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            mirror_success INTEGER,
            mirror_message_id TEXT,
            mirror_error TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS presence_status_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at REAL NOT NULL,
            created_at_iso TEXT NOT NULL,
            session_id TEXT,
            source TEXT NOT NULL DEFAULT 'tool',
            state TEXT NOT NULL,
            status_msg TEXT NOT NULL,
            thoughts TEXT,
            context_json TEXT,
            published INTEGER NOT NULL DEFAULT 0,
            publish_error TEXT,
            status_sha256 TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def _insert_status_log(
    *,
    state: str,
    status_msg: str,
    thoughts: str,
    context: str,
    dry_run: bool,
    success: bool,
    error: str | None = None,
) -> int:
    with _connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO status_log
                (created_at, state, status_msg, thoughts, context, dry_run, success, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                time.time(),
                state,
                status_msg,
                thoughts or "",
                context or "",
                1 if dry_run else 0,
                1 if success else 0,
                error,
            ),
        )
        conn.commit()
        return int(cur.lastrowid or 0)


def _update_status_log(log_id: int, **fields: Any) -> None:
    if not fields:
        return
    allowed = {"success", "error", "mirror_success", "mirror_message_id", "mirror_error"}
    updates = {key: value for key, value in fields.items() if key in allowed}
    if not updates:
        return
    assignments = ", ".join(f"{key}=?" for key in updates)
    values = [updates[key] for key in updates]
    values.append(log_id)
    with _connect() as conn:
        conn.execute(f"UPDATE status_log SET {assignments} WHERE id=?", values)
        conn.commit()


def _insert_legacy_presence_log(
    *,
    state: str,
    status_msg: str,
    thoughts: str,
    context: str,
    dry_run: bool,
    published: bool,
    error: str | None = None,
) -> int:
    """Append to the historical table older rollout checks already inspect."""
    created_at = time.time()
    context_json = json.dumps(
        {"context": context or "", "dry_run": bool(dry_run)},
        ensure_ascii=False,
    )
    status_hash = hashlib.sha256(status_msg.encode("utf-8")).hexdigest()
    with _connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO presence_status_log
                (created_at, created_at_iso, session_id, source, state, status_msg,
                 thoughts, context_json, published, publish_error, status_sha256)
            VALUES (?, datetime(?, 'unixepoch'), ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                created_at,
                os.environ.get("HERMES_SESSION_ID") or None,
                "matrix_presence_tool",
                state,
                status_msg,
                thoughts or "",
                context_json,
                1 if published else 0,
                error,
                status_hash,
            ),
        )
        conn.commit()
        return int(cur.lastrowid or 0)


def _redact_private_text(text: str) -> str:
    redacted = text
    for pattern in _PRIVATE_PATTERNS:
        redacted = pattern.sub("[REDACTED]", redacted)
    return redacted


def _validate_status(state: str, status_msg: str) -> tuple[bool, str | None, str]:
    state = (state or "").strip().lower()
    status_msg = "" if status_msg is None else str(status_msg)
    rejected_preview = _redact_private_text(status_msg)[:MAX_STATUS_CHARS]

    if state not in VALID_STATES:
        return False, f"status_msg validation failed: invalid state {state!r}", rejected_preview
    if not status_msg.strip():
        return False, "status_msg validation failed: status_msg is required", rejected_preview
    if "\n" in status_msg or "\r" in status_msg:
        return False, "status_msg validation failed: status_msg must be one line", rejected_preview
    if len(status_msg) > MAX_STATUS_CHARS:
        return False, f"status_msg validation failed: status_msg exceeds {MAX_STATUS_CHARS} characters", rejected_preview
    if _redact_private_text(status_msg) != status_msg:
        return False, "status_msg validation failed: status_msg appears to contain private data", rejected_preview
    return True, None, rejected_preview


_TOPIC_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(?i)\b(restic|backup|б[эе]кап|snapshot|hdd|smart|retention)\b"), "резервные копии и ночная автоматика"),
    (re.compile(r"(?i)\b(vpn|proxy|pac|openvpn|tinyproxy|туннел|прокси)\b"), "сетевые туннели и маршруты"),
    (re.compile(r"(?i)\b(matrix|presence|status|статус|e2ee|megolm)\b"), "Matrix-присутствие и сигналы"),
    (re.compile(r"(?i)\b(dashboard|router|profile|модель|provider|token|лимит)\b"), "панель управления и модельный маршрутизатор"),
    (re.compile(r"(?i)\b(wiki|docs|документ|markdown|excel|svg|smart platform)\b"), "документация и аккуратная сборка знаний"),
    (re.compile(r"(?i)\b(image|nano|banana|картин|визуал|avatar|favicon)\b"), "визуальные эксперименты и маленькие образы"),
    (re.compile(r"(?i)\b(cron|systemd|service|timer|journalctl|daemon)\b"), "автономные сервисы и расписания"),
    (re.compile(r"(?i)\b(git|test|pytest|bug|fix|код|repo|commit)\b"), "код, тесты и ремонт механизмов"),
]


def _session_db_path() -> Path:
    return get_hermes_home() / "state.db"


def _clean_session_text(text: Any, max_chars: int = 360) -> str:
    if text is None:
        return ""
    value = str(text)
    if "[CONTEXT COMPACTION" in value:
        return ""
    value = re.sub(r"<tool_use\b.*?</tool_use>", " ", value, flags=re.IGNORECASE | re.DOTALL)
    value = re.sub(r"\s+", " ", value).strip()
    value = _redact_private_text(value)
    if len(value) > max_chars:
        value = value[: max_chars - 1].rstrip() + "…"
    return value


def _infer_session_topics(*parts: str) -> list[str]:
    blob = "\n".join(part for part in parts if part)
    topics: list[str] = []
    for pattern, topic in _TOPIC_RULES:
        if pattern.search(blob) and topic not in topics:
            topics.append(topic)
    return topics[:4]


def _approx_tokens(text: str) -> int:
    # Good enough for budgeting without adding tokenizer dependencies.
    return max(1, int(len(text) / 4)) if text else 0


def _read_recent_sessions_from_state_db(limit: int, lookback_hours: int) -> dict[str, Any]:
    db_path = _session_db_path()
    cutoff = time.time() - lookback_hours * 3600
    if not db_path.exists():
        return {
            "db_path": str(db_path),
            "sessions": [],
            "sessions_read": 0,
            "messages_read": 0,
            "topics": [],
            "error": "state.db not found",
        }

    sessions: list[dict[str, Any]] = []
    messages_read = 0
    topics: list[str] = []
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1.0)
        conn.row_factory = sqlite3.Row
        with conn:
            rows = conn.execute(
                """
                SELECT s.id, s.source, s.title, s.started_at, s.message_count,
                       COALESCE(MAX(m.timestamp), s.started_at) AS last_active
                FROM sessions s
                LEFT JOIN messages m ON m.session_id = s.id
                WHERE COALESCE(s.source, '') NOT IN ('cron', 'tool')
                  AND COALESCE(s.message_count, 0) > 0
                  AND COALESCE(s.archived, 0) = 0
                GROUP BY s.id
                HAVING last_active >= ?
                ORDER BY last_active DESC, s.started_at DESC
                LIMIT ?
                """,
                (cutoff, limit),
            ).fetchall()

            for row in rows:
                sid = str(row["id"])
                first_user_rows = conn.execute(
                    """
                    SELECT content FROM messages
                    WHERE session_id = ? AND role = 'user'
                      AND content IS NOT NULL AND TRIM(content) != ''
                    ORDER BY id ASC
                    LIMIT 2
                    """,
                    (sid,),
                ).fetchall()
                tail_rows = conn.execute(
                    """
                    SELECT role, content FROM messages
                    WHERE session_id = ? AND role IN ('user', 'assistant')
                      AND content IS NOT NULL AND TRIM(content) != ''
                    ORDER BY id DESC
                    LIMIT 5
                    """,
                    (sid,),
                ).fetchall()
                goal_parts = [_clean_session_text(r["content"], 280) for r in first_user_rows]
                tail_parts = [
                    {
                        "role": r["role"],
                        "text": _clean_session_text(r["content"], 320),
                    }
                    for r in reversed(tail_rows)
                ]
                goal_parts = [p for p in goal_parts if p]
                tail_parts = [p for p in tail_parts if p["text"]]
                messages_read += len(first_user_rows) + len(tail_rows)
                title = _clean_session_text(row["title"], 120)
                session_topics = _infer_session_topics(
                    title,
                    " ".join(goal_parts),
                    " ".join(p["text"] for p in tail_parts),
                )
                for topic in session_topics:
                    if topic not in topics:
                        topics.append(topic)
                sessions.append(
                    {
                        "session_id": sid,
                        "source": row["source"],
                        "title": title or None,
                        "started_at": row["started_at"],
                        "last_active": row["last_active"],
                        "message_count": row["message_count"],
                        "topics": session_topics,
                        "opening": goal_parts,
                        "recent_tail": tail_parts,
                    }
                )
    except Exception as exc:
        return {
            "db_path": str(db_path),
            "sessions": sessions,
            "sessions_read": len(sessions),
            "messages_read": messages_read,
            "topics": topics,
            "error": f"state.db read failed: {type(exc).__name__}: {exc}",
        }
    finally:
        try:
            conn.close()  # type: ignore[name-defined]
        except Exception:
            pass

    return {
        "db_path": str(db_path),
        "sessions": sessions,
        "sessions_read": len(sessions),
        "messages_read": messages_read,
        "topics": topics[:10],
    }


def _load_matrix_env_from_dotenv() -> None:
    """Hydrate Matrix env vars from HERMES_HOME/.env without exposing values."""
    env_path = get_hermes_home() / ".env"
    if not env_path.exists():
        return
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return
    wanted = {
        "MATRIX_HOMESERVER",
        "MATRIX_ACCESS_TOKEN",
        "MATRIX_USER_ID",
        "MATRIX_DEVICE_ID",
        *MATRIX_LOG_ROOM_ENV_KEYS,
    }
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key in wanted and not os.environ.get(key):
            os.environ[key] = value.strip().strip('"').strip("'")


def _matrix_credentials() -> tuple[str | None, str | None, str | None]:
    if not (os.environ.get("MATRIX_HOMESERVER") and os.environ.get("MATRIX_ACCESS_TOKEN") and os.environ.get("MATRIX_USER_ID")):
        _load_matrix_env_from_dotenv()
    homeserver = os.environ.get("MATRIX_HOMESERVER") or os.environ.get("HERMES_MATRIX_HOMESERVER")
    token = os.environ.get("MATRIX_ACCESS_TOKEN") or os.environ.get("HERMES_MATRIX_ACCESS_TOKEN")
    user_id = os.environ.get("MATRIX_USER_ID") or os.environ.get("HERMES_MATRIX_USER_ID")
    if homeserver:
        homeserver = homeserver.rstrip("/")
    return homeserver, token, user_id


def _matrix_request(method: str, path: str, payload: dict[str, Any], timeout: float = 10.0) -> dict[str, Any]:
    homeserver, token, _user_id = _matrix_credentials()
    if not homeserver or not token:
        return {"success": False, "error": "Matrix credentials are not configured"}

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        homeserver + path,
        data=body,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            data = json.loads(raw) if raw else {}
            return {"success": True, **data}
    except urllib.error.HTTPError as exc:
        # Do not include request headers or tokens in returned errors.
        return {"success": False, "error": f"Matrix API HTTP {exc.code}"}
    except Exception as exc:
        return {"success": False, "error": f"Matrix API request failed: {type(exc).__name__}: {exc}"}


def _set_matrix_presence(state: str, status_msg: str) -> dict[str, Any]:
    """Set Matrix presence. Kept as a small helper for tests to monkeypatch."""
    _homeserver, _token, user_id = _matrix_credentials()
    if not user_id:
        return {"success": False, "error": "Matrix user id is not configured"}
    encoded_user = urllib.parse.quote(user_id, safe="")
    result = _matrix_request(
        "PUT",
        f"/_matrix/client/v3/presence/{encoded_user}/status",
        {"presence": state, "status_msg": status_msg},
    )
    if result.get("success"):
        result.setdefault("user_id", user_id)
    return result


def _matrix_log_room_id() -> str | None:
    """Return configured Matrix room for presence mirrors, without hardcoding IDs."""
    if not any(os.environ.get(key) for key in MATRIX_LOG_ROOM_ENV_KEYS):
        _load_matrix_env_from_dotenv()
    for key in MATRIX_LOG_ROOM_ENV_KEYS:
        value = os.environ.get(key)
        if value and value.strip():
            return value.strip()
    return None


def _send_matrix_log_message(status_msg: str, state: str, log_id: int) -> dict[str, Any]:
    """Mirror a public status message into the configured Matrix log room."""
    raw_room_id = _matrix_log_room_id()
    if not raw_room_id:
        return {"success": False, "error": "Matrix presence log room is not configured"}
    txn_id = urllib.parse.quote(f"presence-{log_id}-{int(time.time() * 1000)}", safe="")
    room_id = urllib.parse.quote(raw_room_id, safe="")
    return _matrix_request(
        "PUT",
        f"/_matrix/client/v3/rooms/{room_id}/send/m.room.message/{txn_id}",
        {
            "msgtype": "m.text",
            "body": f"Статус изменён: {state}\nТекст статуса: {status_msg}",
        },
    )


def matrix_publish_presence_status(
    state: str,
    status_msg: str,
    thoughts: str = "",
    context: str = "",
    dry_run: bool = False,
) -> str:
    """Validate, optionally publish, and locally log a Matrix presence status."""
    normalized_state = (state or "").strip().lower()
    status_msg = "" if status_msg is None else str(status_msg).strip()
    thoughts = "" if thoughts is None else str(thoughts)
    context = "" if context is None else str(context)
    dry_run = bool(dry_run)

    ok, error, rejected_preview = _validate_status(normalized_state, status_msg)
    if not ok:
        return _json({"success": False, "error": error, "rejected_preview": rejected_preview})

    if dry_run:
        log_id = _insert_status_log(
            state=normalized_state,
            status_msg=status_msg,
            thoughts=thoughts,
            context=context,
            dry_run=True,
            success=True,
        )
        _insert_legacy_presence_log(
            state=normalized_state,
            status_msg=status_msg,
            thoughts=thoughts,
            context=context,
            dry_run=True,
            published=False,
        )
        return _json(
            {
                "success": True,
                "published": False,
                "dry_run": True,
                "state": normalized_state,
                "status_msg": status_msg,
                "log_id": log_id,
            }
        )

    log_id = _insert_status_log(
        state=normalized_state,
        status_msg=status_msg,
        thoughts=thoughts,
        context=context,
        dry_run=False,
        success=False,
    )
    presence = _set_matrix_presence(normalized_state, status_msg)
    if not presence.get("success"):
        error_text = str(presence.get("error") or "Matrix presence publish failed")
        _update_status_log(log_id, success=0, error=error_text)
        _insert_legacy_presence_log(
            state=normalized_state,
            status_msg=status_msg,
            thoughts=thoughts,
            context=context,
            dry_run=False,
            published=False,
            error=error_text,
        )
        return _json(
            {
                "success": False,
                "published": False,
                "dry_run": False,
                "state": normalized_state,
                "status_msg": status_msg,
                "log_id": log_id,
                "presence": presence,
                "error": error_text,
            }
        )

    mirror = _send_matrix_log_message(status_msg, normalized_state, log_id)
    _insert_legacy_presence_log(
        state=normalized_state,
        status_msg=status_msg,
        thoughts=thoughts,
        context=context,
        dry_run=False,
        published=True,
    )
    _update_status_log(
        log_id,
        success=1,
        error=None,
        mirror_success=1 if mirror.get("success") else 0,
        mirror_message_id=mirror.get("message_id") or mirror.get("event_id"),
        mirror_error=None if mirror.get("success") else str(mirror.get("error") or "Matrix mirror failed"),
    )
    return _json(
        {
            "success": True,
            "published": True,
            "dry_run": False,
            "state": normalized_state,
            "status_msg": status_msg,
            "log_id": log_id,
            "presence": presence,
            "mirror": mirror,
        }
    )


def matrix_presence_status_history(limit: int = 20) -> str:
    """Return recent local presence status log rows."""
    try:
        limit = max(1, min(100, int(limit)))
    except (TypeError, ValueError):
        limit = 20

    if not _db_path().exists():
        return _json({"success": True, "history": []})

    with _connect() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, created_at, state, status_msg, dry_run, success, error,
                   mirror_success, mirror_message_id, mirror_error
            FROM status_log
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return _json({"success": True, "history": [dict(row) for row in rows]})


def matrix_recent_work_sessions(limit: int = 10, lookback_hours: int = 168) -> str:
    """Read real recent Hermes sessions and return compact generation context.

    This intentionally reads `state.db` directly in read-only mode so cron runs can
    ground status generation in what was actually discussed without loading full
    transcripts or depending on the interactive `session_search` tool.
    """
    try:
        limit = max(1, min(50, int(limit)))
    except (TypeError, ValueError):
        limit = 10
    try:
        lookback_hours = max(1, min(24 * 30, int(lookback_hours)))
    except (TypeError, ValueError):
        lookback_hours = 168

    session_context = _read_recent_sessions_from_state_db(limit, lookback_hours)
    status_history = json.loads(matrix_presence_status_history(limit=12)).get("history", [])
    status_lines = [
        _redact_private_text(str(row.get("status_msg") or ""))
        for row in status_history[:5]
        if isinstance(row, dict) and row.get("status_msg")
    ]
    topic_text = ", ".join(session_context.get("topics") or []) or "нет устойчивых тем"
    if session_context.get("sessions_read"):
        brief = (
            f"Read {session_context['sessions_read']} real sessions / "
            f"{session_context['messages_read']} message snippets from the last "
            f"{lookback_hours}h. Themes: {topic_text}."
        )
    else:
        brief = f"No non-cron work sessions found in the last {lookback_hours}h."
    if status_lines:
        brief += " Recent statuses: " + "; ".join(status_lines[:3])

    payload = {
        "success": True,
        "lookback_hours": lookback_hours,
        "brief": brief,
        "sessions_read": session_context.get("sessions_read", 0),
        "messages_read": session_context.get("messages_read", 0),
        "topics": session_context.get("topics", []),
        "sessions": session_context.get("sessions", []),
        "status_history": status_history,
        "estimated_context_tokens": _approx_tokens(json.dumps(session_context, ensure_ascii=False)),
    }
    if session_context.get("error"):
        payload["warning"] = session_context["error"]
    return _json(payload)


# --- Registry ---
registry.register(
    name="matrix_recent_work_sessions",
    toolset="matrix",
    schema=RECENT_WORK_SESSIONS_SCHEMA,
    handler=lambda args, **kw: matrix_recent_work_sessions(
        limit=args.get("limit", 10),
        lookback_hours=args.get("lookback_hours", 168),
    ),
    emoji="🟢",
)

registry.register(
    name="matrix_publish_presence_status",
    toolset="matrix",
    schema=PRESENCE_PUBLISH_SCHEMA,
    handler=lambda args, **kw: matrix_publish_presence_status(
        state=args.get("state", ""),
        status_msg=args.get("status_msg", ""),
        thoughts=args.get("thoughts", ""),
        context=args.get("context", ""),
        dry_run=args.get("dry_run", False),
    ),
    emoji="🟢",
)

registry.register(
    name="matrix_presence_status_history",
    toolset="matrix",
    schema=PRESENCE_HISTORY_SCHEMA,
    handler=lambda args, **kw: matrix_presence_status_history(limit=args.get("limit", 20)),
    emoji="🕘",
)
