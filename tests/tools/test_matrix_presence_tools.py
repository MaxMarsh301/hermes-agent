import json
import sqlite3
import time


def _load_module():
    import tools.matrix_presence_tools as mpt
    return mpt


def test_matrix_presence_tools_register_under_matrix_toolset():
    mpt = _load_module()  # noqa: F841
    from tools.registry import registry
    from toolsets import resolve_toolset

    for name in {
        "matrix_recent_work_sessions",
        "matrix_publish_presence_status",
        "matrix_presence_status_history",
    }:
        entry = registry.get_entry(name)
        assert entry is not None
        assert entry.toolset == "matrix"
        assert name in resolve_toolset("matrix")


def test_publish_rejects_private_status_text(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    mpt = _load_module()

    result = json.loads(
        mpt.matrix_publish_presence_status(
            state="online",
            status_msg="working from /home/anton/.hermes/.env",
            thoughts="private note",
            context="context",
            dry_run=True,
        )
    )

    assert result["success"] is False
    assert "status_msg" in result["error"]
    assert "[REDACTED]" in result["rejected_preview"]
    assert "/home/anton" not in result["rejected_preview"]


def test_dry_run_publish_logs_status_without_matrix_calls(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    mpt = _load_module()

    def fail_presence(*args, **kwargs):
        raise AssertionError("dry-run must not call Matrix presence")

    def fail_mirror(*args, **kwargs):
        raise AssertionError("dry-run must not send mirror message")

    monkeypatch.setattr(mpt, "_set_matrix_presence", fail_presence)
    monkeypatch.setattr(mpt, "_send_matrix_log_message", fail_mirror)

    result = json.loads(
        mpt.matrix_publish_presence_status(
            state="online",
            status_msg="маленький зелёный огонёк считает вдохи сервера",
            thoughts="testing dry run",
            context="redacted test context",
            dry_run=True,
        )
    )

    assert result["success"] is True
    assert result["published"] is False
    assert result["dry_run"] is True
    assert result["status_msg"] == "маленький зелёный огонёк считает вдохи сервера"
    assert result["log_id"] >= 1

    db_path = tmp_path / "matrix_presence_status.db"
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT state, status_msg, thoughts, context, dry_run, success FROM status_log WHERE id=?",
            (result["log_id"],),
        ).fetchone()

    assert row == (
        "online",
        "маленький зелёный огонёк считает вдохи сервера",
        "testing dry run",
        "redacted test context",
        1,
        1,
    )

    with sqlite3.connect(db_path) as conn:
        legacy_row = conn.execute(
            "SELECT state, status_msg, thoughts, published, publish_error FROM presence_status_log ORDER BY id DESC LIMIT 1"
        ).fetchone()

    assert legacy_row == (
        "online",
        "маленький зелёный огонёк считает вдохи сервера",
        "testing dry run",
        0,
        None,
    )


def test_successful_publish_sets_presence_logs_and_mirrors(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    mpt = _load_module()
    calls = {"presence": [], "mirror": []}

    def fake_presence(state, status_msg):
        calls["presence"].append((state, status_msg))
        return {"success": True, "user_id": "@hermes:example.org"}

    def fake_mirror(status_msg, state, log_id):
        calls["mirror"].append((status_msg, state, log_id))
        return {"success": True, "message_id": "$event"}

    monkeypatch.setattr(mpt, "_set_matrix_presence", fake_presence)
    monkeypatch.setattr(mpt, "_send_matrix_log_message", fake_mirror)

    result = json.loads(
        mpt.matrix_publish_presence_status(
            state="online",
            status_msg="под панелью проснулся добрый пиксель",
            thoughts="testing live publish",
            context="redacted context",
            dry_run=False,
        )
    )

    assert result["success"] is True
    assert result["published"] is True
    assert result["status_msg"] == "под панелью проснулся добрый пиксель"
    assert result["mirror"]["success"] is True
    assert calls["presence"] == [("online", "под панелью проснулся добрый пиксель")]
    assert calls["mirror"] == [("под панелью проснулся добрый пиксель", "online", result["log_id"])]

    with sqlite3.connect(tmp_path / "matrix_presence_status.db") as conn:
        row = conn.execute(
            "SELECT state, status_msg, dry_run, success, error, mirror_success, mirror_message_id FROM status_log WHERE id=?",
            (result["log_id"],),
        ).fetchone()

    assert row == ("online", "под панелью проснулся добрый пиксель", 0, 1, None, 1, "$event")

    with sqlite3.connect(tmp_path / "matrix_presence_status.db") as conn:
        legacy_row = conn.execute(
            "SELECT state, status_msg, thoughts, published, publish_error FROM presence_status_log ORDER BY id DESC LIMIT 1"
        ).fetchone()

    assert legacy_row == ("online", "под панелью проснулся добрый пиксель", "testing live publish", 1, None)


def test_matrix_log_message_includes_state_and_status(monkeypatch):
    mpt = _load_module()
    captured = {}
    monkeypatch.setenv("MATRIX_PRESENCE_LOG_ROOM_ID", "!presence-log:example.org")

    def fake_request(method, path, payload, timeout=10.0):
        captured["method"] = method
        captured["path"] = path
        captured["payload"] = payload
        return {"success": True, "event_id": "$mirror"}

    monkeypatch.setattr(mpt, "_matrix_request", fake_request)

    result = mpt._send_matrix_log_message("под панелью проснулся добрый пиксель", "online", 42)

    assert result == {"success": True, "event_id": "$mirror"}
    assert captured["method"] == "PUT"
    assert "%21presence-log%3Aexample.org" in captured["path"]
    assert captured["payload"] == {
        "msgtype": "m.text",
        "body": "Статус изменён: online\nТекст статуса: под панелью проснулся добрый пиксель",
    }


def test_matrix_log_message_reports_missing_room(monkeypatch):
    mpt = _load_module()
    for key in mpt.MATRIX_LOG_ROOM_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(mpt, "_load_matrix_env_from_dotenv", lambda: None)

    result = mpt._send_matrix_log_message("public status", "online", 42)

    assert result == {"success": False, "error": "Matrix presence log room is not configured"}


def test_matrix_recent_work_sessions_reads_real_state_db(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    mpt = _load_module()
    now = time.time()
    db_path = tmp_path / "state.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                title TEXT,
                started_at REAL NOT NULL,
                ended_at REAL,
                message_count INTEGER DEFAULT 0,
                archived INTEGER DEFAULT 0
            );
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT,
                timestamp REAL NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO sessions(id, source, title, started_at, message_count, archived) VALUES (?, ?, ?, ?, ?, 0)",
            ("s1", "matrix", "VPN и backup", now - 100, 4),
        )
        conn.executemany(
            "INSERT INTO messages(session_id, role, content, timestamp) VALUES (?, ?, ?, ?)",
            [
                ("s1", "user", "Настрой VPN proxy и проверь backup", now - 90),
                ("s1", "assistant", "Готово: туннель и restic проверены", now - 80),
                ("s1", "user", "Не пиши /home/anton/secret в публичный статус", now - 70),
                ("s1", "assistant", "Ок, публичный текст будет абстрактный", now - 60),
            ],
        )

    result = json.loads(mpt.matrix_recent_work_sessions(limit=5, lookback_hours=24))

    assert result["success"] is True
    assert result["sessions_read"] == 1
    assert result["messages_read"] > 0
    assert "сетевые туннели и маршруты" in result["topics"]
    assert "резервные копии и ночная автоматика" in result["topics"]
    encoded = json.dumps(result, ensure_ascii=False)
    assert "/home/anton/secret" not in encoded
    assert "[REDACTED]" in encoded
