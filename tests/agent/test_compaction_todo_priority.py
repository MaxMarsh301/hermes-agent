from agent.conversation_compression import (
    _ensure_compressed_has_user_turn,
    _inject_todo_snapshot_before_latest_user,
)


def test_todo_snapshot_is_assistant_owned_and_precedes_latest_real_user():
    compressed = [
        {"role": "assistant", "content": "Compressed history"},
        {"role": "user", "content": "Implement the approved stages"},
    ]

    _inject_todo_snapshot_before_latest_user(compressed, "[>] stale verification todo")

    assert compressed[-1] == {
        "role": "user",
        "content": "Implement the approved stages",
    }
    assert compressed[-2]["role"] == "assistant"
    assert "stale verification todo" in compressed[-2]["content"]
    assert compressed[-2]["_compaction_todo_snapshot"] is True
    assert sum(message["role"] == "user" for message in compressed) == 1


def test_todo_snapshot_never_creates_a_user_message_when_summary_has_no_assistant():
    compressed = [{"role": "user", "content": "Newest real request"}]

    _inject_todo_snapshot_before_latest_user(compressed, "[ ] older task")

    assert [message["role"] for message in compressed] == ["assistant", "user"]
    assert compressed[-1]["content"] == "Newest real request"
    assert compressed[0]["_compaction_todo_snapshot"] is True


def test_missing_user_is_restored_before_todo_snapshot_is_inserted():
    original = [
        {"role": "user", "content": "Approved implementation request"},
        {"role": "assistant", "content": "Working"},
        {"role": "tool", "content": "partial result"},
    ]
    compressed = [{"role": "assistant", "content": "Summary only"}]

    _ensure_compressed_has_user_turn(original, compressed)
    _inject_todo_snapshot_before_latest_user(compressed, "[>] old todo")

    assert compressed[-1]["role"] == "user"
    assert compressed[-1]["content"] == "Approved implementation request"
    assert all(
        not (message["role"] == "user" and "old todo" in message.get("content", ""))
        for message in compressed
    )