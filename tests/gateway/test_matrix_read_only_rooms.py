"""Regression tests for Matrix read-only room policy."""

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

try:
    import mautrix as _mautrix_probe  # noqa: F401
except ImportError:  # pragma: no cover - optional dependency in some shards
    pytest.skip("mautrix not installed", allow_module_level=True)

from gateway.config import PlatformConfig
from plugins.platforms.matrix.adapter import MatrixAdapter, _apply_yaml_config


def _make_adapter(extra=None):
    cfg = PlatformConfig(
        enabled=True,
        token="tok",
        extra={
            "homeserver": "https://matrix.example.org",
            "user_id": "@bot:example.org",
            "require_mention": False,
            **(extra or {}),
        },
    )
    adapter = MatrixAdapter(cfg)
    adapter._user_id = "@bot:example.org"
    return adapter


def test_matrix_read_only_rooms_parse_from_config():
    adapter = _make_adapter({"read_only_rooms": ["!readonly:example.org", "  "]})

    assert adapter._read_only_rooms == {"!readonly:example.org"}
    assert adapter.get_diagnostics().get("policy", {}).get("read_only_room_count") == 1


def test_matrix_read_only_rooms_parse_from_env(monkeypatch):
    monkeypatch.setenv("MATRIX_READ_ONLY_ROOMS", "!one:example.org, !two:example.org")

    adapter = _make_adapter()

    assert adapter._read_only_rooms == {"!one:example.org", "!two:example.org"}


@pytest.mark.asyncio
async def test_matrix_read_only_room_drops_before_mention_free_room_or_thread_logic(monkeypatch):
    adapter = _make_adapter(
        {
            "free_response_rooms": ["!readonly:example.org"],
            "read_only_rooms": ["!readonly:example.org"],
        }
    )
    adapter._threads.mark("$thread")
    receipts = []
    adapter._background_read_receipt = lambda room_id, event_id: receipts.append((room_id, event_id))
    monkeypatch.setattr(
        adapter,
        "_resolve_room_identity",
        AsyncMock(side_effect=AssertionError("read-only drop must happen before identity lookup")),
    )

    ctx = await adapter._resolve_message_context(
        "!readonly:example.org",
        "@alice:example.org",
        "$event",
        "@bot please answer",
        {
            "body": "@bot please answer",
            "m.mentions": {"user_ids": ["@bot:example.org"]},
        },
        {"rel_type": "m.thread", "event_id": "$thread"},
    )

    assert ctx is None
    assert receipts == [("!readonly:example.org", "$event")]


def test_matrix_yaml_bridge_exports_read_only_and_dm_auto_thread(monkeypatch):
    for key in ("MATRIX_READ_ONLY_ROOMS", "MATRIX_DM_AUTO_THREAD"):
        monkeypatch.delenv(key, raising=False)

    result = _apply_yaml_config(
        {},
        {
            "read_only_rooms": ["!readonly:example.org", "!other:example.org"],
            "dm_auto_thread": True,
        },
    )

    assert result == {"dm_auto_thread": True}
    assert os.getenv("MATRIX_READ_ONLY_ROOMS") == "!readonly:example.org,!other:example.org"
    assert os.getenv("MATRIX_DM_AUTO_THREAD") == "true"
