"""Regression coverage for Matrix plaintext opt-in and Megolm key recovery."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.matrix.adapter import EventType, MatrixAdapter


def _adapter() -> MatrixAdapter:
    return MatrixAdapter(
        PlatformConfig(
            enabled=True,
            token="test-token",
            extra={"homeserver": "https://matrix.example.org", "user_id": "@bot:example.org"},
        )
    )


def test_plaintext_outgoing_is_disabled_by_default_and_only_accepts_explicit_values(monkeypatch):
    monkeypatch.delenv("MATRIX_PLAINTEXT_OUTGOING", raising=False)
    assert not _adapter()._plaintext_outgoing_enabled()

    for value in ("true", "TRUE", "1", "yes", "on", " On "):
        monkeypatch.setenv("MATRIX_PLAINTEXT_OUTGOING", value)
        assert _adapter()._plaintext_outgoing_enabled()

    for value in ("", "false", "0", "no", "off", "enabled", "random"):
        monkeypatch.setenv("MATRIX_PLAINTEXT_OUTGOING", value)
        assert not _adapter()._plaintext_outgoing_enabled()


@pytest.mark.asyncio
async def test_plaintext_send_dispatches_direct_event_only_when_enabled(monkeypatch):
    adapter = _adapter()
    api = SimpleNamespace(request=AsyncMock(return_value={"event_id": "$plain"}), get_txn_id=lambda: "txn/id")
    adapter._client = SimpleNamespace(api=api, send_message_event=AsyncMock(return_value="$encrypted"))

    monkeypatch.delenv("MATRIX_PLAINTEXT_OUTGOING", raising=False)
    assert (await adapter.send("!room:example.org", "x")).message_id == "$encrypted"
    adapter._client.send_message_event.assert_awaited_once()
    api.request.assert_not_awaited()

    monkeypatch.setenv("MATRIX_PLAINTEXT_OUTGOING", "yes")
    assert (await adapter.send("!room:example.org", "x")).message_id == "$plain"
    api.request.assert_awaited_once()
    request_args = api.request.await_args
    assert "/rooms/%21room%3Aexample.org/send/m.room.message/txn%2Fid" in request_args.args[1]
    assert request_args.kwargs["metrics_method"] == "send_plain_room_message"
    # A reaction must retain the normal encrypted/mautrix dispatch path.
    await adapter._send_room_message_event("!room:example.org", EventType.REACTION, {"x": 1})
    assert adapter._client.send_message_event.await_count == 2


@pytest.mark.asyncio
async def test_missing_key_recovery_is_bounded_fail_safe_and_redacts_secrets(monkeypatch, caplog):
    adapter = _adapter()
    monkeypatch.setenv("MATRIX_ROOM_KEY_REQUEST_TIMEOUT", "999999")
    secret = "SUPPRESSED-MEGOLM-SECRET"
    crypto = SimpleNamespace(
        crypto_store=SimpleNamespace(has_group_session=AsyncMock(return_value=False)),
        request_room_key=AsyncMock(side_effect=RuntimeError(secret)),
        decrypt_megolm_event=AsyncMock(),
    )
    client = SimpleNamespace(
        crypto=crypto,
        query_keys=AsyncMock(return_value=SimpleNamespace(device_keys={"@alice:example.org": {"DEVICE": object()}})),
        dispatch_event=MagicMock(),
    )
    adapter._client = client
    event = SimpleNamespace(
        room_id="!room:example.org",
        sender="@alice:example.org",
        event_id="$event",
        source={"ciphertext": secret},
        content=SimpleNamespace(session_id=secret, sender_key=secret, device_id="DEVICE"),
    )

    await adapter._on_room_encrypted_missing_key(event)

    crypto.request_room_key.assert_awaited_once()
    assert crypto.request_room_key.await_args.kwargs["timeout"] == 30.0
    assert not adapter._megolm_key_requests_in_flight
    assert secret not in caplog.text
    crypto.decrypt_megolm_event.assert_not_awaited()
    client.dispatch_event.assert_not_called()


@pytest.mark.asyncio
async def test_missing_key_recovery_retries_decrypt_and_dispatches_event():
    adapter = _adapter()
    decrypted = object()
    crypto = SimpleNamespace(
        crypto_store=SimpleNamespace(has_group_session=AsyncMock(return_value=False)),
        request_room_key=AsyncMock(return_value=True),
        decrypt_megolm_event=AsyncMock(return_value=decrypted),
    )
    client = SimpleNamespace(
        crypto=crypto,
        query_keys=AsyncMock(return_value=SimpleNamespace(device_keys={"@alice:example.org": {"DEVICE": object()}})),
        dispatch_event=MagicMock(),
    )
    adapter._client = client
    event = SimpleNamespace(
        room_id="!room:example.org",
        sender="@alice:example.org",
        source={"ciphertext": "opaque"},
        content=SimpleNamespace(session_id="session", sender_key="sender-key", device_id="DEVICE"),
    )

    await adapter._on_room_encrypted_missing_key(event)

    crypto.decrypt_megolm_event.assert_awaited_once_with(event)
    client.dispatch_event.assert_called_once_with(decrypted, event.source)
    assert not adapter._megolm_key_requests_in_flight
