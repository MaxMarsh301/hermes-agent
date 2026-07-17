import base64
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
import sqlite3

import pytest
from multidict import CIMultiDict

from gateway import anton_origin_v2
from gateway.anton_origin_v2 import (
    AntonOriginV2Error, MAX_BODY_BYTES, MAX_CONTINUATION_BYTES,
    validate_synthesize_continuation, verify,
)


CONTEXT = {
    "origin": "anton",
    "originConversationId": "conversation_0123456789abcdef0123456789abcdef",
    "hermesSessionId": "anton-chat-0123456789abcdef0123456789abcdef",
    "protocol": "anton.delegation.v1",
    "mode": "parent_continuation",
}


def _headers(
    body, *, key_id="current", secret="current-secret", timestamp=None, nonce=None, context=CONTEXT,
    context_raw=None, preimage_method="POST", preimage_path="/v1/runs",
):
    timestamp = timestamp or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:23] + "Z"
    nonce = nonce or base64.urlsafe_b64encode(os.urandom(32)).decode().rstrip("=")
    context_raw = context_raw if context_raw is not None else json.dumps(
        context, sort_keys=True, separators=(",", ":")
    ).encode()
    encoded_context = base64.urlsafe_b64encode(context_raw).decode().rstrip("=")
    digest = hashlib.sha256(body).hexdigest()
    preimage = f"anton-run-origin-v2\n{preimage_method}\n{preimage_path}\n{timestamp}\n{nonce}\n{key_id}\n{digest}\n{encoded_context}\n"
    return CIMultiDict({
        "X-Anton-Origin-Version": "anton-run-origin-v2",
        "X-Anton-Origin-Key-Id": key_id,
        "X-Anton-Origin-Timestamp": timestamp,
        "X-Anton-Origin-Nonce": nonce,
        "X-Anton-Origin-Body-SHA256": digest,
        "X-Anton-Origin-Context": encoded_context,
        "X-Anton-Origin-Signature": hmac.new(secret.encode(), preimage.encode(), hashlib.sha256).hexdigest(),
    })


@pytest.fixture(autouse=True)
def v2_env(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("ANTON_RUN_ORIGIN_V2_ENABLED", "true")
    monkeypatch.setenv("ANTON_RUN_ORIGIN_V2_CURRENT_KEY_ID", "current")
    monkeypatch.setenv("ANTON_RUN_ORIGIN_V2_CURRENT_SECRET", "current-secret")


def test_exact_preimage_accepts_current_and_previous_rotation(monkeypatch):
    body = b'{"input":"hello"}'
    assert verify(_headers(body), body).key_id == "current"
    monkeypatch.setenv("ANTON_RUN_ORIGIN_V2_PREVIOUS_KEY_ID", "previous")
    monkeypatch.setenv("ANTON_RUN_ORIGIN_V2_PREVIOUS_SECRET", "previous-secret")
    assert verify(_headers(body, key_id="previous", secret="previous-secret"), body).key_id == "previous"


@pytest.mark.parametrize("kind", ["missing", "partial", "bad_signature", "body_mismatch", "stale", "future", "unknown_key", "malformed_context", "duplicate"])
def test_invalid_envelopes_fail_closed(kind):
    body = b'{"input":"hello"}'
    headers = _headers(body)
    if kind == "missing":
        headers = CIMultiDict()
        assert verify(headers, body) is None
        return
    if kind == "partial":
        del headers["X-Anton-Origin-Nonce"]
    elif kind == "bad_signature":
        headers["X-Anton-Origin-Signature"] = "0" * 64
    elif kind == "body_mismatch":
        body = b'{"input":"other"}'
    elif kind == "stale":
        timestamp = (datetime.now(timezone.utc) - timedelta(seconds=301)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:23] + "Z"
        headers = _headers(body, timestamp=timestamp)
    elif kind == "future":
        timestamp = (datetime.now(timezone.utc) + timedelta(seconds=31)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:23] + "Z"
        headers = _headers(body, timestamp=timestamp)
    elif kind == "unknown_key":
        headers = _headers(body, key_id="unknown", secret="unknown-secret")
    elif kind == "malformed_context":
        headers["X-Anton-Origin-Context"] = "not*base64"
    elif kind == "duplicate":
        headers.add("X-Anton-Origin-Nonce", "A" * 43)
    with pytest.raises(AntonOriginV2Error):
        verify(headers, body)


def test_replay_and_oversize_are_rejected():
    body = b'{"input":"hello"}'
    headers = _headers(body)
    assert verify(headers, body) is not None
    with pytest.raises(AntonOriginV2Error):
        verify(headers, body)
    oversized = b"x" * (MAX_BODY_BYTES + 1)
    with pytest.raises(AntonOriginV2Error):
        verify(_headers(oversized), oversized)


def test_feature_disabled_rejects_complete_envelope(monkeypatch):
    body = b'{"input":"hello"}'
    monkeypatch.delenv("ANTON_RUN_ORIGIN_V2_ENABLED")
    with pytest.raises(AntonOriginV2Error):
        verify(_headers(body), body)


@pytest.mark.parametrize(
    ("preimage_method", "preimage_path"),
    [("GET", "/v1/runs"), ("POST", "/v1/other")],
)
def test_wrong_preimage_method_or_path_is_rejected(preimage_method, preimage_path):
    body = b'{"input":"hello"}'
    with pytest.raises(AntonOriginV2Error):
        verify(_headers(body, preimage_method=preimage_method, preimage_path=preimage_path), body)


def test_validly_signed_noncanonical_context_is_rejected_before_context_use():
    body = b'{"input":"hello"}'
    noncanonical = json.dumps(CONTEXT, sort_keys=False, separators=(", ", ": ")).encode()
    with pytest.raises(AntonOriginV2Error):
        verify(_headers(body, context_raw=noncanonical), body)


@pytest.mark.parametrize(
    "body",
    [
        b'{"z":2,"a":1}',
        b'{"a":1, "z":2}',
        b'{"a":1,"a":2}',
        b'["not","an object"]',
        b'{"a":"\xff"}',
    ],
)
def test_signed_runs_body_must_be_canonical_before_nonce_claim(body):
    with pytest.raises(AntonOriginV2Error):
        verify(_headers(body), body)
    assert not anton_origin_v2._replay_path().exists()


def test_signed_canonical_multi_field_runs_body_is_accepted():
    body = json.dumps({"a": 1, "z": "two"}, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    assert verify(_headers(body), body).key_id == "current"


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity", "1e9999"])
def test_validly_signed_nonfinite_body_is_detail_free_and_never_claims_nonce(monkeypatch, token):
    body = f'{{"input":{token}}}'.encode("ascii")
    claimed: list[tuple[str, str]] = []
    monkeypatch.setattr(anton_origin_v2, "_claim_nonce", lambda key_id, nonce, **_: claimed.append((key_id, nonce)) or True)

    with pytest.raises(AntonOriginV2Error) as error:
        verify(_headers(body), body)

    assert str(error.value) == ""
    assert claimed == []


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity", "1e9999"])
def test_validly_signed_nonfinite_context_is_detail_free_and_never_claims_nonce(monkeypatch, token):
    body = b'{"input":"hello"}'
    context_raw = f'{{"origin":{token}}}'.encode("ascii")
    claimed: list[tuple[str, str]] = []
    monkeypatch.setattr(anton_origin_v2, "_claim_nonce", lambda key_id, nonce, **_: claimed.append((key_id, nonce)) or True)

    with pytest.raises(AntonOriginV2Error) as error:
        verify(_headers(body, context_raw=context_raw), body)

    assert str(error.value) == ""
    assert claimed == []


def test_nonfinite_rejection_does_not_poison_nonce():
    nonce = "A" * 43
    rejected = b'{"input":1e9999}'
    with pytest.raises(AntonOriginV2Error):
        verify(_headers(rejected, nonce=nonce), rejected)

    accepted = b'{"input":"canonical"}'
    assert verify(_headers(accepted, nonce=nonce), accepted).key_id == "current"


@pytest.mark.parametrize(
    "headers",
    [
        lambda body: _headers(body, timestamp="not-a-timestamp"),
        lambda body: CIMultiDict({**_headers(body), "X-Anton-Origin-Context": "%%%"}),
    ],
)
def test_malformed_timestamp_or_base64_is_rejected(headers):
    body = b'{"input":"hello"}'
    with pytest.raises(AntonOriginV2Error):
        verify(headers(body), body)


def test_overlong_context_is_rejected():
    body = b'{"input":"hello"}'
    with pytest.raises(AntonOriginV2Error):
        verify(_headers(body, context_raw=b'{"padding":"' + b"x" * 1024 + b'"}'), body)


def test_replay_store_prunes_before_insert_never_exceeding_bound(monkeypatch):
    monkeypatch.setattr(anton_origin_v2, "REPLAY_STORE_MAX_ROWS", 2)
    assert anton_origin_v2._claim_nonce("current", "A" * 43, now_epoch=1)
    assert anton_origin_v2._claim_nonce("current", "B" * 43, now_epoch=2)
    path = anton_origin_v2._replay_path()
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TRIGGER reject_replay_overflow BEFORE INSERT ON anton_origin_v2_replay "
            "WHEN (SELECT COUNT(*) FROM anton_origin_v2_replay) >= 2 "
            "BEGIN SELECT RAISE(ABORT, 'replay store exceeded limit'); END"
        )
        conn.commit()
    assert anton_origin_v2._claim_nonce("current", "C" * 43, now_epoch=3)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM anton_origin_v2_replay").fetchone()[0] == 2


def _continuation(summary="HOSTILE_PRIVATE_MARKER"):
    return {
        "version": "hermes-delegation-completion-v1",
        "deliveryId": "delivery_0123456789abcdef0123456789abcdef",
        "delegationId": "deleg_0123abcd",
        "parentRunId": "run_0123456789abcdef0123456789abcdef",
        "parentSessionId": "anton-chat-0123456789abcdef0123456789abcdef",
        "deliveryTarget": "anton:conversation_0123456789abcdef0123456789abcdef",
        "status": "completed",
        "occurredAt": "2026-07-16T12:34:56.789Z",
        "completion": {
            "summary": summary,
            "completedChildren": 1,
            "failedChildren": 0,
            "interruptedChildren": 0,
            "unknownChildren": 0,
        },
    }


def test_synthesize_continuation_returns_only_private_projection_without_ids():
    prompt = validate_synthesize_continuation(_continuation())
    assert "HOSTILE_PRIVATE_MARKER" in prompt
    assert "completed=1" in prompt
    for private_id in ("delivery_", "deleg_", "run_", "anton-chat-", "anton:conversation_"):
        assert private_id not in prompt


@pytest.mark.parametrize("field,value", [
    ("deliveryId", "delivery_BAD"),
    ("delegationId", "-bad"),
    ("parentRunId", "run_ABC"),
    ("parentSessionId", "anton-chat-ABC"),
    ("deliveryTarget", "anton:conversation:0123456789abcdef0123456789abcdef"),
    ("status", "pending"),
    ("occurredAt", "2026-07-16T12:34:56Z"),
])
def test_synthesize_continuation_rejects_invalid_ids_target_status_and_timestamp(field, value):
    payload = _continuation()
    payload[field] = value
    with pytest.raises(AntonOriginV2Error):
        validate_synthesize_continuation(payload)


@pytest.mark.parametrize("mutate", [
    lambda p: p.update(extra=True),
    lambda p: p.pop("deliveryId"),
    lambda p: p["completion"].update(extra=True),
    lambda p: p["completion"].update(completedChildren=True),
    lambda p: p["completion"].update(completedChildren=10_001),
    lambda p: p["completion"].update(completedChildren=0),
    lambda p: p["completion"].update(summary="x" * 2049),
])
def test_synthesize_continuation_rejects_exact_keys_and_count_bounds(mutate):
    payload = _continuation()
    mutate(payload)
    with pytest.raises(AntonOriginV2Error):
        validate_synthesize_continuation(payload)


def test_synthesize_continuation_rejects_canonical_oversize():
    payload = _continuation("x" * 2048)
    payload["delegationId"] = "a" * 128
    # Keep schema valid while pushing the canonical encoded object over its cap.
    payload["completion"]["summary"] = "🙂" * 2048
    assert len(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()) > MAX_CONTINUATION_BYTES
    with pytest.raises(AntonOriginV2Error):
        validate_synthesize_continuation(payload)
