import asyncio
import hashlib
import hmac
import importlib
import json
import logging
import sqlite3
from datetime import datetime, timezone

import httpx
import pytest

from tests.plugins.test_anton_gateway import load_plugin


DELIVERY_ID = "delivery_0123456789abcdef0123456789abcdef"
CONVERSATION = "conversation_0123456789abcdef0123456789abcdef"
RUN_ID = "run_0123456789abcdef0123456789abcdef"
SESSION_ID = "anton-chat-0123456789abcdef0123456789abcdef"


def _modules():
    plugin = load_plugin()
    return (
        importlib.import_module(plugin.__name__ + ".outbox"),
        importlib.import_module(plugin.__name__ + ".client"),
        importlib.import_module(plugin.__name__ + ".config"),
        importlib.import_module(plugin.__name__ + ".platform"),
    )


def _body(delivery_id=DELIVERY_ID):
    return json.dumps({
        "version": "hermes-delegation-completion-v1", "deliveryId": delivery_id,
        "delegationId": "deleg_test", "parentRunId": RUN_ID, "parentSessionId": SESSION_ID,
        "deliveryTarget": f"anton:{CONVERSATION}", "status": "completed",
        "occurredAt": "2026-07-16T12:34:56.789Z",
        "completion": {"summary": "safe", "completedChildren": 1, "failedChildren": 0,
                       "interruptedChildren": 0, "unknownChildren": 0},
    }, sort_keys=True, separators=(",", ":")).encode()


def _config(config_module, key="deleg-secret", key_id="deleg-current"):
    return config_module.AntonConfig("https://gateway.example", "resolver", "cron", "resolver-id", "cron-id",
                                     enabled=True, delegation_delivery_key=key,
                                     delegation_delivery_key_id=key_id)


def _delegation_env(monkeypatch, *, include_cron=False):
    for name in ("ANTON_RESOLVER_SECRET", "ANTON_RESOLVER_KEY_ID", "ANTON_CRON_DELIVERY_SECRET", "ANTON_CRON_DELIVERY_KEY_ID"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ANTON_GATEWAY_ENABLED", "true")
    monkeypatch.setenv("ANTON_GATEWAY_URL", "https://gateway.example")
    monkeypatch.setenv("ANTON_RUN_ORIGIN_V2_ENABLED", "true")
    monkeypatch.setenv("ANTON_RUN_ORIGIN_V2_CURRENT_KEY_ID", "origin-current")
    monkeypatch.setenv("ANTON_RUN_ORIGIN_V2_CURRENT_SECRET", "origin-secret")
    monkeypatch.setenv("ANTON_DELEGATION_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("ANTON_DELEGATION_DELIVERY_CURRENT_KEY_ID", "deleg-current")
    monkeypatch.setenv("ANTON_DELEGATION_DELIVERY_CURRENT_SECRET", "deleg-secret")
    if include_cron:
        monkeypatch.setenv("ANTON_RESOLVER_KEY_ID", "resolver-id")
        monkeypatch.setenv("ANTON_RESOLVER_SECRET", "resolver-secret")
        monkeypatch.setenv("ANTON_CRON_DELIVERY_KEY_ID", "cron-id")
        monkeypatch.setenv("ANTON_CRON_DELIVERY_SECRET", "cron-secret")


def test_migration_is_additive_idempotent_and_never_reinterprets_cron_json(tmp_path):
    outbox_module, *_ = _modules()
    db = tmp_path / "state.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE legacy_unrelated (id INTEGER PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO legacy_unrelated VALUES (1, 'keep')")
        conn.execute("CREATE TABLE cron_delivery_outbox (payload_json TEXT)")
        conn.execute("INSERT INTO cron_delivery_outbox VALUES ('{\"deliveryId\":\"cron-id\"}')")
    outbox_module.DelegationOutbox(db)
    outbox_module.DelegationOutbox(db)  # idempotent re-open
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT value FROM legacy_unrelated").fetchone() == ("keep",)
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "anton_delegation_outbox" in tables and "cron_delivery_outbox" in tables
        columns = {row[1] for row in conn.execute("PRAGMA table_info(anton_delegation_outbox)")}
        assert {"delivery_id", "body", "body_sha256", "lease_token", "lease_version", "attempts"} <= columns
        indexes = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
        assert {"idx_anton_delegation_outbox_due", "idx_anton_delegation_outbox_lease", "idx_anton_delegation_outbox_retention"} <= indexes
        assert conn.execute("SELECT payload_json FROM cron_delivery_outbox").fetchone() == ('{"deliveryId":"cron-id"}',)


def test_ingest_orders_create_before_ack_and_releases_exact_claim_on_failures(monkeypatch, tmp_path):
    outbox_module, *_ = _modules()
    body, digest = _body(), hashlib.sha256(_body()).hexdigest()
    handoff = {"delegation_id": "deleg_test", "delivery_id": DELIVERY_ID, "route": {"route": "anton"},
               "body_json": body.decode(), "body_sha256": digest, "claim_token": "token", "claim_version": 7}
    calls = []
    monkeypatch.setattr("tools.async_delegation.claim_anton_handoffs", lambda **_: [handoff])
    monkeypatch.setattr("tools.async_delegation.release_anton_handoff", lambda *args: calls.append(("release", args)) or True)
    monkeypatch.setattr("tools.async_delegation.mark_anton_handoff_completed", lambda *args: calls.append(("ack", args)) or True)
    outbox = outbox_module.DelegationOutbox(tmp_path / "state.db")
    original = outbox.create_or_confirm
    def ordered(*args):
        assert calls == []
        calls.append(("create", args)); return original(*args)
    monkeypatch.setattr(outbox, "create_or_confirm", ordered)
    assert outbox.ingest_handoffs() == 1
    assert [kind for kind, _ in calls] == ["create", "ack"]

    calls.clear(); monkeypatch.setattr(outbox, "create_or_confirm", lambda *_: (_ for _ in ()).throw(RuntimeError("create failed")))
    assert outbox.ingest_handoffs() == 0
    assert calls == [("release", ("deleg_test", "token", 7))]

    calls.clear(); monkeypatch.setattr(outbox, "create_or_confirm", original)
    monkeypatch.setattr("tools.async_delegation.mark_anton_handoff_completed", lambda *_: False)
    assert outbox.ingest_handoffs() == 0
    assert outbox.get(DELIVERY_ID)["state"] == "pending"  # durable copy survives lost ledger ack
    assert calls == [("release", ("deleg_test", "token", 7))]

    # Recovery receives a new lease: the immutable identical body is confirmed then acked.
    handoff["claim_token"], handoff["claim_version"] = "token-2", 8
    calls.clear(); monkeypatch.setattr("tools.async_delegation.mark_anton_handoff_completed", lambda *args: calls.append(("ack", args)) or True)
    assert outbox.ingest_handoffs() == 1 and calls == [("ack", ("deleg_test", "token-2", 8))]
    handoff["body_sha256"] = "0" * 64
    calls.clear()
    assert outbox.ingest_handoffs() == 0
    assert calls == [("release", ("deleg_test", "token-2", 8))]
    assert outbox.get(DELIVERY_ID)["body"] == body


def test_disabled_or_missing_current_key_never_claims_or_creates(monkeypatch, tmp_path):
    outbox_module, _, config_module, platform = _modules()
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _delegation_env(monkeypatch)
    monkeypatch.setenv("ANTON_DELEGATION_DELIVERY_ENABLED", "false")
    monkeypatch.setattr(outbox_module.DelegationOutbox, "ingest_handoffs", lambda *_: pytest.fail("must not claim"))
    assert not config_module.detached_completion_sink_ready()
    assert asyncio.run(platform.retry_delegation_due()) == 0
    assert not (tmp_path / "state.db").exists()


@pytest.mark.parametrize("timeout", ["nonnumeric", "0", "-1", "120.1"])
def test_invalid_timeout_fails_readiness_before_outbox_instantiation_or_handoff_claim(monkeypatch, tmp_path, timeout):
    outbox_module, _, config_module, platform = _modules()
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _delegation_env(monkeypatch)
    monkeypatch.setenv("ANTON_GATEWAY_TIMEOUT", timeout)
    calls = []

    def forbidden(*_args, **_kwargs):
        calls.append(True)
        raise AssertionError("must not instantiate outbox or claim handoffs")

    monkeypatch.setattr(outbox_module, "DelegationOutbox", forbidden)
    monkeypatch.setattr("tools.async_delegation.claim_anton_handoffs", forbidden)
    assert not config_module.detached_completion_sink_ready()
    assert asyncio.run(platform.retry_delegation_due()) == 0
    assert calls == []
    assert not (tmp_path / "state.db").exists()


def test_maximum_valid_timeout_remains_ready(monkeypatch):
    _, _, config_module, _ = _modules()
    _delegation_env(monkeypatch)
    monkeypatch.setenv("ANTON_GATEWAY_TIMEOUT", "120")
    assert config_module.detached_completion_sink_ready()


@pytest.mark.parametrize("name,value", [
    ("ANTON_RUN_ORIGIN_V2_CURRENT_KEY_ID", " origin-current"),
    ("ANTON_RUN_ORIGIN_V2_CURRENT_KEY_ID", "origin/current"),
    ("ANTON_DELEGATION_DELIVERY_CURRENT_KEY_ID", "deleg current"),
    ("ANTON_DELEGATION_DELIVERY_CURRENT_KEY_ID", ""),
])
def test_malformed_current_key_ids_fail_readiness_before_handoff_ingest(monkeypatch, tmp_path, name, value):
    outbox_module, _, config_module, platform = _modules()
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _delegation_env(monkeypatch)
    monkeypatch.setenv(name, value)
    monkeypatch.setattr(outbox_module.DelegationOutbox, "ingest_handoffs", lambda *_: pytest.fail("must not ingest"))
    assert not config_module.detached_completion_sink_ready()
    assert asyncio.run(platform.retry_delegation_due()) == 0
    assert not (tmp_path / "state.db").exists()


def test_competing_claims_reclaim_fences_stale_settles_and_attempts_once(tmp_path):
    outbox_module, *_ = _modules()
    now = [1000.0]
    one, two = (outbox_module.DelegationOutbox(tmp_path / "state.db", clock=lambda: now[0]) for _ in range(2))
    body, digest = _body(), hashlib.sha256(_body()).hexdigest()
    one.create_or_confirm(DELIVERY_ID, {"route": "anton"}, body, digest)
    first = one.due()[0]
    assert two.due() == []
    assert first["attempts"] == 1
    now[0] += 181
    reclaimed = two.due()[0]
    assert reclaimed["leaseVersion"] == first["leaseVersion"] + 1 and reclaimed["leaseToken"] != first["leaseToken"]
    assert not one.delivered(DELIVERY_ID, first["leaseToken"], first["leaseVersion"], {}, "queued")
    assert not one.retry(DELIVERY_ID, first["leaseToken"], first["leaseVersion"], "network")
    assert not one.dead_letter(DELIVERY_ID, first["leaseToken"], first["leaseVersion"], "http_409")
    assert two.get(DELIVERY_ID)["attempts"] == 2 and two.delivered(DELIVERY_ID, reclaimed["leaseToken"], reclaimed["leaseVersion"], {}, "queued")


def test_full_jitter_base_cap_and_twenty_attempt_dead_letter_without_21st_claim(tmp_path):
    outbox_module, *_ = _modules()
    now = [0.0]
    outbox = outbox_module.DelegationOutbox(tmp_path / "state.db", clock=lambda: now[0], random_fn=lambda: .5)
    body, digest = _body(), hashlib.sha256(_body()).hexdigest()
    outbox.create_or_confirm(DELIVERY_ID, {}, body, digest)
    delays = []
    for attempt in range(1, 21):
        claim = outbox.due()[0]
        before = now[0]
        assert claim["attempts"] == attempt
        assert outbox.retry(DELIVERY_ID, claim["leaseToken"], claim["leaseVersion"], "network")
        row = outbox.get(DELIVERY_ID)
        if attempt < 20:
            delays.append(row["nextAttemptAt"] - before)
            now[0] = row["nextAttemptAt"]
        else:
            assert row["state"] == "dead_letter"
    assert delays == [5.0 + .5 * (min(900.0, 5.0 * 2 ** (n - 1)) - 5.0) for n in range(1, 20)]
    assert min(delays) >= 5.0 and max(delays) <= 900.0
    assert outbox.due() == []


def test_expired_twentieth_claim_is_dead_lettered_and_stale_owner_cannot_settle(tmp_path):
    outbox_module, *_ = _modules()
    now = [0.0]
    outbox = outbox_module.DelegationOutbox(tmp_path / "state.db", clock=lambda: now[0])
    body, digest = _body(), hashlib.sha256(_body()).hexdigest()
    outbox.create_or_confirm(DELIVERY_ID, {}, body, digest)
    for _ in range(19):
        claim = outbox.due(limit=1)[0]
        assert outbox.retry(DELIVERY_ID, claim["leaseToken"], claim["leaseVersion"], "network")
        now[0] = outbox.get(DELIVERY_ID)["nextAttemptAt"]
    twentieth = outbox.due(limit=1)[0]
    assert twentieth["attempts"] == 20
    now[0] += 181
    assert outbox.due(limit=1) == []
    row = outbox.get(DELIVERY_ID)
    assert row["state"] == "dead_letter" and row["errorCode"] == "attempts_exhausted"
    assert not outbox.retry(DELIVERY_ID, twentieth["leaseToken"], twentieth["leaseVersion"], "network")
    assert not outbox.delivered(DELIVERY_ID, twentieth["leaseToken"], twentieth["leaseVersion"], {}, "queued")


@pytest.mark.asyncio
async def test_callback_wire_exact_bytes_digest_preimage_nonce_and_fresh_retry(monkeypatch):
    _, client_module, config_module, _ = _modules()
    body, captures = _body(), []
    nonces = iter(["a" * 43, "b" * 43])
    monkeypatch.setattr(client_module.secrets, "token_urlsafe", lambda _: next(nonces) + "=")
    async def handler(request):
        captures.append((await request.aread(), dict(request.headers)))
        return httpx.Response(202, json={"version": client_module.DELEGATION_VERSION, "deliveryId": DELIVERY_ID,
                                         "accepted": True, "deduplicated": False, "outcome": "queued"})
    client = client_module.AntonGatewayClient(_config(config_module), transport=httpx.MockTransport(handler),
        clock=lambda: datetime(2026, 7, 16, 12, 34, 56, 789000, tzinfo=timezone.utc))
    await client.deliver_delegation(body, DELIVERY_ID); await client.deliver_delegation(body, DELIVERY_ID)
    digest = hashlib.sha256(body).hexdigest()
    assert [item[0] for item in captures] == [body, body]
    for sent, headers in captures:
        assert headers["content-type"] == "application/json; charset=utf-8"
        assert headers["x-anton-delegation-body-sha256"] == digest
        assert len(headers["x-anton-delegation-nonce"]) == 43
        assert headers["x-anton-delegation-timestamp"] == "2026-07-16T12:34:56.789Z"
        preimage = f"{client_module.DELEGATION_VERSION}\nPOST\n{client_module.DELEGATION_PATH}\n{headers['x-anton-delegation-timestamp']}\n{headers['x-anton-delegation-nonce']}\ndeleg-current\n{digest}\n".encode()
        assert headers["x-anton-delegation-signature"] == hmac.new(b"deleg-secret", preimage, hashlib.sha256).hexdigest()
    assert captures[0][1]["x-anton-delegation-signature"] != captures[1][1]["x-anton-delegation-signature"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status,ack", [
    (202, b"not-json"), (202, b"x" * 2049),
    (202, json.dumps({"version":"hermes-delegation-completion-v1","deliveryId":"other","accepted":True,"deduplicated":False,"outcome":"queued"}).encode()),
    (202, json.dumps({"version":"hermes-delegation-completion-v1","deliveryId":DELIVERY_ID,"accepted":False,"deduplicated":False,"outcome":"queued"}).encode()),
    (201, json.dumps({"version":"hermes-delegation-completion-v1","deliveryId":DELIVERY_ID,"accepted":True,"deduplicated":False,"outcome":"queued"}).encode()),
    (200, json.dumps({"version":"hermes-delegation-completion-v1","deliveryId":DELIVERY_ID,"accepted":True,"deduplicated":False,"outcome":"queued"}).encode()),
    (202, json.dumps({"version":"hermes-delegation-completion-v1","deliveryId":DELIVERY_ID,"accepted":True,"deduplicated":True,"outcome":"queued"}).encode()),
    # All otherwise-valid semantic fields plus an unrecognized private field is still invalid.
    (202, json.dumps({"version":"hermes-delegation-completion-v1","deliveryId":DELIVERY_ID,"accepted":True,"deduplicated":False,"outcome":"queued","private":"HOSTILE_MARKER"}).encode()),
])
async def test_only_exact_202_or_200_ack_is_eligible_for_durable_storage(status, ack):
    _, client_module, config_module, _ = _modules()
    async def handler(_): return httpx.Response(status, content=ack)
    with pytest.raises(client_module.AntonTransportError) as error:
        await client_module.AntonGatewayClient(_config(config_module), transport=httpx.MockTransport(handler)).deliver_delegation(_body(), DELIVERY_ID)
    assert error.value.retryable and error.value.code in {"invalid_ack", "ack_too_large"}


@pytest.mark.asyncio
@pytest.mark.parametrize("status,deduplicated", [(202, False), (200, True)])
async def test_exact_safe_ack_is_the_only_ack_returned_to_durable_settlement(status, deduplicated):
    _, client_module, config_module, _ = _modules()
    ack = {"version": client_module.DELEGATION_VERSION, "deliveryId": DELIVERY_ID,
           "accepted": True, "deduplicated": deduplicated, "outcome": "target_gone"}
    async def handler(_): return httpx.Response(status, json=ack)
    returned = await client_module.AntonGatewayClient(_config(config_module), transport=httpx.MockTransport(handler)).deliver_delegation(_body(), DELIVERY_ID)
    assert returned == ack


@pytest.mark.asyncio
@pytest.mark.parametrize("status,retryable", [(400, False), (413, False), (415, False), (401, False), (409, False), (302, False), (404, True), (429, True), (500, True), (503, True)])
async def test_status_matrix_and_retry_after(status, retryable):
    _, client_module, config_module, _ = _modules()
    async def handler(_): return httpx.Response(status, headers={"Retry-After": "17"})
    with pytest.raises(client_module.AntonTransportError) as raised:
        await client_module.AntonGatewayClient(_config(config_module), transport=httpx.MockTransport(handler)).deliver_delegation(_body(), DELIVERY_ID)
    assert raised.value.retryable is retryable
    if status == 429: assert raised.value.retry_after == 17


@pytest.mark.asyncio
async def test_network_timeout_tls_are_retryable_and_invalid_retry_after_falls_back():
    _, client_module, config_module, _ = _modules()
    async def broken(_): raise httpx.ConnectError("no route")
    with pytest.raises(client_module.AntonTransportError) as error:
        await client_module.AntonGatewayClient(_config(config_module), transport=httpx.MockTransport(broken)).deliver_delegation(_body(), DELIVERY_ID)
    assert error.value.retryable
    async def limited(_): return httpx.Response(429, headers={"Retry-After": "nonsense"})
    with pytest.raises(client_module.AntonTransportError) as error:
        await client_module.AntonGatewayClient(_config(config_module), transport=httpx.MockTransport(limited)).deliver_delegation(_body(), DELIVERY_ID)
    assert error.value.retry_after is None


@pytest.mark.asyncio
async def test_retry_after_accepts_future_http_date_and_rejects_past_or_invalid_dates():
    _, client_module, config_module, _ = _modules()
    now = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)
    for header, expected in [
        ("Thu, 16 Jul 2026 12:00:17 GMT", 17.0),
        ("Thu, 16 Jul 2026 11:59:59 GMT", None),
        ("not-a-date", None),
    ]:
        async def limited(_request, header=header):
            return httpx.Response(429, headers={"Retry-After": header})
        with pytest.raises(client_module.AntonTransportError) as error:
            await client_module.AntonGatewayClient(
                _config(config_module), clock=lambda: now, transport=httpx.MockTransport(limited)
            ).deliver_delegation(_body(), DELIVERY_ID)
        assert error.value.retry_after == expected


@pytest.mark.asyncio
async def test_purpose_separation_readiness_and_sender_need_no_cron_keys(monkeypatch):
    _, _, config_module, platform = _modules()
    _delegation_env(monkeypatch)  # intentionally no resolver/cron configuration
    assert config_module.detached_completion_sink_ready()
    captured = []
    class Client:
        def __init__(self, config): captured.append(config)
        async def deliver_delegation(self, body, delivery_id):
            assert body == _body() and delivery_id == DELIVERY_ID
            return {"outcome": "queued"}
    monkeypatch.setattr(platform, "AntonGatewayClient", Client)
    assert await platform._send_delegation_record({"body": _body(), "bodySha256": hashlib.sha256(_body()).hexdigest(), "deliveryId": DELIVERY_ID}) == {"outcome": "queued"}
    assert captured[0].delegation_delivery_key == "deleg-secret"


@pytest.mark.asyncio
async def test_adapter_single_loop_disconnect_and_disabled_no_loop(monkeypatch):
    _, _, _, platform = _modules()
    starts = []
    async def loop(self):
        starts.append(True)
        await asyncio.Event().wait()
    monkeypatch.setattr(platform.AntonAdapter, "_delegation_loop", loop)
    monkeypatch.setattr(platform, "detached_completion_sink_ready", lambda: True)
    adapter = object.__new__(platform.AntonAdapter); adapter._delegation_task = None
    assert await adapter.connect() and await adapter.connect(is_reconnect=True)
    await asyncio.sleep(0)  # allow the single scheduled background loop to begin
    assert starts == [True]
    await adapter.disconnect(); assert adapter._delegation_task is None
    monkeypatch.setattr(platform, "detached_completion_sink_ready", lambda: False)
    disabled = object.__new__(platform.AntonAdapter); disabled._delegation_task = None
    await disabled.connect(); assert disabled._delegation_task is None


def test_sync_cron_retry_does_not_drain_delegation_even_when_ready(monkeypatch):
    outbox_module, _, _, platform = _modules()
    class EmptyCronOutbox:
        def due(self, *, limit):
            assert limit == 3
            return []
    drained = []
    async def drain(*, limit):
        drained.append(limit)
        return 99
    monkeypatch.setattr(outbox_module, "AntonOutbox", EmptyCronOutbox)
    monkeypatch.setattr(platform, "detached_completion_sink_ready", lambda: True)
    monkeypatch.setattr(platform, "retry_delegation_due", drain)
    assert platform.retry_due(limit=3) == 0
    assert drained == []


@pytest.mark.asyncio
async def test_retry_delegation_claims_one_record_immediately_before_each_send(monkeypatch):
    outbox_module, _, _, platform = _modules()
    events, records = [], [
        {"deliveryId": "one", "body": b"one", "bodySha256": hashlib.sha256(b"one").hexdigest(), "leaseToken": "t1", "leaseVersion": 1},
        {"deliveryId": "two", "body": b"two", "bodySha256": hashlib.sha256(b"two").hexdigest(), "leaseToken": "t2", "leaseVersion": 1},
    ]
    class Outbox:
        def ingest_handoffs(self, *, limit):
            events.append(("ingest", limit)); return 0
        def due(self, *, limit):
            events.append(("due", limit)); return [records.pop(0)] if records else []
        def delivered(self, delivery_id, *_args):
            events.append(("settle", delivery_id)); return True
    monkeypatch.setattr(platform, "detached_completion_sink_ready", lambda: True)
    monkeypatch.setattr(platform, "_send_delegation_record", lambda record: events.append(("send", record["deliveryId"])) or asyncio.sleep(0, result={"outcome": "queued"}))
    monkeypatch.setattr(outbox_module, "DelegationOutbox", Outbox)
    assert await platform.retry_delegation_due(limit=2) == 2
    assert events == [("ingest", 2), ("due", 1), ("send", "one"), ("settle", "one"), ("due", 1), ("send", "two"), ("settle", "two")]


def test_actual_slice_b_ledger_handoff_to_typed_outbox_then_callback(monkeypatch, tmp_path):
    outbox_module, _, _, platform = _modules()
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _delegation_env(monkeypatch, include_cron=True)
    import tools.async_delegation as ledger
    body = _body()
    digest = hashlib.sha256(body).hexdigest()
    route = {"protocol": "anton.delegation.v1", "deliveryTarget": f"anton:{CONVERSATION}",
             "originConversationId": CONVERSATION, "parentRunId": RUN_ID, "parentSessionId": SESSION_ID}
    # This is a persisted terminal ledger record: no child executor/runner is involved in recovery.
    with ledger._connect() as conn:
        conn.execute("""INSERT INTO async_delegations
            (delegation_id,origin_session,origin_ui_session_id,state,dispatched_at,completed_at,updated_at,
             delivery_kind,anton_route_json,delivery_id,anton_body_json,anton_body_sha256,handoff_state,handoff_updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("deleg_test", "origin", "", "completed", 1.0, 2.0, 2.0, "anton", json.dumps(route, sort_keys=True),
             DELIVERY_ID, body.decode(), digest, "pending", 2.0))
    outbox = outbox_module.DelegationOutbox(tmp_path / "state.db")
    assert outbox.ingest_handoffs(limit=1) == 1
    assert outbox.get(DELIVERY_ID)["state"] == "pending"
    with ledger._connect() as conn:
        assert conn.execute("SELECT handoff_state FROM async_delegations WHERE delegation_id='deleg_test'").fetchone() == ("completed",)
    sent = []
    async def fake_callback(record):
        sent.append(record["body"])
        return {"version": "hermes-delegation-completion-v1", "deliveryId": DELIVERY_ID,
                "accepted": True, "deduplicated": False, "outcome": "queued"}
    monkeypatch.setattr(platform, "_send_delegation_record", fake_callback)
    monkeypatch.setattr(outbox_module, "DelegationOutbox", lambda: outbox)
    assert asyncio.run(platform.retry_delegation_due(limit=1)) == 1
    assert sent == [body] and outbox.get(DELIVERY_ID)["state"] == "delivered"
    assert ledger.claim_anton_handoffs(limit=1) == []  # recovery does not rerun the completed child


def test_hostile_private_markers_never_reach_logs_on_delivery_failure(caplog):
    _, client_module, config_module, _ = _modules()
    caplog.set_level(logging.DEBUG)
    hostile = "HOSTILE_SECRET_MARKER /private/path summary route"
    async def handler(_): raise httpx.ConnectError(hostile)
    with pytest.raises(client_module.AntonTransportError):
        asyncio.run(client_module.AntonGatewayClient(_config(config_module), transport=httpx.MockTransport(handler)).deliver_delegation(_body(), DELIVERY_ID))
    assert hostile not in caplog.text
