import importlib.util
import queue
import sys
from pathlib import Path
from datetime import datetime, timezone
import pytest
import httpx
from gateway.config import PlatformConfig

ROOT = Path(__file__).parents[2] / "plugins" / "anton-gateway"

def load_plugin():
    name = "hermes_plugins.anton_gateway"
    if name in sys.modules:
        return sys.modules[name]
    import types
    parent = types.ModuleType("hermes_plugins"); parent.__path__=[]
    sys.modules.setdefault("hermes_plugins", parent)
    spec=importlib.util.spec_from_file_location(name, ROOT / "__init__.py", submodule_search_locations=[str(ROOT)])
    module=importlib.util.module_from_spec(spec); sys.modules[name]=module; spec.loader.exec_module(module)
    return module

def test_explicit_target_is_strict_and_does_not_need_home(monkeypatch):
    plugin=load_plugin()
    refs=sys.modules[plugin.__name__ + ".references"]
    monkeypatch.delenv("ANTON_HOME_CONVERSATION_ID", raising=False)
    target, conversation = refs.parse_delivery_target("anton:conversation_0123456789abcdef0123456789abcdef")
    assert target == "anton:conversation_0123456789abcdef0123456789abcdef"
    assert conversation.startswith("conversation_")
    with pytest.raises(ValueError): refs.parse_delivery_target("anton:project_gateway")
    with pytest.raises(ValueError): refs.require_no_thread("thread")

def test_outbox_retries_keep_the_same_delivery_id(tmp_path):
    plugin=load_plugin()
    import importlib
    Outbox=importlib.import_module(plugin.__name__ + ".outbox").AntonOutbox
    outbox=Outbox(tmp_path / "outbox.json")
    record=outbox.create({"deliveryId":"delivery_fixed", "scheduleId":"cron_a", "executionId":"run_a", "deliveryTarget":"anton:conversation_0123456789abcdef0123456789abcdef", "payload":{"schemaVersion":"1"}})
    assert record["state"] == "pending"
    claimed=outbox.due(datetime.now(timezone.utc))
    assert claimed[0]["deliveryId"] == "delivery_fixed"
    retried=outbox.retry_or_dead_letter("delivery_fixed", claimed[0]["leaseToken"], "network", True)
    assert retried["deliveryId"] == "delivery_fixed" and retried["state"] == "pending"
    from datetime import timedelta
    claimed_again=outbox.due(datetime.now(timezone.utc) + timedelta(seconds=3))
    dead=outbox.retry_or_dead_letter("delivery_fixed", claimed_again[0]["leaseToken"], "bad_target", False)
    assert dead["deliveryId"] == "delivery_fixed" and dead["state"] == "dead_letter"


def test_gateway_headers_match_fixed_gateway_hmac_preimage_and_literals():
    plugin = load_plugin()
    import importlib
    client_module = importlib.import_module(plugin.__name__ + ".client")
    config_module = importlib.import_module(plugin.__name__ + ".config")
    body = b'{"reference":"anton:project_0123456789abcdef0123456789abcdef"}'
    client = client_module.AntonGatewayClient(
        config_module.AntonConfig(
            base_url="https://gateway.example",
            resolver_key="resolver-fixed-secret",
            delivery_key="delivery-fixed-secret",
            resolver_key_id="resolver-v1",
            delivery_key_id="delivery-v1",
            enabled=True,
        ),
        clock=lambda: datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
    )

    assert client._headers("resolver", body) == {
        "Content-Type": "application/json",
        "X-AG-Key-Id": "resolver-v1",
        "X-AG-Timestamp": "2025-01-02T03:04:05Z",
        "X-AG-Signature": "sha256=28500fc4563223694359a749646f4960703b52ab7229a9735aa9cb6352d9c9c3",
        "X-AG-Resolver-Scope": "anton.resolve",
    }
    delivery_headers = client._headers("delivery", body)
    assert delivery_headers["X-AG-Signature"].startswith("sha256=")
    assert "X-AG-Resolver-Scope" not in delivery_headers


def test_config_uses_gateway_authoritative_env_names_and_fails_closed(monkeypatch):
    plugin = load_plugin()
    import importlib
    config_module = importlib.import_module(plugin.__name__ + ".config")

    monkeypatch.setenv("ANTON_GATEWAY_ENABLED", "true")
    monkeypatch.setenv("ANTON_GATEWAY_URL", "https://gateway.example/")
    monkeypatch.setenv("ANTON_RESOLVER_KEY_ID", "resolver-v1")
    monkeypatch.setenv("ANTON_RESOLVER_SECRET", "resolver-secret")
    monkeypatch.setenv("ANTON_CRON_DELIVERY_KEY_ID", "delivery-v1")
    monkeypatch.setenv("ANTON_CRON_DELIVERY_SECRET", "delivery-secret")

    config = config_module.AntonConfig.from_env()
    assert config.base_url == "https://gateway.example"
    assert (config.resolver_key_id, config.resolver_key) == ("resolver-v1", "resolver-secret")
    assert (config.delivery_key_id, config.delivery_key) == ("delivery-v1", "delivery-secret")

    monkeypatch.delenv("ANTON_CRON_DELIVERY_SECRET")
    with pytest.raises(ValueError, match="configuration is incomplete"):
        config_module.AntonConfig.from_env()


@pytest.mark.asyncio
async def test_adapter_is_instantiable_and_returns_minimal_chat_info(monkeypatch):
    """Plugin registration must precede construction of its dynamic Platform."""
    # tests/conftest.py deliberately strips credential-shaped variables. Seed
    # this feature test's fake credentials locally; do not weaken that fixture.
    monkeypatch.setenv("ANTON_GATEWAY_ENABLED", "true")
    monkeypatch.setenv("ANTON_GATEWAY_URL", "https://gateway.example")
    monkeypatch.setenv("ANTON_RESOLVER_KEY_ID", "resolver-test")
    monkeypatch.setenv("ANTON_RESOLVER_SECRET", "resolver-secret")
    monkeypatch.setenv("ANTON_CRON_DELIVERY_KEY_ID", "delivery-test")
    monkeypatch.setenv("ANTON_CRON_DELIVERY_SECRET", "delivery-secret")
    plugin = load_plugin()
    from gateway.config import Platform
    from gateway.platform_registry import PlatformEntry, platform_registry

    # Exercise the plugin's real register() lifecycle, but restore both
    # registry states afterwards so this test cannot make later tests accept
    # ANTON as a dynamically registered platform.
    previous_entry = platform_registry._entries.get("anton")
    previous_deferred = platform_registry._deferred.get("anton")
    had_dynamic_member = "anton" in Platform._value2member_map_

    class RegistryContext:
        def register_tool(self, **_kwargs):
            pass

        def register_platform(self, **kwargs):
            platform_registry.register(PlatformEntry(source="plugin", **kwargs))

    try:
        plugin.register(RegistryContext())
        adapter = platform_registry.create_adapter("anton", PlatformConfig(enabled=True))
        assert adapter is not None
        assert await adapter.get_chat_info("conversation_0123456789abcdef0123456789abcdef") == {
            "name": "conversation_0123456789abcdef0123456789abcdef",
            "type": "dm",
        }
    finally:
        platform_registry.unregister("anton")
        if previous_entry is not None:
            platform_registry.register(previous_entry)
        if previous_deferred is not None:
            platform_registry.register_deferred("anton", previous_deferred)
        if not had_dynamic_member:
            Platform._value2member_map_.pop("anton", None)
            Platform._member_map_.pop("ANTON", None)


@pytest.mark.parametrize("ack", [
    {"schemaVersion": "2", "status": "accepted", "deliveryId": "delivery-fixed", "messageId": "message-1", "deduplicated": False},
    {"schemaVersion": "1", "status": "queued", "deliveryId": "delivery-fixed", "messageId": "message-1", "deduplicated": False},
    {"schemaVersion": "1", "status": "accepted", "deliveryId": "other-delivery", "messageId": "message-1", "deduplicated": False},
    {"schemaVersion": "1", "status": "accepted", "deliveryId": "delivery-fixed", "messageId": "", "deduplicated": False},
    {"schemaVersion": "1", "status": "accepted", "deliveryId": "delivery-fixed", "messageId": "message-1", "deduplicated": "false"},
])
def test_delivery_ack_rejects_malformed_required_fields(ack):
    plugin = load_plugin()
    import importlib
    client_module = importlib.import_module(plugin.__name__ + ".client")
    platform = importlib.import_module(plugin.__name__ + ".platform")

    with pytest.raises(client_module.AntonTransportError, match="invalid_ack"):
        platform._validate_ack(ack, "delivery-fixed")


def _enable_delegation_sink(monkeypatch):
    monkeypatch.setenv("ANTON_GATEWAY_ENABLED", "true")
    monkeypatch.setenv("ANTON_DELEGATION_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("ANTON_GATEWAY_URL", "https://gateway.example")
    monkeypatch.setenv("ANTON_RESOLVER_KEY_ID", "resolver-v1")
    monkeypatch.setenv("ANTON_RESOLVER_SECRET", "resolver-secret")
    monkeypatch.setenv("ANTON_CRON_DELIVERY_KEY_ID", "delivery-v1")
    monkeypatch.setenv("ANTON_CRON_DELIVERY_SECRET", "delivery-secret")
    monkeypatch.setenv("ANTON_DELEGATION_DELIVERY_KEY_ID", "delegation-v1")
    monkeypatch.setenv("ANTON_DELEGATION_DELIVERY_SECRET", "delegation-secret")


def _enabled_client(client_module, config_module, transport, *, base_url="https://gateway.example"):
    return client_module.AntonGatewayClient(
        config_module.AntonConfig(
            base_url=base_url,
            resolver_key="resolver-fixed-secret",
            delivery_key="delivery-fixed-secret",
            resolver_key_id="resolver-v1",
            delivery_key_id="delivery-v1",
            enabled=True,
        ),
        clock=lambda: datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        transport=transport,
    )


@pytest.mark.asyncio
async def test_client_preserves_signed_raw_body_and_headers_with_httpx_transport():
    plugin = load_plugin()
    import importlib
    client_module = importlib.import_module(plugin.__name__ + ".client")
    config_module = importlib.import_module(plugin.__name__ + ".config")
    captured = {}

    async def handler(request):
        captured["url"] = str(request.url)
        captured["body"] = await request.aread()
        captured["headers"] = dict(request.headers.raw)
        return httpx.Response(200, json={"ok": True})

    client = _enabled_client(client_module, config_module, httpx.MockTransport(handler))
    body = b'{"nonCanonical": true, "unchanged":"\\xc3\\xa9"}'
    assert await client.deliver({"ignored": True}, body) == {"ok": True}
    assert captured["url"] == "https://gateway.example/internal/anton-cron-deliveries/v1"
    assert captured["body"] == body
    assert captured["headers"][b"X-AG-Key-Id"] == b"delivery-v1"
    assert captured["headers"][b"X-AG-Timestamp"] == b"2025-01-02T03:04:05Z"
    assert captured["headers"][b"X-AG-Signature"] == b"sha256=1c0e784aa2b837db154dc70729a047571d6ae348f1f321c84826e9d57f5bf0e6"


@pytest.mark.asyncio
async def test_client_does_not_follow_redirects():
    plugin = load_plugin()
    import importlib
    client_module = importlib.import_module(plugin.__name__ + ".client")
    config_module = importlib.import_module(plugin.__name__ + ".config")
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        return httpx.Response(302, headers={"location": "https://other.example/"})

    client = _enabled_client(client_module, config_module, httpx.MockTransport(handler))
    with pytest.raises(client_module.AntonTransportError, match="http_302") as error:
        await client.resolve({"reference": "anton:project_x"})
    assert not error.value.retryable and calls == 1


@pytest.mark.asyncio
async def test_client_rejects_oversized_requests_and_responses():
    plugin = load_plugin()
    import importlib
    client_module = importlib.import_module(plugin.__name__ + ".client")
    config_module = importlib.import_module(plugin.__name__ + ".config")
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=b"x" * (client_module.MAX_RESPONSE_BYTES + 1))

    client = _enabled_client(client_module, config_module, httpx.MockTransport(handler))
    with pytest.raises(client_module.AntonTransportError, match="request_too_large"):
        await client._post("resolver", "/internal/anton-resolver/v1/resolve", b"x" * (client_module.MAX_RESOLVER_REQUEST_BYTES + 1))
    with pytest.raises(client_module.AntonTransportError, match="request_too_large"):
        await client.deliver({}, b"x" * (client_module.MAX_DELIVERY_REQUEST_BYTES + 1))
    assert calls == 0
    with pytest.raises(client_module.AntonTransportError, match="response_too_large"):
        await client.resolve({"reference": "anton:project_x"})


@pytest.mark.parametrize("url, allowed", [
    ("https://gateway.example", True),
    ("http://127.0.0.1:8080", True),
    ("http://[::1]:8080", True),
    ("http://192.168.1.10", True),
    ("http://gateway.example", False),
    ("http://192.0.2.1", False),
    ("ftp://127.0.0.1", False),
    ("https://user:pass@gateway.example", False),
    ("https://gateway.example/#fragment", False),
])
def test_gateway_url_policy(url, allowed):
    plugin = load_plugin()
    import importlib
    config_module = importlib.import_module(plugin.__name__ + ".config")
    if allowed:
        assert config_module.validate_base_url(url) == url.rstrip("/")
    else:
        with pytest.raises(ValueError):
            config_module.validate_base_url(url)
    assert config_module.validate_base_url("http://gateway.example", allow_insecure_http=True) == "http://gateway.example"


@pytest.mark.asyncio
@pytest.mark.parametrize("status,retryable", [(400, False), (409, False), (408, True), (429, True), (500, True), (599, True)])
async def test_http_status_retry_classification(status, retryable):
    plugin = load_plugin()
    import importlib
    client_module = importlib.import_module(plugin.__name__ + ".client")
    config_module = importlib.import_module(plugin.__name__ + ".config")

    async def handler(request):
        return httpx.Response(status, json={"error": "ignored"})

    client = _enabled_client(client_module, config_module, httpx.MockTransport(handler))
    with pytest.raises(client_module.AntonTransportError) as error:
        await client.resolve({"reference": "anton:project_x"})
    assert error.value.code == f"http_{status}"
    assert error.value.retryable is retryable


def test_delegation_outbox_is_immutable_and_fences_stale_leases(tmp_path):
    plugin = load_plugin()
    import importlib
    Outbox = importlib.import_module(plugin.__name__ + ".outbox").AntonOutbox
    outbox = Outbox(tmp_path / "outbox.json")
    payload = {"schemaVersion": "1", "deliveryId": "delegation:d1:terminal"}
    first = outbox.create_or_confirm_same({
        "deliveryId": "delegation:d1:terminal", "deliveryType": "delegation.result",
        "payload": payload,
    })
    assert first["payloadSha256"]
    assert outbox.create_or_confirm_same({
        "deliveryId": "delegation:d1:terminal", "deliveryType": "delegation.result",
        "payload": payload,
    })["deliveryId"] == first["deliveryId"]
    with pytest.raises(ValueError, match="payload hash conflict"):
        outbox.create_or_confirm_same({"deliveryId": "delegation:d1:terminal", "deliveryType": "delegation.result", "payload": {"changed": True}})
    old = outbox.due()[0]
    outbox.transition(old["deliveryId"], expected_token=old["leaseToken"], leaseExpiresAt="2000-01-01T00:00:00+00:00")
    new = outbox.due()[0]
    assert new["leaseVersion"] > old["leaseVersion"]
    assert outbox.transition(old["deliveryId"], expected_token=old["leaseToken"], state="delivered") is None


def test_delegation_headers_bind_exact_method_path_nonce_and_digest():
    plugin = load_plugin()
    import importlib
    client_module = importlib.import_module(plugin.__name__ + ".client")
    config_module = importlib.import_module(plugin.__name__ + ".config")
    client = client_module.AntonGatewayClient(config_module.AntonConfig(
        base_url="https://gateway.example", resolver_key="r", delivery_key="c",
        resolver_key_id="r1", delivery_key_id="c1", delegation_delivery_key="d",
        delegation_delivery_key_id="d1", enabled=True,
    ), clock=lambda: datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc), nonce_factory=lambda: "a" * 32)
    headers = client._headers("delegation", b'{"x":1}')
    assert headers["X-AG-Protocol-Version"] == "1"
    assert headers["X-AG-Nonce"] == "a" * 32
    assert headers["X-AG-Body-SHA256"] == "5041bf1f713df204784353e82f6a4a535931cb64f1f4b4a5aeaffcb720918b22"


def test_disabled_delegation_sink_does_not_enqueue_stale_handoff(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.delenv("ANTON_DELEGATION_DELIVERY_ENABLED", raising=False)
    plugin = load_plugin()
    import importlib
    sink = importlib.import_module(plugin.__name__ + ".completion_sink")
    outbox_module = importlib.import_module(plugin.__name__ + ".outbox")
    from tools import async_delegation as ad

    metadata = {"origin": "anton", "deliveryTarget": "conversation_x", "parentRunId": "run-1", "parentSessionId": "session-1"}
    event = {"delegation_id": "deleg_disabled", "status": "completed", "completed_at": 2.0, "delivery_metadata": metadata}
    ad._persist_dispatch({"delegation_id": "deleg_disabled", "session_key": "", "origin_ui_session_id": "", "parent_session_id": "session-1", "dispatched_at": 1.0, "delivery_metadata": metadata})
    ad._persist_completion(event, {"status": "completed"})
    assert ad.claim_completion_delivery("deleg_disabled", "disabled-claim")
    assert not sink.handoff(event, "disabled-claim")
    assert outbox_module.AntonOutbox()._read() == []
    assert ad.claim_completion_delivery("deleg_disabled", "enabled-later")


@pytest.mark.asyncio
async def test_delegation_delivery_is_default_off(monkeypatch):
    plugin = load_plugin()
    import importlib
    client_module = importlib.import_module(plugin.__name__ + ".client")
    config_module = importlib.import_module(plugin.__name__ + ".config")
    client = client_module.AntonGatewayClient(config_module.AntonConfig(
        base_url="https://gateway.example", resolver_key="r", delivery_key="c",
        resolver_key_id="r1", delivery_key_id="c1", delegation_delivery_key="d",
        delegation_delivery_key_id="d1", enabled=True,
    ))
    monkeypatch.delenv("ANTON_DELEGATION_DELIVERY_ENABLED", raising=False)
    with pytest.raises(client_module.AntonTransportError, match="delegation_disabled"):
        await client.deliver_delegation({"deliveryId": "delegation:d1:terminal"})


def test_delegation_handoff_persists_deterministic_outbox_before_ack(tmp_path, monkeypatch):
    """A retry after a crash confirms the same record rather than changing it."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _enable_delegation_sink(monkeypatch)
    plugin = load_plugin()
    import importlib
    sink = importlib.import_module(plugin.__name__ + ".completion_sink")
    outbox_module = importlib.import_module(plugin.__name__ + ".outbox")
    from tools import async_delegation as ad

    record = {
        "delegation_id": "deleg_handoff", "session_key": "", "origin_ui_session_id": "",
        "parent_session_id": "session-1", "dispatched_at": 1.0,
        "delivery_metadata": {
            "origin": "anton", "deliveryTarget": "conversation_0123456789abcdef0123456789abcdef",
            "parentRunId": "run-1", "parentSessionId": "session-1",
        },
    }
    event = {
        "type": "async_delegation", "delegation_id": "deleg_handoff", "status": "completed",
        "summary": "done", "completed_at": 2.0, "delivery_metadata": record["delivery_metadata"],
    }
    ad._persist_dispatch(record)
    ad._persist_completion(event, {"status": "completed", "summary": "done"})
    assert ad.claim_completion_delivery("deleg_handoff", "claim-1")
    assert sink.handoff(event, "claim-1")
    assert ad.get_durable_delegation("deleg_handoff")["delivery_state"] == "delivered"

    saved = outbox_module.AntonOutbox()._read()
    assert len(saved) == 1 and saved[0]["state"] == "pending"
    assert saved[0]["payload"]["occurredAt"] == "1970-01-01T00:00:02+00:00"
    # A process that died after writing the outbox but before ledger ACK may
    # repeat only this immutable payload and must not create a conflict.
    ad._persist_completion(event, {"status": "completed", "summary": "done"})
    assert ad.claim_completion_delivery("deleg_handoff", "claim-2")
    assert sink.handoff(event, "claim-2")
    assert len(outbox_module.AntonOutbox()._read()) == 1


def test_delegation_handoff_failure_releases_claim_and_startup_retries_sink(tmp_path, monkeypatch):
    """An outbox failure cannot strand or locally reroute an ANTON completion."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _enable_delegation_sink(monkeypatch)
    plugin = load_plugin()
    import importlib
    sink = importlib.import_module(plugin.__name__ + ".completion_sink")
    outbox_module = importlib.import_module(plugin.__name__ + ".outbox")
    from tools import async_delegation as ad

    metadata = {
        "origin": "anton", "deliveryTarget": "conversation_0123456789abcdef0123456789abcdef",
        "parentRunId": "run-1", "parentSessionId": "session-1",
    }
    event = {
        "type": "async_delegation", "delegation_id": "deleg_retry_handoff",
        "status": "completed", "summary": "done", "completed_at": 2.0,
        "delivery_metadata": metadata,
    }
    ad._persist_dispatch({
        "delegation_id": event["delegation_id"], "session_key": "", "origin_ui_session_id": "",
        "parent_session_id": "session-1", "dispatched_at": 1.0, "delivery_metadata": metadata,
    })
    ad._persist_completion(event, {"status": "completed", "summary": "done"})

    class BrokenOutbox:
        def create_or_confirm_same(self, _record):
            raise OSError("disk unavailable")

    assert ad.claim_completion_delivery(event["delegation_id"], "failed-claim")
    monkeypatch.setattr(sink, "AntonOutbox", BrokenOutbox)
    with pytest.raises(OSError, match="disk unavailable"):
        sink.handoff(event, "failed-claim")
    # Claim release makes recovery immediate rather than waiting for its lease.
    assert ad.claim_completion_delivery(event["delegation_id"], "retry-claim")
    assert ad.release_completion_delivery(event["delegation_id"], "retry-claim")

    monkeypatch.setattr(sink, "AntonOutbox", outbox_module.AntonOutbox)
    restored = queue.Queue()
    assert ad.restore_undelivered_completions(restored) == 0
    assert restored.empty()
    assert ad.get_durable_delegation(event["delegation_id"])["delivery_state"] == "delivered"
    saved = outbox_module.AntonOutbox()._read()
    assert len(saved) == 1 and saved[0]["deliveryId"] == "delegation:deleg_retry_handoff:terminal"
