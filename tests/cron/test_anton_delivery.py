"""Focused durable ANTON cron-delivery contract tests (no network)."""
import asyncio
import concurrent.futures
import importlib
import importlib.util
import sys
import types
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from cron import scheduler


ROOT = Path(__file__).parents[2] / "plugins" / "anton-gateway"


def load_plugin():
    name = "hermes_plugins.anton_gateway"
    if name in sys.modules:
        return sys.modules[name]
    parent = sys.modules.setdefault("hermes_plugins", types.ModuleType("hermes_plugins"))
    parent.__path__ = getattr(parent, "__path__", [])
    spec = importlib.util.spec_from_file_location(
        name, ROOT / "__init__.py", submodule_search_locations=[str(ROOT)]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "job",
    [
        {"id": "cron-explicit", "deliver": "anton:conversation_0123456789abcdef0123456789abcdef"},
        {"id": "cron-origin", "deliver": "origin", "origin": {"platform": "anton", "chat_id": "conversation_0123456789abcdef0123456789abcdef"}},
    ],
)
def test_explicit_and_trusted_origin_use_one_durable_path(monkeypatch, job):
    plugin = load_plugin()
    outbox_module = importlib.import_module(plugin.__name__ + ".outbox")
    records = []

    class MemoryOutbox:
        def create_and_claim(self, record):
            # This is the atomic before-network durability+ownership boundary.
            claimed = {**record, "state": "in_flight", "leaseToken": "first-owner"}
            records.append(claimed)
            return claimed
        def transition(self, *_args, **_kwargs):
            return {}
        def retry_or_dead_letter(self, *_args, **_kwargs):
            return {}

    sent = []
    async def standalone(*_args, **_kwargs):
        assert len(records) == 1
        sent.append(True)
        return {"success": True, "message_id": "accepted"}

    monkeypatch.setattr(outbox_module, "AntonOutbox", MemoryOutbox)
    monkeypatch.setattr("tools.send_message_tool._send_via_adapter", standalone)
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)
    from gateway.platform_registry import PlatformEntry, platform_registry
    monkeypatch.setitem(
        platform_registry._entries, "anton",
        PlatformEntry("anton", "ANTON", lambda _config: None, lambda: True),
    )
    import gateway.config
    monkeypatch.setattr(gateway.config, "load_gateway_config", lambda: SimpleNamespace(platforms={}))

    assert scheduler._deliver_result(job, "finished") is None
    assert sent == [True]
    payload = records[0]["payload"]
    assert payload["deliveryId"] == records[0]["deliveryId"]
    assert payload["message"]["content"] == "finished"
    assert payload["deliveryTarget"] == records[0]["deliveryTarget"]


def test_outbox_recovers_stale_lease_and_retry_sends_exact_stored_payload(monkeypatch, tmp_path):
    plugin = load_plugin()
    outbox_module = importlib.import_module(plugin.__name__ + ".outbox")
    platform = importlib.import_module(plugin.__name__ + ".platform")
    outbox = outbox_module.AntonOutbox(tmp_path / "outbox.json")
    payload = {
        "schemaVersion": "1", "deliveryId": "delivery-fixed", "scheduleId": "cron-a",
        "executionId": "run-a", "deliveryTarget": "anton:conversation_0123456789abcdef0123456789abcdef",
        "origin": "anton", "occurredAt": "2025-01-01T00:00:00+00:00",
        "message": {"kind": "cron.result", "role": "assistant", "content": "stored", "status": "completed"},
    }
    outbox.create({"deliveryId": "delivery-fixed", "scheduleId": "cron-a", "executionId": "run-a", "deliveryTarget": payload["deliveryTarget"], "payload": payload})
    claimed = outbox.due(datetime.now(timezone.utc))
    assert claimed[0]["state"] == "in_flight"
    outbox.transition("delivery-fixed", expected_token=claimed[0]["leaseToken"], leaseExpiresAt=(datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat())
    recovered = outbox.due(datetime.now(timezone.utc))
    assert recovered[0]["deliveryId"] == "delivery-fixed"
    # The recovery claim is in-flight; expire its lease once more so retry_due
    # owns it and proves it sends the immutable stored payload.
    outbox.transition("delivery-fixed", expected_token=recovered[0]["leaseToken"], leaseExpiresAt=(datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat())

    seen = []
    async def send_payload(value):
        seen.append(value)
        return {"success": True}
    monkeypatch.setattr(outbox_module, "AntonOutbox", lambda: outbox)
    monkeypatch.setattr(platform, "send_payload", send_payload)
    assert platform.retry_due() == 1
    assert seen == [payload]


def test_live_and_standalone_preserve_the_same_delivery_context(monkeypatch):
    plugin = load_plugin()
    platform = importlib.import_module(plugin.__name__ + ".platform")
    from cron.delivery_context import CronDeliveryContext

    context = CronDeliveryContext(
        "cron-a", "run-fixed", "delivery-fixed",
        "anton:conversation_0123456789abcdef0123456789abcdef", "2025-01-01T00:00:00+00:00",
    )
    seen = []

    async def send_delivery(content, received_context, thread_id=None):
        seen.append((content, received_context.delivery_id, thread_id))
        return {"success": True, "message_id": "accepted"}

    monkeypatch.setattr(platform, "send_delivery", send_delivery)
    live = object.__new__(platform.AntonAdapter)
    live_result = asyncio.run(live.send(
        "conversation_0123456789abcdef0123456789abcdef", "finished",
        metadata={"delivery_context": context},
    ))
    standalone_result = asyncio.run(platform.standalone_sender(
        None, "conversation_0123456789abcdef0123456789abcdef", "finished",
        delivery_context=context,
    ))
    assert live_result.success and standalone_result["success"]
    assert seen == [("finished", "delivery-fixed", None), ("finished", "delivery-fixed", None)]


def test_tick_calls_registered_bounded_retry_hook(monkeypatch, tmp_path):
    called = []
    monkeypatch.setattr(scheduler, "_get_lock_paths", lambda: (tmp_path, tmp_path / "tick.lock"))
    monkeypatch.setattr(scheduler, "get_due_jobs", lambda: [])
    from gateway.platform_registry import platform_registry
    monkeypatch.setattr(platform_registry, "get", lambda name: SimpleNamespace(cron_retry_due_fn=lambda limit: called.append((name, limit))))
    assert scheduler.tick(verbose=False) == 0
    assert called == [("anton", 10)]


@pytest.mark.asyncio
async def test_sync_cron_hook_does_not_create_delegation_coroutine_in_async_context(monkeypatch):
    plugin = load_plugin()
    outbox_module = importlib.import_module(plugin.__name__ + ".outbox")
    platform = importlib.import_module(plugin.__name__ + ".platform")

    class EmptyCronOutbox:
        def due(self, *, limit):
            return []

    called = []
    async def delegation_drain(*, limit):
        called.append(limit)

    monkeypatch.setattr(outbox_module, "AntonOutbox", EmptyCronOutbox)
    monkeypatch.setattr(platform, "detached_completion_sink_ready", lambda: True)
    monkeypatch.setattr(platform, "retry_delegation_due", delegation_drain)
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        assert platform.retry_due(limit=2) == 0
    assert called == []
    assert not [warning for warning in captured if "never awaited" in str(warning.message)]


def test_all_excludes_anton(monkeypatch):
    monkeypatch.setattr(scheduler, "_iter_home_target_platforms", lambda: ["telegram", "anton"])
    monkeypatch.setattr(scheduler, "_get_home_target_chat_id", lambda name: "home" if name in {"telegram", "anton"} else None)
    assert scheduler._resolve_delivery_targets({"deliver": "all"}) == [
        {"platform": "telegram", "chat_id": "home", "thread_id": None}
    ]


def test_initial_retryable_anton_failure_retries_immutable_payload_without_rerunning_agent(monkeypatch, tmp_path):
    """A classified first send failure stays pending and retry_due reuses it."""
    plugin = load_plugin()
    outbox_module = importlib.import_module(plugin.__name__ + ".outbox")
    platform_module = importlib.import_module(plugin.__name__ + ".platform")
    outbox = outbox_module.AntonOutbox(tmp_path / "outbox.json")
    monkeypatch.setattr(outbox_module, "AntonOutbox", lambda: outbox)
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)
    from gateway.platform_registry import PlatformEntry, platform_registry
    monkeypatch.setitem(
        platform_registry._entries, "anton",
        PlatformEntry("anton", "ANTON", lambda _config: None, lambda: True),
    )
    import gateway.config
    monkeypatch.setattr(gateway.config, "load_gateway_config", lambda: SimpleNamespace(platforms={}))

    initial_sends = []

    async def transient_failure(*_args, **_kwargs):
        initial_sends.append(True)
        return {"error": "connection reset", "code": "network", "retryable": True}

    monkeypatch.setattr("tools.send_message_tool._send_via_adapter", transient_failure)
    job = {"id": "cron-retry", "deliver": "anton:conversation_0123456789abcdef0123456789abcdef"}
    assert scheduler._deliver_result(job, "finished") == "ANTON delivery pending/failed: network"
    assert initial_sends == [True]

    record = outbox._read()[0]
    assert record["state"] == "pending"
    assert record["attempts"] == 1
    assert record["errorCode"] == "network"
    delivery_id, payload = record["deliveryId"], record["payload"]
    # It is pending after the first owned send; make the backoff due without
    # creating another lease, so retry_due itself performs the claim.
    rows = outbox._read()
    rows[0]["nextAttemptAt"] = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    outbox._write(rows)

    retried_payloads = []

    async def deliver_stored_payload(value):
        retried_payloads.append(value)
        return {"success": True}

    monkeypatch.setattr(platform_module, "send_payload", deliver_stored_payload)
    assert platform_module.retry_due() == 1
    assert initial_sends == [True]  # retry does not run the cron agent/send path
    assert retried_payloads == [payload]
    delivered = outbox._read()[0]
    assert delivered["deliveryId"] == delivery_id
    assert delivered["payload"] == payload
    assert delivered["state"] == "delivered"


def test_initial_permanent_anton_failure_dead_letters(monkeypatch, tmp_path):
    plugin = load_plugin()
    outbox_module = importlib.import_module(plugin.__name__ + ".outbox")
    platform_module = importlib.import_module(plugin.__name__ + ".platform")
    client_module = importlib.import_module(plugin.__name__ + ".client")
    outbox = outbox_module.AntonOutbox(tmp_path / "outbox.json")
    monkeypatch.setattr(outbox_module, "AntonOutbox", lambda: outbox)
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)
    from gateway.platform_registry import PlatformEntry, platform_registry
    monkeypatch.setitem(
        platform_registry._entries, "anton",
        PlatformEntry(
            "anton", "ANTON", lambda _config: None, lambda: True,
            standalone_sender_fn=platform_module.standalone_sender,
        ),
    )
    import gateway.config
    monkeypatch.setattr(gateway.config, "load_gateway_config", lambda: SimpleNamespace(platforms={}))

    async def permanent_failure(*_args, **_kwargs):
        # Direct ANTON validation/auth HTTP 4xx classifications stay permanent.
        raise client_module.AntonTransportError("http_401", retryable=False)

    monkeypatch.setattr(platform_module, "send_delivery", permanent_failure)
    job = {"id": "cron-permanent", "deliver": "anton:conversation_0123456789abcdef0123456789abcdef"}
    assert scheduler._deliver_result(job, "finished") == "ANTON delivery pending/failed: http_401"
    record = outbox._read()[0]
    assert record["state"] == "dead_letter"
    assert record["attempts"] == 1
    assert record["errorCode"] == "http_401"


def test_live_adapter_timeout_cancels_and_retries_before_lease_expiry(monkeypatch, tmp_path):
    """A live send may use ANTON's full transport timeout without dead-lettering."""
    plugin = load_plugin()
    config_module = importlib.import_module(plugin.__name__ + ".config")
    outbox_module = importlib.import_module(plugin.__name__ + ".outbox")
    outbox = outbox_module.AntonOutbox(tmp_path / "outbox.json")
    monkeypatch.setattr(outbox_module, "AntonOutbox", lambda: outbox)

    class TimeoutFuture:
        timeout = None
        cancelled = False

        def result(self, timeout):
            self.timeout = timeout
            raise concurrent.futures.TimeoutError()

        def cancel(self):
            self.cancelled = True
            return True

    future = TimeoutFuture()

    class Router:
        def __init__(self, *_args):
            pass

        async def _deliver_to_platform(self, *_args):
            return None

    def schedule(coro, _loop):
        coro.close()
        return future

    monkeypatch.setattr("gateway.delivery.DeliveryRouter", Router)
    monkeypatch.setattr("agent.async_utils.safe_schedule_threadsafe", schedule)
    monkeypatch.setattr(
        "gateway.config.load_gateway_config", lambda: SimpleNamespace(platforms={})
    )
    from gateway.platform_registry import PlatformEntry, platform_registry
    monkeypatch.setitem(
        platform_registry._entries, "anton",
        PlatformEntry("anton", "ANTON", lambda _config: None, lambda: True),
    )
    from gateway.config import Platform

    job = {"id": "cron-live-timeout", "deliver": "anton:conversation_0123456789abcdef0123456789abcdef"}
    adapters = {Platform("anton"): object()}
    loop = SimpleNamespace(is_running=lambda: True)

    assert scheduler._deliver_result(job, "finished", adapters=adapters, loop=loop) == (
        "ANTON delivery pending/failed: timeout"
    )
    assert future.timeout >= config_module.MAX_TRANSPORT_TIMEOUT_SECONDS
    assert future.timeout < outbox.MIN_LEASE_SECONDS
    assert future.cancelled is True
    record = outbox._read()[0]
    assert record["state"] == "pending"
    assert record["attempts"] == 1
    assert record["errorCode"] == "timeout"


def test_stale_outbox_owner_cannot_settle_a_newer_lease(tmp_path):
    plugin = load_plugin()
    outbox_module = importlib.import_module(plugin.__name__ + ".outbox")
    outbox = outbox_module.AntonOutbox(tmp_path / "outbox.json")
    first = outbox.create_and_claim({
        "deliveryId": "delivery-fenced", "scheduleId": "cron-a", "executionId": "run-a",
        "deliveryTarget": "anton:conversation_0123456789abcdef0123456789abcdef",
        "payload": {"schemaVersion": "1"},
    })
    outbox.transition(
        "delivery-fenced", expected_token=first["leaseToken"],
        leaseExpiresAt=(datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),
    )
    second = outbox.due()[0]
    assert second["leaseToken"] != first["leaseToken"]
    assert second["leaseVersion"] == first["leaseVersion"] + 1
    assert outbox.transition("delivery-fenced", expected_token=first["leaseToken"], state="delivered") is None
    assert outbox.retry_or_dead_letter("delivery-fenced", first["leaseToken"], "network", True) is None
    assert outbox._read()[0]["state"] == "in_flight"
