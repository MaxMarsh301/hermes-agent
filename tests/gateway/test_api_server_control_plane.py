"""Typed Hermes control-plane contracts for sessions and Runs."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter
from hermes_state import SessionDB


@pytest.fixture
def control_plane(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home"))
    db = SessionDB(tmp_path / "state.db")
    adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"key": "test-secret"}))
    adapter._session_db = db
    try:
        yield adapter, db
    finally:
        db.close()


def _app(adapter):
    app = web.Application()
    app.router.add_get("/v1/capabilities", adapter._handle_capabilities)
    app.router.add_post("/api/sessions/{session_id}/compress", adapter._handle_session_compress)
    app.router.add_patch("/api/sessions/{session_id}/settings", adapter._handle_session_settings)
    app.router.add_get("/api/sessions/{session_id}/runtime-status", adapter._handle_session_runtime_status)
    app.router.add_get("/api/sessions/{session_id}/background-tasks", adapter._handle_session_background_tasks)
    app.router.add_post("/v1/runs/{run_id}/steer", adapter._handle_steer_run)
    app.router.add_post("/v1/runs/{run_id}/queue", adapter._handle_queue_run)
    app.router.add_get("/v1/runs/{run_id}/queue", adapter._handle_list_run_queue)
    app.router.add_delete("/v1/runs/{run_id}/queue/{item_id}", adapter._handle_cancel_run_queue_item)
    return app


def _auth():
    return {"Authorization": "Bearer test-secret"}


def _seed_transcript(db, session_id="session-a"):
    db.create_session(session_id, "api_server", model="hermes-agent")
    for role, content in (
        ("user", "one"), ("assistant", "two"),
        ("user", "three"), ("assistant", "four"),
    ):
        db.append_message(session_id, role, content)
    return session_id


@pytest.mark.asyncio
async def test_control_routes_require_auth_and_are_advertised(control_plane):
    adapter, db = control_plane
    _seed_transcript(db)
    async with TestClient(TestServer(_app(adapter))) as client:
        assert (await client.patch("/api/sessions/session-a/settings", json={"fastMode": False})).status == 401
        response = await client.get("/v1/capabilities", headers=_auth())
        payload = await response.json()
    for feature in ("run_steer", "durable_run_queue", "session_compression", "session_settings", "session_background_tasks"):
        assert payload["features"][feature] is True
    assert payload["features"]["background_task_cancellation"] is False
    assert payload["endpoints"]["run_queue_create"]["path"] == "/v1/runs/{run_id}/queue"


@pytest.mark.asyncio
async def test_compression_preview_is_strict_side_effect_free_and_active_safe(control_plane):
    adapter, db = control_plane
    _seed_transcript(db)
    adapter._create_agent = MagicMock(side_effect=AssertionError("preview constructed an agent"))
    async with TestClient(TestServer(_app(adapter))) as client:
        unknown = await client.post(
            "/api/sessions/session-a/compress", headers=_auth(),
            json={"mode": "standard", "surprise": True},
        )
        assert unknown.status == 400
        preview = await client.post(
            "/api/sessions/session-a/compress", headers=_auth(),
            json={"mode": "standard", "keepRecentExchanges": 1, "preview": True},
        )
        body = await preview.json()
        assert preview.status == 200
        assert body["preview"] is True and body["eligible"] is True
        assert body["before"]["measurement"] == "estimated"
        assert db._conn.execute("SELECT COUNT(*) FROM context_states").fetchone()[0] == 0
        adapter._run_statuses["run-live"] = {
            "status": "running", "logical_session_id": "session-a",
        }
        conflict = await client.post(
            "/api/sessions/session-a/compress", headers=_auth(),
            json={"mode": "standard", "preview": True},
        )
        assert conflict.status == 409


@pytest.mark.asyncio
async def test_compression_executes_and_rolls_back_without_leaking_provider_errors(control_plane):
    adapter, db = control_plane
    _seed_transcript(db)
    fake = SimpleNamespace(
        session_id="session-a",
        _last_compaction_in_place=False,
        _compress_context=lambda *args, **kwargs: ([
            {"role": "user", "content": "summary request"},
            {"role": "assistant", "content": "summary"},
        ], None),
    )
    adapter._create_agent = lambda **kwargs: fake
    async with TestClient(TestServer(_app(adapter))) as client:
        response = await client.post(
            "/api/sessions/session-a/compress", headers=_auth(),
            json={"mode": "standard"},
        )
        body = await response.json()
        assert response.status == 200
        assert body["originalSessionId"] == body["resultingSessionId"] == "session-a"
        assert body["compressionCount"] == 1
        assert body["after"]["measurement"] == "estimated"

        original = db.get_messages_as_conversation("session-a")
        fake._compress_context = lambda messages, *args, **kwargs: (messages, None)
        no_progress = await client.post(
            "/api/sessions/session-a/compress", headers=_auth(),
            json={"mode": "standard"},
        )
        assert no_progress.status == 500
        assert db.get_messages_as_conversation("session-a") == original
        assert db.get_or_create_context_state("session-a")["compression_epoch"] == 1

        secret = "upstream token sk-provider-secret"
        def fail_after_mutation(*args, **kwargs):
            db.replace_messages("session-a", [{"role": "user", "content": "corrupt"}])
            raise RuntimeError(secret)
        fake._compress_context = fail_after_mutation
        failed = await client.post(
            "/api/sessions/session-a/compress", headers=_auth(),
            json={"mode": "standard"},
        )
        failed_body = await failed.json()
        assert failed.status == 500
        assert secret not in json.dumps(failed_body)
        assert db.get_messages_as_conversation("session-a") == original


@pytest.mark.asyncio
async def test_settings_are_allowlisted_isolated_and_drive_agent_construction(control_plane):
    adapter, db = control_plane
    db.create_session("one", "api_server", model="old", model_config={"preserve": {"x": 1}})
    db.create_session("two", "api_server", model="old")
    adapter._model_name = "primary"
    adapter._model_routes = {
        "fast-route": {"model": "gpt-5.6", "provider": "openai", "api_key": "private-upstream"},
    }
    async with TestClient(TestServer(_app(adapter))) as client:
        arbitrary = await client.patch(
            "/api/sessions/one/settings", headers=_auth(), json={"model": "made-up"},
        )
        assert arbitrary.status == 400
        response = await client.patch(
            "/api/sessions/one/settings", headers=_auth(),
            json={"model": "fast-route", "reasoningEffort": "high", "fastMode": True},
        )
        payload = await response.json()
        assert response.status == 200
        assert payload["configured"] == {
            "model": "fast-route", "reasoningEffort": "high", "fastMode": True,
        }
        runtime = await (await client.get(
            "/api/sessions/one/runtime-status", headers=_auth(),
        )).json()
        assert runtime["configuredModel"] == "fast-route"
        assert runtime["effectiveModel"] == "gpt-5.6"
        assert runtime["reasoningEffort"] == "high"
        assert runtime["fastMode"] is True
        adapter._run_statuses["live"] = {"status": "running", "session_id": "one"}
        assert (await client.patch(
            "/api/sessions/one/settings", headers=_auth(), json={"fastMode": False},
        )).status == 409

    saved = json.loads(db.get_session("one")["model_config"])
    assert saved["preserve"] == {"x": 1}
    assert "api_key" not in saved["api_session_overrides"]
    assert adapter._session_control_overrides("two") == {}

    constructed = MagicMock()
    with (
        patch("run_agent.AIAgent", return_value=constructed) as agent_type,
        patch("gateway.run._resolve_runtime_agent_kwargs", return_value={"api_key": "default"}),
        patch("gateway.run._resolve_runtime_agent_kwargs_for_provider", return_value={"api_key": "resolved"}),
        patch("gateway.run._resolve_gateway_model", return_value="primary-model"),
        patch("gateway.run._load_gateway_config", return_value={}),
        patch("gateway.run._current_max_iterations", return_value=10),
        patch("gateway.run.GatewayRunner._load_reasoning_config", return_value=None),
        patch("gateway.run.GatewayRunner._load_service_tier", return_value=None),
        patch("gateway.run.GatewayRunner._load_fallback_model", return_value=None),
        patch("hermes_cli.tools_config._get_platform_tools", return_value=[]),
    ):
        adapter._run_statuses.clear()
        adapter._create_agent(session_id="one", settings_session_id="one")
        kwargs = agent_type.call_args.kwargs
    assert kwargs["model"] == "gpt-5.6"
    assert kwargs["reasoning_config"] == {"effort": "high"}
    assert kwargs["service_tier"] == "priority"
    assert kwargs["request_overrides"] == {"service_tier": "priority"}
    assert kwargs["api_key"] == "private-upstream"


@pytest.mark.asyncio
async def test_steer_reports_truthful_acceptance_and_rejects_nonrunning_states(control_plane):
    adapter, _ = control_plane
    agent = MagicMock()
    agent.steer.return_value = True
    adapter._active_run_agents["run_live"] = agent
    adapter._run_statuses["run_live"] = {"status": "running"}
    adapter._run_statuses["run_wait"] = {"status": "waiting_for_approval"}
    async with TestClient(TestServer(_app(adapter))) as client:
        accepted = await client.post(
            "/v1/runs/run_live/steer", headers=_auth(), json={"text": "change direction"},
        )
        assert accepted.status == 202
        assert await accepted.json() == {
            "object": "hermes.run.steer", "runId": "run_live",
            "state": "accepted", "accepted": True, "applied": False,
        }
        waiting = await client.post(
            "/v1/runs/run_wait/steer", headers=_auth(), json={"text": "no"},
        )
        assert waiting.status == 409
        invalid = await client.post(
            "/v1/runs/run_live/steer", headers=_auth(), json={"text": "x", "extra": 1},
        )
        assert invalid.status == 400


@pytest.mark.asyncio
async def test_durable_queue_lists_cancels_and_dispatches_after_terminal(control_plane):
    adapter, db = control_plane
    _seed_transcript(db, "logical")
    db.get_or_create_context_state("logical")
    adapter._run_statuses["parent"] = {
        "status": "running", "logical_session_id": "logical",
        "effective_session_id": "logical", "session_id": "logical",
    }
    adapter._set_run_status("parent", "running", logical_session_id="logical", effective_session_id="logical")
    async with TestClient(TestServer(_app(adapter))) as client:
        first = await client.post(
            "/v1/runs/parent/queue", headers={**_auth(), "Idempotency-Key": "same"},
            json={"content": "follow up", "attachments": []},
        )
        assert first.status == 202
        first_body = await first.json()
        assert first_body["state"] == "queued" and "dispatchRunId" not in first_body
        repeated = await client.post(
            "/v1/runs/parent/queue", headers={**_auth(), "Idempotency-Key": "same"},
            json={"content": "follow up", "attachments": []},
        )
        assert repeated.status == 200
        assert (await repeated.json())["itemId"] == first_body["itemId"]
        second = await client.post(
            "/v1/runs/parent/queue", headers=_auth(), json={"content": "cancel me"},
        )
        second_id = (await second.json())["itemId"]
        listing = await (await client.get("/v1/runs/parent/queue", headers=_auth())).json()
        assert [item["position"] for item in listing["data"]] == [1, 2]
        cancelled = await client.delete(
            f"/v1/runs/parent/queue/{second_id}", headers=_auth(),
        )
        assert cancelled.status == 200
        assert (await cancelled.json())["state"] == "cancelled"

    adapter._run_agent = MagicMock()
    async def complete(**kwargs):
        return {"final_response": "done", "session_id": "logical"}, {"total_tokens": 1}
    adapter._run_agent.side_effect = complete
    adapter._set_run_status("parent", "completed", logical_session_id="logical", effective_session_id="logical")
    adapter._schedule_api_queue_dispatch("logical")
    await adapter._queue_dispatch_tasks["logical"]
    assert db.get_api_queue_item(first_body["itemId"])["state"] == "completed"
    assert db.get_api_queue_item(second_id)["state"] == "cancelled"
    assert adapter._run_agent.call_count == 1


@pytest.mark.asyncio
async def test_queue_recovery_reclaims_once_and_stop_policy_keeps_queued(control_plane, tmp_path, monkeypatch):
    adapter, db = control_plane
    _seed_transcript(db, "recover")
    db.get_or_create_context_state("recover")
    db.save_api_run("old-parent", "recover", "recover", 0, "running")
    item_id = "queue_" + "a" * 32
    db.reserve_api_queue_item(
        item_id=item_id, parent_run_id="old-parent", logical_session_id="recover",
        session_key="recover", content="resume", attachment_ids=[], attachment_snapshot=[],
        dispatch_run_id="run_" + "b" * 32, idempotency_scope=None,
        idempotency_key=None, body_digest=None,
    )
    db.set_api_queue_item_state(item_id, "running")
    second = APIServerAdapter(PlatformConfig(enabled=True, extra={"key": "test-secret"}))
    second._session_db = db
    calls = 0
    async def complete(**kwargs):
        nonlocal calls
        calls += 1
        return {"final_response": "recovered", "session_id": "recover"}, {}
    second._run_agent = complete
    await second._recover_api_run_queue()
    await second._queue_dispatch_tasks["recover"]
    assert calls == 1
    assert db.get_api_run("old-parent")["status"] == "failed"
    assert db.get_api_queue_item(item_id)["state"] == "completed"

    db.save_api_run("stopped-parent", "recover", "recover", 1, "running")
    queued_id = "queue_" + "c" * 32
    db.reserve_api_queue_item(
        item_id=queued_id, parent_run_id="stopped-parent", logical_session_id="recover",
        session_key="recover", content="keep", attachment_ids=[], attachment_snapshot=[],
        dispatch_run_id="run_" + "d" * 32, idempotency_scope=None,
        idempotency_key=None, body_digest=None,
    )
    second._run_statuses["stopped-parent"] = {"status": "running"}
    second._active_run_tasks["stopped-parent"] = asyncio.current_task()
    request_stop_agent = MagicMock()
    second._active_run_agents["stopped-parent"] = request_stop_agent
    # Stop interrupts only the parent; the durable follow-up remains queued.
    request_stop_agent.interrupt("stop")
    assert db.get_api_queue_item(queued_id)["state"] == "queued"


@pytest.mark.asyncio
async def test_background_tasks_projection_excludes_prompts_results_and_tool_output(control_plane):
    adapter, db = control_plane
    db.create_session("background", "api_server")
    db._conn.execute(
        """INSERT INTO async_delegations (
               delegation_id, origin_session, origin_ui_session_id,
               parent_session_id, state, dispatched_at, updated_at,
               task_json, event_json, result_json
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "deleg_safe", "origin", "background", "background", "completed", 1, 2,
            json.dumps({"prompt": "PRIVATE PROMPT"}),
            json.dumps({"tool_output": "PRIVATE TOOL"}),
            json.dumps({"result": "PRIVATE RESULT"}),
        ),
    )
    db._conn.commit()
    async with TestClient(TestServer(_app(adapter))) as client:
        response = await client.get(
            "/api/sessions/background/background-tasks", headers=_auth(),
        )
        payload = await response.json()
    assert response.status == 200
    item = payload["data"][0]
    assert item.pop("elapsedSeconds") >= 0
    assert item == {
        "id": "deleg_safe", "state": "completed", "goal": "Delegated task",
        "goalPublic": True,
        "createdAt": 1.0, "updatedAt": 2.0, "completedAt": None,
        "toolCallCount": None, "parentSessionId": "background",
        "parentRunId": None,
        "summary": "Delegated work reached a terminal state.",
        "summaryPublic": True,
    }
    serialized = json.dumps(payload)
    for private in ("PRIVATE PROMPT", "PRIVATE TOOL", "PRIVATE RESULT", "task_json", "result_json"):
        assert private not in serialized
