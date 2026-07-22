"""Tests for /v1/runs endpoints: start, status, events, and stop.

Covers:
- POST /v1/runs — start a run (202)
- GET /v1/runs/{run_id} — poll run status
- GET /v1/runs/{run_id}/events — SSE event stream
- POST /v1/runs/{run_id}/stop — interrupt a running agent
- Auth, error handling, and cleanup
"""

import asyncio
import hashlib
import re
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from aiohttp import FormData, web
from aiohttp.test_utils import TestClient, TestServer

from gateway.config import PlatformConfig
from gateway.platforms.api_server import (
    APIServerAdapter,
    _RunEventBuffer,
    _approval_event_choices,
    _public_approval_request,
    cors_middleware,
    security_headers_middleware,
)
from tools import approval as approval_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("smart_denied", "allow_permanent", "expected"),
    [
        (False, True, ["once", "session", "deny"]),
        (False, False, ["once", "session", "deny"]),
        (True, True, ["once", "session", "deny"]),
        (True, False, ["once", "session", "deny"]),
    ],
)
def test_approval_event_choices_follow_backend_capabilities(
    smart_denied, allow_permanent, expected
):
    assert _approval_event_choices(
        smart_denied=smart_denied,
        allow_permanent=allow_permanent,
    ) == expected


def test_public_approval_request_is_opaque_allowlisted_and_never_reflects_raw_metadata():
    approval_id = "approval_" + "A" * 32
    event = _public_approval_request({
        "approval_id": approval_id,
        "description": "<think>reasoning</think> curl https://token:SECRET@example.test /private/SECRET",
        "pattern_key": "raw-rule",
        "args": {"Authorization": "Bearer SECRET"},
    })

    assert event == {
        "approvalId": approval_id,
        "action": "terminal.command",
        "summary": "Выполнить действие в терминале",
        "purpose": "Подтвердить потенциально опасное действие для текущей задачи",
        "riskCode": "other_dangerous_action",
        "choices": ["once", "session", "deny"],
    }
    public = repr(event)
    for private in ("SECRET", "sudo", "rm -rf", "/private/", "https://", "reasoning", "raw-rule", "args"):
        assert private not in public


def test_public_memory_skill_and_review_metadata_never_reflect_content():
    memory = APIServerAdapter._public_memory_tool_fields(
        {"target": "memory", "operations": [{"action": "add", "content": "token=SECRET"}]},
        '{"success": true, "current_chars": 42}',
    )
    assert memory == {"memory_target": "memory", "operation_count": 1, "staged": False, "committed": True}

    skill = APIServerAdapter._public_skill_tool_fields(
        {"action": "patch", "name": "cross-repo-signed-delivery", "new_string": "SECRET"},
        '{"success": true, "message": "Patched SKILL.md in skill \'cross-repo-signed-delivery\' (1 replacement).", "diff": "SECRET"}',
    )
    assert skill == {"skill_action": "patch", "staged": False, "committed": True, "replacement_count": 1}

    review = APIServerAdapter._public_review_summary(
        "💾 Self-improvement review: Memory updated · 📝 Patched SKILL.md in skill 'cross-repo-signed-delivery' (1 replacement)."
    )
    assert review == {
        "event": "review.summary",
        "review_status": "changed",
        "memory_updated": True,
        "skill_updates": [{"name": "cross-repo-signed-delivery", "action": "patch", "replacements": 1}],
        "change_count": 2,
    }
    assert "SECRET" not in repr((memory, skill, review))
    assert APIServerAdapter._public_review_summary("💾 Self-improvement review: token=SECRET") is None
    assert APIServerAdapter._public_memory_tool_fields(
        {"target": "memory", "action": "add"}, '{"success": true, "staged": true}'
    )["staged"] is True
    assert APIServerAdapter._public_review_summary(
        "💾 Self-improvement review: Skill 'new-skill' created."
    )["skill_updates"] == [{"name": "new-skill", "action": "create"}]
    assert APIServerAdapter._public_review_summary(
        "💾 Self-improvement review: No changes."
    )["review_status"] == "no_changes"


def test_public_approval_command_preview_preserves_benign_and_dangerous_shell_structure():
    approval_id = "approval_" + "A" * 32
    benign = "printf '%s\\n' \"$HOME\" | sed -n '1p'\nprintf 'done'"
    dangerous = "sudo rm -rf -- /tmp/target && echo done"

    assert _public_approval_request({"approval_id": approval_id, "command": benign})["commandPreview"] == benign
    assert _public_approval_request({"approval_id": approval_id, "command": dangerous})["commandPreview"] == dangerous


def test_public_approval_command_preview_forces_credential_redaction():
    command = "curl -H 'Authorization: Bearer sk-live-SECRET' https://example.test/api"
    event = _public_approval_request({"approval_id": "approval_" + "A" * 32, "command": command})

    preview = event["commandPreview"]
    assert "sk-live-SECRET" not in preview
    assert "Authorization: Bearer ***" in preview
    assert "curl -H" in preview and "https://example.test/api" in preview


def test_public_approval_command_preview_is_control_safe_utf8_bounded_and_truncated():
    command = "\n".join(f"printf '界' # {index} " + "界" * 500 for index in range(13)) + "\x1b"
    event = _public_approval_request({"approval_id": "approval_" + "A" * 32, "command": command})

    preview = event["commandPreview"]
    assert event["commandTruncated"] is True
    assert len(preview.encode("utf-8")) <= 2048
    assert len(preview.split("\n")) <= 12
    assert "\x1b" not in preview
    assert preview.encode("utf-8").decode("utf-8") == preview


def test_public_approval_command_preview_omits_missing_or_empty_command():
    for approval_data in ({"approval_id": "approval_" + "A" * 32}, {"approval_id": "approval_" + "A" * 32, "command": ""}):
        event = _public_approval_request(approval_data)
        assert "commandPreview" not in event
        assert "commandTruncated" not in event


def _make_adapter(api_key: str = "") -> APIServerAdapter:
    """Create an adapter with optional API key."""
    extra = {}
    if api_key:
        extra["key"] = api_key
    config = PlatformConfig(enabled=True, extra=extra)
    adapter = APIServerAdapter(config)
    return adapter


def _create_runs_app(adapter: APIServerAdapter) -> web.Application:
    """Create an aiohttp app with /v1/runs routes registered."""
    mws = [mw for mw in (cors_middleware, security_headers_middleware) if mw is not None]
    app = web.Application(middlewares=mws)
    app["api_server_adapter"] = adapter
    app.router.add_post("/v1/uploads", adapter._handle_upload)
    app.router.add_delete("/v1/uploads/{file_id}", adapter._handle_delete_upload)
    app.router.add_post("/v1/runs", adapter._handle_runs)
    app.router.add_get("/v1/runs/{run_id}", adapter._handle_get_run)
    app.router.add_get("/v1/runs/{run_id}/events", adapter._handle_run_events)
    app.router.add_get("/v1/runs/{run_id}/artifacts", adapter._handle_run_artifacts)
    app.router.add_get("/v1/runs/{run_id}/artifacts/{artifact_id}/download", adapter._handle_download_artifact)
    app.router.add_post("/v1/runs/{run_id}/approval", adapter._handle_run_approval)
    app.router.add_post("/v1/runs/{run_id}/clarification", adapter._handle_run_clarification)
    app.router.add_post("/v1/runs/{run_id}/stop", adapter._handle_stop_run)
    return app


def _make_slow_agent(**kwargs):
    """Create a mock agent that blocks in run_conversation until interrupted.

    Returns (mock_agent, agent_ready_event, interrupt_event) where
    agent_ready_event is set once run_conversation starts, and
    interrupt_event is set when interrupt() is called.
    """
    ready = threading.Event()
    interrupted = threading.Event()

    mock_agent = MagicMock()

    def _do_interrupt(message=None):
        interrupted.set()

    mock_agent.interrupt = MagicMock(side_effect=_do_interrupt)

    def _slow_run(user_message=None, conversation_history=None, task_id=None):
        ready.set()
        # Block until interrupt() is called
        interrupted.wait(timeout=10)
        return {"final_response": "interrupted"}

    mock_agent.run_conversation.side_effect = _slow_run
    mock_agent.session_prompt_tokens = 0
    mock_agent.session_completion_tokens = 0
    mock_agent.session_total_tokens = 0

    return mock_agent, ready, interrupted


@pytest.fixture
def adapter():
    return _make_adapter()


@pytest.fixture
def auth_adapter():
    return _make_adapter(api_key="sk-secret")


# ---------------------------------------------------------------------------
# POST /v1/runs — start a run
# ---------------------------------------------------------------------------


class TestStartRun:
    @pytest.mark.asyncio
    async def test_start_returns_202(self, adapter):
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            with patch.object(adapter, "_create_agent") as mock_create:
                mock_agent = MagicMock()
                mock_agent.run_conversation.return_value = {"final_response": "done"}
                mock_agent.session_prompt_tokens = 10
                mock_agent.session_completion_tokens = 5
                mock_agent.session_total_tokens = 15
                mock_create.return_value = mock_agent

                resp = await cli.post("/v1/runs", json={"input": "hello"})
                assert resp.status == 202
                data = await resp.json()
                assert data["status"] == "started"
                assert data["run_id"].startswith("run_")

                status_resp = await cli.get(f"/v1/runs/{data['run_id']}")
                assert status_resp.status == 200
                status = await status_resp.json()
                assert status["run_id"] == data["run_id"]
                assert status["status"] in {"queued", "running", "completed"}
                assert status["object"] == "hermes.run"

    @pytest.mark.asyncio
    async def test_background_review_summary_is_emitted_after_completed_before_stream_close(self, adapter):
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            with patch.object(adapter, "_create_agent") as mock_create:
                agent = MagicMock()
                agent.session_prompt_tokens = agent.session_completion_tokens = agent.session_total_tokens = 0

                def run_with_review(**_kwargs):
                    def review():
                        time.sleep(0.05)
                        agent.background_review_callback(
                            "💾 Self-improvement review: 📝 Patched SKILL.md in skill 'cross-repo-signed-delivery' (1 replacement)."
                        )

                    thread = threading.Thread(target=review, daemon=True)
                    agent._background_review_thread = thread
                    thread.start()
                    return {"final_response": "done"}

                agent.run_conversation.side_effect = run_with_review
                mock_create.return_value = agent

                started = await cli.post("/v1/runs", json={"input": "review"})
                run_id = (await started.json())["run_id"]
                response = await cli.get(f"/v1/runs/{run_id}/events")
                body = await response.text()

        completed_index = body.index('"event": "run.completed"')
        review_index = body.index('"event": "review.summary"')
        assert completed_index < review_index
        assert '"skill_updates": [{"name": "cross-repo-signed-delivery", "action": "patch", "replacements": 1}]' in body
        assert "stream closed" in body

    @pytest.mark.asyncio
    async def test_first_turn_emits_generated_title_after_completion(self, adapter):
        app = _create_runs_app(adapter)
        with patch("agent.title_generator.generate_title", return_value="Диагностика шлюза") as generate:
            async with TestClient(TestServer(app)) as cli:
                with patch.object(adapter, "_create_agent") as mock_create:
                    agent = MagicMock()
                    agent.run_conversation.return_value = {"final_response": "Готовый ответ"}
                    agent.session_prompt_tokens = agent.session_completion_tokens = agent.session_total_tokens = 0
                    mock_create.return_value = agent

                    first = await cli.post("/v1/runs", json={"input": "Почему упал шлюз?"})
                    first_id = (await first.json())["run_id"]
                    for _ in range(50):
                        first_events = adapter._run_streams[first_id].events_after(0)
                        if any(event.get("event") == "conversation.title" for event in first_events):
                            break
                        await asyncio.sleep(0.02)
                    assert [event["event"] for event in first_events][-2:] == ["run.completed", "conversation.title"]
                    assert first_events[-1]["title"] == "Диагностика шлюза"
                    generate.assert_called_once_with(
                        "Почему упал шлюз?", "Готовый ответ", timeout=8.0,
                    )

                    followup = await cli.post(
                        "/v1/runs",
                        json={"input": "Продолжай", "conversation_history": [{"role": "user", "content": "Первый вопрос"}]},
                    )
                    followup_id = (await followup.json())["run_id"]
                    for _ in range(50):
                        if adapter._run_statuses[followup_id]["status"] == "completed":
                            break
                        await asyncio.sleep(0.02)
                    await asyncio.sleep(0.05)
                    assert not any(
                        event.get("event") == "conversation.title"
                        for event in adapter._run_streams[followup_id].events_after(0)
                    )
                    assert generate.call_count == 1

    @pytest.mark.asyncio
    async def test_start_invalid_json_returns_400(self, adapter):
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            resp = await cli.post(
                "/v1/runs",
                data="not json",
                headers={"Content-Type": "application/json"},
            )
        assert resp.status == 400

    @pytest.mark.asyncio
    async def test_start_missing_input_returns_400(self, adapter):
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            resp = await cli.post("/v1/runs", json={"model": "test"})
            assert resp.status == 400
            data = await resp.json()
            assert "input" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_start_empty_input_returns_400(self, adapter):
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            resp = await cli.post("/v1/runs", json={"input": ""})
        assert resp.status == 400

    @pytest.mark.asyncio
    async def test_start_invalid_history_does_not_allocate_run(self, adapter):
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            resp = await cli.post(
                "/v1/runs",
                json={"input": "hello", "conversation_history": {"role": "user"}},
            )
        assert resp.status == 400
        assert adapter._run_streams == {}
        assert adapter._run_statuses == {}

    @pytest.mark.asyncio
    async def test_start_requires_auth(self, auth_adapter):
        app = _create_runs_app(auth_adapter)
        async with TestClient(TestServer(app)) as cli:
            resp = await cli.post("/v1/runs", json={"input": "hello"})
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_start_with_valid_auth(self, auth_adapter):
        app = _create_runs_app(auth_adapter)
        async with TestClient(TestServer(app)) as cli:
            with patch.object(auth_adapter, "_create_agent") as mock_create:
                mock_agent = MagicMock()
                mock_agent.run_conversation.return_value = {"final_response": "ok"}
                mock_agent.session_prompt_tokens = 0
                mock_agent.session_completion_tokens = 0
                mock_agent.session_total_tokens = 0
                mock_create.return_value = mock_agent

                resp = await cli.post(
                    "/v1/runs",
                    json={"input": "hello"},
                    headers={"Authorization": "Bearer sk-secret"},
                )
                assert resp.status == 202


# ---------------------------------------------------------------------------
# Phase 5 — bounded upload, multimodal run input, and artifacts
# ---------------------------------------------------------------------------


class TestRunFiles:
    @pytest.mark.asyncio
    async def test_text_upload_is_session_owned_and_becomes_run_input(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
        adapter = _make_adapter(api_key="sk-secret")
        app = _create_runs_app(adapter)
        headers = {"Authorization": "Bearer sk-secret", "X-Hermes-Session-Key": "anton-chat:conversation-a"}
        async with TestClient(TestServer(app)) as cli:
            form = FormData()
            form.add_field("file", b"alpha,beta\n1,2\n", filename="notes.csv", content_type="text/csv")
            uploaded = await cli.post("/v1/uploads", data=form, headers=headers)
            assert uploaded.status == 201
            file_data = await uploaded.json()
            assert file_data["id"].startswith("file_")
            assert file_data["kind"] == "text"

            with patch.object(adapter, "_create_agent") as mock_create:
                agent = MagicMock()
                agent.run_conversation.return_value = {"final_response": "done"}
                agent.session_prompt_tokens = agent.session_completion_tokens = agent.session_total_tokens = 0
                mock_create.return_value = agent
                started = await cli.post(
                    "/v1/runs", json={"input": "Summarize the attachment", "attachments": [file_data["id"]]}, headers=headers
                )
                assert started.status == 202
                run_id = (await started.json())["run_id"]
                for _ in range(30):
                    status = await (await cli.get(f"/v1/runs/{run_id}", headers=headers)).json()
                    if status["status"] == "completed":
                        break
                    await asyncio.sleep(0.02)
                assert status["status"] == "completed"
                user_message = mock_create.return_value.run_conversation.call_args.kwargs["user_message"]
                assert "Uploaded file: notes.csv" in user_message
                assert "alpha,beta" in user_message

            denied = await cli.post(
                "/v1/runs",
                json={"input": "steal", "attachments": [file_data["id"]]},
                headers={"Authorization": "Bearer sk-secret", "X-Hermes-Session-Key": "anton-chat:conversation-b"},
            )
            assert denied.status == 404

    @pytest.mark.asyncio
    async def test_image_upload_becomes_multimodal_part_and_artifact_download_is_owned(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
        adapter = _make_adapter(api_key="sk-secret")
        app = _create_runs_app(adapter)
        headers = {"Authorization": "Bearer sk-secret", "X-Hermes-Session-Key": "anton-chat:conversation-a"}
        async with TestClient(TestServer(app)) as cli:
            form = FormData()
            form.add_field("file", b"\x89PNG\r\n\x1a\nimage", filename="photo.png", content_type="image/png")
            uploaded = await cli.post("/v1/uploads", data=form, headers=headers)
            file_id = (await uploaded.json())["id"]
            with patch.object(adapter, "_create_agent") as mock_create:
                agent = MagicMock()
                agent.session_prompt_tokens = agent.session_completion_tokens = agent.session_total_tokens = 0

                def write_artifact(**kwargs):
                    prompt = mock_create.call_args.kwargs["ephemeral_system_prompt"]
                    artifact_dir = prompt.split("write it only under ", 1)[1].split(". Do not", 1)[0]
                    output = Path(artifact_dir) / "report.txt"
                    output.write_text("artifact contents", encoding="utf-8")
                    return {"final_response": f"Saved {output}"}

                agent.run_conversation.side_effect = write_artifact
                mock_create.return_value = agent
                started = await cli.post(
                    "/v1/runs", json={"input": "Describe and export", "attachments": [file_id]}, headers=headers
                )
                run_id = (await started.json())["run_id"]
                for _ in range(30):
                    status = await (await cli.get(f"/v1/runs/{run_id}", headers=headers)).json()
                    if status["status"] == "completed":
                        break
                    await asyncio.sleep(0.02)
                assert status["status"] == "completed"
                assert "<artifact>" in status["output"]
                assert len(status["artifacts"]) == 1
                parts = agent.run_conversation.call_args.kwargs["user_message"]
                assert isinstance(parts, list)
                assert parts[-1]["type"] == "image_url"

                listed = await cli.get(f"/v1/runs/{run_id}/artifacts", headers=headers)
                assert listed.status == 200
                artifact = (await listed.json())["data"][0]
                download = await cli.get(
                    f"/v1/runs/{run_id}/artifacts/{artifact['id']}/download", headers=headers
                )
                assert download.status == 200
                assert await download.text() == "artifact contents"
                foreign = await cli.get(
                    f"/v1/runs/{run_id}/artifacts/{artifact['id']}/download",
                    headers={"Authorization": "Bearer sk-secret", "X-Hermes-Session-Key": "anton-chat:conversation-b"},
                )
                assert foreign.status == 404



class TestRunStatus:
    @pytest.mark.asyncio
    async def test_status_completed_run_includes_output_and_usage(self, adapter):
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            with patch.object(adapter, "_create_agent") as mock_create:
                mock_agent = MagicMock()
                mock_agent.run_conversation.return_value = {"final_response": "done"}
                mock_agent.session_prompt_tokens = 4
                mock_agent.session_completion_tokens = 2
                mock_agent.session_total_tokens = 6
                mock_create.return_value = mock_agent

                resp = await cli.post("/v1/runs", json={"input": "hello"})
                data = await resp.json()
                run_id = data["run_id"]

                for _ in range(20):
                    status_resp = await cli.get(f"/v1/runs/{run_id}")
                    assert status_resp.status == 200
                    status = await status_resp.json()
                    if status["status"] == "completed":
                        break
                    await asyncio.sleep(0.05)

                assert status["status"] == "completed"
                assert status["output"] == "done"
                assert status["usage"]["total_tokens"] == 6
                assert status["last_event"] == "run.completed"

    @pytest.mark.asyncio
    async def test_status_reflects_explicit_session_id(self, adapter):
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            with patch.object(adapter, "_create_agent") as mock_create:
                mock_agent = MagicMock()
                mock_agent.run_conversation.return_value = {"final_response": "done"}
                mock_agent.session_prompt_tokens = 0
                mock_agent.session_completion_tokens = 0
                mock_agent.session_total_tokens = 0
                mock_create.return_value = mock_agent

                resp = await cli.post(
                    "/v1/runs",
                    json={"input": "hello", "session_id": "space-session"},
                )
                data = await resp.json()
                run_id = data["run_id"]

                for _ in range(20):
                    status_resp = await cli.get(f"/v1/runs/{run_id}")
                    status = await status_resp.json()
                    if status["status"] == "completed":
                        break
                    await asyncio.sleep(0.05)

                mock_agent.run_conversation.assert_called_once()
                assert mock_agent.run_conversation.call_args.kwargs["task_id"] == "space-session"
                assert status["session_id"] == "space-session"

    @pytest.mark.asyncio
    async def test_status_not_found_returns_404(self, adapter):
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            resp = await cli.get("/v1/runs/run_nonexistent")
        assert resp.status == 404

    @pytest.mark.asyncio
    async def test_status_requires_auth(self, auth_adapter):
        app = _create_runs_app(auth_adapter)
        async with TestClient(TestServer(app)) as cli:
            resp = await cli.get("/v1/runs/run_any")
        assert resp.status == 401


# ---------------------------------------------------------------------------
# GET /v1/runs/{run_id}/events — SSE event stream
# ---------------------------------------------------------------------------


class TestRunEvents:
    @pytest.mark.asyncio
    async def test_events_stream_returns_completed(self, adapter):
        """Events stream should receive run.completed when agent finishes."""
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            with patch.object(adapter, "_create_agent") as mock_create:
                mock_agent = MagicMock()
                mock_agent.run_conversation.return_value = {"final_response": "Hello!"}
                mock_agent.session_prompt_tokens = 10
                mock_agent.session_completion_tokens = 5
                mock_agent.session_total_tokens = 15
                mock_create.return_value = mock_agent

                # Start run
                resp = await cli.post("/v1/runs", json={"input": "hello"})
                assert resp.status == 202
                data = await resp.json()
                run_id = data["run_id"]

                # Subscribe to events
                events_resp = await cli.get(f"/v1/runs/{run_id}/events")
                assert events_resp.status == 200
                body = await events_resp.text()

                # Should contain run.completed
                assert "run.completed" in body
                assert "Hello!" in body

    @pytest.mark.asyncio
    async def test_answer_replay_keeps_content_but_excludes_tool_results_and_provider_errors(self, adapter):
        """Runs is bounded authenticated answer recovery, not a raw tool/provider trace."""
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            with patch.object(adapter, "_create_agent") as create_agent:
                agent = MagicMock()
                agent.session_prompt_tokens = agent.session_completion_tokens = agent.session_total_tokens = 0

                def fail_with_private_provider_error(**_kwargs):
                    started = create_agent.call_args.kwargs["tool_start_callback"]
                    completed = create_agent.call_args.kwargs["tool_complete_callback"]
                    started("call-1", "read_file", {"path": "/private/SECRET"})
                    completed("call-1", "read_file", {"path": "/private/SECRET"}, "provider SECRET result")
                    return {"failed": True, "error": "provider SECRET diagnostic"}

                agent.run_conversation.side_effect = fail_with_private_provider_error
                create_agent.return_value = agent
                started = await cli.post("/v1/runs", json={"input": "hello"})
                run_id = (await started.json())["run_id"]
                for _ in range(30):
                    if adapter._run_statuses[run_id]["status"] == "failed":
                        break
                    await asyncio.sleep(0.02)

        events = adapter._run_streams[run_id].events_after(0)
        assert any(event["event"] == "run.failed" for event in events)
        assert "provider SECRET" not in repr(events)
        assert all("result" not in event for event in events)
        assert all("error" not in event for event in events)

    @pytest.mark.asyncio
    async def test_events_reconnect_replays_then_continues_through_terminal_event(self, adapter):
        app = _create_runs_app(adapter)
        run_id = "run_reconnect"
        adapter._run_streams[run_id] = _RunEventBuffer(asyncio.get_running_loop(), max_events=8)
        adapter._run_streams_created[run_id] = time.time()
        adapter._run_statuses[run_id] = {"run_id": run_id, "status": "running"}

        adapter._emit_run_event(run_id, {"event": "run.started", "run_id": run_id})
        await asyncio.sleep(0)
        async with TestClient(TestServer(app)) as cli:
            first = await cli.get(f"/v1/runs/{run_id}/events")
            assert b"run.started" in await first.content.readuntil(b"\n\n")
            first.close()
            await asyncio.sleep(0)
            assert run_id in adapter._run_streams

            adapter._emit_run_event(run_id, {"event": "run.completed", "run_id": run_id})
            adapter._close_run_event_emitter(run_id)
            await asyncio.sleep(0)
            replay = await cli.get(f"/v1/runs/{run_id}/events")
            body = await replay.text()

        assert body.index("run.started") < body.index("run.completed")
        assert ": stream closed" in body

    @pytest.mark.asyncio
    async def test_event_replay_retention_is_bounded_and_keeps_newest_events(self, adapter):
        run_id = "run_bounded_replay"
        adapter._run_streams[run_id] = _RunEventBuffer(asyncio.get_running_loop(), max_events=2)
        adapter._emit_run_event(run_id, {"event": "run.started", "run_id": run_id})
        adapter._emit_run_event(run_id, {"event": "tool.started", "run_id": run_id})
        adapter._emit_run_event(run_id, {"event": "run.completed", "run_id": run_id})
        await asyncio.sleep(0)

        retained = adapter._run_streams[run_id].events_after(0)
        assert [event["event"] for event in retained] == ["tool.started", "run.completed"]
        assert [event["seq"] for event in retained] == [2, 3]

    @pytest.mark.asyncio
    async def test_event_buffer_coalesces_flood_wakes_and_leaves_no_task_after_close(self):
        loop = asyncio.get_running_loop()
        stream = _RunEventBuffer(loop, max_events=8)
        for index in range(10_000):
            stream.append({"seq": index})
        wake = stream._notify_task
        assert wake is not None
        # A burst has one outstanding notifier, not a Task per event.
        assert sum(task is wake for task in asyncio.all_tasks() if not task.done()) == 1
        stream.close()
        await asyncio.sleep(0)
        assert stream._notify_task is None
        assert wake.done()

    @pytest.mark.asyncio
    async def test_simultaneous_event_subscribers_receive_identical_ordered_events(self, adapter):
        app = _create_runs_app(adapter)
        run_id = "run_fanout"
        adapter._run_streams[run_id] = _RunEventBuffer(asyncio.get_running_loop(), max_events=8)
        adapter._run_streams_created[run_id] = time.time()
        adapter._run_statuses[run_id] = {"run_id": run_id, "status": "running"}

        async with TestClient(TestServer(app)) as cli:
            left, right = await asyncio.gather(
                cli.get(f"/v1/runs/{run_id}/events"),
                cli.get(f"/v1/runs/{run_id}/events"),
            )
            adapter._emit_run_event(run_id, {"event": "tool.started", "run_id": run_id})
            adapter._emit_run_event(run_id, {"event": "run.completed", "run_id": run_id})
            adapter._close_run_event_emitter(run_id)
            await asyncio.sleep(0)
            left_body, right_body = await asyncio.gather(left.text(), right.text())

        for body in (left_body, right_body):
            assert body.index("tool.started") < body.index("run.completed")
            assert ": stream closed" in body


    @pytest.mark.asyncio
    async def test_approval_response_without_pending_returns_409(self, adapter):
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            with patch.object(adapter, "_create_agent") as mock_create:
                mock_agent = MagicMock()
                mock_agent.run_conversation.return_value = {"final_response": "done"}
                mock_agent.session_prompt_tokens = 0
                mock_agent.session_completion_tokens = 0
                mock_agent.session_total_tokens = 0
                mock_create.return_value = mock_agent

                resp = await cli.post("/v1/runs", json={"input": "hello"})
                data = await resp.json()
                run_id = data["run_id"]

                approval_resp = await cli.post(
                    f"/v1/runs/{run_id}/approval",
                    json={"approvalId": "approval_" + "A" * 32, "choice": "once"},
                )
                assert approval_resp.status == 409
                approval_data = await approval_resp.json()
                assert approval_data["error"]["code"] in {
                    "approval_not_active",
                    "approval_not_pending",
                }

    @pytest.mark.asyncio
    async def test_approval_requires_exact_pending_id_and_first_terminal_wins(self, adapter):
        app = _create_runs_app(adapter)
        run_id = "run_bool_parse"
        approval_id = "approval_" + "A" * 32
        adapter._run_statuses[run_id] = {"run_id": run_id, "status": "running"}
        adapter._run_approval_sessions[run_id] = "session-123"
        adapter._run_approval_ids[run_id] = {approval_id}
        pending = approval_mod._ApprovalEntry({"approval_id": approval_id})
        with approval_mod._lock:
            approval_mod._gateway_queues["session-123"] = [pending]

        async with TestClient(TestServer(app)) as cli:
            missing = await cli.post(f"/v1/runs/{run_id}/approval", json={"choice": "once"})
            assert missing.status == 400
            wrong = await cli.post(
                f"/v1/runs/{run_id}/approval", json={"approvalId": "approval_" + "B" * 32, "choice": "once"}
            )
            assert wrong.status == 409
            approval_resp = await cli.post(
                f"/v1/runs/{run_id}/approval", json={"approvalId": approval_id, "choice": "deny"}
            )
            approval_data = await approval_resp.json()
            duplicate = await cli.post(
                f"/v1/runs/{run_id}/approval", json={"approvalId": approval_id, "choice": "once"}
            )

        assert approval_resp.status == 200
        assert approval_data["approvalId"] == approval_id
        assert pending.result == "deny" and pending.event.is_set()
        assert duplicate.status == 409

    @pytest.mark.asyncio
    async def test_actual_manager_emitted_approval_id_resolves_once_and_is_run_scoped(self, adapter):
        """Runs accepts the exact opaque ID allocated and notified by approval.py."""
        app = _create_runs_app(adapter)
        run_id = "run_manager_allocated_id"
        session_key = "manager-allocated-session"
        adapter._run_statuses[run_id] = {"run_id": run_id, "status": "waiting_for_approval"}
        adapter._run_approval_sessions[run_id] = session_key
        adapter._run_approval_ids[run_id] = set()
        emitted: dict[str, str] = {}
        notified = threading.Event()
        result: dict[str, object] = {}

        def notify(approval_data):
            public = _public_approval_request(approval_data)
            assert public is not None
            emitted.update(public)
            adapter._run_approval_ids[run_id].add(public["approvalId"])
            notified.set()

        worker = threading.Thread(
            target=lambda: result.update(
                approval_mod._await_gateway_decision(
                    session_key,
                    notify,
                    {"command": "danger", "description": "danger", "pattern_key": "danger"},
                )
            ),
        )
        worker.start()
        assert notified.wait(timeout=3)
        approval_id = emitted["approvalId"]
        assert re.fullmatch(r"approval_[A-Za-z0-9_-]{32}", approval_id)

        async with TestClient(TestServer(app)) as cli:
            first = await cli.post(
                f"/v1/runs/{run_id}/approval", json={"approvalId": approval_id, "choice": "deny"}
            )
            second = await cli.post(
                f"/v1/runs/{run_id}/approval", json={"approvalId": approval_id, "choice": "once"}
            )
        worker.join(timeout=3)

        assert first.status == 200
        assert second.status == 409
        assert not worker.is_alive()
        assert result["resolved"] is True
        assert result["choice"] == "deny"

    @pytest.mark.asyncio
    async def test_exact_approval_resolution_is_scoped_to_target_run(self, auth_adapter):
        """Same client session_id must not let one run approve another run's queue."""
        app = _create_runs_app(auth_adapter)
        async with TestClient(TestServer(app)) as cli:
            with patch.object(auth_adapter, "_create_agent") as mock_create:
                victim_agent, victim_ready, victim_interrupted = _make_slow_agent()
                attacker_agent, attacker_ready, attacker_interrupted = _make_slow_agent()
                mock_create.side_effect = [victim_agent, attacker_agent]

                victim_resp = await cli.post(
                    "/v1/runs",
                    json={"input": "victim", "session_id": "shared-project"},
                    headers={"Authorization": "Bearer sk-secret"},
                )
                attacker_resp = await cli.post(
                    "/v1/runs",
                    json={"input": "attacker", "session_id": "shared-project"},
                    headers={"Authorization": "Bearer sk-secret"},
                )
                assert victim_resp.status == 202
                assert attacker_resp.status == 202
                victim_run = (await victim_resp.json())["run_id"]
                attacker_run = (await attacker_resp.json())["run_id"]

                victim_ready.wait(timeout=3.0)
                attacker_ready.wait(timeout=3.0)
                assert auth_adapter._run_approval_sessions[victim_run] == victim_run
                assert auth_adapter._run_approval_sessions[attacker_run] == attacker_run
                assert auth_adapter._run_approval_sessions[victim_run] != auth_adapter._run_approval_sessions[attacker_run]

                victim_entry = approval_mod._ApprovalEntry({
                    "approval_id": "approval_" + "V" * 32,
                    "command": "bash -c victim-danger",
                    "description": "victim approval",
                    "pattern_keys": ["shell-c"],
                })
                attacker_entry = approval_mod._ApprovalEntry({
                    "approval_id": "approval_" + "A" * 32,
                    "command": "bash -c attacker-danger",
                    "description": "attacker approval",
                    "pattern_keys": ["shell-c"],
                })
                with approval_mod._lock:
                    approval_mod._gateway_queues[victim_run] = [victim_entry]
                    approval_mod._gateway_queues[attacker_run] = [attacker_entry]
                auth_adapter._run_approval_ids[victim_run] = {victim_entry.data["approval_id"]}
                auth_adapter._run_approval_ids[attacker_run] = {attacker_entry.data["approval_id"]}

                approval_resp = await cli.post(
                    f"/v1/runs/{attacker_run}/approval",
                    json={"approvalId": "approval_" + "A" * 32, "choice": "session"},
                    headers={"Authorization": "Bearer sk-secret"},
                )
                approval_data = await approval_resp.json()

                assert approval_resp.status == 200
                assert approval_data["resolved"] == 1
                assert attacker_entry.result == "session"
                assert attacker_entry.event.is_set()
                assert victim_entry.result is None
                assert not victim_entry.event.is_set()
                with approval_mod._lock:
                    assert approval_mod._gateway_queues[victim_run] == [victim_entry]
                    assert victim_run in approval_mod._gateway_queues
                    assert attacker_run not in approval_mod._gateway_queues

                # Clean up the synthetic pending victim approval and unblock the
                # slow test agents so their background run tasks can finish.
                with approval_mod._lock:
                    approval_mod._gateway_queues.pop(victim_run, None)
                victim_interrupted.set()
                attacker_interrupted.set()


    @pytest.mark.asyncio
    async def test_events_not_found_returns_404(self, adapter):
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            resp = await cli.get("/v1/runs/run_nonexistent/events")
        assert resp.status == 404

    @pytest.mark.asyncio
    async def test_events_requires_auth(self, auth_adapter):
        app = _create_runs_app(auth_adapter)
        async with TestClient(TestServer(app)) as cli:
            resp = await cli.get("/v1/runs/run_any/events")
        assert resp.status == 401


class TestRunPublicToolLifecycle:
    @pytest.mark.asyncio
    async def test_lifecycle_is_correlated_and_legacy_progress_cannot_duplicate(self, adapter):
        run_id = "run-tool-correlation"
        adapter._run_streams[run_id] = asyncio.Queue()
        adapter._run_statuses[run_id] = {"run_id": run_id, "status": "running"}
        loop = asyncio.get_running_loop()
        progress = adapter._make_run_event_callback(run_id, loop)
        started, completed = adapter._make_run_tool_callbacks(run_id, loop)

        # This is still fired by the executor, but has no stable ID and must
        # never reach Runs as a second lifecycle event.
        progress("tool.started", "read_file", "private preview", {"path": "/secret/a.txt"})
        started("call_123", "read_file", {"path": "/secret/a.txt", "token": "do-not-leak"})
        progress("tool.completed", "read_file", None, None, result="do-not-leak")
        completed("call_123", "read_file", {"path": "/secret/a.txt"}, "do-not-leak")
        await asyncio.sleep(0)

        events = [adapter._run_streams[run_id].get_nowait() for _ in range(2)]
        assert [event["event"] for event in events] == ["tool.started", "tool.completed"]
        assert {event["tool_call_id"] for event in events} == {"call_123"}
        assert [event["action_kind"] for event in events] == ["read", "read"]
        assert events[0]["target"] == "a.txt"
        assert events[0]["target_kind"] == "file"
        assert "preview" not in events[0]
        assert "result" not in events[1]
        assert "token" not in repr(events)
        assert "/secret/" not in repr(events)

    @pytest.mark.asyncio
    async def test_public_targets_are_basename_skill_or_hostname_only(self, adapter):
        run_id = "run-tool-targets"
        adapter._run_streams[run_id] = asyncio.Queue()
        adapter._run_statuses[run_id] = {"run_id": run_id, "status": "running"}
        started, _ = adapter._make_run_tool_callbacks(run_id, asyncio.get_running_loop())

        started("file", "write_file", {"path": r"C:\\private\\report.txt", "content": "SECRET"})
        started("patch", "patch", {"path": "/private/settings.toml", "patch": "SECRET"})
        started("skill", "skill_view", {"name": "hermes-agent", "other": "SECRET"})
        started("web", "browser_navigate", {"url": "https://user:SECRET@example.test/x?q=SECRET"})
        await asyncio.sleep(0)
        events = [adapter._run_streams[run_id].get_nowait() for _ in range(4)]

        assert [event["target"] for event in events] == ["report.txt", "settings.toml", "hermes-agent", "example.test"]
        assert [event["target_kind"] for event in events] == ["file", "file", "skill", "website"]
        assert "SECRET" not in repr(events)
        assert "https://" not in repr(events)
        assert "private" not in repr(events)

    @pytest.mark.asyncio
    async def test_action_kind_mapping_and_browser_actions_are_allowlisted(self, adapter):
        run_id = "run-tool-kinds"
        adapter._run_streams[run_id] = asyncio.Queue()
        adapter._run_statuses[run_id] = {"run_id": run_id, "status": "running"}
        started, completed = adapter._make_run_tool_callbacks(run_id, asyncio.get_running_loop())
        expected = {
            "read_file": "read", "write_file": "write", "patch": "edit",
            "search_files": "search", "terminal": "execute", "execute_code": "execute",
            "web_search": "search", "browser_navigate": "navigate",
            "browser_click": "interact", "browser_type": "interact",
            "browser_snapshot": "inspect", "skill_view": "read",
        }
        for index, tool_name in enumerate(expected):
            call_id = f"call-{index}"
            started(call_id, tool_name, {"selector": "SECRET", "text": "SECRET", "code": "SECRET"})
            completed(call_id, tool_name, {"code": "SECRET"}, "SECRET")
        await asyncio.sleep(0)

        events = [adapter._run_streams[run_id].get_nowait() for _ in range(2 * len(expected))]
        assert [event["tool"] for event in events[::2]] == list(expected)
        assert [event["action_kind"] for event in events[::2]] == list(expected.values())
        assert [event["action_kind"] for event in events[1::2]] == list(expected.values())
        for event in events:
            assert "target" not in event
            assert "target_kind" not in event
            assert "SECRET" not in repr(event)

    @pytest.mark.asyncio
    async def test_delegate_count_and_execute_code_preview_are_safe(self, adapter):
        run_id = "run-delegate-preview"
        adapter._run_streams[run_id] = asyncio.Queue()
        adapter._run_statuses[run_id] = {"run_id": run_id, "status": "running"}
        started, completed = adapter._make_run_tool_callbacks(run_id, asyncio.get_running_loop())
        code = "# SECRET comment\npassword = 'SECRET'\namount = 123.45\nmessage = f'token={SECRET}'\n"

        started("delegate", "delegate_task", {"tasks": [{"goal": "SECRET"}, {"goal": "SECRET"}]})
        completed("delegate", "delegate_task", {}, "SECRET")
        started("code", "execute_code", {"code": code, "context": "SECRET", "session_id": "raw-session"})
        started("bad-code", "execute_code", {"code": 123})
        started("long-code", "execute_code", {"code": "x = 1\n" * 121})
        started("unknown", "plugin__leak", {"code": code, "tasks": [{"goal": "SECRET"}]})
        await asyncio.sleep(0)
        events = [adapter._run_streams[run_id].get_nowait() for _ in range(6)]

        assert events[0]["tool"] == "delegate_task"
        assert events[0]["action_kind"] == events[1]["action_kind"] == "delegate"
        assert events[0]["agent_count"] == 2
        assert "agent_count" not in events[1]
        preview = events[2]["code_preview"]
        assert events[2]["code_truncated"] is False
        compact_preview = preview.replace(" ", "")
        assert "value='…'" in compact_preview and "value=0" in compact_preview
        assert "SECRET" not in preview and "comment" not in preview and "123.45" not in preview
        assert "code_preview" not in events[3] and "code_truncated" not in events[3]
        assert events[4]["code_truncated"] is True
        assert len(events[4]["code_preview"].splitlines()) <= 120
        assert events[5]["tool"] == "tool" and events[5]["action_kind"] == "generic"
        public = repr(events)
        for forbidden in ("SECRET", "raw-session", "context", "session_id", "plugin__leak"):
            assert forbidden not in public

    @pytest.mark.asyncio
    async def test_search_scope_is_basename_only_and_unknown_tools_fail_closed(self, adapter):
        run_id = "run-tool-search-private"
        adapter._run_streams[run_id] = asyncio.Queue()
        adapter._run_statuses[run_id] = {"run_id": run_id, "status": "running"}
        started, completed = adapter._make_run_tool_callbacks(run_id, asyncio.get_running_loop())

        started("search", "search_files", {"path": "/private/project/src", "pattern": "SECRET"})
        started("plugin", "plugin__leak", {"path": "/private/SECRET", "password": "SECRET"})
        completed("plugin", "plugin__leak", {"command": "SECRET"}, "SECRET")
        await asyncio.sleep(0)
        events = [adapter._run_streams[run_id].get_nowait() for _ in range(3)]

        assert events[0]["tool"] == "search_files"
        assert events[0]["action_kind"] == "search"
        assert events[0]["target"] == "src"
        assert events[0]["target_kind"] == "file"
        assert events[1]["tool"] == events[2]["tool"] == "tool"
        assert events[1]["action_kind"] == events[2]["action_kind"] == "generic"
        public = repr(events)
        for forbidden in ("SECRET", "plugin__leak", "/private/", "pattern", "password", "command", "result"):
            assert forbidden not in public

    @pytest.mark.asyncio
    async def test_reasoning_becomes_one_static_notice_without_native_text(self, adapter):
        run_id = "run-reasoning-private"
        adapter._run_streams[run_id] = asyncio.Queue()
        adapter._run_statuses[run_id] = {"run_id": run_id, "status": "running"}
        callback = adapter._make_run_event_callback(run_id, asyncio.get_running_loop())

        callback("reasoning.available", "_thinking", "SECRET chain of thought", {"command": "SECRET"})
        callback("reasoning.available", "_thinking", "SECOND SECRET", None)
        await asyncio.sleep(0)

        event = adapter._run_streams[run_id].get_nowait()
        assert event["event"] == "progress.notice"
        assert event["kind"] == "thinking"
        assert "message" not in event
        assert adapter._run_streams[run_id].empty()
        assert "SECRET" not in repr(event)
        assert "text" not in event and "preview" not in event

    @pytest.mark.asyncio
    async def test_runs_wires_interim_callback_and_ignores_already_streamed(self, adapter):
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            with patch.object(adapter, "_create_agent") as create_agent:
                agent = MagicMock()
                agent.session_prompt_tokens = agent.session_completion_tokens = agent.session_total_tokens = 0

                def run_conversation(**_kwargs):
                    callback = create_agent.call_args.kwargs["interim_assistant_callback"]
                    callback("I will inspect https://user:SECRET@example.test/x", already_streamed=True)
                    callback("<think>private</think>Then continue", already_streamed=False)
                    return {"final_response": "done"}

                agent.run_conversation.side_effect = run_conversation
                create_agent.return_value = agent
                started = await cli.post("/v1/runs", json={"input": "hello"})
                run_id = (await started.json())["run_id"]
                for _ in range(30):
                    if adapter._run_statuses[run_id]["status"] == "completed":
                        break
                    await asyncio.sleep(0.02)

        queued = adapter._run_streams[run_id].events_after(0)
        commentary = [event for event in queued if isinstance(event, dict) and event["event"] == "assistant.commentary"]
        assert [event["text"] for event in commentary] == [
            "I will inspect [redacted-url]",
            "Then continue",
        ]
        assert [event["seq"] for event in commentary] == sorted(event["seq"] for event in commentary)
        assert "SECRET" not in repr(commentary)

    @pytest.mark.asyncio
    async def test_assistant_commentary_is_callback_only_safe_and_sequenced(self, adapter):
        run_id = "run-commentary"
        adapter._run_streams[run_id] = asyncio.Queue()
        adapter._run_statuses[run_id] = {"run_id": run_id, "status": "running"}
        loop = asyncio.get_running_loop()
        started, _ = adapter._make_run_tool_callbacks(run_id, loop)

        # The projection intentionally receives only public interim callback
        # text; reasoning/progress and ordinary message deltas have distinct
        # transports and cannot manufacture commentary.
        commentary = lambda text, already_streamed=False: (
            adapter._emit_run_event(run_id, {
                "event": "assistant.commentary", "run_id": run_id,
                "timestamp": 1, "text": adapter._public_assistant_commentary(text),
            }, loop)
            if adapter._public_assistant_commentary(text) is not None else False
        )
        commentary("I will inspect https://user:SECRET@example.test/x at /private/SECRET\\napi_key=SECRET", already_streamed=True)
        started("call-commentary", "read_file", {"path": "/private/file.txt"})
        await asyncio.sleep(0)
        events = [adapter._run_streams[run_id].get_nowait() for _ in range(2)]

        assert [event["event"] for event in events] == ["assistant.commentary", "tool.started"]
        assert events[0]["seq"] < events[1]["seq"]
        assert "[redacted-url]" in events[0]["text"]
        assert "[redacted-path]" in events[0]["text"]
        assert "SECRET" not in repr(events[0])

    def test_commentary_rejects_private_tags_and_enforces_bounds(self, adapter):
        assert adapter._public_assistant_commentary("<think>private</think>Visible") == "Visible"
        assert adapter._public_assistant_commentary("Visible <scratchpad>") is None
        assert adapter._public_assistant_commentary("<memory-context>private</memory-context>") is None
        bounded = adapter._public_assistant_commentary("😀" * 2_000 + "\nextra" * 20)
        assert bounded is not None
        assert len(bounded) <= 1_000
        assert len(bounded.encode("utf-8")) <= 4_096
        assert len(bounded.splitlines()) <= 8

    @pytest.mark.parametrize(
        "uri",
        (
            "http://user:secret@example.test/a?q=secret",
            "https://user:secret@example.test/a?q=secret",
            "ws://user:secret@example.test/a?q=secret",
            "wss://user:secret@example.test/a?q=secret",
            "ftp://user:secret@example.test/a?q=secret",
            "file://user:secret@example.test/a?q=secret",
            "custom+scheme://user:secret@example.test/a?q=secret",
        ),
    )
    def test_commentary_redacts_all_uri_schemes(self, adapter, uri):
        sanitized = adapter._public_assistant_commentary(f"Проверяю {uri} сейчас")
        assert sanitized == "Проверяю [redacted-url] сейчас"
        assert "secret" not in sanitized

    def test_commentary_redacts_separator_prefixed_absolute_paths_and_keeps_safe_prose(self, adapter):
        sanitized = adapter._public_assistant_commentary(
            r"Проверяю path=/secret и (/home/a), файл:/etc/passwd, path=C:\Users\x, (\\server\share). a/b готово."
        )
        assert sanitized == (
            "Проверяю path=[redacted-path] и ([redacted-path]), файл:[redacted-path], "
            "path=[redacted-path], ([redacted-path]). a/b готово."
        )
        assert adapter._public_assistant_commentary(
            "[redacted-url] и [redacted-path] — безопасные метки; обычная русская проза разрешена."
        ) is not None

    def test_goals_and_commentary_force_shared_secret_redaction(self, adapter):
        with patch("gateway.platforms.api_server.redact_sensitive_text", side_effect=lambda text, **kwargs: text) as redactor:
            assert adapter._public_agent_goal("Review token=RAW_SECRET") is not None
            assert adapter._public_assistant_commentary("Check token=RAW_SECRET") is not None
        assert [call.kwargs.get("force") for call in redactor.call_args_list] == [True, True]

    def test_public_goal_is_multiline_bounded_and_rejects_private_payloads(self, adapter):
        raw = "\n".join(f"step {index}: review 😀" for index in range(100))
        projected = adapter._public_agent_goal(raw)
        assert projected is not None
        goal, truncated = projected
        assert truncated is True
        assert len(goal) <= 8_000
        assert len(goal.encode("utf-8")) <= 32 * 1024
        assert len(goal.splitlines()) == 80
        assert adapter._public_agent_goal("safe <think>private</think> goal") is None
        assert adapter._public_agent_goal("safe [reasoning]private[/reasoning] goal") is None
        assert adapter._public_agent_goal("safe <memory>private</memory> goal") is None
        assert adapter._public_agent_goal("safe\x01goal") == ("safe goal", False)
        assert adapter._public_agent_goal("safe\u202egoal") == ("safe goal", False)

    def test_public_goal_multibyte_byte_boundary_is_utf8_safe_and_deterministic(self, adapter, monkeypatch):
        monkeypatch.setattr(APIServerAdapter, "_PUBLIC_AGENT_GOAL_MAX_BYTES", 13)
        monkeypatch.setattr(APIServerAdapter, "_PUBLIC_AGENT_GOAL_MAX_LENGTH", 99)
        first = adapter._public_agent_goal("😀😀😀😀")
        second = adapter._public_agent_goal("😀😀😀😀")
        assert first == second == ("😀😀😀", True)

    @pytest.mark.asyncio
    async def test_sequencer_orders_mixed_producers_and_rejects_late_callbacks(self, adapter):
        run_id = "run-sequenced"
        queue = asyncio.Queue()
        adapter._run_streams[run_id] = queue
        adapter._run_statuses[run_id] = {"run_id": run_id, "status": "running"}
        loop = asyncio.get_running_loop()
        started, completed = adapter._make_run_tool_callbacks(run_id, loop)

        # An executor-thread callback is scheduled before loop-thread producers.
        worker = threading.Thread(target=started, args=("call_1", "read_file", {"path": "/private/a.txt"}))
        worker.start()
        worker.join()
        adapter._emit_run_event(run_id, {
            "event": "message.delta", "run_id": run_id, "timestamp": 1, "delta": "ok",
        })
        completed("call_1", "read_file", {"path": "/private/a.txt"}, "private result")
        adapter._emit_run_event(run_id, {
            "event": "run.completed", "run_id": run_id, "timestamp": 2, "output": "done", "usage": {},
        })
        adapter._close_run_event_emitter(run_id)
        started("call_late", "read_file", {"path": "/private/late.txt"})
        await asyncio.sleep(0)

        queued = [queue.get_nowait() for _ in range(5)]
        events = queued[:-1]
        assert [event["seq"] for event in events] == [1, 2, 3, 4]
        assert [event["event"] for event in events] == [
            "tool.started", "message.delta", "tool.completed", "run.completed",
        ]
        assert events[0]["tool_call_id"] == events[2]["tool_call_id"] == "call_1"
        assert events[-1]["event"] == "run.completed"
        assert queued[-1] is None
        assert queue.empty()


# ---------------------------------------------------------------------------
# POST /v1/runs/{run_id}/clarification — resolve structured clarification
# ---------------------------------------------------------------------------


class TestRunClarification:
    @pytest.mark.asyncio
    async def test_clarification_response_is_run_scoped_and_emits_no_user_text(self, adapter):
        app = _create_runs_app(adapter)
        run_id = "run_clarify"
        adapter._run_statuses[run_id] = {"run_id": run_id, "status": "waiting_for_clarification"}
        adapter._run_clarification_ids[run_id] = {"clarify-1"}
        adapter._run_streams[run_id] = asyncio.Queue()

        async with TestClient(TestServer(app)) as cli:
            with patch("tools.clarify_gateway.resolve_gateway_clarify", return_value=True) as resolve:
                response = await cli.post(
                    f"/v1/runs/{run_id}/clarification",
                    json={"clarification_id": "clarify-1", "response": "production"},
                )

        assert response.status == 200
        resolve.assert_called_once_with("clarify-1", "production")
        await asyncio.sleep(0)
        event = adapter._run_streams[run_id].get_nowait()
        assert event["event"] == "clarification.responded"
        assert event["clarification_id"] == "clarify-1"
        assert "response" not in event

    @pytest.mark.asyncio
    async def test_clarification_response_rejects_id_owned_by_another_run(self, adapter):
        app = _create_runs_app(adapter)
        adapter._run_statuses["run-a"] = {"run_id": "run-a", "status": "waiting_for_clarification"}
        adapter._run_clarification_ids["run-a"] = {"clarify-a"}

        async with TestClient(TestServer(app)) as cli:
            response = await cli.post(
                "/v1/runs/run-a/clarification",
                json={"clarification_id": "clarify-b", "response": "nope"},
            )

        assert response.status == 409


@pytest.mark.asyncio
async def test_subagent_lifecycle_is_per_agent_redacted_and_bounded(adapter):
    run_id = "run-background"
    adapter._run_streams[run_id] = asyncio.Queue()
    adapter._run_statuses[run_id] = {"run_id": run_id, "status": "running"}
    callback = adapter._make_run_event_callback(run_id, asyncio.get_running_loop())

    secret_goal = "Audit https://token:SECRET@example.test/a at /private/SECRET and C:\\secret\\x; api_key=SECRET\nnext"
    callback(
        "subagent.start", preview="private preview", subagent_id="raw-child-a", child_session_id="raw-session",
        task_index=0, task_count=2, goal=secret_goal, tool_count=1,
    )
    callback(
        "subagent.tool", subagent_id="raw-child-b", task_index=1, task_count=2,
        goal="B", tool_count=2, context="SECRET", result="SECRET", model="private-model",
    )
    callback(
        "subagent.complete", subagent_id="raw-child-a", task_index=0, task_count=2,
        goal=secret_goal, tool_count=3, status="timeout", duration_seconds=4.5,
    )
    # Invalid identities and bounds fail closed rather than creating an uncorrelated card.
    callback("subagent.start", child_session_id="raw-session", task_index=0, task_count=1, goal="SECRET")
    callback("subagent.start", subagent_id="bad", task_index=100, task_count=101, goal="SECRET")
    await asyncio.sleep(0)

    events = [adapter._run_streams[run_id].get_nowait() for _ in range(3)]
    assert [event["status"] for event in events] == ["running", "running", "failed"]
    assert events[0]["agent_id"] == events[2]["agent_id"] != events[1]["agent_id"]
    assert events[0]["task_index"] == 0 and events[1]["task_index"] == 1
    assert events[0]["task_count"] == events[1]["task_count"] == 2
    assert events[2]["tool_count"] == 3 and events[2]["duration"] == 4.5
    assert len(events[0]["goal"]) <= 8_000
    assert len(events[0]["goal"].encode("utf-8")) <= 32 * 1024
    assert events[0]["goal_truncated"] is False
    public = repr(events)
    for forbidden in (
        "SECRET", "raw-child", "raw-session", "private preview", "private-model", "context", "result", "/private/", "C:\\\\secret",
    ):
        assert forbidden not in public
    assert adapter._run_streams[run_id].empty()


@pytest.mark.asyncio
async def test_subagent_projection_uses_run_scoped_hmac_and_strict_sanitizers(adapter):
    child_id = "raw-child-customer-ida"
    raw_hash = hashlib.sha256(child_id.encode()).hexdigest()[:32]
    base = {
        "subagent_id": child_id,
        "task_index": 0,
        "task_count": 1,
        "tool_count": 0,
    }
    first = adapter._public_background_event("run-hmac-a", "subagent.start", base)
    same = adapter._public_background_event("run-hmac-a", "subagent.tool", base)
    other_run = adapter._public_background_event("run-hmac-b", "subagent.start", base)
    interrupted = adapter._public_background_event(
        "run-hmac-a", "subagent.complete", {**base, "status": "interrupted", "duration_seconds": 2},
    )
    nonterminal = adapter._public_background_event(
        "run-hmac-a", "subagent.tool", {**base, "status": "failed", "duration_seconds": 2},
    )

    assert first and same and other_run and interrupted and nonterminal
    assert first["agent_id"] == same["agent_id"] != other_run["agent_id"]
    assert re.fullmatch(r"agent_[A-Za-z0-9_-]{24}", first["agent_id"])
    assert child_id not in first["agent_id"] and raw_hash not in first["agent_id"]
    assert interrupted["status"] == "failed"
    assert nonterminal["status"] == "running" and "duration" not in nonterminal

    secret_goal = (
        "\U0001f600" * 200
        + " https://user:RAW_URL_SECRET@example.test/private "
        + "/private/RAW_PATH_SECRET Authorization: Bearer RAW_AUTH_SECRET "
        + "Cookie=RAW_COOKIE_SECRET token=RAW_TOKEN_SECRET"
    )
    goal_event = adapter._public_background_event(
        "run-hmac-a", "subagent.start", {**base, "goal": secret_goal},
    )
    assert goal_event and "goal" in goal_event
    goal = goal_event["goal"]
    assert len(goal) <= 8_000 and len(goal.encode("utf-8")) <= 32 * 1024
    assert goal_event["goal_truncated"] is False
    for private in ("RAW_URL_SECRET", "RAW_PATH_SECRET", "RAW_AUTH_SECRET", "RAW_COOKIE_SECRET", "RAW_TOKEN_SECRET", "https://", "/private/"):
        assert private not in goal
    sensitive_goal = adapter._public_agent_goal(
        "Visit https://user:RAW_URL_SECRET@example.test/a at /private/RAW_PATH_SECRET; "
        "Authorization: Bearer RAW_AUTH_SECRET Cookie=RAW_COOKIE_SECRET token=RAW_TOKEN_SECRET"
    )
    assert sensitive_goal is not None
    sensitive_goal, sensitive_goal_truncated = sensitive_goal
    assert sensitive_goal_truncated is False
    for private in ("RAW_URL_SECRET", "RAW_PATH_SECRET", "RAW_AUTH_SECRET", "RAW_COOKIE_SECRET", "RAW_TOKEN_SECRET", "https://", "/private/"):
        assert private not in sensitive_goal

    code = "# customer name must vanish\ncustomer_name = 123\nread_file('RAW_LITERAL')\nresult = json.loads('RAW')\n"
    preview = adapter._public_execute_code_preview(code)
    assert preview is not None
    skeleton, truncated = preview
    assert not truncated
    assert "customer_name" not in skeleton and "result" not in skeleton
    assert "RAW_LITERAL" not in skeleton and "123" not in skeleton and "customer name" not in skeleton
    assert "read_file" in skeleton and "json" in skeleton
    assert adapter._public_execute_code_preview("broken = 'unterminated") is None
    assert adapter._public_execute_code_preview("x = 1\x00") is None


# ---------------------------------------------------------------------------
# Run lifecycle TTL sweeping
# ---------------------------------------------------------------------------


class TestRunLifecycleSweep:
    def test_sweep_keeps_transport_with_active_subscriber(self, adapter):
        run_id = "run_subscribed"
        queue = asyncio.Queue()
        adapter._run_streams[run_id] = queue
        adapter._run_streams_created[run_id] = 0
        adapter._run_stream_subscribers.add(run_id)

        adapter._sweep_orphaned_runs_once(time.time())

        assert adapter._run_streams[run_id] is queue
        assert run_id in adapter._run_streams_created

    @pytest.mark.asyncio
    async def test_expired_live_run_keeps_bounded_transport_for_reconnect(self, adapter):
        """An active run keeps its bounded replay log after a client disconnect."""
        app = _create_runs_app(adapter)
        adapter._max_concurrent_runs = 1

        async with TestClient(TestServer(app)) as cli:
            with patch.object(adapter, "_create_agent") as mock_create:
                mock_agent, agent_ready, _ = _make_slow_agent()
                mock_create.return_value = mock_agent

                start_resp = await cli.post("/v1/runs", json={"input": "hello"})
                assert start_resp.status == 202
                run_id = (await start_resp.json())["run_id"]
                assert agent_ready.wait(timeout=3.0)

                task = adapter._active_run_tasks[run_id]
                assert isinstance(task, asyncio.Task)
                assert not task.done()

                pending = approval_mod._ApprovalEntry({
                    "approval_id": "approval_" + "A" * 32,
                    "command": "bash -c long-running",
                    "description": "approval after stream TTL",
                    "pattern_keys": ["shell-c"],
                })
                with approval_mod._lock:
                    approval_mod._gateway_queues[run_id] = [pending]
                adapter._run_approval_ids[run_id] = {pending.data["approval_id"]}

                adapter._run_streams_created[run_id] -= adapter._RUN_STREAM_TTL + 1
                # Exercise one real sweeper iteration without waiting 60 seconds.
                with patch(
                    "gateway.platforms.api_server.asyncio.sleep",
                    side_effect=[None, asyncio.CancelledError()],
                ):
                    with pytest.raises(asyncio.CancelledError):
                        await adapter._sweep_orphaned_runs()

                assert adapter._active_run_tasks[run_id] is task
                assert adapter._active_run_agents[run_id] is mock_agent
                assert run_id in adapter._run_streams
                assert run_id in adapter._run_streams_created
                assert adapter._run_approval_sessions[run_id] == run_id

                limited = adapter._concurrency_limited_response()
                assert limited is not None
                assert limited.status == 429

                approval_resp = await cli.post(
                    f"/v1/runs/{run_id}/approval",
                    json={"approvalId": pending.data['approval_id'], "choice": "once"},
                )
                assert approval_resp.status == 200
                assert pending.event.is_set()
                assert pending.result == "once"

                stop_resp = await cli.post(f"/v1/runs/{run_id}/stop")
                assert stop_resp.status == 200
                mock_agent.interrupt.assert_called_once_with("Stop requested via API")

    @pytest.mark.asyncio
    async def test_expired_live_transport_retains_new_deltas_within_bound(self, adapter):
        """Active runs continue recording a bounded replay log after their original TTL."""
        app = _create_runs_app(adapter)

        async with TestClient(TestServer(app)) as cli:
            with patch.object(adapter, "_create_agent") as mock_create:
                mock_agent, agent_ready, _ = _make_slow_agent()
                mock_create.return_value = mock_agent

                start_resp = await cli.post("/v1/runs", json={"input": "hello"})
                run_id = (await start_resp.json())["run_id"]
                assert agent_ready.wait(timeout=3.0)
                expired_queue = adapter._run_streams[run_id]
                stream_delta = mock_create.call_args.kwargs["stream_delta_callback"]

                adapter._run_streams_created[run_id] -= adapter._RUN_STREAM_TTL + 1
                adapter._sweep_orphaned_runs_once(time.time())
                before = len(expired_queue.events_after(0))
                stream_delta("must-not-buffer")
                await asyncio.sleep(0)
                assert len(expired_queue.events_after(0)) == before + 1
                mock_agent.interrupt("finish test")
                for _ in range(40):
                    if run_id not in adapter._active_run_tasks:
                        break
                    await asyncio.sleep(0.05)

                assert len(expired_queue.events_after(0)) <= adapter._RUN_EVENT_RETENTION

    @pytest.mark.asyncio
    async def test_long_running_stream_retains_replay_for_ttl_after_terminal_close(self, adapter):
        """A long run gets a full replay TTL from terminal emitter close."""
        app = _create_runs_app(adapter)
        run_id = "run_long_then_terminal"
        stream = _RunEventBuffer(asyncio.get_running_loop(), max_events=8)
        active_task = MagicMock()
        active_task.done.return_value = False
        adapter._run_streams[run_id] = stream
        adapter._run_streams_created[run_id] = time.time() - adapter._RUN_STREAM_TTL - 1
        adapter._active_run_tasks[run_id] = active_task
        adapter._run_statuses[run_id] = {"run_id": run_id, "status": "running"}

        adapter._emit_run_event(run_id, {"event": "run.started", "run_id": run_id})
        adapter._sweep_orphaned_runs_once(time.time())
        assert run_id in adapter._run_streams

        adapter._emit_run_event(run_id, {"event": "run.completed", "run_id": run_id})
        adapter._run_statuses[run_id]["status"] = "completed"
        adapter._close_run_event_emitter(run_id)
        terminal_closed_at = adapter._run_streams_created[run_id]
        active_task.done.return_value = True
        await asyncio.sleep(0)

        adapter._sweep_orphaned_runs_once(terminal_closed_at + adapter._RUN_STREAM_TTL - 1)
        assert run_id in adapter._run_streams
        async with TestClient(TestServer(app)) as cli:
            replay = await cli.get(f"/v1/runs/{run_id}/events")
            body = await replay.text()
        assert body.index("run.started") < body.index("run.completed")
        assert ": stream closed" in body

        adapter._sweep_orphaned_runs_once(terminal_closed_at + adapter._RUN_STREAM_TTL + 1)
        assert run_id not in adapter._run_streams
        assert run_id not in adapter._run_streams_created

    @pytest.mark.asyncio
    async def test_terminal_stream_hard_expires_despite_stalled_subscriber(self, adapter):
        """A non-reading SSE token cannot pin a completed replay buffer forever."""
        run_id = "run_terminal_stalled_reader"
        stream = _RunEventBuffer(asyncio.get_running_loop(), max_events=8)
        adapter._run_streams[run_id] = stream
        adapter._run_streams_created[run_id] = time.time() - adapter._RUN_STREAM_TTL - 1
        adapter._run_statuses[run_id] = {"run_id": run_id, "status": "completed"}
        stalled_subscriber = (run_id, object())
        adapter._run_stream_subscribers.add(stalled_subscriber)
        adapter._emit_run_event(run_id, {"event": "message.delta", "run_id": run_id, "delta": "answer"})
        adapter._close_run_event_emitter(run_id)
        await asyncio.sleep(0)

        adapter._sweep_orphaned_runs_once(time.time() + adapter._RUN_STREAM_TTL + 1)

        assert run_id not in adapter._run_streams
        assert run_id not in adapter._run_streams_created
        assert stalled_subscriber not in adapter._run_stream_subscribers

    @pytest.mark.asyncio
    async def test_expired_orphan_run_state_is_reaped(self, adapter):
        run_id = "run_expired_orphan"
        adapter._run_streams[run_id] = asyncio.Queue()
        adapter._run_streams_created[run_id] = 0
        adapter._run_approval_sessions[run_id] = run_id

        pending = approval_mod._ApprovalEntry({
            "command": "bash -c orphaned",
            "description": "orphaned approval",
            "pattern_keys": ["shell-c"],
        })
        with approval_mod._lock:
            approval_mod._gateway_queues[run_id] = [pending]

        with patch(
            "gateway.platforms.api_server.asyncio.sleep",
            side_effect=[None, asyncio.CancelledError()],
        ):
            with pytest.raises(asyncio.CancelledError):
                await adapter._sweep_orphaned_runs()

        assert run_id not in adapter._run_streams
        assert run_id not in adapter._run_streams_created
        assert run_id not in adapter._run_approval_sessions
        assert pending.event.is_set()
        with approval_mod._lock:
            assert run_id not in approval_mod._gateway_queues


# ---------------------------------------------------------------------------
# POST /v1/runs/{run_id}/stop — interrupt a running agent
# ---------------------------------------------------------------------------


class TestStopRun:
    @pytest.mark.asyncio
    async def test_stop_before_agent_creation_prevents_run_start(self, adapter):
        """A stop accepted while queued must prevent agent construction."""
        app = _create_runs_app(adapter)
        original_create_task = asyncio.create_task
        task_started = asyncio.Event()
        allow_task = asyncio.Event()

        def _delayed_create_task(coro):
            async def _delayed():
                task_started.set()
                await allow_task.wait()
                return await coro

            return original_create_task(_delayed())

        with patch("gateway.platforms.api_server.asyncio.create_task", side_effect=_delayed_create_task), \
             patch.object(adapter, "_create_agent") as mock_create:
            async with TestClient(TestServer(app)) as cli:
                resp = await cli.post("/v1/runs", json={"input": "hello"})
                run_id = (await resp.json())["run_id"]
                await task_started.wait()

                stop_resp = await cli.post(f"/v1/runs/{run_id}/stop")
                assert stop_resp.status == 200
                allow_task.set()

                for _ in range(20):
                    if run_id not in adapter._active_run_tasks:
                        break
                    await asyncio.sleep(0.05)

                mock_create.assert_not_called()
                assert adapter._run_statuses[run_id]["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_stop_keeps_uncooperative_executor_tracked_until_exit(self, adapter):
        """Cancelling an asyncio wrapper must not hide its live executor thread."""
        app = _create_runs_app(adapter)
        run_can_finish = threading.Event()
        run_finished = threading.Event()

        async with TestClient(TestServer(app)) as cli:
            with patch.object(adapter, "_create_agent") as mock_create:
                mock_agent = MagicMock()
                mock_agent.session_prompt_tokens = 0
                mock_agent.session_completion_tokens = 0
                mock_agent.session_total_tokens = 0
                started = threading.Event()

                def _run_conversation(*_args, **_kwargs):
                    started.set()
                    run_can_finish.wait(timeout=5)
                    run_finished.set()
                    return {"final_response": "late result"}

                mock_agent.run_conversation.side_effect = _run_conversation
                mock_create.return_value = mock_agent

                resp = await cli.post("/v1/runs", json={"input": "hello"})
                run_id = (await resp.json())["run_id"]
                assert started.wait(timeout=3)

                stop_resp = await cli.post(f"/v1/runs/{run_id}/stop")
                assert stop_resp.status == 200
                await asyncio.sleep(0.1)

                assert not run_finished.is_set()
                assert run_id in adapter._active_run_agents
                assert run_id in adapter._active_run_tasks
                assert adapter._run_statuses[run_id]["status"] == "stopping"

                run_can_finish.set()
                for _ in range(40):
                    if run_id not in adapter._active_run_tasks:
                        break
                    await asyncio.sleep(0.05)

                assert run_id not in adapter._active_run_agents
                assert run_id not in adapter._active_run_tasks
                assert adapter._run_statuses[run_id]["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_stop_running_agent(self, adapter):
        """Stop should interrupt the agent and cancel the task."""
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            with patch.object(adapter, "_create_agent") as mock_create:
                mock_agent, agent_ready, _ = _make_slow_agent()
                mock_create.return_value = mock_agent

                # Start run
                resp = await cli.post("/v1/runs", json={"input": "hello"})
                assert resp.status == 202
                data = await resp.json()
                run_id = data["run_id"]

                # Wait for agent to start running in the thread
                agent_ready.wait(timeout=3.0)
                await asyncio.sleep(0.1)

                # Verify agent ref is stored
                assert run_id in adapter._active_run_agents

                # Stop the run
                stop_resp = await cli.post(f"/v1/runs/{run_id}/stop")
                assert stop_resp.status == 200
                stop_data = await stop_resp.json()
                assert stop_data["run_id"] == run_id
                assert stop_data["status"] == "stopping"

                # Agent interrupt should have been called
                mock_agent.interrupt.assert_called_once_with("Stop requested via API")

                status_resp = await cli.get(f"/v1/runs/{run_id}")
                assert status_resp.status == 200
                status_data = await status_resp.json()
                assert status_data["status"] in {"stopping", "cancelled"}

                # Refs should be cleaned up
                await asyncio.sleep(0.5)
                assert run_id not in adapter._active_run_agents
                assert run_id not in adapter._active_run_tasks

    @pytest.mark.asyncio
    async def test_stop_nonexistent_run_returns_404(self, adapter):
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            resp = await cli.post("/v1/runs/run_nonexistent/stop")
        assert resp.status == 404

    @pytest.mark.asyncio
    async def test_stop_requires_auth(self, auth_adapter):
        app = _create_runs_app(auth_adapter)
        async with TestClient(TestServer(app)) as cli:
            resp = await cli.post("/v1/runs/run_any/stop")
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_stop_already_completed_run_returns_404(self, adapter):
        """Stopping a run that already finished should return 404 (refs cleaned up)."""
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            with patch.object(adapter, "_create_agent") as mock_create:
                mock_agent = MagicMock()
                mock_agent.run_conversation.return_value = {"final_response": "done"}
                mock_agent.session_prompt_tokens = 0
                mock_agent.session_completion_tokens = 0
                mock_agent.session_total_tokens = 0
                mock_create.return_value = mock_agent

                # Start and wait for completion
                resp = await cli.post("/v1/runs", json={"input": "hello"})
                assert resp.status == 202
                data = await resp.json()
                run_id = data["run_id"]

                await asyncio.sleep(0.3)

                # Run should be done, refs cleaned up
                assert run_id not in adapter._active_run_agents

                # Stop should return 404
                stop_resp = await cli.post(f"/v1/runs/{run_id}/stop")
                assert stop_resp.status == 404

    @pytest.mark.asyncio
    async def test_stop_interrupt_exception_does_not_crash(self, adapter):
        """If agent.interrupt() raises, stop should still succeed."""
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            with patch.object(adapter, "_create_agent") as mock_create:
                mock_agent, agent_ready, interrupted = _make_slow_agent()

                # Override the interrupt side_effect to raise. Still trip
                # ``interrupted`` so the slow_run thread unblocks at teardown
                # — without this the agent thread blocks the full 10s
                # timeout and the test teardown waits the same amount.
                def _raising_interrupt(message=None):
                    interrupted.set()
                    raise RuntimeError("interrupt failed")

                mock_agent.interrupt = MagicMock(side_effect=_raising_interrupt)
                mock_create.return_value = mock_agent

                resp = await cli.post("/v1/runs", json={"input": "hello"})
                assert resp.status == 202
                data = await resp.json()
                run_id = data["run_id"]

                agent_ready.wait(timeout=3.0)
                await asyncio.sleep(0.1)

                stop_resp = await cli.post(f"/v1/runs/{run_id}/stop")
                assert stop_resp.status == 200
                stop_data = await stop_resp.json()
                assert stop_data["status"] == "stopping"

    @pytest.mark.asyncio
    async def test_stop_sends_sentinel_to_events_stream(self, adapter):
        """After stop, the events stream should close."""
        app = _create_runs_app(adapter)
        async with TestClient(TestServer(app)) as cli:
            with patch.object(adapter, "_create_agent") as mock_create:
                mock_agent, agent_ready, _ = _make_slow_agent()
                mock_create.return_value = mock_agent

                # Start run
                resp = await cli.post("/v1/runs", json={"input": "hello"})
                assert resp.status == 202
                data = await resp.json()
                run_id = data["run_id"]

                agent_ready.wait(timeout=3.0)
                await asyncio.sleep(0.1)

                # Subscribe to events in background
                events_task = asyncio.ensure_future(
                    cli.get(f"/v1/runs/{run_id}/events")
                )

                await asyncio.sleep(0.1)

                # Stop the run
                stop_resp = await cli.post(f"/v1/runs/{run_id}/stop")
                assert stop_resp.status == 200

                # Events stream should close
                events_resp = await asyncio.wait_for(events_task, timeout=5.0)
                assert events_resp.status == 200
                body = await events_resp.text()
                # Stream should have received run.failed and closed
                assert "run.failed" in body or "stream closed" in body
