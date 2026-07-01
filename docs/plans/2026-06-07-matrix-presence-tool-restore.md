# Matrix Presence Tool Restore Implementation Plan

> **For Hermes:** Execute immediately; this is an approved fix request from Max.

**Goal:** Restore the `matrix` toolset used by the Matrix presence cron and ensure every status change is also mirrored to the Matrix log room.

**Architecture:** Add a focused built-in tool module `tools/matrix_presence_tools.py` with three registry tools: recent context, history, and publish. Publishing owns validation, `MatrixAdapter.set_presence(...)`, SQLite logging, and best-effort Matrix log-room mirror.

**Tech Stack:** Python stdlib, Hermes tool registry, Matrix adapter, SQLite, pytest.

---

- [x] Diagnose missing module and current registry/toolset merge behavior.
- [ ] Add tests first for tool registration, public-status validation/redaction, SQLite logging, and mirror-message behavior.
- [ ] Implement `tools/matrix_presence_tools.py` with `get_hermes_home()` paths and no secret output.
- [ ] Update cron `matrix-presence-status-3x-daily` prompt/config so it uses the restored tools and requires log-room mirror output.
- [ ] Verify with targeted pytest.
- [ ] Verify runtime discovery: registry, `resolve_toolset('matrix')`, and `get_tool_definitions(enabled_toolsets=['matrix'])` all expose the tools.
- [ ] Run dry-run publish and inspect SQLite output.
- [ ] Run one live publish/cron execution and confirm live Matrix presence plus Matrix log-room message.

## Acceptance Criteria

- `tools.matrix_presence_tools` imports successfully.
- `matrix_recent_work_sessions`, `matrix_publish_presence_status`, and `matrix_presence_status_history` are registered under toolset `matrix`.
- `matrix_publish_presence_status(..., dry_run=True)` writes a dry-run SQLite row without calling Matrix APIs.
- Non-dry-run publish sets Matrix presence, records the attempted result, and posts `Новый статус: <status_msg>` to room `!CfcJjncQIRYblHggHG:matrix.mst4.ru` when publish succeeds.
- Public status validation rejects/severely redacts obvious secrets, file paths, room IDs, MXIDs, URLs/IPs, and multiline/private text.
- Cron prompt explicitly requires final output to include whether presence changed and the exact public status text.
