# Implementation plan: Hermes plugin `anton-gateway` and reliable ANTON cron delivery

## 1. Goal, scope, and invariants

Implement the Hermes side of the Gateway contract in `docs/anton-reference-resolver-cron-delivery-contract.md` without changing Gateway ownership boundaries:

- Hermes owns cron schedules, firing, execution state, the delivery outbox, retry scheduling, and dead-letter state.
- Gateway owns ANTON project/conversation/message persistence, inbox idempotency, unread/SSE/UI effects, reference ancestry validation, and authorization of the destination conversation.
- A single plugin named `anton-gateway` registers exactly one tool, `anton_resolve`, and one platform, `anton`.
- `anton` platform `chat_id` is the Gateway `conversation_id`; the only persisted cron routing form is `anton:conversation_<32 lowercase hex>`.
- A human reference (`anton:project/...[/conversation[/message]]`) is only accepted for resolver and create/update normalization. It is never a fire-time delivery target.
- Each cron fire has one immutable `deliveryId`, created and durably persisted **before** a route/send attempt. Live delivery, standalone delivery, every retry, and observability use that exact ID.
- No secret values, real environment values, browser credentials, raw signatures, full payloads, or artifact bytes appear in code comments, logs, job list output, or this plan.

## 2. Prerequisites and decisions to lock before coding

1. **Gateway service contract review.** Treat `anton-reference-resolver-cron-delivery-contract.md` as authoritative. Obtain versioned request/response schemas and service-auth conventions from Gateway owners before enabling either endpoint:
   - `POST /internal/anton-resolver/v1/resolve`;
   - `POST /internal/anton-cron-deliveries/v1`.
2. **Plugin distribution model.** Add a plugin package in the Hermes repository as `plugins/anton-gateway/` (rather than a core `tools/` module), with `plugin.yaml`, `__init__.py`, and small internal modules. Its manifest uses `name: anton-gateway`, `kind: backend`, and `provides_tools: [anton_resolve]`.
3. **Configuration surface.** Define a typed, documented plugin configuration read from `plugins.entries.anton-gateway` plus named environment *keys* only (base URL, resolver/delivery auth key selector, timeout, body bounds, retry policy, feature flag). Never make the plugin auto-enable merely because a URL is present. Fail closed if required configuration is absent or invalid.
4. **Delivery data model choice.** Use a profile-local, append-safe durable store under `get_hermes_home() / "cron"` (for example `anton_delivery_outbox.json` plus lock/atomic replacement), not `jobs.json` alone. Keep schedule records compact; retain a stable `execution_id`, `delivery_id`, canonical payload digest, state, attempts, timing, and safe error code in outbox records. Establish retention/compaction policy before implementation.
5. **No implicit migration behavior.** Existing jobs retain current behavior. ANTON behavior is introduced only when the plugin is enabled and a target is explicitly ANTON-normalized. Legacy jobs do not acquire an ANTON origin or delivery target by inference.

## 3. Plugin layout and registration

Create the following implementation units:

```text
plugins/anton-gateway/
  plugin.yaml
  __init__.py
  config.py                 # validated plugin config and feature gates
  references.py             # strict ANTON target/reference validators
  client.py                 # shared bounded signed HTTP transport
  resolver_tool.py          # schema + handler for anton_resolve
  platform.py               # AntonAdapter + standalone sender
  delivery.py               # canonical payload builder and response classifier
  outbox.py                 # durable outbox, lease/claim, retry/dead-letter
  origin_context.py         # trusted contextvars and validation
```

Implementation steps:

1. In `plugins/anton-gateway/__init__.py`, make `register(ctx)` import only lightweight registration code and call:
   - `ctx.register_tool(name="anton_resolve", toolset="anton-gateway", ...)`;
   - `ctx.register_platform(name="anton", label="ANTON", adapter_factory=..., check_fn=..., cron_deliver_env_var="ANTON_HOME_CONVERSATION_ID", standalone_sender_fn=...)`.
2. Follow the existing `PluginContext.register_tool` contract in `hermes_cli/plugins.py` and platform registration contract in `gateway/platform_registry.py`; do not bypass registries or mutate their private maps.
3. Ensure the platform factory returns an adapter conforming to `gateway.platforms.base.BasePlatformAdapter`: `connect`, `disconnect`, `send`, and `name`. It is a delivery-only adapter; it must not pretend to ingest Gateway browser/chat events.
4. Make `check_fn` validate dependency availability and safe static configuration shape without network calls or secrets in diagnostics. `validate_config` and `is_connected` must remain deterministic and must not turn a missing home target into a valid explicit target.
5. Keep imports lazy enough that ordinary CLI paths do not require HTTP libraries or initiate network activity. This matches deferred plugin platform loading in `gateway/platform_registry.py`.

## 4. Strict identifiers, target normalization, and resolver tool

### 4.1 Pure validation module

Implement pure helpers in `plugins/anton-gateway/references.py`, independently unit-testable:

- `parse_human_reference(reference)` accepts only exact `anton:{project}`, `anton:{project}/{conversation}`, and `anton:{project}/{conversation}/{message}` syntax from the Gateway contract; maximum input length is 512; no trim, decoding, Unicode normalization, URL interpretation, aliases, extra segments, or case folding.
- `validate_conversation_id(value)` accepts only `conversation_[0-9a-f]{32}`.
- `parse_delivery_target(value)` accepts only `anton:conversation_[0-9a-f]{32}` and returns the canonical target/conversation ID.
- `normalize_home_target(value)` accepts only a bare valid `conversation_id` and returns `anton:{conversation_id}`.
- Reject a project/message target, empty target, malformed ID, `anton:` human hierarchy passed to delivery, and any non-null `thread_id`. Do not silently fall back to home or origin after an invalid explicit ANTON target.

### 4.2 `anton_resolve`

1. Add a JSON Schema in `resolver_tool.py` for `anton_resolve(reference, options)` with `reference` required and `options` object-shaped. Mirror contract limits: `include` allowlist, bounded `context.before/after/maxMessages`, boolean options, and `additionalProperties: false` where the tool schema permits it.
2. The handler validates the local shape before network I/O, calls the Gateway resolver through `AntonGatewayClient.resolve`, and returns the stable Gateway envelope unchanged except for local transport errors converted to a safe tool error.
3. Preserve Gateway outcomes (`resolved`, `parent_mismatch`, `invalid_reference`, `not_found`) and nullable envelope fields; do not infer ancestry locally or “repair” a moved reference.
4. Treat returned `content`, message context, attachments, project instructions, and all Gateway-derived text as untrusted quoted data. Document this in the tool description and avoid feeding it into policy/configuration pathways.
5. Gate the tool with plugin configuration/service auth availability. It is registered as toolset `anton-gateway`, but cron jobs opt into it only through `enabled_toolsets` when they need reference resolution; it is not globally enabled for all cron agents.

### 4.3 Create/update cron normalization

Extend `tools/cronjob_tools.py` at the create/update validation boundary:

1. Detect ANTON input only when `deliver` is `anton`, `anton:...`, or an explicit ANTON reference field introduced for this integration (prefer a clearly named `anton_reference` over overloading arbitrary `deliver` text).
2. For a human conversation/message reference, call `anton_resolve` client-side with metadata-only scope, require an authorized resolved resource, extract the verified conversation ID, and persist only `deliver: "anton:{conversation_id}"`.
3. Persist the resolver’s canonical human reference only in non-routing audit/display metadata (for example `anton.display_reference`), never as `deliver` or `delivery_target`.
4. Reject a project reference for delivery; a message reference is valid only as create/update selection context. A conversation moved later remains routable because the stored target is conversation-only.
5. Update `cron.jobs.create_job` / `update_job` validation and normalization so direct Python callers cannot bypass the tool boundary. Preserve existing non-ANTON target semantics.
6. For `deliver=anton` (bare platform) resolve `ANTON_HOME_CONVERSATION_ID` only through the platform registry’s `cron_deliver_env_var`; missing/empty/invalid value is an explicit resolution error. An explicit canonical target remains valid even when the home variable is absent.

## 5. Shared signed HTTP client and protocol parity

Implement `plugins/anton-gateway/client.py` as the only network implementation used by both resolver and delivery.

1. Use an async HTTP client with explicit connect/read/write/total timeouts, TLS verification enabled by default, a configurable private/loopback deployment policy, bounded redirects (prefer disabled), bounded request/response body sizes, `application/json`, and no retry hidden in the HTTP library.
2. Produce the canonical JSON bytes once for a delivery. Sign the **exact raw bytes** with the designated delivery key purpose, key ID, timestamp, and method/path according to the versioned Gateway contract. Use a separate resolver auth purpose/scope; never reuse browser auth, a generic bearer key, or a delivery signing key for resolver access.
3. Keep signing material behind a narrow provider interface. Redact authorization headers/signatures and raw bodies in exceptions/logs. Use safe identifiers, HTTP status family, correlation ID, and short machine error code only.
4. Validate the Gateway acknowledgement schema. A 2xx acknowledgement must identify the accepted `deliveryId` (or contract-equivalent idempotent acknowledgement) before an outbox record becomes delivered.
5. Map outcomes centrally in `delivery.py`:
   - retryable: connection/DNS/TLS transient failure, timeout, `408`, `429` (honor bounded `Retry-After`), and `5xx`;
   - terminal: signature/schema/auth/target/authorization/ancestry failures and other `4xx` except declared retryable statuses;
   - same `deliveryId` + conflicting canonical payload is terminal security failure and must be audited safely.
6. Live `AntonAdapter.send(...)` and `standalone_sender_fn(...)` call the same payload-builder/client method. They may differ only in adapter lifecycle; they may not use different headers, unsigned fallback, target grammar, retry classification, or ID generation.

## 6. Extend the internal delivery contracts for per-fire metadata

Current seams lose stable fire metadata: `cron.scheduler` currently routes mostly `job_id`, `DeliveryRouter` only carries generic metadata, and `PlatformEntry.standalone_sender_fn` receives no delivery context. Extend them deliberately rather than tunneling IDs through unvalidated strings.

1. Add an internal `CronDeliveryContext` / dataclass (choose a low-coupling module such as `cron/delivery_context.py`) containing immutable `schedule_id`, `execution_id`, `delivery_id`, normalized target, occurred-at timestamp, origin provenance, content/artifact references, and canonical payload digest.
2. Update `gateway/platform_registry.PlatformEntry.standalone_sender_fn` contract to receive an optional typed `delivery_context`/`metadata` keyword while preserving existing plugin senders through a backwards-compatible default. Update `tools/send_message_tool._send_via_adapter` and `_send_to_platform` to forward it unchanged when present.
3. Update `gateway/delivery.DeliveryRouter.deliver` and `_deliver_to_platform` to accept/pass the same context to live adapters, with no target reconstruction and no conversion to an untrusted message field.
4. Update the adapter `send` interface carefully (optional metadata/context) so existing platforms are unaffected. `AntonAdapter.send` requires the context and rejects `thread_id` rather than ignoring it.
5. In `cron/scheduler.py`, create/claim the outbox record before calling any live/standalone route. Pass its immutable context through `_deliver_result` exactly once. Never mint `deliveryId` in `DeliveryRouter`, `AntonAdapter`, standalone sender, retry worker, or Gateway request fallback.

## 7. Durable outbox, retry, and dead-letter lifecycle

Implement `plugins/anton-gateway/outbox.py` with the same profile-local locking and atomic-write standards used by `cron/jobs.py` (`_jobs_lock`, `atomic_replace` patterns), but in a distinct store to avoid corrupting or bloating `jobs.json`.

1. **Record creation.** At each claimed fire in `cron.scheduler.run_one_job`, create an `executionId` and a globally unique opaque `deliveryId`; write an outbox record in `pending` state before attempting ANTON delivery. For one cron fire with one ANTON target there is one record; if future fan-out is allowed, define one record per target with separate `deliveryId` rather than sharing an ID across destinations.
2. **Immutable fields.** Persist `deliveryId`, schedule/job ID, execution ID, canonical `anton:conversation_...` target, origin snapshot, occurred-at, canonical payload bytes or integrity digest plus reconstructible bounded payload, and schema version. Do not mutate these across retries.
3. **Mutable fields.** Track state (`pending`, `in_flight`, `delivered`, `failed`, `dead_letter`), attempt count, last attempt/next attempt times, lease owner/expiry, final safe error code, and Gateway correlation ID if safely supplied.
4. **Atomicity/recovery.** Use file locks plus atomic replacement for every state transition. Recover expired `in_flight` leases after crash without changing ID/payload. Make outbox claiming safe for gateway ticker, standalone scheduler, manual `cronjob(action='run')`, and any external provider path.
5. **Retry policy.** Persist exponential backoff with bounded jitter and finite attempt/time budget. Retry only centrally classified transient failures. Do not block the scheduler tick with sleep: a retry worker/tick scans due outbox records. A terminal response goes directly to `failed`/`dead_letter`; an exhausted retryable record becomes `dead_letter`.
6. **Completion semantics.** Gateway 2xx changes an outbox record to `delivered`; only then clear the delivery error for that execution. A job’s agent execution may be `ok` while its delivery is pending/dead-letter, so expose distinct execution and delivery states rather than rerunning the agent just to resend output.
7. **Retention and operations.** Retain terminal records for a bounded configurable period/count, compact atomically, provide an operator-only inspect/retry command or API later (never automatic payload editing), and emit metrics for pending age, attempts, delivered, terminal failures, and dead letters.
8. **No false success.** Do not let `DeadTargetRegistry`, silence filtering, output truncation-to-local-file, attachment path processing, or session mirroring turn an ANTON pending failure into delivery success. ANTON must not use `MEDIA:/local/path` semantics; artifacts are contract-bounded metadata only.

## 8. Scheduler routing behavior and ANTON platform constraints

Modify `cron/scheduler.py` target resolution paths (`_resolve_single_delivery_target`, `_resolve_delivery_targets`, `_resolve_delivery_target`, `_deliver_result`) as follows:

1. Consult plugin platforms through `platform_registry` and recognize `anton` because its entry declares `cron_deliver_env_var="ANTON_HOME_CONVERSATION_ID"`.
2. Canonicalize `anton:{conversationId}` at create/update and revalidate it at fire time without calling the resolver. Explicit ANTON target is independent of a home target.
3. Reject `anton:{conversationId}:{threadId}` and every non-null `thread_id` for ANTON from scheduler, router, live adapter, and standalone sender. Do not use generic `platform:chat:thread` splitting for ANTON without its strict branch.
4. `deliver=origin` uses the stored trusted ANTON origin only if it contains a valid `originConversationId`; no origin means an explicit target-resolution error for ANTON, not an arbitrary home/channel fallback. Keep existing non-ANTON fallback behavior unchanged.
5. Preserve `deliver=all` behavior for existing platforms, but exclude ANTON from automatic broad fan-out unless a future explicit product decision and Gateway authorization model permit it.
6. Bypass legacy response wrapper/footer, local file truncation, media extraction, and Hermes mirror for the ANTON structured delivery path. Build the Gateway `cron.result` payload with bounded content/artifact metadata and required provenance instead.

## 9. Trusted cron-origin context on `/v1/runs`

Introduce `plugins/anton-gateway/origin_context.py` with private `contextvars.ContextVar` state and narrow public bind/get/reset functions. The trusted values are `origin`, `originReference`, `originConversationId`, `hermesSessionId`, `scheduleId`, `executionId`, and normalized `deliveryTarget`.

1. In `gateway/platforms/api_server.py`, extend only the authenticated `/v1/runs` service-to-service path. After `_check_auth`, parse dedicated origin headers and/or a strictly typed body metadata object according to the Gateway contract; enforce `extra=forbid`, sizes, exact ANTON ID/target validators, and service authorization before binding context.
2. Bind the trusted context around the executor/run lifetime in both `_run_agent` and `/v1/runs` `_run_sync` paths, and reset it in `finally` alongside session/approval context. Do not place trusted values in process environment variables or global mutable state.
3. Do not change `_bind_api_server_session`: it must continue to set `platform="api_server"` and `async_delivery=False`. Trusted provenance is additional context, not a platform spoof.
4. Update `tools/cronjob_tools._origin_from_env` to first read trusted context and construct the persisted origin snapshot from it; only when absent use current `gateway.session_context` env-derived origin behavior. User prompt/body/env fields must not be able to forge or override a trusted value.
5. Store immutable origin provenance with the schedule/execution. Explicit `deliver="anton:conversation_..."` always wins for routing. `deliver="origin"` may use trusted origin; an origin conversation is provenance and need not equal a separately authorized delivery target.
6. Preserve current cron default of fresh session. If Gateway later requests conversation continuity, make it an explicit policy that supplies bounded validated `conversation_history`; never assume `hermesSessionId` alone hydrates history.

## 10. Gateway dependency contract and boundary checks

Before enabling production traffic, Gateway must provide and test:

1. Resolver endpoint with the stable v1 envelope, strict human-reference parsing, exact ancestry, scoped projections, bounded context/cursors, safe `parent_mismatch`/`not_found` behavior, and no secret/raw credential fields.
2. Delivery endpoint with strict v1 schema/body bounds; separate service key namespace/purpose; timestamp/key-ID/replay checks; constant-time signature validation; TLS/private network policy; target validation limited to `anton:conversation_id`; authorization independent of submitted parent/origin; and safe auditing.
3. Transactional inbox keyed by `deliveryId`: same canonical payload returns idempotent success; a changed canonical payload returns `409 idempotency_conflict`; only a new accepted delivery creates one assistant message, unread increment, and SSE/UI event.
4. Gateway-owned provenance storage (`deliveryId`, schedule/execution IDs, origin) either additively on messages or in an immutable linked table; no Hermes-generated message IDs/timestamps/unread counts are authoritative.
5. Bounded content/artifact policy: opaque artifact IDs/metadata only, no local paths, arbitrary URLs, base64 bytes, or credentials; partial artifact handling must not violate message idempotency.
6. Version negotiation/compatibility: Hermes sends `schemaVersion: "1"`; Gateway rejects unknown incompatible versions safely. Both sides publish safe machine-readable errors and correlation IDs.

## 11. Bite-sized implementation order

1. Add plugin scaffold/manifest and registration tests without network behavior.
2. Add pure reference/target validators and their exhaustive unit tests.
3. Implement typed plugin config and shared HTTP/signing abstractions with fake transport tests.
4. Implement `anton_resolve`, tool schema, toolset gating, and resolver contract tests.
5. Add create/update normalization plus audit metadata; test direct tool and direct `create_job`/`update_job` paths.
6. Add trusted origin contextvars and API-server parsing/binding/reset tests, preserving API-server session semantics.
7. Introduce `CronDeliveryContext` and backward-compatible metadata propagation through scheduler, `DeliveryRouter`, `send_message_tool`, `PlatformEntry`, and live/standalone adapter seams.
8. Implement outbox storage, atomic transitions, recovery leases, retry scheduler, dead-letter visibility, and then attach it to ANTON-only `_deliver_result` handling.
9. Implement `AntonAdapter` and standalone sender as wrappers over one client/payload implementation; test byte-for-byte/signature-input parity.
10. Integrate with a Gateway contract test environment, then ship behind a feature flag and canary policy.

## 12. Test plan and exact test locations

Add focused tests rather than broad integration-only coverage:

- `tests/plugins/anton_gateway/test_registration.py`: manifest, `register(ctx)`, tool name/toolset, platform entry, `cron_deliver_env_var`, adapter contract, and no eager network/import side effect.
- `tests/plugins/anton_gateway/test_references.py`: valid IDs; invalid uppercase/whitespace/URL/percent encoded/extra segment/project/message target; no trim/normalization; `thread_id` rejection.
- `tests/plugins/anton_gateway/test_resolver_tool.py`: schema rejection, bounded options, service error redaction, stable pass-through envelope, untrusted content handling, and toolset availability.
- `tests/plugins/anton_gateway/test_client.py`: exact-body signing input, key ID/timestamp headers, response bounds, timeout classification, no secret logging, and resolver/delivery auth-purpose separation.
- `tests/plugins/anton_gateway/test_platform_delivery.py`: live `send` and standalone sender produce identical canonical request/payload/`deliveryId`; reject missing context/non-null thread; idempotent acknowledgement handling.
- `tests/plugins/anton_gateway/test_outbox.py`: write-before-send, atomic transition, crash/lease recovery, same-ID retries, jitter/backoff bounds, retryable vs terminal classifier, exhaustion to dead letter, retention/compaction, and concurrent claim behavior.
- `tests/tools/test_cronjob_tools.py`: ANTON reference normalization at create/update; project rejection; message-to-conversation extraction; explicit target works without home target; invalid home/origin produces error; toolset opt-in is persisted.
- `tests/cron/test_jobs.py`: schedule persistence includes canonical delivery target and audit metadata but never human reference as routing key; direct callers cannot inject invalid ANTON routes.
- `tests/cron/test_scheduler.py`: plugin `anton` discovery via `cron_deliver_env_var`; `deliver=origin`; explicit target precedence; thread rejection; no ANTON fan-out in `all`; per-fire IDs are generated once and forwarded through live/standalone/retry; agent execution success remains distinct from outbox state.
- `tests/gateway/test_delivery.py`: `DeliveryRouter` passes typed context without rewriting it and ANTON bypasses generic thread/media/truncation behavior while existing adapters retain regression coverage.
- `tests/tools/test_send_message_tool.py`: optional `delivery_context` forwarding to standalone plugins is backward compatible and ANTON preserves it.
- `tests/gateway/test_platform_registry.py`: optional standalone context contract and deferred plugin registration remain compatible.
- `tests/gateway/test_api_server.py`: authenticated valid trusted origin binds only for the request; invalid/unknown fields fail closed; context resets after exception/concurrent request; `platform=api_server` and `async_delivery=False` remain unchanged; explicit target does not depend on origin.
- Gateway/Hermes contract suite (owned jointly): resolver parsing/ancestry/move outcomes; delivery signature/replay/schema/size failures; target authorization; duplicate same payload and conflict payload; one message/unread/SSE; timeout/5xx retry and permanent 4xx dead-letter; artifact bounds; origin A → target B.

Run targeted pytest files after each slice, then the affected full suites (`tests/plugins`, `tests/cron`, `tests/gateway`, `tests/tools`). Add a hermetic fake Gateway server for signed HTTP/inbox scenarios; do not depend on a developer’s live Gateway or real keys.

## 13. Verification, rollout, and rollback

### Verification gates

1. Static/import checks confirm `anton-gateway` can load with no configured secrets and ordinary Hermes startup remains unaffected.
2. Unit tests prove strict target parsing, no implicit fallback for malformed ANTON input, one ID per fire, and parity of live/standalone canonical bytes.
3. Durability tests inject crash points after outbox write, before/after request, and before acknowledgement persistence; recovery retries the same ID/payload exactly once from Hermes’s perspective.
4. Contract tests prove Gateway inbox idempotency and conflict behavior, authoritative destination validation, and no duplicate Gateway message/unread/SSE on retry.
5. Security tests cover invalid/expired/replayed signatures, wrong key purpose/key ID, content/body bounds, forged cron-origin context, and log redaction.
6. Regression suites prove non-ANTON scheduler, API server, plugin registration, and existing standalone senders still work.

### Rollout

1. Land disabled-by-default code, schemas, metrics, and safe audit events; do not enable requests merely by deployment.
2. Enable resolver only for a least-privilege internal test caller, then canary with synthetic references.
3. Enable ANTON delivery for a canary schedule using a test conversation; force duplicate, timeout, 5xx, and restart/recovery scenarios before broad rollout.
4. Monitor safe metrics: outbox pending age/count, delivery success/error class, retries, dead letters, idempotent acknowledgements, target-validation rejects, and Gateway correlation IDs.
5. Gradually enable create/update normalization and cron delivery behind separate flags so resolver availability does not become a fire-time dependency.

### Rollback

- Disable outbound ANTON delivery via feature flag first; leave pending outbox records durable and visible rather than deleting them or fabricating delivery success.
- Disable new schedule creation/normalization independently; existing canonical jobs remain readable and can be paused.
- Roll back code only after confirming stored outbox/job schema is additive and older code ignores unknown fields safely. Do not rewrite IDs, erase provenance, or auto-replay dead letters during rollback.
- Re-enable only after Gateway contract/version compatibility and an operator-reviewed backlog disposition are confirmed.
