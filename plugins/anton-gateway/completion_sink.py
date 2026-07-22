"""Feature-gated durable handoff from the async-delegation ledger to ANTON."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .client import canonical_json
from .config import AntonConfig
from .outbox import AntonOutbox


def available(origin: object | None) -> bool:
    """Only an already HMAC-verified ANTON origin may enable API async delivery."""
    return bool(origin is not None and getattr(origin, "origin", None) == "anton" and AntonConfig.from_env().delegation_delivery_available())


def dispatch_metadata(*, origin: object, parent_run_id: str, parent_session_id: str, profile: str) -> dict[str, str]:
    if not available(origin):
        return {}
    conversation = getattr(origin, "origin_conversation_id", "")
    if not isinstance(conversation, str) or not conversation:
        return {}
    return {"origin": "anton", "originConversationId": conversation, "deliveryTarget": f"conversation:{conversation}",
            "parentRunId": parent_run_id, "parentSessionId": parent_session_id, "profile": profile, "protocolVersion": "1"}


def handoff(event: dict[str, Any], claim_id: str) -> bool:
    """Persist immutable outbox payload before acknowledging the completion claim."""
    metadata = event.get("delivery_metadata") or {}
    if not isinstance(metadata, dict) or metadata.get("origin") != "anton":
        return False
    delegation_id = str(event.get("delegation_id") or "")
    if not delegation_id:
        return False
    # Dispatch metadata can outlive an operator disabling the feature or
    # retiring its purpose key.  Do not create a new stale outbox record in
    # that state; release the ledger claim for a future explicitly enabled
    # recovery instead.
    try:
        enabled = AntonConfig.from_env().delegation_delivery_available()
    except ValueError:
        enabled = False
    if not enabled:
        from tools.async_delegation import release_completion_delivery
        release_completion_delivery(delegation_id, claim_id)
        return False
    required_metadata = ("deliveryTarget", "parentRunId", "parentSessionId")
    if any(not isinstance(metadata.get(key), str) or not metadata[key] for key in required_metadata):
        return False
    status = str(event.get("status") or "failed")
    if status not in {"completed", "failed", "interrupted", "unknown", "error"}:
        status = "failed"
    visible_status = "failed" if status == "error" else status
    content = str(event.get("summary") or event.get("error") or "Background delegation completed.")[:65536]
    # `event` was durably written before this function runs. Derive every
    # payload field from it so recovery confirms identical bytes, rather than
    # generating a conflicting timestamp for the same delivery ID.
    completed_at = event.get("completed_at")
    if isinstance(completed_at, (int, float)):
        occurred_at = datetime.fromtimestamp(completed_at, timezone.utc).isoformat()
    elif isinstance(completed_at, str) and completed_at:
        occurred_at = completed_at
    else:
        occurred_at = datetime.now(timezone.utc).isoformat()
    payload = {"schemaVersion": "1", "deliveryId": f"delegation:{delegation_id}:terminal", "delegationId": delegation_id,
               "deliveryTarget": metadata["deliveryTarget"], "origin": "anton", "parentRunId": metadata["parentRunId"],
               "parentSessionId": metadata["parentSessionId"], "status": visible_status,
               "occurredAt": occurred_at,
               "message": {"kind": "delegation.result", "role": "assistant", "content": content, "status": visible_status}}
    # The ledger claim protects the completion while its immutable handoff is
    # made.  Never strand it on an outbox/fs failure: release it so the normal
    # startup recovery path can retry.  If acknowledgement itself loses a
    # claim, the outbox already holds the exact payload and the next attempt
    # can safely confirm that same record.
    from tools.async_delegation import complete_completion_delivery, release_completion_delivery
    try:
        AntonOutbox().create_or_confirm_same({
            "deliveryId": payload["deliveryId"],
            "deliveryType": "delegation.result",
            "payload": payload,
        })
        if complete_completion_delivery(delegation_id, claim_id):
            return True
    except Exception:
        release_completion_delivery(delegation_id, claim_id)
        raise
    release_completion_delivery(delegation_id, claim_id)
    return False
