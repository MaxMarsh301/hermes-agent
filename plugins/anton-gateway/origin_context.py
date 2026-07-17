"""Request-scoped authenticated ANTON cron provenance."""
from __future__ import annotations
from contextvars import ContextVar, Token
from dataclasses import dataclass, replace
import re
from typing import Mapping
from .references import parse_delivery_target, validate_conversation_id

_origin: ContextVar["AntonOrigin | None"] = ContextVar("anton_trusted_cron_origin", default=None)

@dataclass(frozen=True)
class AntonOrigin:
    origin: str
    origin_reference: str | None
    origin_conversation_id: str
    hermes_session_id: str | None
    schedule_id: str | None
    execution_id: str | None
    delivery_target: str | None
    protocol: str | None = None
    mode: str | None = None
    parent_run_id: str | None = None


_ANTON_SESSION_ID_RE = re.compile(r"anton-chat-[0-9a-f]{32}\Z")
_PARENT_RUN_ID_RE = re.compile(r"run_[0-9a-f]{32}\Z")

def parse_origin(value: object) -> AntonOrigin:
    if not isinstance(value, Mapping) or set(value) - {"origin", "originReference", "originConversationId", "hermesSessionId", "scheduleId", "executionId", "deliveryTarget"}:
        raise ValueError("invalid trusted ANTON origin")
    if value.get("origin") != "anton":
        raise ValueError("invalid trusted ANTON origin")
    conversation = validate_conversation_id(value.get("originConversationId"))
    target = value.get("deliveryTarget")
    if target is not None:
        target, _ = parse_delivery_target(target)
    strings = {k: value.get(k) for k in ("originReference", "hermesSessionId", "scheduleId", "executionId")}
    if any(v is not None and (not isinstance(v, str) or len(v) > 512) for v in strings.values()):
        raise ValueError("invalid trusted ANTON origin")
    return AntonOrigin("anton", strings["originReference"], conversation, strings["hermesSessionId"], strings["scheduleId"], strings["executionId"], target)


def parse_v2_origin(value: object) -> AntonOrigin:
    """Parse only the exact authenticated ANTON Runs origin v2 context."""
    required = {"origin", "originConversationId", "hermesSessionId", "protocol", "mode"}
    if not isinstance(value, Mapping) or set(value) != required:
        raise ValueError("invalid trusted ANTON origin v2")
    if value.get("origin") != "anton" or value.get("protocol") != "anton.delegation.v1":
        raise ValueError("invalid trusted ANTON origin v2")
    mode = value.get("mode")
    if mode not in {"parent_continuation", "synthesize_only"}:
        raise ValueError("invalid trusted ANTON origin v2")
    conversation = validate_conversation_id(value.get("originConversationId"))
    session_id = value.get("hermesSessionId")
    if not isinstance(session_id, str) or not _ANTON_SESSION_ID_RE.fullmatch(session_id):
        raise ValueError("invalid trusted ANTON origin v2")
    return AntonOrigin(
        "anton", None, conversation, session_id, None, None,
        f"anton:{conversation}", "anton.delegation.v1", mode,
    )


def bind_parent_run(origin: AntonOrigin, parent_run_id: str) -> AntonOrigin:
    """Immutably bind the server-generated Runs ID to an authenticated v2 origin."""
    if origin.protocol != "anton.delegation.v1" or not _PARENT_RUN_ID_RE.fullmatch(parent_run_id):
        raise ValueError("invalid trusted ANTON origin v2")
    if origin.parent_run_id is not None:
        raise ValueError("trusted ANTON origin already bound")
    return replace(origin, parent_run_id=parent_run_id)

def bind(value: AntonOrigin) -> Token:
    return _origin.set(value)
def reset(token: Token) -> None:
    _origin.reset(token)
def get() -> AntonOrigin | None:
    return _origin.get()
