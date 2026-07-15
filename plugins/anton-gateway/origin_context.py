"""Request-scoped authenticated ANTON cron provenance."""
from __future__ import annotations
from contextvars import ContextVar, Token
from dataclasses import dataclass
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

def bind(value: AntonOrigin) -> Token:
    return _origin.set(value)
def reset(token: Token) -> None:
    _origin.reset(token)
def get() -> AntonOrigin | None:
    return _origin.get()
