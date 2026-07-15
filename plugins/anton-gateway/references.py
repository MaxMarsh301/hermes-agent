"""Strict, side-effect-free ANTON identifiers."""
from __future__ import annotations
import re

_CONVERSATION = re.compile(r"^conversation_[0-9a-f]{32}$")
_PROJECT = re.compile(r"^(?:project_gateway|project_[0-9a-f]{32})$")
_MESSAGE = re.compile(r"^message_[0-9a-f]{32}$")


def validate_conversation_id(value: object) -> str:
    if not isinstance(value, str) or not _CONVERSATION.fullmatch(value):
        raise ValueError("invalid ANTON conversation id")
    return value


def parse_delivery_target(value: object) -> tuple[str, str]:
    if not isinstance(value, str) or not value.startswith("anton:"):
        raise ValueError("invalid ANTON delivery target")
    conversation_id = validate_conversation_id(value[6:])
    return f"anton:{conversation_id}", conversation_id


def normalize_home_target(value: object) -> str:
    return "anton:" + validate_conversation_id(value)


def parse_human_reference(reference: object) -> dict[str, str | None]:
    if not isinstance(reference, str) or len(reference) > 512 or not reference.startswith("anton:"):
        raise ValueError("invalid_reference")
    parts = reference[6:].split("/")
    if not 1 <= len(parts) <= 3 or any(not part for part in parts):
        raise ValueError("invalid_reference")
    if not _PROJECT.fullmatch(parts[0]):
        raise ValueError("invalid_reference")
    if len(parts) > 1 and not _CONVERSATION.fullmatch(parts[1]):
        raise ValueError("invalid_reference")
    if len(parts) > 2 and not _MESSAGE.fullmatch(parts[2]):
        raise ValueError("invalid_reference")
    return {"project_id": parts[0], "conversation_id": parts[1] if len(parts) > 1 else None,
            "message_id": parts[2] if len(parts) > 2 else None, "reference": reference}


def require_no_thread(thread_id: object) -> None:
    if thread_id is not None:
        raise ValueError("ANTON does not support thread_id")
