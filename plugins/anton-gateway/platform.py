from __future__ import annotations
import asyncio
from datetime import datetime, timezone
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.config import Platform
from .config import AntonConfig
from .client import AntonGatewayClient, canonical_json, AntonTransportError
from .references import parse_delivery_target, require_no_thread


def build_payload(content: str, context) -> tuple[dict, bytes]:
    if context is None: raise ValueError("ANTON delivery requires immutable delivery context")
    target, _ = parse_delivery_target(context.delivery_target)
    payload = {"schemaVersion":"1", "deliveryId":context.delivery_id, "scheduleId":context.schedule_id,
      "executionId":context.execution_id, "deliveryTarget":target, "origin":"anton",
      "message":{"kind":"cron.result", "role":"assistant", "content":content, "status":"completed"},
      "occurredAt":context.occurred_at}
    return payload, canonical_json(payload)

def _validate_ack(ack: object, delivery_id: str) -> dict:
    """Only an exact v1 accepted acknowledgement commits an outbox record."""
    if not isinstance(ack, dict) or ack.get("schemaVersion") != "1" or ack.get("status") != "accepted":
        raise AntonTransportError("invalid_ack")
    if ack.get("deliveryId") != delivery_id:
        raise AntonTransportError("invalid_ack")
    if not isinstance(ack.get("messageId"), str) or not ack["messageId"]:
        raise AntonTransportError("invalid_ack")
    if not isinstance(ack.get("deduplicated"), bool):
        raise AntonTransportError("invalid_ack")
    return ack


async def send_payload(payload: dict, body: bytes | None = None):
    """Send an already-persisted protocol payload without rebuilding it."""
    delivery_id = payload.get("deliveryId")
    if not isinstance(delivery_id, str) or not delivery_id:
        raise ValueError("ANTON payload requires deliveryId")
    body = canonical_json(payload) if body is None else body
    ack = _validate_ack(await AntonGatewayClient(AntonConfig.from_env()).deliver(payload, body), delivery_id)
    return {"success": True, "message_id": ack["messageId"], "raw_response": ack}


async def send_delivery(content: str, context, thread_id=None):
    require_no_thread(thread_id)
    payload, body = build_payload(content, context)
    return await send_payload(payload, body)

class AntonAdapter(BasePlatformAdapter):
    def __init__(self, config): super().__init__(config, Platform("anton"))
    @property
    def name(self): return "anton"
    async def connect(self, is_reconnect=False): self._running=True; return True
    async def disconnect(self): self._running=False
    async def send(self, chat_id, content, reply_to=None, metadata=None):
        context = (metadata or {}).get("delivery_context")
        try:
            result = await send_delivery(content, context, (metadata or {}).get("thread_id"))
            return SendResult(success=True, message_id=result["message_id"])
        except Exception as exc:
            # DeliveryRouter returns SendResult instead of re-raising. Preserve
            # ANTON's classification for the durable scheduler's retry policy.
            return SendResult(
                success=False,
                error=str(exc),
                retryable=getattr(exc, "retryable", False),
                raw_response={"code": getattr(exc, "code", "adapter_send_failed")},
            )

    async def get_chat_info(self, chat_id: str) -> dict:
        """ANTON delivery targets are conversation-like direct channels."""
        return {"name": chat_id, "type": "dm"}

async def standalone_sender(config, chat_id, message, *, thread_id=None, delivery_context=None, **_):
    return await send_delivery(message, delivery_context, thread_id)


def retry_due(limit: int = 10) -> int:
    """Deliver persisted records only; cron jobs/agents are never re-executed."""
    from .outbox import AntonOutbox

    outbox = AntonOutbox()
    delivered = 0
    for record in outbox.due(limit=limit):
        payload = record["payload"]
        token = record["leaseToken"]
        try:
            # The outbox is the source of truth: retry precisely this payload,
            # never regenerate it or re-run the scheduled agent.
            asyncio.run(send_payload(payload))
        except Exception as exc:
            outbox.retry_or_dead_letter(
                record["deliveryId"], token, getattr(exc, "code", "delivery_failed"),
                bool(getattr(exc, "retryable", False)),
            )
        else:
            if outbox.transition(
                record["deliveryId"], expected_token=token,
                state="delivered", errorCode=None, leaseExpiresAt=None,
            ) is not None:
                delivered += 1
    return delivered

def check_requirements(): return True
def validate_config(_):
    try: AntonConfig.from_env(); return True
    except ValueError: return False
