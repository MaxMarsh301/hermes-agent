from __future__ import annotations
import asyncio
import hashlib
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.config import Platform
from .config import AntonConfig, detached_completion_sink_ready
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
    if not isinstance(ack, dict) or ack.get("schemaVersion") != "1" or ack.get("status") != "accepted":
        raise AntonTransportError("invalid_ack")
    if ack.get("deliveryId") != delivery_id or not isinstance(ack.get("messageId"), str) or not ack["messageId"]:
        raise AntonTransportError("invalid_ack")
    if not isinstance(ack.get("deduplicated"), bool): raise AntonTransportError("invalid_ack")
    return ack


async def send_payload(payload: dict, body: bytes | None = None):
    delivery_id = payload.get("deliveryId")
    if not isinstance(delivery_id, str) or not delivery_id: raise ValueError("ANTON payload requires deliveryId")
    body = canonical_json(payload) if body is None else body
    ack = _validate_ack(await AntonGatewayClient(AntonConfig.from_env()).deliver(payload, body), delivery_id)
    return {"success": True, "message_id": ack["messageId"], "raw_response": ack}


async def send_delivery(content: str, context, thread_id=None):
    require_no_thread(thread_id)
    payload, body = build_payload(content, context)
    return await send_payload(payload, body)


async def _send_delegation_record(record):
    body = record["body"]
    if not isinstance(body, bytes) or hashlib.sha256(body).hexdigest() != record["bodySha256"]:
        raise AntonTransportError("digest_mismatch", False)
    return await AntonGatewayClient(AntonConfig.delegation_from_env()).deliver_delegation(body, record["deliveryId"])


async def retry_delegation_due(limit: int = 10) -> int:
    """Bounded, independent recovery; it only drains persisted work."""
    if not detached_completion_sink_ready(): return 0
    from .outbox import DelegationOutbox
    outbox = DelegationOutbox()
    outbox.ingest_handoffs(limit=limit)
    delivered = 0
    for _ in range(max(0, int(limit))):
        records = outbox.due(limit=1)
        if not records:
            break
        record = records[0]
        try:
            ack = await _send_delegation_record(record)
        except Exception as exc:
            code = getattr(exc, "code", "delegation_transport")
            status = int(code[5:]) if isinstance(code, str) and code.startswith("http_") and code[5:].isdigit() else None
            if getattr(exc, "retryable", True):
                outbox.retry(record["deliveryId"], record["leaseToken"], record["leaseVersion"], code, status, getattr(exc, "retry_after", None))
            else:
                outbox.dead_letter(record["deliveryId"], record["leaseToken"], record["leaseVersion"], code, status)
        else:
            if outbox.delivered(record["deliveryId"], record["leaseToken"], record["leaseVersion"], ack, ack["outcome"]): delivered += 1
    return delivered


class AntonAdapter(BasePlatformAdapter):
    def __init__(self, config):
        super().__init__(config, Platform("anton")); self._delegation_task = None
    @property
    def name(self): return "anton"
    async def connect(self, is_reconnect=False):
        self._running = True
        if detached_completion_sink_ready() and self._delegation_task is None:
            self._delegation_task = asyncio.create_task(self._delegation_loop())
        return True
    async def disconnect(self):
        self._running = False
        task, self._delegation_task = self._delegation_task, None
        if task:
            task.cancel()
            try: await task
            except asyncio.CancelledError: pass
    async def _delegation_loop(self):
        while self._running:
            try: await retry_delegation_due(limit=10)
            except Exception: pass
            try: await asyncio.sleep(5)
            except asyncio.CancelledError: raise
    async def send(self, chat_id, content, reply_to=None, metadata=None):
        context = (metadata or {}).get("delivery_context")
        try:
            result = await send_delivery(content, context, (metadata or {}).get("thread_id"))
            return SendResult(success=True, message_id=result["message_id"])
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=getattr(exc, "retryable", False), raw_response={"code": getattr(exc, "code", "adapter_send_failed")})
    async def get_chat_info(self, chat_id: str) -> dict:
        return {"name": chat_id, "type": "dm"}


async def standalone_sender(config, chat_id, message, *, thread_id=None, delivery_context=None, **_):
    return await send_delivery(message, delivery_context, thread_id)


def retry_due(limit: int = 10) -> int:
    """Deliver persisted cron records only; delegation recovery is adapter-owned."""
    from .outbox import AntonOutbox
    outbox, delivered = AntonOutbox(), 0
    for record in outbox.due(limit=limit):
        payload, token = record["payload"], record["leaseToken"]
        try: asyncio.run(send_payload(payload))
        except Exception as exc:
            outbox.retry_or_dead_letter(record["deliveryId"], token, getattr(exc, "code", "delivery_failed"), bool(getattr(exc, "retryable", False)))
        else:
            if outbox.transition(record["deliveryId"], expected_token=token, state="delivered", errorCode=None, leaseExpiresAt=None) is not None: delivered += 1
    return delivered


def check_requirements(): return True
def validate_config(_):
    try: AntonConfig.from_env(); return True
    except ValueError: return False
