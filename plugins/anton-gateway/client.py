from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import secrets
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from .config import AntonConfig, validate_base_url

MAX_RESOLVER_REQUEST_BYTES = 16 * 1024
MAX_DELIVERY_REQUEST_BYTES = 256 * 1024
MAX_RESPONSE_BYTES = 1_048_576
MAX_DELEGATION_REQUEST_BYTES = 65_536
MAX_DELEGATION_ACK_BYTES = 2_048
DELEGATION_VERSION = "hermes-delegation-completion-v1"
DELEGATION_PATH = "/internal/anton-delegation-deliveries/v1"


class AntonTransportError(RuntimeError):
    def __init__(self, code: str, retryable: bool = False, retry_after: float | None = None):
        super().__init__(code)
        self.code, self.retryable, self.retry_after = code, retryable, retry_after


def canonical_json(value: object) -> bytes:
    """The sole JSON encoding used for a signed Gateway body."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def canonical_completion_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def iso_timestamp(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("ANTON timestamps must be timezone-aware")
    return now.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def delegation_timestamp(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("ANTON timestamps must be timezone-aware")
    utc = now.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


def _request_limit(purpose: str) -> int:
    if purpose == "resolver":
        return MAX_RESOLVER_REQUEST_BYTES
    if purpose == "delivery":
        return MAX_DELIVERY_REQUEST_BYTES
    raise ValueError("invalid ANTON request purpose")


def _retryable_status(status: int) -> bool:
    return status in {408, 429} or 500 <= status <= 599


def parse_retry_after(value: str | None, now: datetime) -> float | None:
    """Parse Retry-After delta-seconds or an IMF-fixdate against sender time."""
    if not isinstance(value, str):
        return None
    value = value.strip()
    if value.isascii() and value.isdecimal():
        return float(value)
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if retry_at.tzinfo is None or now.tzinfo is None:
        return None
    seconds = (retry_at.astimezone(timezone.utc) - now.astimezone(timezone.utc)).total_seconds()
    return seconds if seconds >= 0 else None


class AntonGatewayClient:
    def __init__(self, config: AntonConfig, *, clock=None, transport: httpx.AsyncBaseTransport | None = None):
        self.config, self._clock, self._transport = config, clock or (lambda: datetime.now(timezone.utc)), transport
        # Validate direct enabled AntonConfig construction too, not just environment loading.
        # A disabled client retains its established `disabled` transport error.
        self._base_url = (
            validate_base_url(config.base_url, allow_insecure_http=config.allow_insecure_http)
            if config.enabled
            else config.base_url
        )

    def _headers(self, purpose: str, body: bytes) -> dict[str, str]:
        _request_limit(purpose)
        key = self.config.resolver_key if purpose == "resolver" else self.config.delivery_key
        key_id = self.config.resolver_key_id if purpose == "resolver" else self.config.delivery_key_id
        timestamp = iso_timestamp(self._clock())
        # Gateway signs precisely this: no method/path prefix and no re-encoded body.
        signature = hmac.new(
            key.encode("utf-8"), timestamp.encode("utf-8") + b"\n" + body, hashlib.sha256
        ).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "X-AG-Key-Id": key_id,
            "X-AG-Timestamp": timestamp,
            "X-AG-Signature": f"sha256={signature}",
        }
        if purpose == "resolver":
            headers["X-AG-Resolver-Scope"] = "anton.resolve"
        return headers

    def _delegation_headers(self, body: bytes, key_id: str | None = None) -> dict[str, str]:
        key_id = key_id or self.config.delegation_delivery_key_id
        try:
            key = self.config.delegation_key_for(key_id)
        except ValueError:
            raise AntonTransportError("disabled")
        timestamp = delegation_timestamp(self._clock())
        nonce = secrets.token_urlsafe(32).rstrip("=")
        digest = hashlib.sha256(body).hexdigest()
        preimage = f"{DELEGATION_VERSION}\nPOST\n{DELEGATION_PATH}\n{timestamp}\n{nonce}\n{key_id}\n{digest}\n".encode("utf-8")
        signature = hmac.new(key.encode("utf-8"), preimage, hashlib.sha256).hexdigest()
        return {"Content-Type": "application/json; charset=utf-8", "X-Anton-Delegation-Version": DELEGATION_VERSION,
                "X-Anton-Delegation-Key-Id": key_id,
                "X-Anton-Delegation-Timestamp": timestamp, "X-Anton-Delegation-Nonce": nonce,
                "X-Anton-Delegation-Body-SHA256": digest, "X-Anton-Delegation-Signature": signature}

    async def deliver_delegation(self, body: bytes, delivery_id: str, *, key_id: str | None = None) -> dict[str, Any]:
        """Send stored completion bytes; this is deliberately separate from cron delivery."""
        if not self.config.enabled or not isinstance(body, bytes) or len(body) > MAX_DELEGATION_REQUEST_BYTES:
            raise AntonTransportError("disabled" if not self.config.enabled else "request_too_large")
        timeout = httpx.Timeout(self.config.timeout, connect=self.config.timeout, read=self.config.timeout, write=self.config.timeout, pool=self.config.timeout)
        try:
            async with asyncio.timeout(self.config.timeout):
                async with httpx.AsyncClient(timeout=timeout, follow_redirects=False, transport=self._transport) as client:
                    async with client.stream("POST", self._base_url + DELEGATION_PATH, headers=self._delegation_headers(body, key_id), content=body) as response:
                        status, chunks, size = response.status_code, [], 0
                        async for chunk in response.aiter_bytes():
                            size += len(chunk)
                            if size > MAX_DELEGATION_ACK_BYTES: raise AntonTransportError("ack_too_large", True)
                            chunks.append(chunk)
        except AntonTransportError: raise
        except (asyncio.TimeoutError, httpx.TimeoutException, httpx.RequestError): raise AntonTransportError("network", True) from None
        if status in {400, 401, 409, 413, 415} or 300 <= status < 400:
            raise AntonTransportError(f"http_{status}", False)
        if status == 429:
            try:
                retry_after = parse_retry_after(response.headers.get("Retry-After"), self._clock())
            except (TypeError, ValueError):
                retry_after = None
            raise AntonTransportError("http_429", True, retry_after)
        if status == 404 or status >= 500 or status < 200 or status >= 300:
            raise AntonTransportError(f"http_{status}", True)
        try: ack = json.loads(b"".join(chunks))
        except (ValueError, UnicodeDecodeError): raise AntonTransportError("invalid_ack", True) from None
        expected_keys = {"version", "deliveryId", "accepted", "deduplicated", "outcome"}
        if (not isinstance(ack, dict) or set(ack) != expected_keys or ack.get("version") != DELEGATION_VERSION
                or ack.get("deliveryId") != delivery_id or ack.get("accepted") is not True
                or ack.get("outcome") not in {"queued", "target_gone"}):
            raise AntonTransportError("invalid_ack", True)
        expected_dedup = status == 200
        if status not in {200, 202} or ack.get("deduplicated") is not expected_dedup:
            raise AntonTransportError("invalid_ack", True)
        return ack

    async def _post(self, purpose: str, path: str, body: bytes) -> dict[str, Any]:
        if not self.config.enabled:
            raise AntonTransportError("disabled")
        if not isinstance(body, bytes):
            raise AntonTransportError("invalid_request")
        if len(body) > _request_limit(purpose):
            raise AntonTransportError("request_too_large")

        # httpx has no wall-clock total timeout; the outer timeout bounds connection,
        # write, read, and pool acquisition as one request as well as each explicit phase.
        timeout = httpx.Timeout(
            self.config.timeout,
            connect=self.config.timeout,
            read=self.config.timeout,
            write=self.config.timeout,
            pool=self.config.timeout,
        )
        try:
            async with asyncio.timeout(self.config.timeout):
                async with httpx.AsyncClient(
                    timeout=timeout,
                    follow_redirects=False,
                    transport=self._transport,
                ) as client:
                    async with client.stream(
                        "POST", self._base_url + path, headers=self._headers(purpose, body), content=body
                    ) as response:
                        if not 200 <= response.status_code < 300:
                            raise AntonTransportError(
                                f"http_{response.status_code}", _retryable_status(response.status_code)
                            )
                        chunks: list[bytes] = []
                        size = 0
                        async for chunk in response.aiter_bytes():
                            size += len(chunk)
                            if size > MAX_RESPONSE_BYTES:
                                raise AntonTransportError("response_too_large")
                            chunks.append(chunk)
                        raw = b"".join(chunks)
        except AntonTransportError:
            raise
        except (asyncio.TimeoutError, httpx.TimeoutException):
            raise AntonTransportError("network", True) from None
        except httpx.RequestError:
            raise AntonTransportError("network", True) from None

        try:
            decoded = json.loads(raw)
        except (TypeError, ValueError, UnicodeDecodeError):
            raise AntonTransportError("invalid_response") from None
        if not isinstance(decoded, dict):
            raise AntonTransportError("invalid_response")
        return decoded

    async def resolve(self, payload: dict) -> dict[str, Any]:
        return await self._post("resolver", "/internal/anton-resolver/v1/resolve", canonical_json(payload))

    async def deliver(self, payload: dict, body: bytes | None = None) -> dict[str, Any]:
        return await self._post(
            "delivery", "/internal/anton-cron-deliveries/v1", body if body is not None else canonical_json(payload)
        )
