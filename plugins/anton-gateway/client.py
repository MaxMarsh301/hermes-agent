from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import secrets
from datetime import datetime, timezone
from typing import Any

import httpx

from .config import AntonConfig, validate_base_url

MAX_RESOLVER_REQUEST_BYTES = 16 * 1024
MAX_DELIVERY_REQUEST_BYTES = 256 * 1024
MAX_RESPONSE_BYTES = 1_048_576


class AntonTransportError(RuntimeError):
    def __init__(self, code: str, retryable: bool = False):
        super().__init__(code)
        self.code, self.retryable = code, retryable


def canonical_json(value: object) -> bytes:
    """The sole JSON encoding used for a signed Gateway body."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def iso_timestamp(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("ANTON timestamps must be timezone-aware")
    return now.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _request_limit(purpose: str) -> int:
    if purpose == "resolver":
        return MAX_RESOLVER_REQUEST_BYTES
    if purpose in {"delivery", "delegation"}:
        return MAX_DELIVERY_REQUEST_BYTES
    raise ValueError("invalid ANTON request purpose")


def _retryable_status(status: int) -> bool:
    return status in {408, 429} or 500 <= status <= 599


class AntonGatewayClient:
    def __init__(self, config: AntonConfig, *, clock=None, transport: httpx.AsyncBaseTransport | None = None, nonce_factory=None):
        self.config, self._clock, self._transport = config, clock or (lambda: datetime.now(timezone.utc)), transport
        self._nonce_factory = nonce_factory or (lambda: secrets.token_urlsafe(24))
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
        if purpose == "delegation":
            key, key_id = self.config.delegation_delivery_key, self.config.delegation_delivery_key_id
            nonce = self._nonce_factory()
            digest = hashlib.sha256(body).hexdigest()
            preimage = "\n".join(("1", "POST", "/internal/anton-delegation-deliveries/v1", timestamp, nonce, digest)).encode("utf-8")
            signature = hmac.new(key.encode("utf-8"), preimage, hashlib.sha256).hexdigest()
            return {"Content-Type": "application/json", "X-AG-Key-Id": key_id, "X-AG-Timestamp": timestamp,
                    "X-AG-Protocol-Version": "1", "X-AG-Nonce": nonce, "X-AG-Body-SHA256": digest,
                    "X-AG-Signature": f"sha256={signature}"}
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
        return await self._post("delivery", "/internal/anton-cron-deliveries/v1", body if body is not None else canonical_json(payload))

    async def deliver_delegation(self, payload: dict, body: bytes | None = None) -> dict[str, Any]:
        # A leftover outbox record must not make the default-off feature emit
        # a request signed with an empty or retired delegation key.
        if not self.config.delegation_delivery_available():
            raise AntonTransportError("delegation_disabled")
        return await self._post("delegation", "/internal/anton-delegation-deliveries/v1", body if body is not None else canonical_json(payload))
