"""Fail-closed ANTON service configuration."""
from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass
from urllib.parse import urlsplit


# The scheduler's live-adapter wait budget is deliberately larger than this
# transport attempt limit, but remains below the outbox lease duration.
MAX_TRANSPORT_TIMEOUT_SECONDS = 120.0


def _enabled() -> bool:
    return os.getenv("ANTON_GATEWAY_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}


def _allow_insecure_http() -> bool:
    """An intentionally explicit escape hatch for non-private HTTP gateways."""
    return os.getenv("ANTON_GATEWAY_ALLOW_INSECURE_HTTP", "").strip().lower() in {
        "1", "true", "yes", "on"
    }


def _is_private_http_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        # Do not resolve names here: DNS can change after validation (DNS rebinding).
        return False
    if address.is_loopback or address.is_link_local:
        return True
    # RFC 1918 is deliberately narrower than ipaddress.is_private, which also
    # labels documentation, reserved, and other non-public address space private.
    return isinstance(address, ipaddress.IPv4Address) and any(
        address in network
        for network in (
            ipaddress.ip_network("10.0.0.0/8"),
            ipaddress.ip_network("172.16.0.0/12"),
            ipaddress.ip_network("192.168.0.0/16"),
        )
    )


def validate_base_url(base_url: str, *, allow_insecure_http: bool = False) -> str:
    """Validate a Gateway origin before it can receive signed requests."""
    if not isinstance(base_url, str) or not base_url:
        raise ValueError("invalid ANTON gateway URL")
    try:
        parsed = urlsplit(base_url)
        port = parsed.port  # Forces invalid ports to fail here rather than during I/O.
    except ValueError as exc:
        raise ValueError("invalid ANTON gateway URL") from exc
    if parsed.scheme not in {"https", "http"} or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("invalid ANTON gateway URL")
    if parsed.fragment or parsed.query or port is not None and not 0 < port <= 65535:
        raise ValueError("invalid ANTON gateway URL")
    if parsed.scheme == "http" and not (allow_insecure_http or _is_private_http_host(parsed.hostname)):
        raise ValueError("insecure ANTON gateway URL is not private")
    return base_url.rstrip("/")


@dataclass(frozen=True)
class AntonConfig:
    base_url: str
    resolver_key: str
    delivery_key: str
    resolver_key_id: str
    delivery_key_id: str
    delegation_delivery_key: str = ""
    delegation_delivery_key_id: str = ""
    timeout: float = 10.0
    enabled: bool = False
    allow_insecure_http: bool = False

    @classmethod
    def from_env(cls) -> "AntonConfig":
        enabled = _enabled()
        config = cls(
            base_url=os.getenv("ANTON_GATEWAY_URL", "").rstrip("/"),
            resolver_key=os.getenv("ANTON_RESOLVER_SECRET", ""),
            delivery_key=os.getenv("ANTON_CRON_DELIVERY_SECRET", ""),
            resolver_key_id=os.getenv("ANTON_RESOLVER_KEY_ID", ""),
            delivery_key_id=os.getenv("ANTON_CRON_DELIVERY_KEY_ID", ""),
            delegation_delivery_key=os.getenv("ANTON_DELEGATION_DELIVERY_SECRET", ""),
            delegation_delivery_key_id=os.getenv("ANTON_DELEGATION_DELIVERY_KEY_ID", ""),
            timeout=float(os.getenv("ANTON_GATEWAY_TIMEOUT", "10")),
            enabled=enabled,
            allow_insecure_http=_allow_insecure_http(),
        )
        if config.timeout <= 0 or config.timeout > MAX_TRANSPORT_TIMEOUT_SECONDS:
            raise ValueError("invalid ANTON gateway timeout")
        if enabled:
            if not config.resolver_key or not config.delivery_key or not config.resolver_key_id or not config.delivery_key_id:
                raise ValueError("ANTON gateway enabled but configuration is incomplete")
            try:
                validate_base_url(config.base_url, allow_insecure_http=config.allow_insecure_http)
            except ValueError as exc:
                raise ValueError("ANTON gateway enabled but configuration is incomplete") from exc
        return config

    def delegation_delivery_available(self) -> bool:
        """Feature-off is indistinguishable from absent; require a complete sink."""
        return self.enabled and os.getenv("ANTON_DELEGATION_DELIVERY_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"} and bool(
            self.base_url and self.delegation_delivery_key and self.delegation_delivery_key_id
        )
