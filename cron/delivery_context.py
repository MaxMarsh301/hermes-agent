"""Immutable metadata propagated for one cron delivery fire."""
from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class CronDeliveryContext:
    schedule_id: str
    execution_id: str
    delivery_id: str
    delivery_target: str
    occurred_at: str
    origin: dict | None = None
