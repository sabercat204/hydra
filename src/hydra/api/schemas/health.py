"""Health response schemas."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel


class EngineBackpressureResponse(BaseModel):
    engine: str
    queue_depth: int
    soft_limit: int
    hard_limit: int
    state: Literal["CLEAR", "THROTTLED", "BLOCKED"]


class BackpressureResponse(BaseModel):
    overall: Literal["CLEAR", "THROTTLED", "BLOCKED"]
    engines: dict[str, EngineBackpressureResponse]
    checked_at: str


class SchedulerHealthResponse(BaseModel):
    status: Literal["OK", "DEGRADED", "UNREACHABLE"]
    active_adapters: int
    active_by_cadence: dict[str, int]
    backpressure: BackpressureResponse
    storage_health: dict[str, Any]
    adapter_health: dict[str, Any]
    dead_streams: list[str]
    checked_at: str
