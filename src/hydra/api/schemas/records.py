"""Record query schemas."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class RecordQueryParams(BaseModel):
    tiers: list[int] | None = None
    time_start: str | None = None
    time_end: str | None = None
    region: str | None = None
    min_confidence: float = Field(0.0, ge=0.0, le=1.0)
    limit: int = Field(1000, ge=1, le=10_000)


class TimeseriesQueryParams(BaseModel):
    stream_ids: list[str]
    time_start: str
    time_end: str
    aggregation: Literal["raw", "1m", "5m", "1h", "1d"] = "raw"
    fields: list[str] | None = None


class TextSearchParams(BaseModel):
    query: str = Field(..., min_length=1, max_length=1000)
    tiers: list[int] | None = None
    time_start: str | None = None
    time_end: str | None = None
    limit: int = Field(100, ge=1, le=1000)


class RecordResponse(BaseModel):
    stream_id: str
    tier: int
    timestamp: str
    geo: dict | None = None
    payload: dict[str, Any]
    source_meta: dict[str, Any]
    raw_hash: str
    ingested_at: str
    confidence: float
    tags: list[str]


class RecordsByTierResponse(BaseModel):
    records: dict[int, list[RecordResponse]]
    total: int
