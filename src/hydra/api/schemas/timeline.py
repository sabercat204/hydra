"""Timeline request/response schemas."""

from __future__ import annotations

from pydantic import BaseModel, Field


class TimelineRequest(BaseModel):
    tiers: list[int] | None = None
    time_start: str
    time_end: str
    region: str | None = None
    keywords: list[str] | None = None
    max_events: int = Field(500, ge=1, le=5000)
    min_significance: float = Field(0.0, ge=0.0, le=1.0)


class TimelineEventResponse(BaseModel):
    timestamp: str
    record_hash: str
    tier: int
    stream_id: str
    title: str
    description: str
    geo: dict | None
    significance: float
    correlated_events: list[str]


class EventClusterResponse(BaseModel):
    cluster_id: str
    events: list[str]
    centroid_time: str
    centroid_geo: dict | None
    tier_count: int
    significance: float


class TimelineResultResponse(BaseModel):
    events: list[TimelineEventResponse]
    time_window_start: str
    time_window_end: str
    total_events: int
    tiers_represented: list[int]
    clusters: list[EventClusterResponse]
