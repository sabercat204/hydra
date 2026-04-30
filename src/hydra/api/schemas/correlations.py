"""Correlation request/response schemas."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class RunCorrelationRequest(BaseModel):
    pipeline_id: Literal["geospatial_temporal", "entity_network", "threat_convergence"]
    time_window_start: str | None = None
    time_window_end: str | None = None
    trigger_tiers: list[int] | None = None


class CorrelationQueryParams(BaseModel):
    pipeline_id: str | None = None
    tier_a: int | None = None
    tier_b: int | None = None
    min_confidence: float = Field(0.0, ge=0.0, le=1.0)
    time_start: str | None = None
    time_end: str | None = None


class MatchScoreResponse(BaseModel):
    dimension: str
    score: float
    evidence: dict[str, Any]


class CorrelationResponse(BaseModel):
    correlation_id: str
    pipeline_id: str
    record_a_hash: str
    record_b_hash: str
    tier_a: int
    tier_b: int
    confidence: float
    match_dimensions: dict[str, float]
    evidence: dict[str, Any]
    created_at: str
    tags: list[str]


class CorrelationRunResponse(BaseModel):
    pipeline_id: str
    candidates_queried: int
    pairs_evaluated: int
    correlations_found: int
    correlations_new: int
    correlations_updated: int
    correlations_deduplicated: int
    persisted_pg: int
    persisted_neo4j: int
    duration_ms: float
    time_window_start: str
    time_window_end: str
    trigger_tiers: list[int] | None
