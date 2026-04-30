"""Correlation data models — CorrelationResult, MatchScore, CandidateSet."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from hydra.models.normalized import NormalizedRecord


@dataclass
class MatchScore:
    """Score for a single match dimension."""

    dimension: str  # "spatial" | "temporal" | "entity" | "keyword" | "tag" | "geographic_region"
    score: float  # 0.0–1.0
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass
class CandidateSet:
    """Set of candidate records for correlation."""

    pipeline_id: str
    source_tiers: list[int]
    time_window_start: str  # ISO 8601 UTC
    time_window_end: str  # ISO 8601 UTC
    records: dict[int, list[NormalizedRecord]] = field(default_factory=dict)  # keyed by tier
    total_records: int = 0
    query_duration_ms: float = 0.0


@dataclass
class CorrelationResult:
    """A discovered correlation between two records."""

    correlation_id: str  # UUID4
    pipeline_id: str
    record_a_hash: str
    record_b_hash: str
    tier_a: int
    tier_b: int
    confidence: float  # 0.0–1.0
    match_dimensions: dict[str, float] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    correlation_hash: str = ""  # xxhash64 dedup key
    created_at: str = ""  # ISO 8601 UTC
    tags: list[str] = field(default_factory=list)


@dataclass
class CorrelationRunResult:
    """Summary of a correlation pipeline run."""

    pipeline_id: str
    candidates_queried: int = 0
    pairs_evaluated: int = 0
    correlations_found: int = 0
    correlations_new: int = 0
    correlations_updated: int = 0
    correlations_deduplicated: int = 0
    persisted_pg: int = 0
    persisted_neo4j: int = 0
    duration_ms: float = 0.0
    time_window_start: str = ""
    time_window_end: str = ""
    trigger_tiers: list[int] | None = None


@dataclass
class PersistResult:
    """Result of persisting correlation results."""

    pg_stored: int = 0
    neo4j_stored: int = 0
    pg_errors: list[dict[str, Any]] = field(default_factory=list)
    neo4j_errors: list[dict[str, Any]] = field(default_factory=list)
