"""Analysis data models — IntelligenceProduct, DataBundle, Section, Graph, Timeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from hydra.correlation.models import CorrelationResult
from hydra.models.normalized import NormalizedRecord


# ---------------------------------------------------------------------------
# Product models
# ---------------------------------------------------------------------------

@dataclass
class ProductSection:
    """A single section within an intelligence product."""

    section_id: str
    title: str
    section_type: str  # "narrative" | "table" | "timeline" | "graph_summary" | "map" | "metrics"
    content: str
    records: list[str] = field(default_factory=list)  # raw_hash references
    correlations: list[str] = field(default_factory=list)  # correlation_id references
    confidence: float = 1.0
    order: int = 0


@dataclass
class IntelligenceProduct:
    """Structured analytical output produced by a product generator."""

    product_id: str  # UUID4
    product_type: str  # "situation_report" | "entity_dossier" | "threat_assessment"
    title: str
    classification: str  # AccessLevel: green/yellow/blue/orange/red
    generated_at: str  # ISO 8601 UTC
    time_window_start: str  # ISO 8601 UTC
    time_window_end: str  # ISO 8601 UTC
    sections: list[ProductSection] = field(default_factory=list)
    summary: str = ""
    key_findings: list[str] = field(default_factory=list)
    confidence_score: float = 0.0
    completeness_score: float = 0.0
    source_tiers: list[int] = field(default_factory=list)
    record_count: int = 0
    correlation_count: int = 0
    parameters: dict[str, Any] = field(default_factory=dict)
    product_hash: str = ""
    tags: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Graph models
# ---------------------------------------------------------------------------

@dataclass
class GraphNode:
    node_id: str
    label: str
    tier: int
    properties: dict[str, Any] = field(default_factory=dict)
    degree: int = 0


@dataclass
class CentralityScore:
    node_id: str
    label: str
    metric: str  # "degree" | "betweenness" | "pagerank"
    score: float = 0.0


@dataclass
class GraphEdge:
    source_id: str
    target_id: str
    relationship: str
    properties: dict[str, Any] = field(default_factory=dict)
    confidence: float | None = None


@dataclass
class GraphPath:
    start_id: str
    end_id: str
    path_nodes: list[str] = field(default_factory=list)
    path_edges: list[str] = field(default_factory=list)
    length: int = 0


@dataclass
class GraphResult:
    nodes: list[GraphNode] = field(default_factory=list)
    edges: list[GraphEdge] = field(default_factory=list)
    communities: list[list[str]] = field(default_factory=list)
    central_nodes: list[CentralityScore] = field(default_factory=list)
    path_results: list[GraphPath] = field(default_factory=list)
    query_duration_ms: float = 0.0


# ---------------------------------------------------------------------------
# Timeline models
# ---------------------------------------------------------------------------

@dataclass
class TimelineEvent:
    timestamp: str  # ISO 8601 UTC
    record_hash: str
    tier: int
    stream_id: str
    title: str
    description: str
    geo: dict | None = None
    significance: float = 0.0
    correlated_events: list[str] = field(default_factory=list)


@dataclass
class EventCluster:
    cluster_id: str
    events: list[str] = field(default_factory=list)  # raw_hashes
    centroid_time: str = ""
    centroid_geo: dict | None = None
    tier_count: int = 0
    significance: float = 0.0


@dataclass
class TimelineResult:
    events: list[TimelineEvent] = field(default_factory=list)
    time_window_start: str = ""
    time_window_end: str = ""
    total_events: int = 0
    tiers_represented: list[int] = field(default_factory=list)
    clusters: list[EventCluster] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Data bundle — aggregated data passed to product generators
# ---------------------------------------------------------------------------

@dataclass
class DataBundle:
    """Aggregated data package passed to product generators."""

    records: dict[int, list[NormalizedRecord]] = field(default_factory=dict)
    correlations: list[CorrelationResult] = field(default_factory=list)
    graph_data: GraphResult | None = None
    timeline: TimelineResult | None = None
    time_window_start: str = ""
    time_window_end: str = ""
    total_records: int = 0
    query_duration_ms: float = 0.0


# ---------------------------------------------------------------------------
# Product params
# ---------------------------------------------------------------------------

@dataclass
class ProductParams:
    time_window_start: str | None = None
    time_window_end: str | None = None
    tiers: list[int] | None = None
    region: str | None = None
    entity_id: str | None = None
    entity_name: str | None = None
    keywords: list[str] | None = None
    min_confidence: float = 0.0
    max_records: int = 10_000
    include_graph: bool | None = None
    include_timeline: bool | None = None


# ---------------------------------------------------------------------------
# Threat level thresholds
# ---------------------------------------------------------------------------

@dataclass
class ThreatLevelThresholds:
    moderate_min_tiers: int = 2
    moderate_min_confidence: float = 0.4
    high_min_tiers: int = 3
    high_min_confidence: float = 0.5
    critical_min_tiers: int = 4
    critical_min_confidence: float = 0.7
    critical_temporal_window_s: float = 86400.0
