"""Registry response schemas."""

from __future__ import annotations

from pydantic import BaseModel


class StreamSourceResponse(BaseModel):
    name: str
    url: str
    format: str
    auth: str
    notes: str


class TierResponse(BaseModel):
    id: int
    name: str
    streams: int
    access: str
    formats: list[str]
    cadence: str
    adapter: str
    fallback: str | None
    sources: list[StreamSourceResponse]


class TierListResponse(BaseModel):
    tiers: list[TierResponse]
    total: int


class AnalysisConfigResponse(BaseModel):
    sitrep_max_events_per_tier: int
    sitrep_significance_threshold: float
    sitrep_domain_groups: dict[str, list[int]]
    dossier_network_depth: int
    dossier_max_network_nodes: int
    threat_min_convergence_tiers: int
    timeline_cluster_window_s: float
    timeline_max_events: int
    default_max_records: int
