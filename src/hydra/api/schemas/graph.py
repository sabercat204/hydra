"""Graph query schemas."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class EntityNetworkRequest(BaseModel):
    entity_hashes: list[str] = Field(..., min_length=1, max_length=100)
    max_depth: int = Field(2, ge=1, le=5)
    max_nodes: int = Field(50, ge=1, le=500)


class ShortestPathRequest(BaseModel):
    source_hash: str
    target_hashes: list[str] = Field(..., min_length=1, max_length=20)
    max_length: int = Field(5, ge=1, le=10)


class CentralityQueryParams(BaseModel):
    tier: int | None = None
    metric: Literal["degree", "betweenness", "pagerank"] = "degree"
    top_n: int = Field(20, ge=1, le=100)


class GraphNodeResponse(BaseModel):
    node_id: str
    label: str
    tier: int
    properties: dict[str, Any]
    degree: int


class GraphEdgeResponse(BaseModel):
    source_id: str
    target_id: str
    relationship: str
    properties: dict[str, Any]
    confidence: float | None


class GraphPathResponse(BaseModel):
    start_id: str
    end_id: str
    path_nodes: list[str]
    path_edges: list[str]
    length: int


class CentralityScoreResponse(BaseModel):
    node_id: str
    label: str
    metric: str
    score: float


class GraphResultResponse(BaseModel):
    nodes: list[GraphNodeResponse]
    edges: list[GraphEdgeResponse]
    communities: list[list[str]]
    central_nodes: list[CentralityScoreResponse]
    path_results: list[GraphPathResponse]
    query_duration_ms: float
