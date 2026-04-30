"""Product request/response schemas."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class GenerateProductRequest(BaseModel):
    product_type: Literal["situation_report", "entity_dossier", "threat_assessment"]
    time_window_start: str | None = None
    time_window_end: str | None = None
    tiers: list[int] | None = None
    region: str | None = None
    entity_id: str | None = None
    entity_name: str | None = None
    keywords: list[str] | None = None
    min_confidence: float = Field(0.0, ge=0.0, le=1.0)
    max_records: int = Field(10_000, ge=1, le=100_000)
    include_graph: bool | None = None
    include_timeline: bool | None = None


class ProductSectionResponse(BaseModel):
    section_id: str
    title: str
    section_type: Literal["narrative", "table", "timeline", "graph_summary", "map", "metrics"]
    content: str
    records: list[str]
    correlations: list[str]
    confidence: float
    order: int


class ProductResponse(BaseModel):
    product_id: str
    product_type: str
    title: str
    classification: str
    generated_at: str
    time_window_start: str
    time_window_end: str
    sections: list[ProductSectionResponse]
    summary: str
    key_findings: list[str]
    confidence_score: float
    completeness_score: float
    source_tiers: list[int]
    record_count: int
    correlation_count: int
    parameters: dict[str, Any]
    tags: list[str]


class ProductListResponse(BaseModel):
    products: list[ProductResponse]
