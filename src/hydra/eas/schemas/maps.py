"""Maps / geospatial response schemas (Design §4.5).

Implements the public contract for Capability 4 — Geospatial Exploration:
TileCellResponse (aggregated H3 / geohash cell), FeatureResponse (GeoJSON
Feature), and FeatureCollectionResponse (paged GeoJSON FeatureCollection with
truncation metadata). Satisfies R12.1, R13.1.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

__all__ = [
    "TileCellResponse",
    "FeatureResponse",
    "FeatureCollectionResponse",
]


class TileCellResponse(BaseModel):
    cell_id: str
    strategy: Literal["h3", "geohash"]
    resolution: int = Field(..., ge=0, le=15)
    centroid: tuple[float, float]  # (lon, lat)
    count: int = Field(..., ge=0)
    tier_breakdown: dict[int, int] = Field(default_factory=dict)
    dominant_tag: str | None = None


class FeatureResponse(BaseModel):
    type: Literal["Feature"] = "Feature"
    geometry: dict[str, Any]
    properties: dict[str, Any]


class FeatureCollectionResponse(BaseModel):
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: list[FeatureResponse]
    bbox: tuple[float, float, float, float] | None = None
    aggregation: Literal["raw", "h3", "geohash"] = "raw"
    truncated: bool = False
    total_cells: int | None = None
