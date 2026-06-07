"""Search request / response schemas with faceting."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from hydra.mil_int.schemas.record import MilIntRecord


class SearchFacetValue(BaseModel):
    value: str
    count: int


class SearchFacet(BaseModel):
    name: str
    values: list[SearchFacetValue]


class SearchRequest(BaseModel):
    q: str = Field("", description="Free-text query string")
    tier: list[int] | None = None
    country: list[str] | None = None
    content_type: list[str] | None = None
    access_policy: list[str] | None = None
    language: list[str] | None = None
    min_freshness: float = 0.0
    page: int = Field(1, ge=1)
    page_size: int = Field(20, ge=1, le=200)


class SearchResponse(BaseModel):
    items: list[MilIntRecord]
    total: int
    facets: list[SearchFacet] = []
    page: int
    page_size: int
    next_cursor: Optional[str] = None


__all__ = ["SearchRequest", "SearchResponse", "SearchFacet", "SearchFacetValue"]
