"""Watchlist CRUD schemas."""

from __future__ import annotations

from pydantic import BaseModel, Field


class EntityWatchlistEntry(BaseModel):
    entity_id: str
    name: str
    entity_type: str | None = None
    notes: str | None = None
    added_at: str | None = None


class RegionWatchlistEntry(BaseModel):
    region_code: str = Field(..., min_length=2, max_length=3, description="ISO 3166-1 alpha-2 or alpha-3")
    name: str | None = None
    notes: str | None = None
    added_at: str | None = None


class CreateEntityWatchlistRequest(BaseModel):
    entity_id: str
    name: str
    entity_type: str | None = None
    notes: str | None = None


class CreateRegionWatchlistRequest(BaseModel):
    region_code: str = Field(..., min_length=2, max_length=3)
    name: str | None = None
    notes: str | None = None
