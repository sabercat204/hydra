"""Source-manifest response schemas."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


_AccessPolicy = Literal[
    "open", "registration", "subscription", "restricted", "archived", "monitor_only"
]


class ManifestEntry(BaseModel):
    tier: int = Field(ge=100, le=199)
    tier_name: str
    source_name: str
    url: str
    format: str
    notes: str = ""
    access_policy: _AccessPolicy
    ingestable: bool


class ManifestResponse(BaseModel):
    surface: Literal["mil_int_public_information"] = "mil_int_public_information"
    total_sources: int
    ingestable_sources: int
    entries: list[ManifestEntry]


__all__ = ["ManifestEntry", "ManifestResponse"]
