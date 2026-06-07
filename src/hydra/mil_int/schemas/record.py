"""Record schemas — matches the surface's data model from the LOOM spec."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


_AccessPolicy = Literal[
    "open", "registration", "subscription", "restricted", "archived", "monitor_only"
]


class MilIntRecord(BaseModel):
    """A single mil_int document record served by the API."""

    source_id: str
    surface: Literal["mil_int_public_information"] = "mil_int_public_information"
    tier: int = Field(ge=100, le=199)
    country_org: str = ""
    title: str
    url: str
    content_type: str
    access_policy: _AccessPolicy = "open"
    classification: Literal["UNCLASSIFIED"] = "UNCLASSIFIED"
    ingestion_timestamp: datetime
    content_hash: str
    abstract: str = ""
    keywords: list[str] = Field(default_factory=list)
    geospatial_relevance: bool = False
    freshness_score: float = Field(default=1.0, ge=0.0, le=1.0)
    language: str = "en"


class MilIntRecordList(BaseModel):
    items: list[MilIntRecord]
    total: int
    cursor: str | None = None


__all__ = ["MilIntRecord", "MilIntRecordList"]
