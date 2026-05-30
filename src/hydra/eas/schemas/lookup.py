"""Fast indicator lookup response schemas (Design §4.7).

Implements the public contract for Capability 6 — Fast Indicator Lookup:
IndicatorClass literal, LookupAssetReference (tenant-scoped), LookupRecordSummary,
LookupCVECorrelation, LookupScreenshotRef, and the LookupResponse envelope.
Satisfies R16.1, R17.2. Note: `LookupResponse.asset_reference` is the only
tenant-scoped field per R17.5.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

__all__ = [
    "IndicatorClass",
    "LookupAssetReference",
    "LookupRecordSummary",
    "LookupCVECorrelation",
    "LookupScreenshotRef",
    "LookupResponse",
]


IndicatorClass = Literal["ipv4", "ipv6", "domain", "hostname", "hash"]


class LookupAssetReference(BaseModel):
    """Only populated when indicator matches an asset owned by the caller's tenant."""

    asset_id: UUID
    asset_type: str
    normalized_value: str


class LookupRecordSummary(BaseModel):
    raw_hash: str
    tier: int
    stream_id: str
    timestamp: datetime
    confidence: float


class LookupCVECorrelation(BaseModel):
    cve_id: str
    cvss_v3_score: float | None
    kev_listed: bool
    record_hash: str
    confidence: float


class LookupScreenshotRef(BaseModel):
    record_hash: str
    url: str
    rendered_at: datetime
    phash: str


class LookupResponse(BaseModel):
    indicator: str
    indicator_class: IndicatorClass
    records: list[LookupRecordSummary] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    cve_correlations: list[LookupCVECorrelation] = Field(default_factory=list)
    screenshots: list[LookupScreenshotRef] = Field(default_factory=list)
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    asset_reference: LookupAssetReference | None = None
