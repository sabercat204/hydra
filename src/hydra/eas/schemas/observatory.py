"""Exposure-observatory response schemas (Design §4.8).

Implements the public contract for Capability 7 — Exposure Observatory:
CountryPostureSection (per-country posture snapshot with score + trend delta),
CountryPostureResponse (single-country endpoint view), and
ExposurePostureReportResponse (full multi-country intelligence product).
Satisfies R18.2, R19.5. The `country_code` fields use ISO 3166-1 alpha-2.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

__all__ = [
    "CountryPostureSection",
    "CountryPostureResponse",
    "ExposurePostureReportResponse",
]


class CountryPostureSection(BaseModel):
    country_code: str = Field(..., pattern=r"^[A-Z]{2}$")
    country_name: str
    posture_score: float = Field(..., ge=0.0, le=100.0)
    absolute_delta: float = Field(..., ge=-100.0, le=100.0)
    percent_delta: float
    exposed_asset_count: int
    critical_exposure_count: int
    kev_exposure_count: int
    top_cves: list[str] = Field(default_factory=list)


class CountryPostureResponse(BaseModel):
    country_code: str
    as_of: datetime
    section: CountryPostureSection


class ExposurePostureReportResponse(BaseModel):
    product_id: str
    generated_at: datetime
    countries: list[CountryPostureSection]
    summary: str
    top_countries_by_score: list[str]
