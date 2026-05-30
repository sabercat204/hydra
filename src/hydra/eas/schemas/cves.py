"""CVE and exploit request/response schemas (Design §4.4).

Implements the public contract for Capability 3 — CVE & Exploit Enrichment:
CVEDetailResponse (NVD + EPSS + KEV joined view), CVESearchResult,
CVESearchParams, and ExploitSearchResult (ExploitDB / Metasploit fan-in).
Satisfies R11.1, R11.3, R11.5.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

__all__ = [
    "CVEDetailResponse",
    "CVESearchResult",
    "CVESearchParams",
    "ExploitSearchResult",
]


class CVEDetailResponse(BaseModel):
    cve_id: str = Field(..., pattern=r"^CVE-\d{4}-\d{4,7}$")
    published: datetime
    last_modified: datetime
    cvss_v3_score: float | None = Field(default=None, ge=0.0, le=10.0)
    cvss_v3_vector: str | None = None
    cwe_ids: list[str] = Field(default_factory=list)
    references: list[str] = Field(default_factory=list)
    affected_cpes: list[str] = Field(default_factory=list)
    description: str = ""
    epss_score: float | None = Field(default=None, ge=0.0, le=1.0)
    epss_percentile: float | None = Field(default=None, ge=0.0, le=1.0)
    kev_listed: bool = False
    kev_due_date: datetime | None = None
    known_ransomware_use: bool = False


class CVESearchResult(BaseModel):
    cve_id: str
    cvss_v3_score: float | None
    epss_score: float | None
    kev_listed: bool
    published: datetime


class CVESearchParams(BaseModel):
    vendor: str | None = None
    product: str | None = None
    min_cvss: float | None = Field(default=None, ge=0.0, le=10.0)
    kev_only: bool = False
    published_after: datetime | None = None
    published_before: datetime | None = None


class ExploitSearchResult(BaseModel):
    source: str  # "exploitdb" | "metasploit"
    exploit_id: str
    title: str
    type: str | None = None
    platform: str | None = None
    published_date: datetime | None = None
    cve_ids: list[str] = Field(default_factory=list)
    source_url: str | None = None
