"""EAS configuration — `EASSettings` Pydantic v2 model.

Houses the root `EASSettings` object plus its nested groups, matching Design §9
and the defaults enumerated in R25.2. Validators per R25.3 / R25.4 reject
out-of-range `exposure_matching_tiers`, duplicates, and unsupported
`maps_aggregation_strategy` values. `PostureScoreWeights` enforces that the
five weights sum to `1.0 ± 1e-6` (Property 22 / Property 24).

This module intentionally does NOT import from `src/hydra/config.py` — the
root `HydraSettings` imports `EASSettings` from here, not the other way
around, to avoid a circular dependency.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------- nested groups ----------


class ScreenshotSettings(BaseModel):
    """Nested under `EASSettings.screenshot` — Playwright / OCR knobs (R6, R8)."""

    viewport: tuple[int, int] = Field(default=(1280, 800))
    timeout_seconds: int = Field(default=20, ge=1, le=120)
    user_agent: str = Field(default="HYDRA-Screenshot/1.0")
    ocr_enabled: bool = Field(default=False)
    ocr_max_chars: int = Field(default=8192, ge=256, le=65_536)
    max_concurrency: int = Field(default=4, ge=1, le=32)
    per_host_concurrency: int = Field(default=1, ge=1, le=8)
    max_bytes: int = Field(default=20_000_000, ge=1_048_576)
    private_ip_block_enabled: bool = Field(default=True)
    egress_allowlist: list[str] = Field(default_factory=list)


class ObservatorySettings(BaseModel):
    """Nested under `EASSettings.observatory` — posture report publishing (R18)."""

    publish_snapshot_minio: bool = Field(default=True)
    minio_bucket: str = Field(default="hydra-observatory")
    aggregation_tiers: list[int] = Field(default_factory=lambda: [16, 17, 19, 28, 29])


class PostureScoreWeights(BaseModel):
    """Posture-score weights. Sum must be `1.0 ± 1e-6` per Design §3.8 / Property 22."""

    w_kev: float = Field(default=0.30, ge=0.0, le=1.0)
    w_crit: float = Field(default=0.25, ge=0.0, le=1.0)
    w_vuln_density: float = Field(default=0.20, ge=0.0, le=1.0)
    w_stale: float = Field(default=0.15, ge=0.0, le=1.0)
    w_asset_surface: float = Field(default=0.10, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _sum_to_one(self) -> "PostureScoreWeights":
        total = (
            self.w_kev
            + self.w_crit
            + self.w_vuln_density
            + self.w_stale
            + self.w_asset_surface
        )
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"posture weights must sum to 1.0 (got {total})")
        return self


class CostQuota(BaseModel):
    """Per-tenant daily cost quotas (R22)."""

    screenshots_per_day: int = Field(default=500, ge=0)
    observatory_regenerations_per_day: int = Field(default=5, ge=0)
    lookup_requests_per_day: int = Field(default=100_000, ge=0)
    trends_points_per_day: int = Field(default=10_000_000, ge=0)
    cve_correlations_per_day: int = Field(default=10, ge=0)


class ExposureSeverityMap(BaseModel):
    """Severity label resolution used by `AssetMonitor`."""

    cyber_threat_default: str = "high"
    sanctions_default: str = "medium"
    kev_bonus: str = "critical"
    exploit_available_bonus: str = "critical"


class IndicatorExtractionMap(BaseModel):
    """Payload JSONPath-style expressions to extract indicators per tier."""

    tier_16: list[str] = Field(
        default_factory=lambda: [
            "$.pattern",
            "$.indicators[*]",
            "$.ip",
            "$.domain",
            "$.hostname",
        ]
    )
    tier_17: list[str] = Field(
        default_factory=lambda: [
            "$.url",
            "$.domain",
            "$.hostname",
            "$.author_profile",
        ]
    )
    tier_28: list[str] = Field(
        default_factory=lambda: [
            "$.host",
            "$.organization_domain",
        ]
    )
    tier_29: list[str] = Field(
        default_factory=lambda: [
            "$.affected_hosts[*]",
        ]
    )


class CVEFingerprintMap(BaseModel):
    """Payload JSONPath-style expressions to extract fingerprints per tier."""

    tier_16: list[str] = Field(
        default_factory=lambda: [
            "$.fingerprint",
        ]
    )
    tier_17: list[str] = Field(
        default_factory=lambda: [
            "$.fingerprint",
        ]
    )
    tier_28: list[str] = Field(
        default_factory=lambda: [
            "$.fingerprint",
            "$.service_banner",
        ]
    )


# ---------- root EASSettings ----------


class EASSettings(BaseModel):
    """HYDRA EAS — root configuration object, nested under `HydraSettings.eas`."""

    # Asset monitoring (R1, R3)
    asset_quota_per_tenant: int = Field(default=1_000, ge=1)
    exposure_matching_tiers: list[int] = Field(
        default_factory=lambda: [16, 17, 28, 29]
    )
    exposure_dedup_ttl_seconds: int = Field(default=86_400, ge=60)
    exposure_severity_map: ExposureSeverityMap = Field(
        default_factory=ExposureSeverityMap
    )
    indicator_extraction_map: IndicatorExtractionMap = Field(
        default_factory=IndicatorExtractionMap
    )
    asn_database_path: Path = Field(default=Path("data/asn/ipasn.dat"))

    # Per-tenant alerting (R5.3)
    per_tenant_webhook_url: dict[str, str] = Field(default_factory=dict)

    # Screenshots (R6, R7, R8)
    screenshot: ScreenshotSettings = Field(default_factory=ScreenshotSettings)
    images_search_max_results: int = Field(default=500, ge=1, le=5_000)

    # CVE pipeline (R10)
    cve_fingerprint_map: CVEFingerprintMap = Field(default_factory=CVEFingerprintMap)
    cve_match_mode: Literal["loose", "strict"] = "loose"
    cve_severity_map: dict[str, str] = Field(
        default_factory=lambda: {
            "critical_score_threshold": "9.0",
            "high_score_threshold": "7.0",
        }
    )

    # Maps (R12, R13)
    maps_feature_limit: int = Field(default=5_000, ge=1)
    maps_tile_max_cells: int = Field(default=2_000, ge=1, le=20_000)
    maps_aggregation_strategy: Literal["geohash", "h3"] = "h3"

    # Trends (R14)
    trends_max_window_days: int = Field(default=365, ge=1, le=3_650)
    trends_fallback_enabled: bool = True

    # Lookup (R17)
    lookup_cache_ttl_seconds: int = Field(default=300, ge=1, le=86_400)
    lookup_cache_max_entries: int = Field(default=100_000, ge=100)
    lookup_p95_latency_ms_target: int = Field(default=100, ge=10, le=10_000)
    lookup_cache_redis_db: int = Field(default=3, ge=0, le=15)
    lookup_singleflight_ttl_seconds: int = Field(default=10, ge=1, le=60)

    # Observatory (R18, R19)
    observatory: ObservatorySettings = Field(default_factory=ObservatorySettings)
    posture_score_weights: PostureScoreWeights = Field(
        default_factory=PostureScoreWeights
    )

    # Cost quota (R22)
    cost_quota: CostQuota = Field(default_factory=CostQuota)

    # ---------- validators ----------

    @field_validator("exposure_matching_tiers")
    @classmethod
    def _tiers_in_range(cls, v: list[int]) -> list[int]:
        """R25.3 — every element must be in [1, 29] and no duplicates allowed."""
        if any(t < 1 or t > 29 for t in v):
            raise ValueError("exposure_matching_tiers must be in [1, 29]")
        if len(set(v)) != len(v):
            raise ValueError("exposure_matching_tiers must not contain duplicates")
        return v

    @field_validator("maps_aggregation_strategy")
    @classmethod
    def _strategy_allowed(cls, v: str) -> str:
        """R25.4 — only `geohash` or `h3` are supported."""
        if v not in {"geohash", "h3"}:
            raise ValueError("maps_aggregation_strategy must be 'geohash' or 'h3'")
        return v
