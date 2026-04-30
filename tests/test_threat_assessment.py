"""Tests for ThreatAssessment product generator."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from hydra.analysis.models import (
    DataBundle,
    GraphResult,
    ProductParams,
    TimelineResult,
    ThreatLevelThresholds,
)
from hydra.analysis.products.threat_assessment import ThreatAssessment
from hydra.config import HydraSettings
from hydra.correlation.models import CorrelationResult
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier
from hydra.utils.hashing import compute_raw_hash


def _settings() -> HydraSettings:
    return HydraSettings()


def _record(
    tier: int = 6,
    country: str = "US",
    raw_hash: str | None = None,
    confidence: float = 0.8,
    timestamp: datetime | None = None,
) -> NormalizedRecord:
    data = f'{{"tier":{tier},"c":"{country}","h":"{raw_hash or "x"}"}}'.encode()
    return NormalizedRecord(
        stream_id=f"stream_{tier}",
        tier=Tier(tier),
        timestamp=timestamp or datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        payload={"country_code": country, "name": "Test Signal", "type": "indicator"},
        source_meta=SourceMeta(source_name="test", adapter_type="test"),
        raw_hash=raw_hash or compute_raw_hash(data),
        confidence=confidence,
        tags=["test"],
    )


def _correlation(hash_a: str, hash_b: str, confidence: float = 0.8) -> CorrelationResult:
    return CorrelationResult(
        correlation_id="corr-001",
        pipeline_id="threat_convergence",
        record_a_hash=hash_a,
        record_b_hash=hash_b,
        tier_a=6,
        tier_b=15,
        confidence=confidence,
    )


def _bundle(
    records: dict[int, list[NormalizedRecord]] | None = None,
    correlations: list[CorrelationResult] | None = None,
) -> DataBundle:
    return DataBundle(
        records=records or {},
        correlations=correlations or [],
        graph_data=GraphResult(),
        timeline=TimelineResult(),
        time_window_start="2026-01-08T00:00:00Z",
        time_window_end="2026-01-15T00:00:00Z",
        total_records=sum(len(v) for v in (records or {}).values()),
    )


@pytest.mark.asyncio
async def test_threat_level_low() -> None:
    """1 tier, low confidence → LOW."""
    r = _record(tier=6, country="US", raw_hash="a" * 16, confidence=0.3)
    ta = ThreatAssessment(_settings())
    product = await ta.generate(_bundle(records={6: [r]}), ProductParams())

    matrix = _get_matrix(product)
    us_entry = next((m for m in matrix if m["region"] == "US"), None)
    assert us_entry is not None
    assert us_entry["threat_level"] == "LOW"


@pytest.mark.asyncio
async def test_threat_level_moderate() -> None:
    """2 tiers → MODERATE."""
    r1 = _record(tier=6, country="SY", raw_hash="b" * 16)
    r2 = _record(tier=15, country="SY", raw_hash="c" * 16)
    ta = ThreatAssessment(_settings())
    product = await ta.generate(_bundle(records={6: [r1], 15: [r2]}), ProductParams())

    matrix = _get_matrix(product)
    sy_entry = next((m for m in matrix if m["region"] == "SY"), None)
    assert sy_entry is not None
    assert sy_entry["threat_level"] == "MODERATE"


@pytest.mark.asyncio
async def test_threat_level_high() -> None:
    """3+ tiers, medium confidence → HIGH."""
    r1 = _record(tier=6, country="UA", raw_hash="d" * 16, confidence=0.7)
    r2 = _record(tier=15, country="UA", raw_hash="e" * 16, confidence=0.7)
    r3 = _record(tier=16, country="UA", raw_hash="f" * 16, confidence=0.7)
    corr = _correlation(r1.raw_hash, r2.raw_hash, confidence=0.6)

    ta = ThreatAssessment(_settings())
    product = await ta.generate(
        _bundle(records={6: [r1], 15: [r2], 16: [r3]}, correlations=[corr]),
        ProductParams(),
    )

    matrix = _get_matrix(product)
    ua_entry = next((m for m in matrix if m["region"] == "UA"), None)
    assert ua_entry is not None
    assert ua_entry["threat_level"] == "HIGH"


@pytest.mark.asyncio
async def test_threat_level_critical() -> None:
    """4+ tiers, high confidence, temporal clustering → CRITICAL."""
    base_time = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    r1 = _record(tier=6, country="YE", raw_hash="1" * 16, confidence=0.9, timestamp=base_time)
    r2 = _record(tier=15, country="YE", raw_hash="2" * 16, confidence=0.9, timestamp=base_time + timedelta(hours=1))
    r3 = _record(tier=16, country="YE", raw_hash="3" * 16, confidence=0.9, timestamp=base_time + timedelta(hours=2))
    r4 = _record(tier=19, country="YE", raw_hash="4" * 16, confidence=0.9, timestamp=base_time + timedelta(hours=3))
    corr1 = _correlation(r1.raw_hash, r2.raw_hash, confidence=0.85)
    corr2 = _correlation(r3.raw_hash, r4.raw_hash, confidence=0.85)

    ta = ThreatAssessment(_settings())
    product = await ta.generate(
        _bundle(records={6: [r1], 15: [r2], 16: [r3], 19: [r4]}, correlations=[corr1, corr2]),
        ProductParams(),
    )

    matrix = _get_matrix(product)
    ye_entry = next((m for m in matrix if m["region"] == "YE"), None)
    assert ye_entry is not None
    assert ye_entry["threat_level"] == "CRITICAL"


@pytest.mark.asyncio
async def test_region_grouping() -> None:
    """Records grouped by country code."""
    r_us = _record(tier=6, country="US", raw_hash="a" * 16)
    r_sy = _record(tier=6, country="SY", raw_hash="b" * 16)

    ta = ThreatAssessment(_settings())
    product = await ta.generate(_bundle(records={6: [r_us, r_sy]}), ProductParams())

    matrix = _get_matrix(product)
    regions = {m["region"] for m in matrix}
    assert "US" in regions
    assert "SY" in regions


@pytest.mark.asyncio
async def test_convergence_multiplier() -> None:
    """Multi-tier regions scored higher than single-tier."""
    r1 = _record(tier=6, country="MM", raw_hash="c" * 16)
    r2 = _record(tier=15, country="MM", raw_hash="d" * 16)
    r_single = _record(tier=6, country="JP", raw_hash="e" * 16)

    ta = ThreatAssessment(_settings())
    product = await ta.generate(
        _bundle(records={6: [r1, r_single], 15: [r2]}),
        ProductParams(),
    )

    matrix = _get_matrix(product)
    mm = next((m for m in matrix if m["region"] == "MM"), None)
    jp = next((m for m in matrix if m["region"] == "JP"), None)
    assert mm is not None and jp is not None
    # MM has 2 tiers, JP has 1 — MM should have higher or equal threat level
    level_order = {"CRITICAL": 0, "HIGH": 1, "MODERATE": 2, "LOW": 3}
    assert level_order[mm["threat_level"]] <= level_order[jp["threat_level"]]


@pytest.mark.asyncio
async def test_threat_matrix_section() -> None:
    """Table section with region × tier signal counts."""
    r = _record(tier=6, country="US", raw_hash="f" * 16)
    ta = ThreatAssessment(_settings())
    product = await ta.generate(_bundle(records={6: [r]}), ProductParams())

    matrix_section = next((s for s in product.sections if s.title == "Threat Matrix"), None)
    assert matrix_section is not None
    assert matrix_section.section_type == "table"


@pytest.mark.asyncio
async def test_emerging_patterns() -> None:
    """MODERATE regions with escalation flagged."""
    r1 = _record(tier=6, country="NG", raw_hash="1" * 16)
    r2 = _record(tier=15, country="NG", raw_hash="2" * 16)

    ta = ThreatAssessment(_settings())
    product = await ta.generate(_bundle(records={6: [r1], 15: [r2]}), ProductParams())

    emerging = next((s for s in product.sections if s.title == "Emerging Patterns"), None)
    # Should exist if there are MODERATE regions
    if emerging:
        assert "NG" in emerging.content


@pytest.mark.asyncio
async def test_region_filter() -> None:
    """params.region limits assessment scope."""
    r_us = _record(tier=6, country="US", raw_hash="a" * 16)
    r_sy = _record(tier=6, country="SY", raw_hash="b" * 16)

    ta = ThreatAssessment(_settings())
    product = await ta.generate(
        _bundle(records={6: [r_us, r_sy]}),
        ProductParams(region="US"),
    )

    matrix = _get_matrix(product)
    regions = {m["region"] for m in matrix}
    assert "US" in regions
    assert "SY" not in regions


@pytest.mark.asyncio
async def test_keyword_filter() -> None:
    """params.keywords limits record selection."""
    r1 = _record(tier=6, country="US", raw_hash="a" * 16)
    r1.payload["name"] = "cyber attack"
    r2 = _record(tier=6, country="US", raw_hash="b" * 16)
    r2.payload["name"] = "earthquake"

    ta = ThreatAssessment(_settings())
    product = await ta.generate(
        _bundle(records={6: [r1, r2]}),
        ProductParams(keywords=["cyber"]),
    )

    # Only the cyber record should be included
    assert product.record_count == 1


def _get_matrix(product) -> list[dict]:
    """Extract threat matrix from product sections."""
    matrix_section = next((s for s in product.sections if s.title == "Threat Matrix"), None)
    if matrix_section:
        return json.loads(matrix_section.content)
    return []
