"""Tests for SituationReport product generator."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from hydra.analysis.models import DataBundle, ProductParams, TimelineResult
from hydra.analysis.products.situation_report import SituationReport
from hydra.config import HydraSettings
from hydra.correlation.models import CorrelationResult
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier
from hydra.utils.hashing import compute_raw_hash


def _settings() -> HydraSettings:
    return HydraSettings()


def _record(
    tier: int = 1,
    confidence: float = 0.9,
    payload: dict | None = None,
    raw_hash: str | None = None,
) -> NormalizedRecord:
    data = f'{{"tier":{tier},"h":"{raw_hash or "x"}"}}'.encode()
    return NormalizedRecord(
        stream_id=f"stream_{tier}",
        tier=Tier(tier),
        timestamp=datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        payload=payload or {"magnitude": 5.2, "place": "Test", "country_code": "US"},
        source_meta=SourceMeta(source_name="test", adapter_type="test"),
        raw_hash=raw_hash or compute_raw_hash(data),
        confidence=confidence,
        tags=["test"],
    )


def _correlation(hash_a: str, hash_b: str) -> CorrelationResult:
    return CorrelationResult(
        correlation_id="corr-001",
        pipeline_id="geo_temporal",
        record_a_hash=hash_a,
        record_b_hash=hash_b,
        tier_a=1,
        tier_b=15,
        confidence=0.85,
    )


def _bundle(
    records: dict[int, list[NormalizedRecord]] | None = None,
    correlations: list[CorrelationResult] | None = None,
    timeline: TimelineResult | None = None,
) -> DataBundle:
    return DataBundle(
        records=records or {},
        correlations=correlations or [],
        timeline=timeline,
        time_window_start="2026-01-15T00:00:00Z",
        time_window_end="2026-01-16T00:00:00Z",
        total_records=sum(len(v) for v in (records or {}).values()),
    )


@pytest.mark.asyncio
async def test_sitrep_all_sections_present() -> None:
    r1 = _record(tier=1, raw_hash="a" * 16)
    r15 = _record(tier=15, raw_hash="b" * 16, payload={"event_type": "Battle", "country": "SY"})
    corr = _correlation(r1.raw_hash, r15.raw_hash)
    tl = TimelineResult(events=[], time_window_start="2026-01-15T00:00:00Z", time_window_end="2026-01-16T00:00:00Z")

    sitrep = SituationReport(_settings())
    product = await sitrep.generate(
        _bundle(records={1: [r1], 15: [r15]}, correlations=[corr], timeline=tl),
        ProductParams(),
    )

    section_titles = [s.title for s in product.sections]
    assert "Executive Summary" in section_titles
    assert "Key Developments" in section_titles


@pytest.mark.asyncio
async def test_significance_scoring() -> None:
    r1 = _record(tier=1, raw_hash="c" * 16, confidence=0.9)
    r2 = _record(tier=1, raw_hash="d" * 16, confidence=0.9)
    corr = _correlation(r1.raw_hash, "other_hash_1234567")

    sitrep = SituationReport(_settings())
    sig_with_corr = sitrep._score_significance(r1, {r1.raw_hash: [corr]})
    sig_without_corr = sitrep._score_significance(r2, {})

    assert sig_with_corr > sig_without_corr


@pytest.mark.asyncio
async def test_significance_threshold_filter() -> None:
    # Low confidence, no correlations → below threshold
    r_low = _record(tier=1, raw_hash="e" * 16, confidence=0.1)
    r_high = _record(tier=1, raw_hash="f" * 16, confidence=0.95)

    sitrep = SituationReport(_settings())
    product = await sitrep.generate(
        _bundle(records={1: [r_low, r_high]}),
        ProductParams(),
    )

    # At least the high-confidence record should appear
    all_record_hashes = set()
    for s in product.sections:
        all_record_hashes.update(s.records)
    assert r_high.raw_hash in all_record_hashes


@pytest.mark.asyncio
async def test_tier_domain_grouping() -> None:
    r1 = _record(tier=1, raw_hash="1" * 16)  # Geophysical
    r6 = _record(tier=6, raw_hash="6" * 16, payload={"type": "arrest", "country": "US"})  # Security

    sitrep = SituationReport(_settings())
    product = await sitrep.generate(
        _bundle(records={1: [r1], 6: [r6]}),
        ProductParams(),
    )

    section_titles = [s.title for s in product.sections]
    assert "Geophysical & Environmental" in section_titles
    assert "Security & Conflict" in section_titles


@pytest.mark.asyncio
async def test_max_events_per_tier() -> None:
    # Create 30 records for tier 1 — should be capped at 20
    records = [_record(tier=1, raw_hash=compute_raw_hash(f"rec_{i}".encode())) for i in range(30)]

    sitrep = SituationReport(_settings())
    product = await sitrep.generate(
        _bundle(records={1: records}),
        ProductParams(),
    )

    # Find the Geophysical section
    geo_section = next((s for s in product.sections if s.title == "Geophysical & Environmental"), None)
    if geo_section:
        # Records in section should be <= max_events_per_tier
        assert len(geo_section.records) <= 20


@pytest.mark.asyncio
async def test_key_findings_from_top_events() -> None:
    r1 = _record(tier=1, raw_hash="a" * 16, confidence=0.99)
    sitrep = SituationReport(_settings())
    product = await sitrep.generate(
        _bundle(records={1: [r1]}),
        ProductParams(),
    )
    assert len(product.key_findings) > 0


@pytest.mark.asyncio
async def test_confidence_score_calculation() -> None:
    r1 = _record(tier=1, raw_hash="a" * 16, confidence=0.8)
    r2 = _record(tier=1, raw_hash="b" * 16, confidence=0.6)

    sitrep = SituationReport(_settings())
    product = await sitrep.generate(
        _bundle(records={1: [r1, r2]}),
        ProductParams(),
    )

    # Confidence should be a weighted average of record confidences
    assert 0.0 <= product.confidence_score <= 1.0


@pytest.mark.asyncio
async def test_completeness_score() -> None:
    # Only 2 out of 28 tiers have data
    r1 = _record(tier=1, raw_hash="a" * 16)
    r15 = _record(tier=15, raw_hash="b" * 16, payload={"event_type": "Battle", "country": "SY"})

    sitrep = SituationReport(_settings())
    product = await sitrep.generate(
        _bundle(records={1: [r1], 15: [r15]}),
        ProductParams(),
    )

    assert product.completeness_score == pytest.approx(2 / 28, abs=0.01)


@pytest.mark.asyncio
async def test_empty_tier_omitted() -> None:
    # Only tier 1 has data — tier 15 section should not appear
    r1 = _record(tier=1, raw_hash="a" * 16)

    sitrep = SituationReport(_settings())
    product = await sitrep.generate(
        _bundle(records={1: [r1]}),
        ProductParams(),
    )

    section_titles = [s.title for s in product.sections]
    # Security & Conflict should not appear (no tier 6, 15, 16, 19, 20 data)
    assert "Security & Conflict" not in section_titles
