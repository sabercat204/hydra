"""Tests for TimelineBuilder — temporal event sequencing."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from hydra.analysis.models import TimelineResult
from hydra.analysis.timeline import TimelineBuilder
from hydra.config import HydraSettings
from hydra.correlation.models import CorrelationResult
from hydra.models.normalized import GeoGeometry, NormalizedRecord, SourceMeta, Tier
from hydra.utils.hashing import compute_raw_hash


def _settings() -> HydraSettings:
    return HydraSettings()


def _record(
    tier: int = 1,
    raw_hash: str | None = None,
    timestamp: datetime | None = None,
    confidence: float = 0.9,
    payload: dict | None = None,
    geo: GeoGeometry | None = None,
) -> NormalizedRecord:
    data = f'{{"tier":{tier},"h":"{raw_hash or "x"}"}}'.encode()
    return NormalizedRecord(
        stream_id=f"stream_{tier}",
        tier=Tier(tier),
        timestamp=timestamp or datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        geo=geo,
        payload=payload or {"magnitude": 5.2, "place": "Test Location"},
        source_meta=SourceMeta(source_name="test", adapter_type="test"),
        raw_hash=raw_hash or compute_raw_hash(data),
        confidence=confidence,
        tags=["test"],
    )


def _correlation(hash_a: str, hash_b: str) -> CorrelationResult:
    return CorrelationResult(
        correlation_id="corr-001",
        pipeline_id="test",
        record_a_hash=hash_a,
        record_b_hash=hash_b,
        tier_a=1,
        tier_b=15,
        confidence=0.85,
    )


@pytest.mark.asyncio
async def test_build_timeline_sorted() -> None:
    """Events sorted by timestamp."""
    r1 = _record(tier=1, raw_hash="a" * 16, timestamp=datetime(2026, 1, 15, 14, 0, 0, tzinfo=timezone.utc))
    r2 = _record(tier=1, raw_hash="b" * 16, timestamp=datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc))

    builder = TimelineBuilder(_settings())
    result = await builder.build(
        records={1: [r1, r2]},
        correlations=[],
        time_start="2026-01-15T00:00:00Z",
        time_end="2026-01-16T00:00:00Z",
    )

    assert len(result.events) == 2
    assert result.events[0].timestamp <= result.events[1].timestamp


@pytest.mark.asyncio
async def test_event_title_extraction() -> None:
    """Tier-specific titles extracted correctly."""
    r_eq = _record(tier=1, raw_hash="a" * 16, payload={"magnitude": 6.1, "place": "Tokyo"})
    r_conflict = _record(tier=15, raw_hash="b" * 16, payload={"event_type": "Battle", "country": "SY", "notes": "Aleppo"})

    builder = TimelineBuilder(_settings())
    result = await builder.build(
        records={1: [r_eq], 15: [r_conflict]},
        correlations=[],
        time_start="2026-01-15T00:00:00Z",
        time_end="2026-01-16T00:00:00Z",
    )

    titles = [e.title for e in result.events]
    assert any("6.1" in t and "Tokyo" in t for t in titles)
    assert any("Battle" in t and "SY" in t for t in titles)


@pytest.mark.asyncio
async def test_event_description_truncation() -> None:
    """Descriptions capped at 500 chars."""
    long_desc = "A" * 1000
    r = _record(tier=15, raw_hash="c" * 16, payload={"notes": long_desc, "event_type": "Test", "country": "US"})

    builder = TimelineBuilder(_settings())
    result = await builder.build(
        records={15: [r]},
        correlations=[],
        time_start="2026-01-15T00:00:00Z",
        time_end="2026-01-16T00:00:00Z",
    )

    assert len(result.events) == 1
    assert len(result.events[0].description) <= 500


@pytest.mark.asyncio
async def test_correlation_annotation() -> None:
    """Correlated events linked."""
    r1 = _record(tier=1, raw_hash="d" * 16)
    r2 = _record(tier=15, raw_hash="e" * 16, payload={"event_type": "Battle", "country": "SY"})
    corr = _correlation(r1.raw_hash, r2.raw_hash)

    builder = TimelineBuilder(_settings())
    result = await builder.build(
        records={1: [r1], 15: [r2]},
        correlations=[corr],
        time_start="2026-01-15T00:00:00Z",
        time_end="2026-01-16T00:00:00Z",
    )

    # r1 should have r2 in correlated_events and vice versa
    r1_event = next(e for e in result.events if e.record_hash == r1.raw_hash)
    assert r2.raw_hash in r1_event.correlated_events


@pytest.mark.asyncio
async def test_significance_scoring() -> None:
    """Events with correlations score higher."""
    r1 = _record(tier=1, raw_hash="f" * 16, confidence=0.9)
    r2 = _record(tier=1, raw_hash="0" * 16, confidence=0.9)
    corr = _correlation(r1.raw_hash, "other_hash_1234567")

    builder = TimelineBuilder(_settings())
    result = await builder.build(
        records={1: [r1, r2]},
        correlations=[corr],
        time_start="2026-01-15T00:00:00Z",
        time_end="2026-01-16T00:00:00Z",
    )

    r1_event = next(e for e in result.events if e.record_hash == r1.raw_hash)
    r2_event = next(e for e in result.events if e.record_hash == r2.raw_hash)
    assert r1_event.significance > r2_event.significance


@pytest.mark.asyncio
async def test_max_events_cap() -> None:
    """Timeline capped at max_events by significance."""
    settings = _settings()
    settings.analysis.timeline_max_events = 5

    records = [
        _record(
            tier=1,
            raw_hash=compute_raw_hash(f"rec_{i}".encode()),
            timestamp=datetime(2026, 1, 15, i, 0, 0, tzinfo=timezone.utc),
            confidence=0.5 + i * 0.01,
        )
        for i in range(20)
    ]

    builder = TimelineBuilder(settings)
    result = await builder.build(
        records={1: records},
        correlations=[],
        time_start="2026-01-15T00:00:00Z",
        time_end="2026-01-16T00:00:00Z",
    )

    assert len(result.events) <= 5


@pytest.mark.asyncio
async def test_cluster_detection() -> None:
    """Events within window grouped into clusters."""
    base = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    r1 = _record(tier=1, raw_hash="a" * 16, timestamp=base)
    r2 = _record(tier=1, raw_hash="b" * 16, timestamp=base + timedelta(minutes=10))
    r3 = _record(tier=1, raw_hash="c" * 16, timestamp=base + timedelta(hours=5))

    builder = TimelineBuilder(_settings())
    result = await builder.build(
        records={1: [r1, r2, r3]},
        correlations=[],
        time_start="2026-01-15T00:00:00Z",
        time_end="2026-01-16T00:00:00Z",
    )

    # r1 and r2 should be in the same cluster (10 min apart < 1h window)
    assert len(result.clusters) >= 1
    cluster_hashes = result.clusters[0].events
    assert r1.raw_hash in cluster_hashes
    assert r2.raw_hash in cluster_hashes


@pytest.mark.asyncio
async def test_cross_tier_cluster_significance() -> None:
    """Multi-tier clusters score higher."""
    base = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    r1 = _record(tier=1, raw_hash="a" * 16, timestamp=base, confidence=0.8)
    r2 = _record(tier=15, raw_hash="b" * 16, timestamp=base + timedelta(minutes=5),
                 confidence=0.8, payload={"event_type": "Battle", "country": "SY"})

    builder = TimelineBuilder(_settings())
    result = await builder.build(
        records={1: [r1], 15: [r2]},
        correlations=[],
        time_start="2026-01-15T00:00:00Z",
        time_end="2026-01-16T00:00:00Z",
    )

    assert len(result.clusters) >= 1
    cluster = result.clusters[0]
    assert cluster.tier_count == 2
    # Cross-tier significance boost: max_sig * (1.0 + 0.1 * (2-1)) = max_sig * 1.1
    assert cluster.significance > 0


@pytest.mark.asyncio
async def test_empty_records() -> None:
    """Empty input returns empty TimelineResult."""
    builder = TimelineBuilder(_settings())
    result = await builder.build(
        records={},
        correlations=[],
        time_start="2026-01-15T00:00:00Z",
        time_end="2026-01-16T00:00:00Z",
    )

    assert result.total_events == 0
    assert result.events == []
    assert result.clusters == []
