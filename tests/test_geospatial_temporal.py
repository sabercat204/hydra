"""Tests for GeospatialTemporalPipeline."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional

import pytest

from hydra.config import HydraSettings
from hydra.correlation.models import CandidateSet
from hydra.correlation.pipelines.geospatial_temporal import (
    GeospatialTemporalPipeline,
    TIER_AFFINITY,
    _tier_pair_allowed,
)
from hydra.models.normalized import (
    GeoGeometry,
    NormalizedRecord,
    SourceMeta,
    Tier,
)
from hydra.utils.hashing import compute_raw_hash


def _make_record(
    tier: Tier,
    lon: Optional[float] = None,
    lat: Optional[float] = None,
    timestamp: Optional[datetime] = None,
    raw_suffix: str = "a",
    tags: Optional[list] = None,
) -> NormalizedRecord:
    geo = None
    if lon is not None and lat is not None:
        geo = GeoGeometry(type="Point", coordinates=[lon, lat])
    ts = timestamp or datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    raw = compute_raw_hash(f"test_{raw_suffix}".encode())
    return NormalizedRecord(
        stream_id=f"test_{raw_suffix}",
        tier=tier,
        timestamp=ts,
        geo=geo,
        payload={},
        source_meta=SourceMeta(source_name="test", adapter_type="test"),
        raw_hash=raw,
        tags=tags or [],
    )


@pytest.fixture
def settings() -> HydraSettings:
    return HydraSettings()


@pytest.fixture
def pipeline(settings) -> GeospatialTemporalPipeline:
    return GeospatialTemporalPipeline(settings)


class TestGeospatialTemporalPipeline:
    async def test_spatial_match_within_radius(self, pipeline):
        """Records within 50km produce spatial match."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        rec_a = _make_record(Tier.GEOPHYSICAL_SEISMIC, lon=35.0, lat=31.0, timestamp=now, raw_suffix="a")
        rec_b = _make_record(Tier.NBC_THREAT, lon=35.1, lat=31.1, timestamp=now + timedelta(minutes=10), raw_suffix="b")

        candidates = CandidateSet(
            pipeline_id="geospatial_temporal",
            source_tiers=[1, 20],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=1)).isoformat(),
            records={1: [rec_a], 20: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        assert len(results) >= 1
        assert results[0].confidence > 0

    async def test_spatial_match_outside_radius(self, pipeline):
        """Records beyond 50km produce no match."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        rec_a = _make_record(Tier.GEOPHYSICAL_SEISMIC, lon=0.0, lat=0.0, timestamp=now, raw_suffix="a")
        rec_b = _make_record(Tier.NBC_THREAT, lon=10.0, lat=10.0, timestamp=now, raw_suffix="b")

        candidates = CandidateSet(
            pipeline_id="geospatial_temporal",
            source_tiers=[1, 20],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=1)).isoformat(),
            records={1: [rec_a], 20: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        assert len(results) == 0

    async def test_temporal_match_within_window(self, pipeline):
        """Records within 1 hour produce temporal match."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        rec_a = _make_record(Tier.GEOPHYSICAL_SEISMIC, lon=35.0, lat=31.0, timestamp=now, raw_suffix="a")
        rec_b = _make_record(Tier.NBC_THREAT, lon=35.01, lat=31.01, timestamp=now + timedelta(minutes=30), raw_suffix="b")

        candidates = CandidateSet(
            pipeline_id="geospatial_temporal",
            source_tiers=[1, 20],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=2)).isoformat(),
            records={1: [rec_a], 20: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        assert len(results) >= 1
        assert "temporal" in results[0].match_dimensions

    async def test_temporal_match_outside_window(self, pipeline):
        """Records beyond 1 hour temporal window produce lower/no temporal score."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        rec_a = _make_record(Tier.GEOPHYSICAL_SEISMIC, lon=35.0, lat=31.0, timestamp=now, raw_suffix="a")
        rec_b = _make_record(Tier.NBC_THREAT, lon=35.01, lat=31.01, timestamp=now + timedelta(hours=2), raw_suffix="b")

        candidates = CandidateSet(
            pipeline_id="geospatial_temporal",
            source_tiers=[1, 20],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=3)).isoformat(),
            records={1: [rec_a], 20: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        # Temporal matcher returns None for > 3600s, so only spatial contributes
        # With only spatial at 0.6 weight, confidence may be below threshold
        for r in results:
            assert "temporal" not in r.match_dimensions or r.match_dimensions.get("temporal", 0) == 0

    async def test_composite_score_calculation(self, pipeline):
        """Weighted score: 0.6 × spatial + 0.4 × temporal."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        # Very close in space and time
        rec_a = _make_record(Tier.GEOPHYSICAL_SEISMIC, lon=35.0, lat=31.0, timestamp=now, raw_suffix="a")
        rec_b = _make_record(Tier.NBC_THREAT, lon=35.001, lat=31.001, timestamp=now + timedelta(minutes=5), raw_suffix="b")

        candidates = CandidateSet(
            pipeline_id="geospatial_temporal",
            source_tiers=[1, 20],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=1)).isoformat(),
            records={1: [rec_a], 20: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        assert len(results) == 1
        r = results[0]
        assert "spatial" in r.match_dimensions
        assert "temporal" in r.match_dimensions
        # Verify composite is weighted sum
        expected = 0.6 * r.match_dimensions["spatial"] + 0.4 * r.match_dimensions["temporal"]
        assert abs(r.confidence - expected) < 0.01

    async def test_null_geo_skipped(self, pipeline):
        """Records without geo coordinates excluded."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        rec_a = _make_record(Tier.GEOPHYSICAL_SEISMIC, timestamp=now, raw_suffix="a")  # no geo
        rec_b = _make_record(Tier.NBC_THREAT, lon=35.0, lat=31.0, timestamp=now, raw_suffix="b")

        candidates = CandidateSet(
            pipeline_id="geospatial_temporal",
            source_tiers=[1, 20],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=1)).isoformat(),
            records={1: [rec_a], 20: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        assert len(results) == 0

    def test_tier_affinity_matrix(self):
        """Only declared tier pairs evaluated."""
        assert _tier_pair_allowed(1, 20) is True
        assert _tier_pair_allowed(20, 1) is True  # order independent
        assert _tier_pair_allowed(1, 15) is True
        assert _tier_pair_allowed(18, 19) is True
        # Not in affinity matrix
        assert _tier_pair_allowed(1, 2) is False
        assert _tier_pair_allowed(2, 3) is False

    async def test_geohash_coarse_filter(self, pipeline):
        """Adjacent geohash prefixes included in candidates."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        # Records in adjacent geohash cells but within radius
        rec_a = _make_record(Tier.GEOPHYSICAL_SEISMIC, lon=35.0, lat=31.0, timestamp=now, raw_suffix="a")
        rec_b = _make_record(Tier.NBC_THREAT, lon=35.2, lat=31.2, timestamp=now + timedelta(minutes=5), raw_suffix="b")

        candidates = CandidateSet(
            pipeline_id="geospatial_temporal",
            source_tiers=[1, 20],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=1)).isoformat(),
            records={1: [rec_a], 20: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        # Should find match if within radius (even across geohash boundaries)
        # Distance ~28km, within 50km default
        assert len(results) >= 1

    async def test_confidence_threshold_filter(self, pipeline):
        """Results below 0.5 not emitted."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        # Far apart in time (just under temporal window) and moderate distance
        rec_a = _make_record(Tier.GEOPHYSICAL_SEISMIC, lon=35.0, lat=31.0, timestamp=now, raw_suffix="a")
        rec_b = _make_record(Tier.NBC_THREAT, lon=35.4, lat=31.0, timestamp=now + timedelta(minutes=55), raw_suffix="b")

        candidates = CandidateSet(
            pipeline_id="geospatial_temporal",
            source_tiers=[1, 20],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=2)).isoformat(),
            records={1: [rec_a], 20: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        for r in results:
            assert r.confidence >= 0.5
