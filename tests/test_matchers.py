"""Tests for correlation matchers."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional

import pytest

from hydra.correlation.matchers import (
    EntityIdMatcher,
    EntityNameMatcher,
    GeographicRegionMatcher,
    KeywordCooccurrenceMatcher,
    SpatialProximityMatcher,
    TagOverlapMatcher,
    TemporalCooccurrenceMatcher,
    _extract_keywords,
    _geo_centroid,
    _geohash_prefix,
    _adjacent_geohashes,
)
from hydra.models.normalized import (
    GeoGeometry,
    NormalizedRecord,
    SourceMeta,
    Tier,
)
from hydra.utils.hashing import compute_raw_hash


def _make_record(
    tier: Tier = Tier.GEOPHYSICAL_SEISMIC,
    lon: Optional[float] = None,
    lat: Optional[float] = None,
    timestamp: Optional[datetime] = None,
    payload: Optional[dict] = None,
    tags: Optional[list] = None,
    raw_suffix: str = "a",
) -> NormalizedRecord:
    """Helper to create test records."""
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
        payload=payload or {},
        source_meta=SourceMeta(source_name="test", adapter_type="test"),
        raw_hash=raw,
        tags=tags or [],
    )


class TestSpatialProximityMatcher:
    def test_spatial_proximity_linear_decay(self):
        """Score = 1.0 - (distance / max_distance)."""
        matcher = SpatialProximityMatcher(max_distance_km=100.0)
        # Two points ~11.1 km apart (0.1 degree latitude ≈ 11.1 km)
        rec_a = _make_record(lon=0.0, lat=0.0, raw_suffix="a")
        rec_b = _make_record(lon=0.0, lat=0.1, raw_suffix="b")
        result = matcher.match(rec_a, rec_b)
        assert result is not None
        assert result.dimension == "spatial"
        assert 0.0 < result.score < 1.0
        assert result.evidence["distance_km"] > 0

    def test_spatial_match_outside_radius(self):
        """Records beyond max distance produce no match."""
        matcher = SpatialProximityMatcher(max_distance_km=10.0)
        rec_a = _make_record(lon=0.0, lat=0.0, raw_suffix="a")
        rec_b = _make_record(lon=10.0, lat=10.0, raw_suffix="b")
        result = matcher.match(rec_a, rec_b)
        assert result is None

    def test_spatial_null_geo_returns_none(self):
        """Records without geo return None."""
        matcher = SpatialProximityMatcher()
        rec_a = _make_record(raw_suffix="a")
        rec_b = _make_record(lon=0.0, lat=0.0, raw_suffix="b")
        assert matcher.match(rec_a, rec_b) is None


class TestTemporalCooccurrenceMatcher:
    def test_temporal_cooccurrence_linear_decay(self):
        """Score = 1.0 - (delta / max_delta)."""
        matcher = TemporalCooccurrenceMatcher(max_delta_s=3600.0)
        t1 = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        t2 = t1 + timedelta(seconds=1800)  # 30 min apart
        rec_a = _make_record(timestamp=t1, raw_suffix="a")
        rec_b = _make_record(timestamp=t2, raw_suffix="b")
        result = matcher.match(rec_a, rec_b)
        assert result is not None
        assert result.dimension == "temporal"
        assert abs(result.score - 0.5) < 0.01
        assert result.evidence["time_delta_s"] == 1800.0

    def test_temporal_outside_window(self):
        """Records beyond max delta produce no match."""
        matcher = TemporalCooccurrenceMatcher(max_delta_s=3600.0)
        t1 = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        t2 = t1 + timedelta(hours=2)
        rec_a = _make_record(timestamp=t1, raw_suffix="a")
        rec_b = _make_record(timestamp=t2, raw_suffix="b")
        assert matcher.match(rec_a, rec_b) is None


class TestEntityNameMatcher:
    def test_entity_name_jaro_winkler(self):
        """Correct similarity calculation for similar names."""
        matcher = EntityNameMatcher(similarity_threshold=0.85)
        rec_a = _make_record(
            tier=Tier.SANCTIONS_FINANCIAL_INTEL,
            payload={"name": "Al-Qaeda in the Arabian Peninsula"},
            raw_suffix="a",
        )
        rec_b = _make_record(
            tier=Tier.CONFLICT_EVENT_DATA,
            payload={"actor_name": "Al-Qaeda in the Arabian Peninsula"},
            raw_suffix="b",
        )
        result = matcher.match(rec_a, rec_b)
        assert result is not None
        assert result.dimension == "entity"
        assert result.score >= 0.85

    def test_entity_name_below_threshold(self):
        """Jaro-Winkler < threshold → no match."""
        matcher = EntityNameMatcher(similarity_threshold=0.85)
        rec_a = _make_record(
            tier=Tier.SANCTIONS_FINANCIAL_INTEL,
            payload={"name": "Alpha Corp"},
            raw_suffix="a",
        )
        rec_b = _make_record(
            tier=Tier.CONFLICT_EVENT_DATA,
            payload={"actor_name": "Zeta Industries"},
            raw_suffix="b",
        )
        result = matcher.match(rec_a, rec_b)
        assert result is None


class TestEntityIdMatcher:
    def test_entity_id_exact_match(self):
        """Any shared ID → score 1.0."""
        matcher = EntityIdMatcher()
        rec_a = _make_record(
            tier=Tier.CYBER_THREAT_INTEL,
            payload={"id": "threat-actor--abc123", "name": "APT28"},
            raw_suffix="a",
        )
        rec_b = _make_record(
            tier=Tier.SANCTIONS_FINANCIAL_INTEL,
            payload={"entity_id": "threat-actor--abc123", "name": "Fancy Bear"},
            raw_suffix="b",
        )
        result = matcher.match(rec_a, rec_b)
        assert result is not None
        assert result.score == 1.0
        assert "threat-actor--abc123" in result.evidence["shared_ids"]

    def test_entity_id_no_match(self):
        """No shared IDs → None."""
        matcher = EntityIdMatcher()
        rec_a = _make_record(
            tier=Tier.CYBER_THREAT_INTEL,
            payload={"id": "abc"},
            raw_suffix="a",
        )
        rec_b = _make_record(
            tier=Tier.SANCTIONS_FINANCIAL_INTEL,
            payload={"entity_id": "xyz"},
            raw_suffix="b",
        )
        assert matcher.match(rec_a, rec_b) is None


class TestTagOverlapMatcher:
    def test_tag_overlap_jaccard(self):
        """Correct Jaccard coefficient."""
        matcher = TagOverlapMatcher(min_overlap=2)
        rec_a = _make_record(tags=["cyber", "apt", "russia", "malware"], raw_suffix="a")
        rec_b = _make_record(tags=["cyber", "apt", "china"], raw_suffix="b")
        result = matcher.match(rec_a, rec_b)
        assert result is not None
        assert result.dimension == "tag"
        # intersection = {cyber, apt} = 2, union = {cyber, apt, russia, malware, china} = 5
        assert abs(result.score - 2 / 5) < 0.01

    def test_tag_overlap_below_minimum(self):
        """Less than min_overlap → None."""
        matcher = TagOverlapMatcher(min_overlap=2)
        rec_a = _make_record(tags=["cyber"], raw_suffix="a")
        rec_b = _make_record(tags=["cyber", "apt"], raw_suffix="b")
        assert matcher.match(rec_a, rec_b) is None


class TestKeywordCooccurrenceMatcher:
    def test_keyword_extraction_stopwords(self):
        """Stopwords removed, tokens > 3 chars kept."""
        rec = _make_record(
            payload={"text": "the quick brown fox jumps over the lazy dog"},
            raw_suffix="a",
        )
        keywords = _extract_keywords(rec)
        assert "the" not in keywords
        assert "over" not in keywords
        assert "quick" in keywords
        assert "brown" in keywords
        assert "jumps" in keywords

    def test_keyword_match(self):
        """Shared keywords above threshold produce match."""
        matcher = KeywordCooccurrenceMatcher(min_shared_keywords=3)
        rec_a = _make_record(
            payload={"text": "nuclear facility explosion detected seismic activity"},
            raw_suffix="a",
        )
        rec_b = _make_record(
            payload={"text": "seismic event near nuclear facility underground explosion"},
            raw_suffix="b",
        )
        result = matcher.match(rec_a, rec_b)
        assert result is not None
        assert result.dimension == "keyword"
        assert len(result.evidence["shared_keywords"]) >= 3


class TestGeographicRegionMatcher:
    def test_same_country(self):
        """Same country → 1.0."""
        matcher = GeographicRegionMatcher()
        rec_a = _make_record(payload={"country_code": "IR"}, raw_suffix="a")
        rec_b = _make_record(payload={"country_code": "IR"}, raw_suffix="b")
        result = matcher.match(rec_a, rec_b)
        assert result is not None
        assert result.score == 1.0

    def test_same_subregion(self):
        """Same sub-region → 0.5."""
        matcher = GeographicRegionMatcher()
        rec_a = _make_record(payload={"country_code": "IR"}, raw_suffix="a")
        rec_b = _make_record(payload={"country_code": "IQ"}, raw_suffix="b")
        result = matcher.match(rec_a, rec_b)
        assert result is not None
        assert result.score == 0.5
        assert result.evidence["match_level"] == "sub_region"

    def test_different_region(self):
        """Different regions → None."""
        matcher = GeographicRegionMatcher()
        rec_a = _make_record(payload={"country_code": "US"}, raw_suffix="a")
        rec_b = _make_record(payload={"country_code": "CN"}, raw_suffix="b")
        assert matcher.match(rec_a, rec_b) is None

    def test_geographic_region_no_country(self):
        """No country data → None."""
        matcher = GeographicRegionMatcher()
        rec_a = _make_record(payload={}, raw_suffix="a")
        rec_b = _make_record(payload={"country_code": "US"}, raw_suffix="b")
        assert matcher.match(rec_a, rec_b) is None


class TestGeohashHelpers:
    def test_geohash_prefix(self):
        """Geohash prefix is deterministic."""
        gh = _geohash_prefix(0.0, 0.0)
        assert isinstance(gh, str)
        assert ":" in gh

    def test_adjacent_geohashes(self):
        """Adjacent geohashes include 9 cells (self + 8 neighbours)."""
        gh = _geohash_prefix(0.0, 0.0)
        adj = _adjacent_geohashes(gh)
        assert len(adj) == 9
        assert gh in adj
