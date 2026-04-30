"""Tests for ThreatConvergencePipeline."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pytest

from hydra.config import HydraSettings
from hydra.correlation.models import CandidateSet
from hydra.correlation.pipelines.threat_convergence import ThreatConvergencePipeline
from hydra.models.normalized import (
    NormalizedRecord,
    SourceMeta,
    Tier,
)
from hydra.utils.hashing import compute_raw_hash


def _make_record(
    tier: Tier,
    payload: dict | None = None,
    tags: list[str] | None = None,
    timestamp: datetime | None = None,
    raw_suffix: str = "a",
) -> NormalizedRecord:
    ts = timestamp or datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    raw = compute_raw_hash(f"test_{raw_suffix}".encode())
    return NormalizedRecord(
        stream_id=f"test_{raw_suffix}",
        tier=tier,
        timestamp=ts,
        payload=payload or {},
        source_meta=SourceMeta(source_name="test", adapter_type="test"),
        raw_hash=raw,
        tags=tags or [],
    )


@pytest.fixture
def settings() -> HydraSettings:
    return HydraSettings()


@pytest.fixture
def pipeline(settings) -> ThreatConvergencePipeline:
    return ThreatConvergencePipeline(settings)


class TestThreatConvergencePipeline:
    async def test_region_grouping(self, pipeline):
        """Records grouped by country code."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        rec_a = _make_record(
            Tier.CONFLICT_EVENT_DATA,
            payload={"country_code": "IR", "actor_name": "Group Alpha"},
            tags=["conflict", "iran"],
            timestamp=now,
            raw_suffix="a",
        )
        rec_b = _make_record(
            Tier.CYBER_THREAT_INTEL,
            payload={"country_code": "IR", "name": "Group Alpha"},
            tags=["cyber", "iran"],
            timestamp=now + timedelta(hours=2),
            raw_suffix="b",
        )
        rec_c = _make_record(
            Tier.SANCTIONS_FINANCIAL_INTEL,
            payload={"country_code": "US", "name": "Unrelated"},
            tags=["sanctions"],
            timestamp=now,
            raw_suffix="c",
        )

        candidates = CandidateSet(
            pipeline_id="threat_convergence",
            source_tiers=[15, 16, 19],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=24)).isoformat(),
            records={15: [rec_a], 16: [rec_b], 19: [rec_c]},
            total_records=3,
        )
        results = await pipeline.correlate(candidates)
        # Only IR records should correlate (same region)
        for r in results:
            assert r.evidence.get("geographic_region", {}).get("country") == "IR" or \
                   "IR" in str(r.evidence)

    async def test_temporal_window_24h(self, pipeline):
        """24-hour window applied."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        rec_a = _make_record(
            Tier.CONFLICT_EVENT_DATA,
            payload={"country_code": "IR", "actor_name": "Group Alpha"},
            tags=["conflict", "threat"],
            timestamp=now,
            raw_suffix="a",
        )
        rec_b = _make_record(
            Tier.CYBER_THREAT_INTEL,
            payload={"country_code": "IR", "name": "Group Alpha"},
            tags=["cyber", "threat"],
            timestamp=now + timedelta(hours=12),
            raw_suffix="b",
        )

        candidates = CandidateSet(
            pipeline_id="threat_convergence",
            source_tiers=[15, 16],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=48)).isoformat(),
            records={15: [rec_a], 16: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        # Within 24h window, should find temporal match
        temporal_results = [r for r in results if "temporal" in r.match_dimensions]
        assert len(temporal_results) >= 1

    async def test_convergence_multiplier_3_tiers(self, pipeline):
        """3+ tiers in cluster → 1.2x multiplier."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        rec_a = _make_record(
            Tier.CONFLICT_EVENT_DATA,
            payload={"country_code": "IR", "actor_name": "IRGC"},
            tags=["conflict", "iran", "military"],
            timestamp=now,
            raw_suffix="a",
        )
        rec_b = _make_record(
            Tier.CYBER_THREAT_INTEL,
            payload={"country_code": "IR", "name": "IRGC Cyber"},
            tags=["cyber", "iran", "military"],
            timestamp=now + timedelta(hours=2),
            raw_suffix="b",
        )
        rec_c = _make_record(
            Tier.SANCTIONS_FINANCIAL_INTEL,
            payload={"country_code": "IR", "name": "IRGC"},
            tags=["sanctions", "iran", "military"],
            timestamp=now + timedelta(hours=4),
            raw_suffix="c",
        )

        candidates = CandidateSet(
            pipeline_id="threat_convergence",
            source_tiers=[15, 16, 19],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=24)).isoformat(),
            records={15: [rec_a], 16: [rec_b], 19: [rec_c]},
            total_records=3,
        )
        results = await pipeline.correlate(candidates)
        # With 3 tiers, convergence multiplier should be applied
        for r in results:
            assert r.evidence.get("convergence_multiplier") == 1.2

    async def test_convergence_multiplier_2_tiers(self, pipeline):
        """2 tiers → no multiplier (1.0x)."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        rec_a = _make_record(
            Tier.CONFLICT_EVENT_DATA,
            payload={"country_code": "IR", "actor_name": "Group Alpha"},
            tags=["conflict", "iran"],
            timestamp=now,
            raw_suffix="a",
        )
        rec_b = _make_record(
            Tier.CYBER_THREAT_INTEL,
            payload={"country_code": "IR", "name": "Group Alpha"},
            tags=["cyber", "iran"],
            timestamp=now + timedelta(hours=2),
            raw_suffix="b",
        )

        candidates = CandidateSet(
            pipeline_id="threat_convergence",
            source_tiers=[15, 16],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=24)).isoformat(),
            records={15: [rec_a], 16: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        for r in results:
            assert r.evidence.get("convergence_multiplier") == 1.0

    async def test_confidence_cap_at_1(self, pipeline):
        """Multiplied score capped at 1.0."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        # Create records that would score very high
        rec_a = _make_record(
            Tier.CONFLICT_EVENT_DATA,
            payload={"country_code": "IR", "actor_name": "IRGC"},
            tags=["conflict", "iran", "military", "threat"],
            timestamp=now,
            raw_suffix="a",
        )
        rec_b = _make_record(
            Tier.CYBER_THREAT_INTEL,
            payload={"country_code": "IR", "name": "IRGC"},
            tags=["cyber", "iran", "military", "threat"],
            timestamp=now + timedelta(minutes=5),
            raw_suffix="b",
        )
        rec_c = _make_record(
            Tier.SANCTIONS_FINANCIAL_INTEL,
            payload={"country_code": "IR", "name": "IRGC"},
            tags=["sanctions", "iran", "military", "threat"],
            timestamp=now + timedelta(minutes=10),
            raw_suffix="c",
        )

        candidates = CandidateSet(
            pipeline_id="threat_convergence",
            source_tiers=[15, 16, 19],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=24)).isoformat(),
            records={15: [rec_a], 16: [rec_b], 19: [rec_c]},
            total_records=3,
        )
        results = await pipeline.correlate(candidates)
        for r in results:
            assert r.confidence <= 1.0

    async def test_higher_threshold(self, pipeline):
        """Results below 0.6 not emitted."""
        assert pipeline.confidence_threshold == 0.6
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        # Weak signals
        rec_a = _make_record(
            Tier.CONFLICT_EVENT_DATA,
            payload={"country_code": "IR"},
            tags=["conflict"],
            timestamp=now,
            raw_suffix="a",
        )
        rec_b = _make_record(
            Tier.CYBER_THREAT_INTEL,
            payload={"country_code": "IR"},
            tags=["cyber"],
            timestamp=now + timedelta(hours=20),
            raw_suffix="b",
        )

        candidates = CandidateSet(
            pipeline_id="threat_convergence",
            source_tiers=[15, 16],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=48)).isoformat(),
            records={15: [rec_a], 16: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        for r in results:
            assert r.confidence >= 0.6

    async def test_geographic_region_matcher(self, pipeline):
        """Same country → 1.0, same sub-region → 0.5."""
        from hydra.correlation.matchers import GeographicRegionMatcher

        matcher = GeographicRegionMatcher()
        rec_same = _make_record(Tier.CONFLICT_EVENT_DATA, payload={"country_code": "IR"}, raw_suffix="a")
        rec_same2 = _make_record(Tier.CYBER_THREAT_INTEL, payload={"country_code": "IR"}, raw_suffix="b")
        result = matcher.match(rec_same, rec_same2)
        assert result is not None
        assert result.score == 1.0

        rec_subregion = _make_record(Tier.SANCTIONS_FINANCIAL_INTEL, payload={"country_code": "IQ"}, raw_suffix="c")
        result = matcher.match(rec_same, rec_subregion)
        assert result is not None
        assert result.score == 0.5
