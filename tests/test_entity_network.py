"""Tests for EntityNetworkPipeline."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional

import pytest

from hydra.config import HydraSettings
from hydra.correlation.models import CandidateSet
from hydra.correlation.pipelines.entity_network import EntityNetworkPipeline
from hydra.models.normalized import (
    GeoGeometry,
    NormalizedRecord,
    SourceMeta,
    Tier,
)
from hydra.utils.hashing import compute_raw_hash


def _make_record(
    tier: Tier,
    payload: Optional[dict] = None,
    tags: Optional[list] = None,
    timestamp: Optional[datetime] = None,
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
def pipeline(settings) -> EntityNetworkPipeline:
    return EntityNetworkPipeline(settings, es_engine=None)


class TestEntityNetworkPipeline:
    async def test_exact_id_match(self, pipeline):
        """Shared STIX ID → confidence 1.0 on entity dimension."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        rec_a = _make_record(
            Tier.CYBER_THREAT_INTEL,
            payload={"id": "threat-actor--abc123", "name": "APT28"},
            tags=["cyber", "apt"],
            timestamp=now,
            raw_suffix="a",
        )
        rec_b = _make_record(
            Tier.SANCTIONS_FINANCIAL_INTEL,
            payload={"entity_id": "threat-actor--abc123", "name": "Fancy Bear"},
            tags=["sanctions", "apt"],
            timestamp=now + timedelta(hours=1),
            raw_suffix="b",
        )

        candidates = CandidateSet(
            pipeline_id="entity_network",
            source_tiers=[16, 19],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=48)).isoformat(),
            records={16: [rec_a], 19: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        assert len(results) >= 1
        r = results[0]
        assert r.match_dimensions.get("entity") == 1.0

    async def test_fuzzy_name_match(self, pipeline):
        """Jaro-Winkler ≥ 0.85 → entity match."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        rec_a = _make_record(
            Tier.SANCTIONS_FINANCIAL_INTEL,
            payload={"name": "Islamic Revolutionary Guard Corps"},
            timestamp=now,
            raw_suffix="a",
        )
        rec_b = _make_record(
            Tier.CONFLICT_EVENT_DATA,
            payload={"actor_name": "Islamic Revolutionary Guard Corps"},
            timestamp=now + timedelta(hours=2),
            raw_suffix="b",
        )

        candidates = CandidateSet(
            pipeline_id="entity_network",
            source_tiers=[15, 19],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=48)).isoformat(),
            records={19: [rec_a], 15: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        assert len(results) >= 1
        assert results[0].match_dimensions.get("entity", 0) >= 0.85

    async def test_fuzzy_name_below_threshold(self, pipeline):
        """Jaro-Winkler < 0.85 → no entity match from names alone."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        rec_a = _make_record(
            Tier.SANCTIONS_FINANCIAL_INTEL,
            payload={"name": "Alpha Corp"},
            timestamp=now,
            raw_suffix="a",
        )
        rec_b = _make_record(
            Tier.CONFLICT_EVENT_DATA,
            payload={"actor_name": "Zeta Industries"},
            timestamp=now + timedelta(hours=2),
            raw_suffix="b",
        )

        candidates = CandidateSet(
            pipeline_id="entity_network",
            source_tiers=[15, 19],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=48)).isoformat(),
            records={19: [rec_a], 15: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        # No entity match, and temporal alone won't meet threshold
        entity_matches = [r for r in results if r.match_dimensions.get("entity", 0) >= 0.85]
        assert len(entity_matches) == 0

    async def test_tag_overlap_match(self, pipeline):
        """≥ 2 shared tags → tag match."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        rec_a = _make_record(
            Tier.CYBER_THREAT_INTEL,
            payload={"id": "unique-a", "name": "GroupA"},
            tags=["cyber", "espionage", "russia"],
            timestamp=now,
            raw_suffix="a",
        )
        rec_b = _make_record(
            Tier.CONFLICT_EVENT_DATA,
            payload={"event_id": "unique-b", "actor_name": "GroupB"},
            tags=["cyber", "espionage", "ukraine"],
            timestamp=now + timedelta(hours=1),
            raw_suffix="b",
        )

        candidates = CandidateSet(
            pipeline_id="entity_network",
            source_tiers=[15, 16],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=48)).isoformat(),
            records={16: [rec_a], 15: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        tag_results = [r for r in results if "tag" in r.match_dimensions]
        assert len(tag_results) >= 1

    async def test_keyword_cooccurrence(self, pipeline):
        """≥ 3 shared keywords → keyword match."""
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        rec_a = _make_record(
            Tier.CYBER_THREAT_INTEL,
            payload={
                "id": "unique-a",
                "name": "Threat Group Alpha",
                "description": "advanced persistent threat targeting government infrastructure networks",
            },
            tags=["cyber", "threat", "government", "infrastructure"],
            timestamp=now,
            raw_suffix="a",
        )
        rec_b = _make_record(
            Tier.CONFLICT_EVENT_DATA,
            payload={
                "event_id": "unique-b",
                "actor_name": "Threat Group Alpha",
                "description": "cyber attack targeting government infrastructure critical networks",
            },
            tags=["conflict", "threat", "government", "infrastructure"],
            timestamp=now + timedelta(minutes=30),
            raw_suffix="b",
        )

        candidates = CandidateSet(
            pipeline_id="entity_network",
            source_tiers=[15, 16],
            time_window_start=now.isoformat(),
            time_window_end=(now + timedelta(hours=48)).isoformat(),
            records={16: [rec_a], 15: [rec_b]},
            total_records=2,
        )
        results = await pipeline.correlate(candidates)
        # With entity name match + keyword + tag + temporal, should pass threshold
        assert len(results) >= 1
        kw_results = [r for r in results if "keyword" in r.match_dimensions]
        assert len(kw_results) >= 1

    async def test_entity_extraction_per_tier(self, pipeline):
        """Correct fields extracted per ENTITY_EXTRACTION_MAP."""
        from hydra.correlation.matchers import _extract_entity_ids, _extract_entity_names

        rec_16 = _make_record(
            Tier.CYBER_THREAT_INTEL,
            payload={"id": "stix-123", "mitre_attack_id": "T1059", "name": "APT28"},
            raw_suffix="t16",
        )
        ids = _extract_entity_ids(rec_16)
        assert "stix-123" in ids
        assert "T1059" in ids

        names = _extract_entity_names(rec_16)
        assert "APT28" in names

        rec_19 = _make_record(
            Tier.SANCTIONS_FINANCIAL_INTEL,
            payload={"entity_id": "ofac-456", "name": "Bad Corp", "aliases": ["Evil Inc", "Shady LLC"]},
            raw_suffix="t19",
        )
        ids = _extract_entity_ids(rec_19)
        assert "ofac-456" in ids

        names = _extract_entity_names(rec_19)
        assert "Bad Corp" in names
        assert "Evil Inc" in names
        assert "Shady LLC" in names

    async def test_composite_score_weights(self, pipeline):
        """0.5 entity + 0.2 keyword + 0.15 tag + 0.15 temporal."""
        assert pipeline._scorer.weights == {
            "entity": 0.5,
            "keyword": 0.2,
            "tag": 0.15,
            "temporal": 0.15,
        }
