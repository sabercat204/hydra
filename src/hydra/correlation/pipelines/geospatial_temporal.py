"""Geospatial-Temporal correlation pipeline.

Discovers records from different tiers that co-occur in space and time.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime, timezone

from hydra.config import HydraSettings
from hydra.correlation.matchers import (
    SpatialProximityMatcher,
    TemporalCooccurrenceMatcher,
    _geo_centroid,
    _geohash_prefix,
    _adjacent_geohashes,
)
from hydra.correlation.models import CandidateSet, CorrelationResult, MatchScore
from hydra.correlation.pipelines.base import BasePipeline
from hydra.correlation.scoring import CompositeScorer
from hydra.models.normalized import NormalizedRecord
from hydra.utils.hashing import compute_raw_hash

# Tier affinity matrix — only these cross-tier pairs are evaluated
TIER_AFFINITY: set[tuple[int, int]] = {
    (1, 20),   # Geophysical ↔ NBC Threat
    (1, 15),   # Geophysical ↔ Conflict
    (18, 19),  # Aviation/Maritime ↔ Sanctions
    (18, 15),  # Aviation/Maritime ↔ Conflict
    (2, 20),   # Atmospheric ↔ NBC Threat
    (3, 23),   # Space Weather ↔ Space Situational
    (24, 25),  # Geoscience ↔ Environmental
    (1, 24),   # Geophysical ↔ Geoscience
}


def _tier_pair_allowed(tier_a: int, tier_b: int) -> bool:
    """Check if a tier pair is in the affinity matrix (order-independent)."""
    lo, hi = min(tier_a, tier_b), max(tier_a, tier_b)
    return (lo, hi) in TIER_AFFINITY


class GeospatialTemporalPipeline(BasePipeline):
    """Geo + time co-occurrence across tiers."""

    pipeline_id = "geospatial_temporal"  # type: ignore[assignment]
    source_tiers = [1, 2, 3, 15, 18, 20, 23, 24, 25]  # type: ignore[assignment]

    def __init__(self, settings: HydraSettings) -> None:
        self._spatial_radius_km = settings.correlation.geo_temporal_radius_km
        self._temporal_window_s = settings.correlation.geo_temporal_window_s
        self._matchers = [
            SpatialProximityMatcher(max_distance_km=self._spatial_radius_km),
            TemporalCooccurrenceMatcher(max_delta_s=self._temporal_window_s),
        ]
        self._scorer = CompositeScorer(weights={"spatial": 0.6, "temporal": 0.4})

    async def correlate(self, candidates: CandidateSet) -> list[CorrelationResult]:
        """Correlate records using spatial proximity and temporal co-occurrence.

        Algorithm:
        1. Filter to records with non-null geo.
        2. Build geohash index for coarse spatial filtering.
        3. For each cross-tier pair within spatial radius:
           a. Check temporal co-occurrence.
           b. Score composite confidence.
           c. Emit if above threshold.
        """
        # Step 1: collect geo-enabled records grouped by tier
        geo_records: dict[int, list[tuple[NormalizedRecord, tuple[float, float]]]] = defaultdict(list)
        for tier_id, records in candidates.records.items():
            for rec in records:
                centroid = _geo_centroid(rec)
                if centroid is not None:
                    geo_records[tier_id].append((rec, centroid))

        # Step 2: build geohash index
        geohash_index: dict[str, list[tuple[NormalizedRecord, tuple[float, float], int]]] = defaultdict(list)
        for tier_id, recs in geo_records.items():
            for rec, centroid in recs:
                gh = _geohash_prefix(centroid[0], centroid[1])
                geohash_index[gh].append((rec, centroid, tier_id))

        # Step 3: evaluate cross-tier pairs
        results: list[CorrelationResult] = []
        seen_pairs: set[str] = set()
        pairs_evaluated = 0
        tier_ids = sorted(geo_records.keys())

        for i, tier_a in enumerate(tier_ids):
            for tier_b in tier_ids[i + 1:]:
                if not _tier_pair_allowed(tier_a, tier_b):
                    continue
                for rec_a, centroid_a in geo_records[tier_a]:
                    gh_a = _geohash_prefix(centroid_a[0], centroid_a[1])
                    adjacent = _adjacent_geohashes(gh_a)
                    # Collect tier_b candidates from adjacent geohash cells
                    tier_b_candidates: list[tuple[NormalizedRecord, tuple[float, float]]] = []
                    for gh in adjacent:
                        for rec, centroid, tid in geohash_index.get(gh, []):
                            if tid == tier_b:
                                tier_b_candidates.append((rec, centroid))

                    for rec_b, centroid_b in tier_b_candidates:
                        if pairs_evaluated >= self.max_pairs_per_run:
                            return results

                        # Canonical ordering for dedup
                        hash_a, hash_b = rec_a.raw_hash, rec_b.raw_hash
                        pair_key = f"{min(hash_a, hash_b)}:{max(hash_a, hash_b)}"
                        if pair_key in seen_pairs:
                            continue
                        seen_pairs.add(pair_key)
                        pairs_evaluated += 1

                        # Run matchers
                        scores: list[MatchScore] = []
                        for matcher in self._matchers:
                            ms = matcher.match(rec_a, rec_b)
                            if ms is not None:
                                scores.append(ms)

                        if not scores:
                            continue

                        confidence = self._scorer.score(scores)
                        if confidence < self.confidence_threshold:
                            continue

                        # Canonical tier ordering
                        if tier_a <= tier_b:
                            r_a_hash, r_b_hash = hash_a, hash_b
                            t_a, t_b = tier_a, tier_b
                        else:
                            r_a_hash, r_b_hash = hash_b, hash_a
                            t_a, t_b = tier_b, tier_a

                        corr_hash = compute_raw_hash(
                            f"{min(r_a_hash, r_b_hash)}:{max(r_a_hash, r_b_hash)}:{self.pipeline_id}".encode()
                        )
                        results.append(CorrelationResult(
                            correlation_id=str(uuid.uuid4()),
                            pipeline_id=self.pipeline_id,
                            record_a_hash=r_a_hash,
                            record_b_hash=r_b_hash,
                            tier_a=t_a,
                            tier_b=t_b,
                            confidence=confidence,
                            match_dimensions={ms.dimension: ms.score for ms in scores},
                            evidence={ms.dimension: ms.evidence for ms in scores},
                            correlation_hash=corr_hash,
                            created_at=datetime.now(timezone.utc).isoformat(),
                            tags=sorted(set(rec_a.tags + rec_b.tags)),
                        ))

        return results
