"""Threat Convergence correlation pipeline.

Detects multi-signal threat patterns where independent indicators from
different domains converge on a single threat assessment.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from hydra.config import HydraSettings
from hydra.correlation.matchers import (
    EntityNameMatcher,
    GeographicRegionMatcher,
    TagOverlapMatcher,
    TemporalCooccurrenceMatcher,
    _extract_country,
)
from hydra.correlation.models import CandidateSet, CorrelationResult, MatchScore
from hydra.correlation.pipelines.base import BasePipeline
from hydra.correlation.scoring import CompositeScorer
from hydra.models.normalized import NormalizedRecord
from hydra.utils.hashing import compute_raw_hash


class ThreatConvergencePipeline(BasePipeline):
    """Multi-signal threat pattern detection across tiers."""

    pipeline_id = "threat_convergence"  # type: ignore[assignment]
    source_tiers = [6, 15, 16, 19, 20, 27]  # type: ignore[assignment]
    confidence_threshold = 0.6  # type: ignore[assignment]

    def __init__(self, settings: HydraSettings) -> None:
        self._temporal_window_s = settings.correlation.threat_convergence_window_s
        self._convergence_multiplier = settings.correlation.threat_convergence_multiplier
        self._min_tiers_for_multiplier = settings.correlation.threat_convergence_min_tiers
        self._matchers = [
            TemporalCooccurrenceMatcher(max_delta_s=self._temporal_window_s),
            EntityNameMatcher(similarity_threshold=0.80),
            TagOverlapMatcher(min_overlap=2),
            GeographicRegionMatcher(),
        ]
        self._scorer = CompositeScorer(weights={
            "temporal": 0.25,
            "entity": 0.30,
            "tag": 0.15,
            "geographic_region": 0.30,
        })

    async def correlate(self, candidates: CandidateSet) -> list[CorrelationResult]:
        """Correlate records via multi-signal threat convergence.

        Algorithm:
        1. Group records by geographic region (country code).
        2. Within each region, apply temporal windowing.
        3. For each cross-tier pair: entity, tag, region, temporal matching.
        4. Multi-signal bonus: 3+ tiers → convergence multiplier.
        5. Score and emit.
        """
        # Step 1: group by country
        region_groups: dict[str, list[tuple[int, NormalizedRecord]]] = defaultdict(list)
        unregioned: list[tuple[int, NormalizedRecord]] = []

        for tier_id, records in candidates.records.items():
            for rec in records:
                country = _extract_country(rec)
                if country:
                    region_groups[country].append((tier_id, rec))
                else:
                    unregioned.append((tier_id, rec))

        results: list[CorrelationResult] = []
        seen_pairs: set[str] = set()
        pairs_evaluated = 0

        # Process each region cluster
        for country, cluster in region_groups.items():
            # Determine how many distinct tiers are in this cluster
            tiers_in_cluster = {t for t, _ in cluster}
            use_multiplier = len(tiers_in_cluster) >= self._min_tiers_for_multiplier
            multiplier = self._convergence_multiplier if use_multiplier else 1.0

            # Evaluate cross-tier pairs within the cluster
            for i in range(len(cluster)):
                for j in range(i + 1, len(cluster)):
                    if pairs_evaluated >= self.max_pairs_per_run:
                        return results

                    tier_a, rec_a = cluster[i]
                    tier_b, rec_b = cluster[j]
                    if tier_a == tier_b:
                        continue

                    hash_a, hash_b = rec_a.raw_hash, rec_b.raw_hash
                    pair_key = f"{min(hash_a, hash_b)}:{max(hash_a, hash_b)}"
                    if pair_key in seen_pairs:
                        continue
                    seen_pairs.add(pair_key)
                    pairs_evaluated += 1

                    scores: list[MatchScore] = []
                    for matcher in self._matchers:
                        ms = matcher.match(rec_a, rec_b)
                        if ms is not None:
                            # Keep highest per dimension
                            existing_dims = {s.dimension for s in scores}
                            if ms.dimension in existing_dims:
                                for idx, s in enumerate(scores):
                                    if s.dimension == ms.dimension and ms.score > s.score:
                                        scores[idx] = ms
                            else:
                                scores.append(ms)

                    if not scores:
                        continue

                    confidence = self._scorer.score_with_convergence(scores, multiplier)
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
                        evidence={
                            **{ms.dimension: ms.evidence for ms in scores},
                            "convergence_multiplier": multiplier,
                            "tiers_in_cluster": sorted(tiers_in_cluster),
                        },
                        correlation_hash=corr_hash,
                        created_at=datetime.now(timezone.utc).isoformat(),
                        tags=sorted(set(rec_a.tags + rec_b.tags)),
                    ))

        return results
