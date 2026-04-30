"""Entity Network correlation pipeline.

Resolves entity identities across tiers — sanctioned entities appearing
in conflict data, cyber threat intel, or human rights reports.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from hydra.config import HydraSettings
from hydra.correlation.matchers import (
    EntityIdMatcher,
    EntityNameMatcher,
    KeywordCooccurrenceMatcher,
    TagOverlapMatcher,
    TemporalCooccurrenceMatcher,
    _extract_entity_ids,
    _extract_entity_names,
)
from hydra.correlation.models import CandidateSet, CorrelationResult, MatchScore
from hydra.correlation.pipelines.base import BasePipeline
from hydra.correlation.scoring import CompositeScorer
from hydra.models.normalized import NormalizedRecord
from hydra.utils.hashing import compute_raw_hash


class EntityNetworkPipeline(BasePipeline):
    """Entity identity resolution across tiers."""

    pipeline_id = "entity_network"  # type: ignore[assignment]
    source_tiers = [8, 14, 15, 16, 19, 21]  # type: ignore[assignment]

    def __init__(
        self,
        settings: HydraSettings,
        es_engine: Any | None = None,
    ) -> None:
        self._es = es_engine
        self._matchers = [
            EntityNameMatcher(similarity_threshold=settings.correlation.entity_name_similarity_threshold),
            EntityIdMatcher(),
            TagOverlapMatcher(min_overlap=settings.correlation.entity_min_tag_overlap),
            KeywordCooccurrenceMatcher(min_shared_keywords=settings.correlation.entity_min_shared_keywords),
        ]
        self._temporal_matcher = TemporalCooccurrenceMatcher(max_delta_s=86400.0)
        self._scorer = CompositeScorer(weights={
            "entity": 0.5,
            "keyword": 0.2,
            "tag": 0.15,
            "temporal": 0.15,
        })

    async def correlate(self, candidates: CandidateSet) -> list[CorrelationResult]:
        """Correlate records via entity identity resolution.

        Algorithm:
        1. Extract entity identifiers from each record's payload.
        2. Phase 1 — Exact ID match across tiers.
        3. Phase 2 — Fuzzy name match (Jaro-Winkler).
        4. Phase 3 — Tag and keyword overlap.
        5. Score and emit.
        """
        all_records: list[tuple[int, NormalizedRecord]] = []
        for tier_id, records in candidates.records.items():
            for rec in records:
                all_records.append((tier_id, rec))

        results: list[CorrelationResult] = []
        seen_pairs: set[str] = set()
        pairs_evaluated = 0

        # Build ID index for Phase 1 (exact ID match)
        id_to_records: dict[str, list[tuple[int, NormalizedRecord]]] = defaultdict(list)
        for tier_id, rec in all_records:
            for eid in _extract_entity_ids(rec):
                id_to_records[eid].append((tier_id, rec))

        # Phase 1: exact ID matches (cross-tier only)
        for eid, recs_with_id in id_to_records.items():
            for i in range(len(recs_with_id)):
                for j in range(i + 1, len(recs_with_id)):
                    tier_a, rec_a = recs_with_id[i]
                    tier_b, rec_b = recs_with_id[j]
                    if tier_a == tier_b:
                        continue
                    result = self._evaluate_pair(
                        rec_a, rec_b, tier_a, tier_b, seen_pairs, pairs_evaluated
                    )
                    if result is not None:
                        cr, pairs_evaluated = result
                        results.append(cr)
                    else:
                        pairs_evaluated += 1
                    if pairs_evaluated >= self.max_pairs_per_run:
                        return results

        # Phase 2 & 3: cross-tier pairs not yet seen
        tier_ids = sorted(candidates.records.keys())
        for i, tier_a in enumerate(tier_ids):
            for tier_b in tier_ids[i + 1:]:
                for rec_a in candidates.records.get(tier_a, []):
                    for rec_b in candidates.records.get(tier_b, []):
                        if pairs_evaluated >= self.max_pairs_per_run:
                            return results
                        result = self._evaluate_pair(
                            rec_a, rec_b, tier_a, tier_b, seen_pairs, pairs_evaluated
                        )
                        if result is not None:
                            cr, pairs_evaluated = result
                            results.append(cr)
                        else:
                            pairs_evaluated += 1

        return results

    def _evaluate_pair(
        self,
        rec_a: NormalizedRecord,
        rec_b: NormalizedRecord,
        tier_a: int,
        tier_b: int,
        seen_pairs: set[str],
        pairs_evaluated: int,
    ) -> tuple[CorrelationResult, int] | None:
        """Evaluate a single record pair. Returns (result, new_pairs_count) or None."""
        hash_a, hash_b = rec_a.raw_hash, rec_b.raw_hash
        pair_key = f"{min(hash_a, hash_b)}:{max(hash_a, hash_b)}"
        if pair_key in seen_pairs:
            return None
        seen_pairs.add(pair_key)
        pairs_evaluated += 1

        scores: list[MatchScore] = []
        for matcher in self._matchers:
            ms = matcher.match(rec_a, rec_b)
            if ms is not None:
                # Keep highest score per dimension
                existing = {s.dimension for s in scores}
                if ms.dimension in existing:
                    for idx, s in enumerate(scores):
                        if s.dimension == ms.dimension and ms.score > s.score:
                            scores[idx] = ms
                else:
                    scores.append(ms)

        # Add temporal dimension
        ts = self._temporal_matcher.match(rec_a, rec_b)
        if ts is not None:
            scores.append(ts)

        if not scores:
            return None

        confidence = self._scorer.score(scores)
        if confidence < self.confidence_threshold:
            return None

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
        cr = CorrelationResult(
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
        )
        return (cr, pairs_evaluated)
