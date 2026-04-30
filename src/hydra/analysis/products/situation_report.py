"""Situation Report (SITREP) — periodic overview of significant events."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from hydra.analysis.models import (
    DataBundle,
    IntelligenceProduct,
    ProductParams,
    ProductSection,
)
from hydra.analysis.products.base import BaseProduct
from hydra.config import HydraSettings
from hydra.correlation.models import CorrelationResult
from hydra.models.normalized import NormalizedRecord
from hydra.utils.hashing import compute_raw_hash

# Default domain groupings
DEFAULT_DOMAIN_GROUPS: dict[str, list[int]] = {
    "Geophysical & Environmental": [1, 2, 3, 24, 25],
    "Security & Conflict": [6, 15, 16, 19, 20],
    "Economic & Governance": [5, 8, 9, 10, 11, 12, 13],
    "Health & Human Rights": [7, 21, 26],
    "Space & Science": [4, 22, 23],
    "Infrastructure & Energy": [18, 27],
    "Open Source & Social": [14, 17, 28],
}


class SituationReport(BaseProduct):
    """Periodic overview of significant events across monitored tiers."""

    @property
    def product_type(self) -> str:
        return "situation_report"

    @property
    def source_tiers(self) -> list[int]:
        return list(range(1, 29))

    @property
    def requires_graph(self) -> bool:
        return False

    @property
    def requires_timeline(self) -> bool:
        return True

    @property
    def default_lookback_hours(self) -> float:
        return 24.0

    def __init__(self, settings: HydraSettings) -> None:
        analysis = getattr(settings, "analysis", None)
        self._max_events_per_tier: int = getattr(analysis, "sitrep_max_events_per_tier", 20)
        self._significance_threshold: float = getattr(analysis, "sitrep_significance_threshold", 0.3)
        self._domain_groups: dict[str, list[int]] = getattr(
            analysis, "sitrep_domain_groups", DEFAULT_DOMAIN_GROUPS
        )

    async def generate(
        self, bundle: DataBundle, params: ProductParams
    ) -> IntelligenceProduct:
        """Generate a Situation Report."""
        # Build correlation map: raw_hash -> list[CorrelationResult]
        corr_map: dict[str, list[CorrelationResult]] = {}
        for c in bundle.correlations:
            corr_map.setdefault(c.record_a_hash, []).append(c)
            corr_map.setdefault(c.record_b_hash, []).append(c)

        # 1. Score significance for all records
        scored: list[tuple[NormalizedRecord, float]] = []
        for tier_id, tier_records in bundle.records.items():
            for rec in tier_records:
                sig = self._score_significance(rec, corr_map)
                scored.append((rec, sig))

        # 2. Filter by threshold
        scored = [(r, s) for r, s in scored if s >= self._significance_threshold]

        # 3. Group by tier, take top N per tier
        tier_groups: dict[int, list[tuple[NormalizedRecord, float]]] = {}
        for rec, sig in scored:
            tier_groups.setdefault(int(rec.tier), []).append((rec, sig))
        for tier_id in tier_groups:
            tier_groups[tier_id].sort(key=lambda x: x[1], reverse=True)
            tier_groups[tier_id] = tier_groups[tier_id][: self._max_events_per_tier]

        # 4. Construct sections
        sections: list[ProductSection] = []
        order = 0

        # 4a. Executive Summary
        total_events = sum(len(v) for v in tier_groups.values())
        tiers_with_data = len(tier_groups)
        total_tiers = len(self.source_tiers)
        summary_text = (
            f"This situation report covers the period from {bundle.time_window_start} "
            f"to {bundle.time_window_end}. A total of {total_events} significant events "
            f"were identified across {tiers_with_data} data tiers. "
            f"{len(bundle.correlations)} cross-tier correlations were detected."
        )
        sections.append(
            ProductSection(
                section_id=str(uuid.uuid4()),
                title="Executive Summary",
                section_type="narrative",
                content=summary_text,
                order=order,
            )
        )
        order += 1

        # 4b. Key Developments — top 10 across all tiers
        all_scored = []
        for tier_id, items in tier_groups.items():
            all_scored.extend(items)
        all_scored.sort(key=lambda x: x[1], reverse=True)
        top_10 = all_scored[:10]
        key_dev_lines: list[str] = []
        key_findings: list[str] = []
        for rec, sig in top_10:
            title = self._event_title(rec)
            key_dev_lines.append(f"- [{sig:.2f}] {title}")
            key_findings.append(title)
        sections.append(
            ProductSection(
                section_id=str(uuid.uuid4()),
                title="Key Developments",
                section_type="narrative",
                content="\n".join(key_dev_lines),
                records=[rec.raw_hash for rec, _ in top_10],
                confidence=sum(s for _, s in top_10) / len(top_10) if top_10 else 0.0,
                order=order,
            )
        )
        order += 1

        # 4c. Per-domain sections
        for domain_name, domain_tiers in self._domain_groups.items():
            domain_records: list[tuple[NormalizedRecord, float]] = []
            for t in domain_tiers:
                domain_records.extend(tier_groups.get(t, []))
            if not domain_records:
                continue
            domain_records.sort(key=lambda x: x[1], reverse=True)
            lines = [f"- [{sig:.2f}] {self._event_title(rec)}" for rec, sig in domain_records]
            sections.append(
                ProductSection(
                    section_id=str(uuid.uuid4()),
                    title=domain_name,
                    section_type="narrative",
                    content="\n".join(lines),
                    records=[rec.raw_hash for rec, _ in domain_records],
                    confidence=sum(s for _, s in domain_records) / len(domain_records),
                    order=order,
                )
            )
            order += 1

        # 4d. Cross-Tier Correlations
        if bundle.correlations:
            corr_lines = [
                f"- {c.pipeline_id}: Tier {c.tier_a} ↔ Tier {c.tier_b} "
                f"(confidence: {c.confidence:.2f})"
                for c in sorted(bundle.correlations, key=lambda x: x.confidence, reverse=True)[:20]
            ]
            sections.append(
                ProductSection(
                    section_id=str(uuid.uuid4()),
                    title="Cross-Tier Correlations",
                    section_type="table",
                    content="\n".join(corr_lines),
                    correlations=[c.correlation_id for c in bundle.correlations[:20]],
                    order=order,
                )
            )
            order += 1

        # 4e. Timeline section
        if bundle.timeline and bundle.timeline.events:
            tl_lines = [
                f"- [{e.timestamp}] {e.title} (Tier {e.tier}, sig: {e.significance:.2f})"
                for e in bundle.timeline.events[:50]
            ]
            sections.append(
                ProductSection(
                    section_id=str(uuid.uuid4()),
                    title="Timeline",
                    section_type="timeline",
                    content="\n".join(tl_lines),
                    records=[e.record_hash for e in bundle.timeline.events[:50]],
                    order=order,
                )
            )
            order += 1

        # 5-6. Scoring
        all_confidences = [rec.confidence for rec, _ in all_scored]
        confidence_score = sum(all_confidences) / len(all_confidences) if all_confidences else 0.0
        completeness_score = tiers_with_data / total_tiers if total_tiers > 0 else 0.0

        source_tiers_list = sorted(tier_groups.keys())
        record_count = sum(len(recs) for recs in bundle.records.values())

        # Build product
        product_content = json.dumps(
            {"sections": [s.title for s in sections], "findings": key_findings},
            sort_keys=True,
        )
        product_hash = compute_raw_hash(product_content.encode())

        return IntelligenceProduct(
            product_id=str(uuid.uuid4()),
            product_type=self.product_type,
            title=f"Situation Report — {bundle.time_window_start[:10]} to {bundle.time_window_end[:10]}",
            classification="green",
            generated_at=datetime.now(timezone.utc).isoformat(),
            time_window_start=bundle.time_window_start,
            time_window_end=bundle.time_window_end,
            sections=sections,
            summary=summary_text,
            key_findings=key_findings,
            confidence_score=round(confidence_score, 4),
            completeness_score=round(completeness_score, 4),
            source_tiers=source_tiers_list,
            record_count=record_count,
            correlation_count=len(bundle.correlations),
            parameters={
                "significance_threshold": self._significance_threshold,
                "max_events_per_tier": self._max_events_per_tier,
            },
            product_hash=product_hash,
            tags=["sitrep", "periodic"],
        )

    def _score_significance(
        self,
        record: NormalizedRecord,
        correlation_map: dict[str, list[CorrelationResult]],
    ) -> float:
        """Score a record's significance for SITREP inclusion.

        significance = (0.4 * correlation_factor) +
                       (0.3 * anomaly_factor) +
                       (0.3 * confidence)
        """
        # Correlation factor
        corrs = correlation_map.get(record.raw_hash, [])
        if corrs:
            max_conf = max(c.confidence for c in corrs)
            count_factor = min(1.0, len(corrs) / 5.0)
            correlation_factor = 0.5 * max_conf + 0.5 * count_factor
        else:
            correlation_factor = 0.0

        # Anomaly factor
        anomaly_factor = self._anomaly_factor(record)

        # Confidence
        confidence = record.confidence

        return 0.4 * correlation_factor + 0.3 * anomaly_factor + 0.3 * confidence

    @staticmethod
    def _anomaly_factor(record: NormalizedRecord) -> float:
        """Tier-specific anomaly scoring."""
        p = record.payload
        tier = int(record.tier)

        if tier == 1:
            mag = p.get("magnitude", 0)
            if isinstance(mag, (int, float)) and mag > 4.0:
                return min(1.0, mag / 8.0)
            depth = p.get("depth_km", 100)
            if isinstance(depth, (int, float)) and depth < 10:
                return 0.6
        elif tier == 16:
            cvss = p.get("cvss", p.get("cvss_score", 0))
            if isinstance(cvss, (int, float)) and cvss > 7.0:
                return min(1.0, cvss / 10.0)
        elif tier == 15:
            fatalities = p.get("fatalities", 0)
            if isinstance(fatalities, (int, float)) and fatalities > 0:
                return min(1.0, 0.5 + fatalities / 100.0)
        return 0.5

    @staticmethod
    def _event_title(record: NormalizedRecord) -> str:
        """Generate a human-readable event title."""
        p = record.payload
        tier = int(record.tier)
        if tier == 1:
            return f"{p.get('magnitude', '?')} M earthquake at {p.get('place', 'unknown')}"
        elif tier == 15:
            return f"{p.get('event_type', 'Event')} in {p.get('country', 'unknown')}"
        elif tier == 16:
            return f"{p.get('type', 'Indicator')}: {p.get('name', 'unknown')}"
        elif tier == 19:
            return f"{p.get('entity_name', 'Unknown')} — {p.get('program', 'sanctions')}"
        else:
            return f"Tier {tier}: {record.stream_id}"
