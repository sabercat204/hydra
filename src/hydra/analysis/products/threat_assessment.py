"""Threat Assessment — forward-looking analysis of threat vectors and regions."""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from hydra.analysis.models import (
    DataBundle,
    IntelligenceProduct,
    ProductParams,
    ProductSection,
    ThreatLevelThresholds,
)
from hydra.analysis.products.base import BaseProduct
from hydra.config import HydraSettings
from hydra.correlation.models import CorrelationResult
from hydra.models.normalized import NormalizedRecord
from hydra.utils.hashing import compute_raw_hash


class ThreatAssessment(BaseProduct):
    """Focused analysis of threat vectors, regions, or scenarios."""

    @property
    def product_type(self) -> str:
        return "threat_assessment"

    @property
    def source_tiers(self) -> list[int]:
        return [6, 15, 16, 19, 20, 27]

    @property
    def requires_graph(self) -> bool:
        return True

    @property
    def requires_timeline(self) -> bool:
        return True

    @property
    def default_lookback_hours(self) -> float:
        return 168.0  # 7 days

    def __init__(self, settings: HydraSettings) -> None:
        analysis = getattr(settings, "analysis", None)
        self._thresholds: ThreatLevelThresholds = getattr(
            analysis, "threat_level_thresholds", ThreatLevelThresholds()
        )
        self._min_convergence_tiers: int = getattr(
            analysis, "threat_min_convergence_tiers", 2
        )

    async def generate(
        self, bundle: DataBundle, params: ProductParams
    ) -> IntelligenceProduct:
        """Generate a Threat Assessment."""
        # 1. Scope definition — filter records
        all_records: list[NormalizedRecord] = []
        for tier_id, recs in bundle.records.items():
            for rec in recs:
                if params.region:
                    cc = rec.payload.get("country_code", rec.payload.get("country", ""))
                    if str(cc).upper() != params.region.upper():
                        continue
                if params.keywords:
                    payload_text = json.dumps(rec.payload).lower()
                    if not any(kw.lower() in payload_text for kw in params.keywords):
                        continue
                all_records.append(rec)

        # Build correlation map
        corr_map: dict[str, list[CorrelationResult]] = {}
        for c in bundle.correlations:
            corr_map.setdefault(c.record_a_hash, []).append(c)
            corr_map.setdefault(c.record_b_hash, []).append(c)

        # 2. Signal aggregation — group by region
        region_signals: dict[str, dict[int, list[NormalizedRecord]]] = defaultdict(lambda: defaultdict(list))
        for rec in all_records:
            region = self._extract_region(rec)
            region_signals[region][int(rec.tier)].append(rec)

        # 3. Threat level scoring per region
        region_levels: dict[str, str] = {}
        region_details: dict[str, dict[str, Any]] = {}
        for region, tier_map in region_signals.items():
            tier_count = len(tier_map)
            total_signals = sum(len(recs) for recs in tier_map.values())

            # Average correlation confidence for this region
            region_hashes = {r.raw_hash for recs in tier_map.values() for r in recs}
            region_corrs = [c for h in region_hashes for c in corr_map.get(h, [])]
            avg_conf = (
                sum(c.confidence for c in region_corrs) / len(region_corrs)
                if region_corrs
                else 0.0
            )

            # Check temporal clustering for CRITICAL
            has_temporal_cluster = self._has_temporal_clustering(
                [r for recs in tier_map.values() for r in recs],
                self._thresholds.critical_temporal_window_s,
            )

            level = self._classify_threat_level(
                tier_count, avg_conf, has_temporal_cluster
            )
            region_levels[region] = level
            region_details[region] = {
                "tier_count": tier_count,
                "total_signals": total_signals,
                "avg_confidence": round(avg_conf, 4),
                "tiers": sorted(tier_map.keys()),
                "has_temporal_cluster": has_temporal_cluster,
            }

        # 4. Construct sections
        sections: list[ProductSection] = []
        order = 0

        # 4a. Threat Overview
        critical_regions = [r for r, l in region_levels.items() if l == "CRITICAL"]
        high_regions = [r for r, l in region_levels.items() if l == "HIGH"]
        moderate_regions = [r for r, l in region_levels.items() if l == "MODERATE"]
        low_regions = [r for r, l in region_levels.items() if l == "LOW"]

        overview = (
            f"Threat assessment covering {bundle.time_window_start} to {bundle.time_window_end}. "
            f"Analyzed {len(all_records)} signals across {len(region_signals)} regions. "
            f"CRITICAL: {len(critical_regions)}, HIGH: {len(high_regions)}, "
            f"MODERATE: {len(moderate_regions)}, LOW: {len(low_regions)}."
        )
        sections.append(
            ProductSection(
                section_id=str(uuid.uuid4()),
                title="Threat Overview",
                section_type="narrative",
                content=overview,
                order=order,
            )
        )
        order += 1

        # 4b. Threat Matrix
        matrix_rows: list[dict[str, Any]] = []
        for region in sorted(region_levels, key=lambda r: _level_sort_key(region_levels[r])):
            detail = region_details[region]
            tier_signals: dict[str, int] = {}
            for t in detail["tiers"]:
                tier_signals[str(t)] = len(region_signals[region].get(t, []))
            matrix_rows.append({
                "region": region,
                "threat_level": region_levels[region],
                "tier_signals": tier_signals,
                "total_signals": detail["total_signals"],
                "avg_confidence": detail["avg_confidence"],
            })
        sections.append(
            ProductSection(
                section_id=str(uuid.uuid4()),
                title="Threat Matrix",
                section_type="table",
                content=json.dumps(matrix_rows, indent=2),
                order=order,
            )
        )
        order += 1

        # 4c. Critical Threats
        for region in critical_regions + high_regions:
            detail = region_details[region]
            tier_map = region_signals[region]
            lines: list[str] = [
                f"Region: {region} — Level: {region_levels[region]}",
                f"Tiers reporting: {detail['tiers']} ({detail['tier_count']} tiers)",
                f"Total signals: {detail['total_signals']}",
                f"Average correlation confidence: {detail['avg_confidence']:.2f}",
                "",
                "Contributing indicators:",
            ]
            for t, recs in sorted(tier_map.items()):
                lines.append(f"  Tier {t}: {len(recs)} signals")
                for rec in recs[:5]:
                    lines.append(f"    - {_brief_desc(rec)}")
            sections.append(
                ProductSection(
                    section_id=str(uuid.uuid4()),
                    title=f"Critical Threats — {region}",
                    section_type="narrative",
                    content="\n".join(lines),
                    records=[r.raw_hash for recs in tier_map.values() for r in recs[:10]],
                    confidence=detail["avg_confidence"],
                    order=order,
                )
            )
            order += 1

        # 4d. Emerging Patterns
        emerging: list[str] = []
        for region in moderate_regions:
            detail = region_details[region]
            emerging.append(
                f"- {region}: {detail['tier_count']} tiers, "
                f"{detail['total_signals']} signals, "
                f"confidence {detail['avg_confidence']:.2f}"
            )
        if emerging:
            sections.append(
                ProductSection(
                    section_id=str(uuid.uuid4()),
                    title="Emerging Patterns",
                    section_type="narrative",
                    content="\n".join(emerging),
                    order=order,
                )
            )
            order += 1

        # 4e. Indicator Timeline
        if bundle.timeline and bundle.timeline.events:
            tl_lines = [
                f"- [{e.timestamp}] {e.title} (Tier {e.tier})"
                for e in bundle.timeline.events[:40]
            ]
            sections.append(
                ProductSection(
                    section_id=str(uuid.uuid4()),
                    title="Indicator Timeline",
                    section_type="timeline",
                    content="\n".join(tl_lines),
                    records=[e.record_hash for e in bundle.timeline.events[:40]],
                    order=order,
                )
            )
            order += 1

        # 4f. Network Analysis
        if bundle.graph_data and bundle.graph_data.nodes:
            graph = bundle.graph_data
            graph_content = json.dumps(
                {
                    "nodes": len(graph.nodes),
                    "edges": len(graph.edges),
                    "communities": len(graph.communities),
                    "central_actors": [
                        {"node_id": c.node_id, "score": c.score}
                        for c in graph.central_nodes[:5]
                    ],
                },
                indent=2,
            )
            sections.append(
                ProductSection(
                    section_id=str(uuid.uuid4()),
                    title="Network Analysis",
                    section_type="graph_summary",
                    content=graph_content,
                    order=order,
                )
            )
            order += 1

        # 4g. Assessment Confidence
        data_gaps: list[str] = []
        for t in self.source_tiers:
            if not any(t in detail["tiers"] for detail in region_details.values()):
                data_gaps.append(f"Tier {t}: no data")
        conf_content = json.dumps(
            {
                "methodology": "Multi-tier signal convergence with temporal clustering",
                "time_window": f"{bundle.time_window_start} to {bundle.time_window_end}",
                "source_tiers": self.source_tiers,
                "data_gaps": data_gaps,
                "total_records_analyzed": len(all_records),
                "total_correlations": len(bundle.correlations),
            },
            indent=2,
        )
        sections.append(
            ProductSection(
                section_id=str(uuid.uuid4()),
                title="Assessment Confidence",
                section_type="metrics",
                content=conf_content,
                order=order,
            )
        )
        order += 1

        # 5. Key findings
        key_findings: list[str] = []
        for region in critical_regions:
            detail = region_details[region]
            key_findings.append(
                f"CRITICAL: {region} — {detail['tier_count']} tiers converging, "
                f"{detail['total_signals']} signals."
            )
        for region in high_regions:
            detail = region_details[region]
            key_findings.append(
                f"HIGH: {region} — {detail['tier_count']} tiers, "
                f"{detail['total_signals']} signals."
            )
        if data_gaps:
            key_findings.append(f"Data gaps in {len(data_gaps)} tiers limit assessment confidence.")

        # 6. Scoring
        all_confidences = [r.confidence for r in all_records]
        confidence_score = sum(all_confidences) / len(all_confidences) if all_confidences else 0.0
        tiers_with_data = len({int(r.tier) for r in all_records})
        total_threat_tiers = len(self.source_tiers)
        regions_with_data = len(region_signals)
        regions_of_interest = max(regions_with_data, 1)
        completeness_score = (
            (tiers_with_data / total_threat_tiers) * (regions_with_data / regions_of_interest)
            if total_threat_tiers > 0
            else 0.0
        )

        product_content = json.dumps(
            {"regions": list(region_levels.keys()), "findings": key_findings},
            sort_keys=True,
        )
        product_hash = compute_raw_hash(product_content.encode())

        return IntelligenceProduct(
            product_id=str(uuid.uuid4()),
            product_type=self.product_type,
            title=f"Threat Assessment — {bundle.time_window_start[:10]} to {bundle.time_window_end[:10]}",
            classification="green",
            generated_at=datetime.now(timezone.utc).isoformat(),
            time_window_start=bundle.time_window_start,
            time_window_end=bundle.time_window_end,
            sections=sections,
            summary=overview,
            key_findings=key_findings,
            confidence_score=round(confidence_score, 4),
            completeness_score=round(completeness_score, 4),
            source_tiers=sorted({int(r.tier) for r in all_records}),
            record_count=len(all_records),
            correlation_count=len(bundle.correlations),
            parameters={
                "region": params.region,
                "keywords": params.keywords,
                "min_convergence_tiers": self._min_convergence_tiers,
            },
            product_hash=product_hash,
            tags=["threat_assessment"],
        )

    def _classify_threat_level(
        self,
        tier_count: int,
        avg_confidence: float,
        has_temporal_cluster: bool,
    ) -> str:
        """Classify threat level based on tier convergence and confidence."""
        t = self._thresholds
        if (
            tier_count >= t.critical_min_tiers
            and avg_confidence >= t.critical_min_confidence
            and has_temporal_cluster
        ):
            return "CRITICAL"
        if tier_count >= t.high_min_tiers and avg_confidence >= t.high_min_confidence:
            return "HIGH"
        if tier_count >= t.moderate_min_tiers or (
            tier_count >= 1 and avg_confidence >= t.moderate_min_confidence
        ):
            return "MODERATE"
        return "LOW"

    @staticmethod
    def _extract_region(record: NormalizedRecord) -> str:
        """Extract country/region code from record payload."""
        p = record.payload
        for field in ("country_code", "country", "region", "location_country"):
            val = p.get(field)
            if val and isinstance(val, str):
                return val.upper()[:2] if len(val) <= 3 else val
        return "UNKNOWN"

    @staticmethod
    def _has_temporal_clustering(
        records: list[NormalizedRecord],
        window_s: float,
    ) -> bool:
        """Check if records have temporal clustering within window."""
        if len(records) < 2:
            return False
        timestamps = sorted(
            r.timestamp for r in records if isinstance(r.timestamp, datetime)
        )
        for i in range(len(timestamps) - 1):
            diff = (timestamps[i + 1] - timestamps[i]).total_seconds()
            if diff <= window_s:
                return True
        return False


def _level_sort_key(level: str) -> int:
    """Sort key for threat levels (CRITICAL first)."""
    return {"CRITICAL": 0, "HIGH": 1, "MODERATE": 2, "LOW": 3}.get(level, 4)


def _brief_desc(record: NormalizedRecord) -> str:
    """Brief description of a record for threat assessment sections."""
    p = record.payload
    ts = record.timestamp.isoformat() if isinstance(record.timestamp, datetime) else str(record.timestamp)
    name = p.get("name", p.get("entity_name", p.get("event_type", record.stream_id)))
    return f"[{ts}] {name}"
