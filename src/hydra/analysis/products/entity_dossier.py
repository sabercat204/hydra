"""Entity Dossier — comprehensive entity profile across all tiers."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from hydra.analysis.exceptions import EntityResolutionError
from hydra.analysis.models import (
    DataBundle,
    GraphResult,
    IntelligenceProduct,
    ProductParams,
    ProductSection,
)
from hydra.analysis.products.base import BaseProduct
from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord
from hydra.storage.engines.elasticsearch import ElasticsearchEngine
from hydra.utils.hashing import compute_raw_hash


@dataclass
class EntityResolution:
    """Result of multi-strategy entity resolution."""

    canonical_name: str = ""
    identifiers: dict[str, str] = field(default_factory=dict)
    records: list[NormalizedRecord] = field(default_factory=list)
    match_quality: str = "exact_id"  # "exact_id" | "fuzzy_name" | "graph_expanded"


# Tier-to-section mapping for dossier
DOSSIER_SECTIONS: dict[int, tuple[str, str]] = {
    19: ("Sanctions & Regulatory Status", "table"),
    15: ("Conflict & Event Involvement", "narrative"),
    16: ("Cyber Threat Activity", "narrative"),
    21: ("Human Rights Concerns", "narrative"),
    8: ("International Organization References", "narrative"),
    14: ("Arms & Defense Connections", "narrative"),
}


class EntityDossier(BaseProduct):
    """Comprehensive profile of a specific entity across all tiers."""

    @property
    def product_type(self) -> str:
        return "entity_dossier"

    @property
    def source_tiers(self) -> list[int]:
        return [8, 14, 15, 16, 19, 21]

    @property
    def requires_graph(self) -> bool:
        return True

    @property
    def requires_timeline(self) -> bool:
        return True

    @property
    def default_lookback_hours(self) -> float:
        return 8760.0  # 365 days

    def __init__(
        self, settings: HydraSettings, es_engine: ElasticsearchEngine | None = None
    ) -> None:
        self._es = es_engine
        analysis = getattr(settings, "analysis", None)
        self._max_network_depth: int = getattr(analysis, "dossier_network_depth", 2)
        self._max_network_nodes: int = getattr(analysis, "dossier_max_network_nodes", 50)

    async def generate(
        self, bundle: DataBundle, params: ProductParams
    ) -> IntelligenceProduct:
        """Generate an Entity Dossier.

        Requires params.entity_id or params.entity_name.
        """
        # 1. Entity resolution
        resolution = await self._resolve_entity(params, bundle)
        if not resolution.records:
            raise EntityResolutionError(params.entity_id, params.entity_name)

        # Group resolved records by tier
        tier_records: dict[int, list[NormalizedRecord]] = {}
        for rec in resolution.records:
            tier_records.setdefault(int(rec.tier), []).append(rec)

        # 4. Construct sections
        sections: list[ProductSection] = []
        order = 0

        # 4a. Entity Profile
        first_seen = min(
            (rec.timestamp for rec in resolution.records),
            default=datetime.now(timezone.utc),
        )
        last_seen = max(
            (rec.timestamp for rec in resolution.records),
            default=datetime.now(timezone.utc),
        )
        profile_content = json.dumps(
            {
                "canonical_name": resolution.canonical_name,
                "identifiers": resolution.identifiers,
                "match_quality": resolution.match_quality,
                "first_seen": first_seen.isoformat() if isinstance(first_seen, datetime) else str(first_seen),
                "last_seen": last_seen.isoformat() if isinstance(last_seen, datetime) else str(last_seen),
                "total_records": len(resolution.records),
                "tiers_present": sorted(tier_records.keys()),
            },
            indent=2,
        )
        sections.append(
            ProductSection(
                section_id=str(uuid.uuid4()),
                title="Entity Profile",
                section_type="narrative",
                content=profile_content,
                records=[r.raw_hash for r in resolution.records[:10]],
                confidence=1.0 if resolution.match_quality == "exact_id" else 0.7,
                order=order,
            )
        )
        order += 1

        # 4b-g. Tier-specific sections (only if data exists)
        for tier_id, (section_title, section_type) in DOSSIER_SECTIONS.items():
            recs = tier_records.get(tier_id, [])
            if not recs:
                continue
            lines = []
            for rec in recs[:20]:
                desc = _describe_record(rec)
                lines.append(f"- {desc}")
            sections.append(
                ProductSection(
                    section_id=str(uuid.uuid4()),
                    title=section_title,
                    section_type=section_type,
                    content="\n".join(lines),
                    records=[r.raw_hash for r in recs[:20]],
                    confidence=sum(r.confidence for r in recs) / len(recs),
                    order=order,
                )
            )
            order += 1

        # 4h. Network Analysis (from graph data)
        if bundle.graph_data and bundle.graph_data.nodes:
            graph = bundle.graph_data
            graph_summary = json.dumps(
                {
                    "total_nodes": len(graph.nodes),
                    "total_edges": len(graph.edges),
                    "communities": len(graph.communities),
                    "central_nodes": [
                        {"node_id": c.node_id, "metric": c.metric, "score": c.score}
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
                    content=graph_summary,
                    order=order,
                )
            )
            order += 1

        # 4i. Activity Timeline
        if bundle.timeline and bundle.timeline.events:
            tl_lines = [
                f"- [{e.timestamp}] {e.title} (Tier {e.tier})"
                for e in bundle.timeline.events[:30]
            ]
            sections.append(
                ProductSection(
                    section_id=str(uuid.uuid4()),
                    title="Activity Timeline",
                    section_type="timeline",
                    content="\n".join(tl_lines),
                    records=[e.record_hash for e in bundle.timeline.events[:30]],
                    order=order,
                )
            )
            order += 1

        # 4j. Cross-Tier Correlations
        if bundle.correlations:
            corr_lines = [
                f"- {c.pipeline_id}: Tier {c.tier_a} ↔ Tier {c.tier_b} "
                f"(confidence: {c.confidence:.2f})"
                for c in sorted(bundle.correlations, key=lambda x: x.confidence, reverse=True)[:15]
            ]
            sections.append(
                ProductSection(
                    section_id=str(uuid.uuid4()),
                    title="Cross-Tier Correlations",
                    section_type="table",
                    content="\n".join(corr_lines),
                    correlations=[c.correlation_id for c in bundle.correlations[:15]],
                    order=order,
                )
            )
            order += 1

        # 6. Scoring
        all_confidences = [r.confidence for r in resolution.records]
        confidence_score = sum(all_confidences) / len(all_confidences) if all_confidences else 0.0
        # Boost for exact ID match
        if resolution.match_quality == "exact_id":
            confidence_score = min(1.0, confidence_score * 1.15)

        entity_relevant_tiers = len(DOSSIER_SECTIONS)
        tiers_with_data = sum(1 for t in DOSSIER_SECTIONS if t in tier_records)
        completeness_score = tiers_with_data / entity_relevant_tiers if entity_relevant_tiers > 0 else 0.0

        # Key findings
        key_findings: list[str] = [
            f"Entity '{resolution.canonical_name}' found in {len(tier_records)} tiers "
            f"with {len(resolution.records)} total records.",
        ]
        if 19 in tier_records:
            key_findings.append(f"Active sanctions/regulatory listings: {len(tier_records[19])} records.")
        if 16 in tier_records:
            key_findings.append(f"Cyber threat activity: {len(tier_records[16])} indicators.")
        if bundle.graph_data and bundle.graph_data.nodes:
            key_findings.append(
                f"Network analysis: {len(bundle.graph_data.nodes)} connected entities, "
                f"{len(bundle.graph_data.communities)} communities."
            )

        product_content = json.dumps(
            {"entity": resolution.canonical_name, "findings": key_findings},
            sort_keys=True,
        )
        product_hash = compute_raw_hash(product_content.encode())

        return IntelligenceProduct(
            product_id=str(uuid.uuid4()),
            product_type=self.product_type,
            title=f"Entity Dossier — {resolution.canonical_name}",
            classification="green",
            generated_at=datetime.now(timezone.utc).isoformat(),
            time_window_start=bundle.time_window_start,
            time_window_end=bundle.time_window_end,
            sections=sections,
            summary=(
                f"Comprehensive dossier for entity '{resolution.canonical_name}'. "
                f"Resolved via {resolution.match_quality}. "
                f"{len(resolution.records)} records across {len(tier_records)} tiers."
            ),
            key_findings=key_findings,
            confidence_score=round(confidence_score, 4),
            completeness_score=round(completeness_score, 4),
            source_tiers=sorted(tier_records.keys()),
            record_count=len(resolution.records),
            correlation_count=len(bundle.correlations),
            parameters={
                "entity_id": params.entity_id,
                "entity_name": params.entity_name,
                "network_depth": self._max_network_depth,
                "max_network_nodes": self._max_network_nodes,
            },
            product_hash=product_hash,
            tags=["dossier", "entity"],
        )

    async def _resolve_entity(
        self, params: ProductParams, bundle: DataBundle
    ) -> EntityResolution:
        """Multi-strategy entity resolution.

        Phase 1 — Exact ID match.
        Phase 2 — Fuzzy name match (if Phase 1 yields < 3 records).
        Phase 3 — Graph expansion from matched records.
        """
        resolution = EntityResolution()
        all_records: list[NormalizedRecord] = []

        # Flatten bundle records
        for tier_id, recs in bundle.records.items():
            all_records.extend(recs)

        if params.entity_id:
            # Phase 1: Exact ID match
            matched = self._exact_id_match(all_records, params.entity_id)
            if matched:
                resolution.records = matched
                resolution.match_quality = "exact_id"
                resolution.canonical_name = self._extract_canonical_name(matched)
                resolution.identifiers = {"entity_id": params.entity_id}
                if len(matched) >= 3:
                    return resolution

        if params.entity_name or (params.entity_id and len(resolution.records) < 3):
            # Phase 2: Fuzzy name match
            name = params.entity_name or params.entity_id or ""
            fuzzy_matched = self._fuzzy_name_match(all_records, name)
            # Merge with existing
            seen = {r.raw_hash for r in resolution.records}
            for rec in fuzzy_matched:
                if rec.raw_hash not in seen:
                    resolution.records.append(rec)
                    seen.add(rec.raw_hash)
            if not resolution.canonical_name:
                resolution.canonical_name = self._extract_canonical_name(resolution.records) or name
            if resolution.match_quality == "exact_id" and fuzzy_matched:
                pass  # keep exact_id quality
            elif fuzzy_matched:
                resolution.match_quality = "fuzzy_name"

        # Phase 3: Graph expansion — use records from graph_data if available
        if bundle.graph_data and bundle.graph_data.nodes:
            graph_hashes = {n.node_id for n in bundle.graph_data.nodes}
            existing_hashes = {r.raw_hash for r in resolution.records}
            for rec in all_records:
                if rec.raw_hash in graph_hashes and rec.raw_hash not in existing_hashes:
                    resolution.records.append(rec)
                    existing_hashes.add(rec.raw_hash)
            if resolution.records and resolution.match_quality not in ("exact_id",):
                resolution.match_quality = "graph_expanded"

        if not resolution.canonical_name:
            resolution.canonical_name = params.entity_name or params.entity_id or "Unknown"

        return resolution

    @staticmethod
    def _exact_id_match(
        records: list[NormalizedRecord], entity_id: str
    ) -> list[NormalizedRecord]:
        """Match records by entity ID in payload fields."""
        id_fields = [
            "entity_id", "stix_id", "ofac_id", "actor_id", "case_id",
            "id", "actor1", "actor2", "name",
        ]
        matched: list[NormalizedRecord] = []
        for rec in records:
            for f in id_fields:
                val = rec.payload.get(f)
                if val is not None and str(val) == entity_id:
                    matched.append(rec)
                    break
        return matched

    @staticmethod
    def _fuzzy_name_match(
        records: list[NormalizedRecord], name: str
    ) -> list[NormalizedRecord]:
        """Fuzzy name match using Jaro-Winkler similarity >= 0.80."""
        try:
            import jellyfish
        except ImportError:
            # Fallback to simple substring match
            name_lower = name.lower()
            return [
                r for r in records
                if name_lower in json.dumps(r.payload).lower()
            ]

        name_fields = [
            "entity_name", "name", "actor1", "actor2",
            "threat_actor", "organization", "supplier", "recipient",
            "perpetrator", "victim",
        ]
        matched: list[NormalizedRecord] = []
        name_lower = name.lower()
        for rec in records:
            for f in name_fields:
                val = rec.payload.get(f)
                if val and isinstance(val, str):
                    sim = jellyfish.jaro_winkler_similarity(name_lower, val.lower())
                    if sim >= 0.80:
                        matched.append(rec)
                        break
        return matched

    @staticmethod
    def _extract_canonical_name(records: list[NormalizedRecord]) -> str:
        """Extract the most frequent entity name from matched records."""
        name_fields = ["entity_name", "name", "actor1", "threat_actor", "organization"]
        name_counts: dict[str, int] = {}
        for rec in records:
            for f in name_fields:
                val = rec.payload.get(f)
                if val and isinstance(val, str):
                    name_counts[val] = name_counts.get(val, 0) + 1
        if not name_counts:
            return ""
        return max(name_counts, key=name_counts.get)  # type: ignore[arg-type]


def _describe_record(record: NormalizedRecord) -> str:
    """Generate a brief description of a record for dossier sections."""
    p = record.payload
    tier = int(record.tier)
    ts = record.timestamp.isoformat() if isinstance(record.timestamp, datetime) else str(record.timestamp)

    if tier == 19:
        return f"[{ts}] {p.get('entity_name', 'Unknown')} — {p.get('program', 'N/A')} ({p.get('source', '')})"
    elif tier == 15:
        return f"[{ts}] {p.get('event_type', 'Event')} in {p.get('country', 'unknown')}: {p.get('notes', '')[:100]}"
    elif tier == 16:
        return f"[{ts}] {p.get('type', 'Indicator')}: {p.get('name', 'unknown')}"
    elif tier == 21:
        return f"[{ts}] {p.get('description', p.get('summary', 'Human rights record'))[:150]}"
    elif tier == 8:
        return f"[{ts}] {p.get('organization', 'IO')}: {p.get('title', p.get('description', ''))[:100]}"
    elif tier == 14:
        return f"[{ts}] {p.get('supplier', '')} → {p.get('recipient', '')}: {p.get('description', '')[:100]}"
    else:
        return f"[{ts}] Tier {tier}: {record.stream_id}"
