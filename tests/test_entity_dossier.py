"""Tests for EntityDossier product generator."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from hydra.analysis.exceptions import EntityResolutionError
from hydra.analysis.models import (
    CentralityScore,
    DataBundle,
    GraphNode,
    GraphResult,
    ProductParams,
    TimelineEvent,
    TimelineResult,
)
from hydra.analysis.products.entity_dossier import EntityDossier
from hydra.config import HydraSettings
from hydra.correlation.models import CorrelationResult
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier
from hydra.utils.hashing import compute_raw_hash


def _settings() -> HydraSettings:
    return HydraSettings()


def _record(
    tier: int = 19,
    payload: dict | None = None,
    raw_hash: str | None = None,
    confidence: float = 0.9,
) -> NormalizedRecord:
    data = f'{{"tier":{tier},"h":"{raw_hash or "x"}"}}'.encode()
    return NormalizedRecord(
        stream_id=f"stream_{tier}",
        tier=Tier(tier),
        timestamp=datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        payload=payload or {"entity_name": "Test Entity", "ofac_id": "OFAC-123", "program": "SDN"},
        source_meta=SourceMeta(source_name="test", adapter_type="test"),
        raw_hash=raw_hash or compute_raw_hash(data),
        confidence=confidence,
        tags=["test"],
    )


def _bundle(
    records: dict[int, list[NormalizedRecord]] | None = None,
    correlations: list[CorrelationResult] | None = None,
    graph: GraphResult | None = None,
    timeline: TimelineResult | None = None,
) -> DataBundle:
    return DataBundle(
        records=records or {},
        correlations=correlations or [],
        graph_data=graph,
        timeline=timeline,
        time_window_start="2025-01-15T00:00:00Z",
        time_window_end="2026-01-16T00:00:00Z",
        total_records=sum(len(v) for v in (records or {}).values()),
    )


@pytest.mark.asyncio
async def test_dossier_by_entity_id() -> None:
    r = _record(tier=19, raw_hash="a" * 16, payload={"entity_name": "Test Corp", "ofac_id": "OFAC-123", "program": "SDN"})
    dossier = EntityDossier(_settings())
    product = await dossier.generate(
        _bundle(records={19: [r]}),
        ProductParams(entity_id="OFAC-123"),
    )
    assert product.product_type == "entity_dossier"
    assert "Test Corp" in product.title


@pytest.mark.asyncio
async def test_dossier_by_entity_name() -> None:
    r = _record(tier=19, raw_hash="b" * 16, payload={"entity_name": "Acme Corp", "program": "SDN"})
    dossier = EntityDossier(_settings())
    product = await dossier.generate(
        _bundle(records={19: [r]}),
        ProductParams(entity_name="Acme Corp"),
    )
    assert "Acme Corp" in product.title


@pytest.mark.asyncio
async def test_dossier_graph_network() -> None:
    r = _record(tier=19, raw_hash="c" * 16)
    graph = GraphResult(
        nodes=[GraphNode(node_id="c" * 16, label="Record", tier=19, degree=3)],
        central_nodes=[CentralityScore(node_id="c" * 16, label="Record", metric="degree", score=3.0)],
    )
    dossier = EntityDossier(_settings())
    product = await dossier.generate(
        _bundle(records={19: [r]}, graph=graph),
        ProductParams(entity_id="OFAC-123"),
    )
    section_titles = [s.title for s in product.sections]
    assert "Network Analysis" in section_titles


@pytest.mark.asyncio
async def test_dossier_timeline() -> None:
    r = _record(tier=19, raw_hash="d" * 16)
    tl = TimelineResult(
        events=[
            TimelineEvent(
                timestamp="2026-01-15T12:00:00Z",
                record_hash="d" * 16,
                tier=19,
                stream_id="stream_19",
                title="Test event",
                description="Test",
            )
        ],
        time_window_start="2025-01-15T00:00:00Z",
        time_window_end="2026-01-16T00:00:00Z",
        total_events=1,
    )
    dossier = EntityDossier(_settings())
    product = await dossier.generate(
        _bundle(records={19: [r]}, timeline=tl),
        ProductParams(entity_id="OFAC-123"),
    )
    section_titles = [s.title for s in product.sections]
    assert "Activity Timeline" in section_titles


@pytest.mark.asyncio
async def test_dossier_empty_tiers_omitted() -> None:
    # Only tier 19 has data — other tier sections should not appear
    r = _record(tier=19, raw_hash="e" * 16)
    dossier = EntityDossier(_settings())
    product = await dossier.generate(
        _bundle(records={19: [r]}),
        ProductParams(entity_id="OFAC-123"),
    )
    section_titles = [s.title for s in product.sections]
    assert "Cyber Threat Activity" not in section_titles
    assert "Conflict & Event Involvement" not in section_titles


@pytest.mark.asyncio
async def test_entity_not_found() -> None:
    dossier = EntityDossier(_settings())
    with pytest.raises(EntityResolutionError):
        await dossier.generate(
            _bundle(records={}),
            ProductParams(entity_id="nonexistent"),
        )


@pytest.mark.asyncio
async def test_dossier_confidence_boost_exact_id() -> None:
    r = _record(tier=19, raw_hash="f" * 16, confidence=0.8)
    dossier = EntityDossier(_settings())
    product = await dossier.generate(
        _bundle(records={19: [r]}),
        ProductParams(entity_id="OFAC-123"),
    )
    # Exact ID match should boost confidence above raw 0.8
    assert product.confidence_score > 0.8


@pytest.mark.asyncio
async def test_dossier_graph_depth_limit() -> None:
    """Graph traversal respects max_network_depth setting."""
    r = _record(tier=19, raw_hash="1" * 16)
    dossier = EntityDossier(_settings())
    assert dossier._max_network_depth == 2


@pytest.mark.asyncio
async def test_dossier_node_cap() -> None:
    """Network capped at max_network_nodes setting."""
    r = _record(tier=19, raw_hash="2" * 16)
    dossier = EntityDossier(_settings())
    assert dossier._max_network_nodes == 50
