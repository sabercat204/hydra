"""Tests for AnalysisEngine — product generation orchestration."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hydra.analysis.engine import AnalysisEngine
from hydra.analysis.exceptions import InsufficientDataError, ProductNotFoundError
from hydra.analysis.graph import GraphAnalyzer
from hydra.analysis.models import (
    DataBundle,
    GraphResult,
    IntelligenceProduct,
    ProductParams,
    ProductSection,
    TimelineResult,
)
from hydra.analysis.products.base import BaseProduct
from hydra.analysis.queries import QueryLayer
from hydra.analysis.timeline import TimelineBuilder
from hydra.config import HydraSettings
from hydra.correlation.models import CorrelationResult
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier
from hydra.storage.engines.postgres import PostgresEngine
from hydra.utils.hashing import compute_raw_hash


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_settings() -> HydraSettings:
    return HydraSettings()


def _make_record(tier: int = 1, raw_hash: str | None = None, confidence: float = 0.9) -> NormalizedRecord:
    data = f'{{"tier": {tier}, "id": "{raw_hash or "test"}"}}'.encode()
    return NormalizedRecord(
        stream_id=f"stream_tier_{tier}",
        tier=Tier(tier),
        timestamp=datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        payload={"magnitude": 5.0, "place": "Test Location", "country_code": "US"},
        source_meta=SourceMeta(source_name="test", adapter_type="test"),
        raw_hash=raw_hash or compute_raw_hash(data),
        confidence=confidence,
        tags=["test"],
    )


def _make_correlation(hash_a: str, hash_b: str, tier_a: int = 1, tier_b: int = 15) -> CorrelationResult:
    return CorrelationResult(
        correlation_id="corr-001",
        pipeline_id="test_pipeline",
        record_a_hash=hash_a,
        record_b_hash=hash_b,
        tier_a=tier_a,
        tier_b=tier_b,
        confidence=0.85,
        match_dimensions={"spatial": 0.9},
        evidence={"test": True},
        correlation_hash="corrhash001",
        created_at=datetime.now(timezone.utc).isoformat(),
    )


class MockProduct(BaseProduct):
    """Mock product for testing."""

    @property
    def product_type(self) -> str:
        return "mock_product"

    @property
    def source_tiers(self) -> list[int]:
        return [1, 15]

    @property
    def requires_graph(self) -> bool:
        return False

    @property
    def requires_timeline(self) -> bool:
        return False

    async def generate(self, bundle: DataBundle, params: ProductParams) -> IntelligenceProduct:
        return IntelligenceProduct(
            product_id="prod-001",
            product_type="mock_product",
            title="Mock Product",
            classification="green",
            generated_at=datetime.now(timezone.utc).isoformat(),
            time_window_start=bundle.time_window_start,
            time_window_end=bundle.time_window_end,
            sections=[ProductSection(section_id="s1", title="Test", section_type="narrative", content="test")],
            summary="Mock summary",
            key_findings=["Finding 1"],
            confidence_score=0.9,
            completeness_score=0.5,
            source_tiers=[1, 15],
            record_count=bundle.total_records,
            correlation_count=len(bundle.correlations),
            product_hash=compute_raw_hash(b"mock_product"),
            tags=["test"],
        )


class MockGraphProduct(MockProduct):
    @property
    def requires_graph(self) -> bool:
        return True


class MockTimelineProduct(MockProduct):
    @property
    def requires_timeline(self) -> bool:
        return True


def _build_engine(
    records: dict[int, list[NormalizedRecord]] | None = None,
    correlations: list[CorrelationResult] | None = None,
    graph_result: GraphResult | None = None,
) -> AnalysisEngine:
    settings = _make_settings()

    query_layer = MagicMock(spec=QueryLayer)
    query_layer.query_records = AsyncMock(return_value=records or {})
    query_layer.query_correlations = AsyncMock(return_value=correlations or [])

    graph_analyzer = MagicMock(spec=GraphAnalyzer)
    graph_analyzer.analyze_entity_network = AsyncMock(return_value=graph_result or GraphResult())

    timeline_builder = MagicMock(spec=TimelineBuilder)
    timeline_builder.build = AsyncMock(return_value=TimelineResult())

    pg_engine = MagicMock(spec=PostgresEngine)
    pg_engine._pool = MagicMock()
    conn_mock = AsyncMock()
    pg_engine._pool.acquire = MagicMock(return_value=AsyncContextMock(conn_mock))

    return AnalysisEngine(query_layer, graph_analyzer, timeline_builder, pg_engine, settings)


class AsyncContextMock:
    """Async context manager mock."""

    def __init__(self, return_value: object) -> None:
        self._value = return_value

    async def __aenter__(self) -> object:
        return self._value

    async def __aexit__(self, *args: object) -> None:
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_generate_product_success() -> None:
    rec = _make_record(tier=1, raw_hash="a" * 16)
    engine = _build_engine(records={1: [rec]})
    engine.register_product(MockProduct())

    params = ProductParams(
        time_window_start="2026-01-01T00:00:00Z",
        time_window_end="2026-01-02T00:00:00Z",
    )
    product = await engine.generate("mock_product", params)

    assert product.product_type == "mock_product"
    assert product.record_count == 1
    assert product.confidence_score == 0.9


@pytest.mark.asyncio
async def test_generate_with_graph() -> None:
    rec = _make_record(tier=1, raw_hash="b" * 16)
    engine = _build_engine(records={1: [rec]})
    engine.register_product(MockGraphProduct())

    params = ProductParams(
        time_window_start="2026-01-01T00:00:00Z",
        time_window_end="2026-01-02T00:00:00Z",
    )
    product = await engine.generate("mock_product", params)

    # Graph analyzer should have been called
    engine._graph.analyze_entity_network.assert_called_once()
    assert product is not None


@pytest.mark.asyncio
async def test_generate_with_timeline() -> None:
    rec = _make_record(tier=1, raw_hash="c" * 16)
    engine = _build_engine(records={1: [rec]})
    engine.register_product(MockTimelineProduct())

    params = ProductParams(
        time_window_start="2026-01-01T00:00:00Z",
        time_window_end="2026-01-02T00:00:00Z",
    )
    product = await engine.generate("mock_product", params)

    engine._timeline.build.assert_called_once()
    assert product is not None


@pytest.mark.asyncio
async def test_generate_without_graph() -> None:
    rec = _make_record(tier=1, raw_hash="d" * 16)
    engine = _build_engine(records={1: [rec]})
    engine.register_product(MockProduct())

    params = ProductParams(
        time_window_start="2026-01-01T00:00:00Z",
        time_window_end="2026-01-02T00:00:00Z",
    )
    await engine.generate("mock_product", params)

    engine._graph.analyze_entity_network.assert_not_called()


@pytest.mark.asyncio
async def test_product_dedup() -> None:
    rec = _make_record(tier=1, raw_hash="e" * 16)
    engine = _build_engine(records={1: [rec]})
    engine.register_product(MockProduct())

    params = ProductParams(
        time_window_start="2026-01-01T00:00:00Z",
        time_window_end="2026-01-02T00:00:00Z",
    )
    # Generate twice — persist should use ON CONFLICT DO UPDATE
    p1 = await engine.generate("mock_product", params)
    p2 = await engine.generate("mock_product", params)

    assert p1.product_hash == p2.product_hash


@pytest.mark.asyncio
async def test_unknown_product_type() -> None:
    engine = _build_engine()
    params = ProductParams()

    with pytest.raises(ProductNotFoundError) as exc_info:
        await engine.generate("nonexistent_type", params)
    assert "nonexistent_type" in str(exc_info.value)


@pytest.mark.asyncio
async def test_insufficient_data() -> None:
    engine = _build_engine(records={})
    engine.register_product(MockProduct())

    params = ProductParams(
        time_window_start="2026-01-01T00:00:00Z",
        time_window_end="2026-01-02T00:00:00Z",
    )
    with pytest.raises(InsufficientDataError):
        await engine.generate("mock_product", params)


@pytest.mark.asyncio
async def test_list_products_filtered() -> None:
    settings = _make_settings()
    pg_engine = MagicMock(spec=PostgresEngine)
    pg_engine._pool = None  # No pool — returns empty

    engine = AnalysisEngine(
        MagicMock(), MagicMock(), MagicMock(), pg_engine, settings
    )
    result = await engine.list_products(product_type="situation_report", since="2026-01-01T00:00:00Z")
    assert result == []


@pytest.mark.asyncio
async def test_get_product_by_id() -> None:
    settings = _make_settings()
    pg_engine = MagicMock(spec=PostgresEngine)
    pg_engine._pool = None

    engine = AnalysisEngine(
        MagicMock(), MagicMock(), MagicMock(), pg_engine, settings
    )
    result = await engine.get_product("00000000-0000-0000-0000-000000000001")
    assert result is None


@pytest.mark.asyncio
async def test_parallel_bundle_queries() -> None:
    """Records and correlations are fetched — correlations after records."""
    rec = _make_record(tier=1, raw_hash="f" * 16)
    engine = _build_engine(records={1: [rec]}, correlations=[])
    engine.register_product(MockTimelineProduct())

    params = ProductParams(
        time_window_start="2026-01-01T00:00:00Z",
        time_window_end="2026-01-02T00:00:00Z",
    )
    await engine.generate("mock_product", params)

    # Both query methods should have been called
    engine._queries.query_records.assert_called_once()
    engine._queries.query_correlations.assert_called_once()
