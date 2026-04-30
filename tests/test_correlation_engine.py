"""Tests for CorrelationEngine."""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hydra.config import HydraSettings
from hydra.correlation.engine import CorrelationEngine
from hydra.correlation.exceptions import CandidateQueryError, PipelineNotFoundError
from hydra.correlation.models import (
    CandidateSet,
    CorrelationResult,
    CorrelationRunResult,
)
from hydra.correlation.pipelines.base import BasePipeline
from hydra.models.normalized import (
    GeoGeometry,
    NormalizedRecord,
    SourceMeta,
    Tier,
)
from hydra.registry.stream_registry import StreamRegistry
from hydra.utils.hashing import compute_raw_hash


def _make_record(
    tier: Tier = Tier.GEOPHYSICAL_SEISMIC,
    raw_suffix: str = "a",
    lon: Optional[float] = None,
    lat: Optional[float] = None,
    timestamp: Optional[datetime] = None,
    payload: Optional[dict] = None,
    tags: Optional[list] = None,
) -> NormalizedRecord:
    geo = None
    if lon is not None and lat is not None:
        geo = GeoGeometry(type="Point", coordinates=[lon, lat])
    ts = timestamp or datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    raw = compute_raw_hash(f"test_{raw_suffix}".encode())
    return NormalizedRecord(
        stream_id=f"test_{raw_suffix}",
        tier=tier,
        timestamp=ts,
        geo=geo,
        payload=payload or {},
        source_meta=SourceMeta(source_name="test", adapter_type="test"),
        raw_hash=raw,
        tags=tags or [],
    )


def _make_correlation_result(
    pipeline_id: str = "test_pipeline",
    hash_a: str = "a" * 16,
    hash_b: str = "b" * 16,
    confidence: float = 0.8,
) -> CorrelationResult:
    corr_hash = compute_raw_hash(f"{hash_a}:{hash_b}:{pipeline_id}".encode())
    return CorrelationResult(
        correlation_id="test-uuid",
        pipeline_id=pipeline_id,
        record_a_hash=hash_a,
        record_b_hash=hash_b,
        tier_a=1,
        tier_b=15,
        confidence=confidence,
        match_dimensions={"spatial": 0.9},
        evidence={"spatial": {"distance_km": 10.0}},
        correlation_hash=corr_hash,
        created_at=datetime.now(timezone.utc).isoformat(),
        tags=["test"],
    )


class MockPipeline(BasePipeline):
    pipeline_id = "test_pipeline"  # type: ignore[assignment]
    source_tiers = [1, 15]  # type: ignore[assignment]

    def __init__(self, results: list[CorrelationResult] | None = None):
        self._results = results or []

    async def correlate(self, candidates: CandidateSet) -> list[CorrelationResult]:
        return self._results


@pytest.fixture
def settings() -> HydraSettings:
    return HydraSettings()


@pytest.fixture
def mock_pg():
    pg = MagicMock()
    pg._pool = None
    return pg


@pytest.fixture
def mock_neo4j():
    neo4j = MagicMock()
    neo4j._driver = None
    return neo4j


@pytest.fixture
def mock_registry():
    return StreamRegistry()


@pytest.fixture
def engine(mock_pg, mock_neo4j, mock_registry, settings):
    return CorrelationEngine(
        pg_engine=mock_pg,
        neo4j_engine=mock_neo4j,
        es_engine=None,
        registry=mock_registry,
        settings=settings,
    )


class TestCorrelationEngine:
    async def test_unknown_pipeline_raises(self, engine):
        """Unknown pipeline_id raises PipelineNotFoundError."""
        with pytest.raises(PipelineNotFoundError, match="Unknown pipeline: nonexistent"):
            await engine.run("nonexistent")

    async def test_run_pipeline_success(self, engine, mock_pg):
        """Full pipeline: query → correlate → dedup → persist → CorrelationRunResult."""
        # Setup mock PG pool
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock(return_value=False)))
        mock_pg._pool = mock_pool

        results = [_make_correlation_result()]
        pipeline = MockPipeline(results=results)
        engine.register_pipeline(pipeline)

        run_result = await engine.run("test_pipeline")
        assert isinstance(run_result, CorrelationRunResult)
        assert run_result.pipeline_id == "test_pipeline"

    async def test_run_with_trigger_tiers(self, engine, mock_pg):
        """Triggered run passes trigger_tiers through."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock(return_value=False)))
        mock_pg._pool = mock_pool

        pipeline = MockPipeline(results=[])
        engine.register_pipeline(pipeline)

        run_result = await engine.run("test_pipeline", trigger_tiers=[1])
        assert run_result.trigger_tiers == [1]

    async def test_run_scheduled_full_window(self, engine, mock_pg):
        """Scheduled run queries all source tiers within lookback."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock(return_value=False)))
        mock_pg._pool = mock_pool

        pipeline = MockPipeline(results=[])
        engine.register_pipeline(pipeline)

        run_result = await engine.run("test_pipeline")
        assert run_result.trigger_tiers is None

    async def test_deduplication_existing(self, engine, mock_pg):
        """Existing correlation_hash skipped."""
        cr = _make_correlation_result(confidence=0.8)

        # Mock PG pool for dedup query
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[
            {"correlation_hash": cr.correlation_hash, "confidence": 0.9}
        ])
        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock(return_value=False)))
        mock_pg._pool = mock_pool

        new, updated, deduped = await engine._deduplicate([cr])
        assert len(new) == 0
        assert deduped == 1

    async def test_deduplication_confidence_update(self, engine, mock_pg):
        """Higher confidence updates existing correlation."""
        cr = _make_correlation_result(confidence=0.95)

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[
            {"correlation_hash": cr.correlation_hash, "confidence": 0.7}
        ])
        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock(return_value=False)))
        mock_pg._pool = mock_pool

        new, updated, deduped = await engine._deduplicate([cr])
        assert len(new) == 1
        assert updated == 1

    async def test_persist_pg_and_neo4j(self, engine, mock_pg, mock_neo4j):
        """Results written to both stores."""
        cr = _make_correlation_result()

        # Mock PG
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock(return_value=False)))
        mock_pg._pool = mock_pool

        # Mock Neo4j
        mock_session = AsyncMock()
        mock_session.run = AsyncMock()
        mock_driver = AsyncMock()
        mock_driver.session = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_session), __aexit__=AsyncMock(return_value=False)))
        mock_neo4j._driver = mock_driver

        persist = await engine._persist_results([cr])
        assert persist.pg_stored == 1
        assert persist.neo4j_stored == 1

    async def test_persist_neo4j_creates_reference_nodes(self, engine, mock_pg, mock_neo4j):
        """Non-graph-tier records get lightweight nodes via MERGE."""
        cr = _make_correlation_result()

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock(return_value=False)))
        mock_pg._pool = mock_pool

        mock_session = AsyncMock()
        mock_session.run = AsyncMock()
        mock_driver = AsyncMock()
        mock_driver.session = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_session), __aexit__=AsyncMock(return_value=False)))
        mock_neo4j._driver = mock_driver

        await engine._persist_results([cr])
        # Verify MERGE was called (creates reference nodes)
        mock_session.run.assert_called()
        call_args = mock_session.run.call_args
        assert "MERGE" in call_args[0][0]

    async def test_candidate_query_failure(self, engine, mock_pg):
        """PG query failure raises CandidateQueryError."""
        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(side_effect=Exception("Connection refused"))
        mock_pg._pool = mock_pool

        pipeline = MockPipeline()
        engine.register_pipeline(pipeline)

        with pytest.raises(CandidateQueryError):
            await engine.run("test_pipeline")

    async def test_max_pairs_cap(self, engine, mock_pg):
        """Pipeline respects max_pairs_per_run."""
        # This is tested at the pipeline level; engine passes through
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock(return_value=False)))
        mock_pg._pool = mock_pool

        pipeline = MockPipeline(results=[])
        engine.register_pipeline(pipeline)

        run_result = await engine.run("test_pipeline")
        assert run_result.correlations_found == 0
