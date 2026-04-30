"""Shared API test fixtures for HYDRA P11 tests."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from hydra.analysis.models import (
    CentralityScore,
    EventCluster,
    GraphEdge,
    GraphNode,
    GraphPath,
    GraphResult,
    IntelligenceProduct,
    ProductSection,
    TimelineEvent,
    TimelineResult,
)
from hydra.api.app import create_app
from hydra.api.dependencies import (
    APIKeyRecord,
    set_api_key_store,
    set_engines,
)
from hydra.api.jobs import JobManager
from hydra.api.routers.watchlists import reset_watchlists
from hydra.api.schemas.common import JobStatus
from hydra.config import HydraSettings
from hydra.correlation.models import CorrelationResult, CorrelationRunResult
from hydra.registry.stream_registry import StreamRegistry, StreamSource, StreamTier
from hydra.scheduler.backpressure import BackpressureState, EngineBackpressure
from hydra.scheduler.health import SchedulerHealth
from hydra.storage.health import StorageHealth

TEST_API_KEY = "test-api-key-12345"
TEST_KEY_HASH = hashlib.sha256(TEST_API_KEY.encode()).hexdigest()


@pytest.fixture
def valid_api_key() -> str:
    return TEST_API_KEY


@pytest.fixture
def mock_analysis_engine() -> AsyncMock:
    engine = AsyncMock()
    engine.generate = AsyncMock()
    engine.list_products = AsyncMock(return_value=[])
    engine.get_product = AsyncMock(return_value=None)
    return engine


@pytest.fixture
def mock_correlation_engine() -> AsyncMock:
    engine = AsyncMock()
    engine.run = AsyncMock()
    engine.get_run = AsyncMock(return_value=None)
    engine.list_correlations = AsyncMock(return_value=[])
    engine.get_correlation = AsyncMock(return_value=None)
    return engine


@pytest.fixture
def mock_query_layer() -> AsyncMock:
    ql = AsyncMock()
    ql.query_records = AsyncMock(return_value=[])
    ql.query_timeseries = AsyncMock(return_value={})
    ql.search_text = AsyncMock(return_value=[])
    ql.query_correlations = AsyncMock(return_value=[])
    return ql


@pytest.fixture
def mock_graph_analyzer() -> AsyncMock:
    ga = AsyncMock()
    ga.analyze_entity_network = AsyncMock(
        return_value=GraphResult(query_duration_ms=10.0)
    )
    ga.shortest_paths = AsyncMock(return_value=[])
    ga.centrality_ranking = AsyncMock(return_value=[])
    return ga


@pytest.fixture
def mock_timeline_builder() -> AsyncMock:
    tb = AsyncMock()
    tb.build = AsyncMock(
        return_value=TimelineResult(
            time_window_start="2026-01-01T00:00:00Z",
            time_window_end="2026-01-02T00:00:00Z",
        )
    )
    return tb


@pytest.fixture
def mock_scheduler_health() -> AsyncMock:
    sh = AsyncMock()
    bp = BackpressureState(
        overall="CLEAR",
        engines={
            "postgres": EngineBackpressure(
                engine="postgres", queue_depth=0, soft_limit=1000, hard_limit=5000, state="CLEAR"
            ),
        },
        checked_at=datetime.now(timezone.utc).isoformat(),
    )
    health = SchedulerHealth(
        status="OK",
        active_adapters=5,
        active_by_cadence={"hourly": 2, "daily": 3},
        backpressure=bp,
        storage_health={
            "postgres": StorageHealth(engine="postgres", status="OK", latency_ms=1.0),
        },
        adapter_health={},
        dead_streams=[],
        checked_at=datetime.now(timezone.utc).isoformat(),
    )
    sh.check = AsyncMock(return_value=health)
    return sh


@pytest.fixture
def mock_backpressure_monitor() -> AsyncMock:
    bm = AsyncMock()
    bm.check = AsyncMock(
        return_value=BackpressureState(
            overall="CLEAR",
            engines={
                "postgres": EngineBackpressure(
                    engine="postgres", queue_depth=0, soft_limit=1000, hard_limit=5000, state="CLEAR"
                ),
            },
            checked_at=datetime.now(timezone.utc).isoformat(),
        )
    )
    return bm


@pytest.fixture
def mock_job_manager() -> AsyncMock:
    """Mocked JobManager with in-memory state."""
    jm = AsyncMock(spec=JobManager)
    _jobs: dict[str, JobStatus] = {}

    async def _create() -> str:
        job_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        _jobs[job_id] = JobStatus(
            job_id=job_id, status="pending", created_at=now, updated_at=now,
        )
        return job_id

    async def _get(job_id: str) -> JobStatus | None:
        return _jobs.get(job_id)

    async def _update(job_id: str, status: str, **kwargs: Any) -> None:
        if job_id in _jobs:
            existing = _jobs[job_id]
            _jobs[job_id] = JobStatus(
                job_id=job_id,
                status=status,  # type: ignore[arg-type]
                progress=kwargs.get("progress", existing.progress),
                result_id=kwargs.get("result_id", existing.result_id),
                error=kwargs.get("error", existing.error),
                created_at=existing.created_at,
                updated_at=datetime.now(timezone.utc).isoformat(),
            )

    async def _run_bg(job_id: str, coro: Any) -> None:
        # In tests, just update to running immediately
        await _update(job_id, "running")

    jm.create_job = AsyncMock(side_effect=_create)
    jm.get_job = AsyncMock(side_effect=_get)
    jm.update_job = AsyncMock(side_effect=_update)
    jm.run_in_background = AsyncMock(side_effect=_run_bg)
    jm._jobs = _jobs
    return jm


@pytest.fixture
def mock_registry() -> StreamRegistry:
    """Minimal StreamRegistry with 3 test tiers."""
    tiers = {
        1: StreamTier(
            id=1, name="Geophysical / Seismic", streams=3, access="green",
            formats=["GeoJSON", "QuakeML"], cadence="realtime", adapter="rest_json",
            fallback=None,
            sources=[StreamSource(name="USGS", url="https://earthquake.usgs.gov", format="GeoJSON", auth="none", notes="")],
        ),
        2: StreamTier(
            id=2, name="Atmospheric / Weather", streams=2, access="green",
            formats=["GRIB2", "JSON"], cadence="hourly", adapter="rest_json",
            fallback=None,
            sources=[StreamSource(name="NOAA", url="https://api.weather.gov", format="JSON", auth="none", notes="")],
        ),
        3: StreamTier(
            id=3, name="Space Weather / Solar", streams=1, access="green",
            formats=["JSON"], cadence="15min", adapter="rest_json",
            fallback=None,
            sources=[StreamSource(name="SWPC", url="https://services.swpc.noaa.gov", format="JSON", auth="none", notes="")],
        ),
    }
    return StreamRegistry(tiers=tiers)


@pytest.fixture
def sample_product() -> IntelligenceProduct:
    return IntelligenceProduct(
        product_id=str(uuid.uuid4()),
        product_type="situation_report",
        title="Test SITREP",
        classification="green",
        generated_at=datetime.now(timezone.utc).isoformat(),
        time_window_start="2026-01-01T00:00:00Z",
        time_window_end="2026-01-02T00:00:00Z",
        sections=[
            ProductSection(
                section_id="s1", title="Overview", section_type="narrative",
                content="Test content", records=["abc123"], correlations=[],
                confidence=0.9, order=0,
            ),
        ],
        summary="Test summary",
        key_findings=["Finding 1"],
        confidence_score=0.85,
        completeness_score=0.9,
        source_tiers=[1, 2],
        record_count=10,
        correlation_count=2,
        parameters={"region": "US"},
        tags=["test"],
    )


@pytest.fixture
def sample_correlation() -> CorrelationResult:
    return CorrelationResult(
        correlation_id=str(uuid.uuid4()),
        pipeline_id="geospatial_temporal",
        record_a_hash="a" * 16,
        record_b_hash="b" * 16,
        tier_a=1,
        tier_b=2,
        confidence=0.85,
        match_dimensions={"spatial": 0.9, "temporal": 0.8},
        evidence={"distance_km": 10.5},
        created_at=datetime.now(timezone.utc).isoformat(),
        tags=["test"],
    )


@pytest.fixture
def app(
    mock_analysis_engine,
    mock_correlation_engine,
    mock_query_layer,
    mock_graph_analyzer,
    mock_timeline_builder,
    mock_scheduler_health,
    mock_backpressure_monitor,
    mock_job_manager,
    mock_registry,
):
    """Create test app with mocked dependencies."""
    reset_watchlists()

    test_settings = HydraSettings()
    test_settings.api.rate_limit_enabled = False

    application = create_app(settings=test_settings)

    # Wire dependencies
    set_engines(
        analysis_engine=mock_analysis_engine,
        correlation_engine=mock_correlation_engine,
        query_layer=mock_query_layer,
        graph_analyzer=mock_graph_analyzer,
        timeline_builder=mock_timeline_builder,
        scheduler_health=mock_scheduler_health,
        backpressure_monitor=mock_backpressure_monitor,
        job_manager=mock_job_manager,
        registry=mock_registry,
    )

    # Set up API key store
    set_api_key_store({
        TEST_API_KEY: APIKeyRecord(key_id="test", name="test-key"),
        TEST_KEY_HASH: APIKeyRecord(key_id="test", name="test-key"),
    })

    return application


@pytest.fixture
async def client(app) -> AsyncClient:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
