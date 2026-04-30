"""Tests for TaskRunner — adapter execution + storage routing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier
from hydra.scheduler.backpressure import BackpressureMonitor, BackpressureState, EngineBackpressure
from hydra.scheduler.concurrency import ConcurrencyManager
from hydra.scheduler.exceptions import AdapterResolutionError
from hydra.scheduler.task_runner import TaskResult, TaskRunner
from hydra.storage.router import RouteResult

import sys

# Lazy-import adapter exceptions directly from the module file to avoid
# triggering hydra.adapters.__init__.py (which imports pandasdmx and breaks
# in environments where pandasdmx is incompatible with pydantic v2).
# We use the same approach as task_runner._get_adapter_exceptions() to ensure
# the exception classes are the same objects.
from hydra.scheduler.task_runner import _get_adapter_exceptions

FetchError, ParseError, RateLimitError, ValidationError = _get_adapter_exceptions()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_record(stream_id: str = "test_stream") -> NormalizedRecord:
    return NormalizedRecord(
        stream_id=stream_id,
        tier=Tier(1),
        timestamp=datetime.now(timezone.utc),
        payload={"key": "value"},
        source_meta=SourceMeta(source_name="test", adapter_type="rest_json"),
        raw_hash="a1b2c3d4e5f67890",
    )


def _make_route_result(routed: int = 5, deduped: int = 0, failed: int = 0) -> RouteResult:
    return RouteResult(
        total=routed + deduped + failed,
        routed=routed,
        deduplicated=deduped,
        failed=failed,
        engine_counts={"postgres": routed},
        duration_ms=10.0,
    )


def _make_bp_state(overall: str = "CLEAR") -> BackpressureState:
    engines = {
        "postgres": EngineBackpressure("postgres", 100, 1000, 5000, "CLEAR"),
    }
    return BackpressureState(overall=overall, engines=engines, checked_at=datetime.now(timezone.utc).isoformat())


def _make_tier_mock(adapter: str = "rest_json", fallback: str | None = None, cadence: str = "daily"):
    from hydra.registry.stream_registry import StreamSource, StreamTier
    return StreamTier(
        id=1, name="Test", streams=1, access="5G", formats=["json"],
        cadence=cadence, adapter=adapter, fallback=fallback,
        sources=[StreamSource("test_stream", "https://example.com", "json", "none", "")],
    )


def _build_runner(
    bp_state: BackpressureState | None = None,
    bp_wait_result: bool = True,
    concurrency_acquired: bool = True,
    adapter_records: list[NormalizedRecord] | None = None,
    adapter_error: Exception | None = None,
    route_result: RouteResult | None = None,
    fallback_adapter: str | None = None,
    fallback_records: list[NormalizedRecord] | None = None,
    fallback_error: Exception | None = None,
) -> TaskRunner:
    """Build a TaskRunner with fully mocked dependencies."""
    if bp_state is None:
        bp_state = _make_bp_state("CLEAR")
    if adapter_records is None:
        adapter_records = [_make_record()]
    if route_result is None:
        route_result = _make_route_result(routed=len(adapter_records))

    # Registry
    tier = _make_tier_mock(fallback=fallback_adapter)
    registry = MagicMock()
    registry.tiers = {1: tier}

    # Auth
    auth = MagicMock()
    auth.apply = AsyncMock(return_value=MagicMock())

    # Storage router
    router = MagicMock()
    router.route = AsyncMock(return_value=route_result)

    # Backpressure
    bp = MagicMock(spec=BackpressureMonitor)
    bp.check = AsyncMock(return_value=bp_state)
    bp.wait_for_clear = AsyncMock(return_value=bp_wait_result)

    # Concurrency
    cm = MagicMock(spec=ConcurrencyManager)
    cm.acquire = AsyncMock(return_value=concurrency_acquired)
    cm.release = AsyncMock()

    # Settings
    settings = MagicMock()
    settings.scheduler.global_concurrency_limit = 10
    settings.scheduler.cadence_concurrency_limits = {"daily": 6}

    runner = TaskRunner(
        registry=registry,
        auth_manager=auth,
        storage_router=router,
        backpressure_monitor=bp,
        concurrency_manager=cm,
        settings=settings,
        redis_cache=None,
    )

    # Mock adapter resolution
    mock_adapter = MagicMock()
    if adapter_error:
        mock_adapter.run = AsyncMock(side_effect=adapter_error)
    else:
        mock_adapter.run = AsyncMock(return_value=adapter_records)

    mock_adapter_cls = MagicMock(return_value=mock_adapter)

    if fallback_adapter:
        fallback_mock = MagicMock()
        if fallback_error:
            fallback_mock.run = AsyncMock(side_effect=fallback_error)
        elif fallback_records is not None:
            fallback_mock.run = AsyncMock(return_value=fallback_records)
        else:
            fallback_mock.run = AsyncMock(return_value=adapter_records)
        fallback_cls = MagicMock(return_value=fallback_mock)

        def resolve(adapter_type: str):
            if adapter_type == "rest_json":
                return mock_adapter_cls
            return fallback_cls

        runner._resolve_adapter_class = MagicMock(side_effect=resolve)
    else:
        runner._resolve_adapter_class = MagicMock(return_value=mock_adapter_cls)

    return runner


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestTaskRunner:
    @pytest.mark.asyncio
    async def test_execute_success(self):
        """Full pipeline: adapter.run() → route() → TaskResult with status='success'."""
        records = [_make_record(), _make_record()]
        runner = _build_runner(adapter_records=records, route_result=_make_route_result(routed=2))

        result = await runner.execute("test_stream")

        assert result.status == "success"
        assert result.records_fetched == 2
        assert result.records_routed == 2
        assert result.error is None

    @pytest.mark.asyncio
    async def test_execute_with_fallback(self):
        """Primary FetchError triggers fallback adapter; fallback_used=True."""
        fallback_records = [_make_record()]
        runner = _build_runner(
            adapter_error=FetchError("primary down"),
            fallback_adapter="fdsn",
            fallback_records=fallback_records,
            route_result=_make_route_result(routed=1),
        )

        result = await runner.execute("test_stream")

        assert result.status == "success"
        assert result.fallback_used is True
        assert result.records_fetched == 1

    @pytest.mark.asyncio
    async def test_fallback_not_triggered_on_parse_error(self):
        """ParseError does not trigger fallback."""
        runner = _build_runner(
            adapter_error=ParseError("bad format"),
            fallback_adapter="fdsn",
        )

        result = await runner.execute("test_stream")

        assert result.status == "failed"
        assert "bad format" in result.error

    @pytest.mark.asyncio
    async def test_fallback_not_triggered_on_rate_limit(self):
        """RateLimitError does not trigger fallback."""
        runner = _build_runner(
            adapter_error=RateLimitError("throttled", retry_after=5.0),
            fallback_adapter="fdsn",
        )

        result = await runner.execute("test_stream")

        assert result.status == "failed"
        assert "throttled" in result.error

    @pytest.mark.asyncio
    async def test_execute_backpressure_blocked(self):
        """BLOCKED state → TaskResult status='skipped'."""
        bp = _make_bp_state("BLOCKED")
        runner = _build_runner(bp_state=bp)

        result = await runner.execute("test_stream")

        assert result.status == "skipped"
        assert "BLOCKED" in result.error

    @pytest.mark.asyncio
    async def test_execute_backpressure_throttled_clears(self):
        """THROTTLED → wait → clears → execution proceeds."""
        bp = _make_bp_state("THROTTLED")
        runner = _build_runner(bp_state=bp, bp_wait_result=True)

        result = await runner.execute("test_stream")

        assert result.status == "success"
        assert result.backpressure_delayed is True

    @pytest.mark.asyncio
    async def test_execute_backpressure_throttled_timeout(self):
        """THROTTLED → wait → timeout → TaskResult status='skipped'."""
        bp = _make_bp_state("THROTTLED")
        runner = _build_runner(bp_state=bp, bp_wait_result=False)

        result = await runner.execute("test_stream")

        assert result.status == "skipped"
        assert result.backpressure_delayed is True
        assert "THROTTLED" in result.error

    @pytest.mark.asyncio
    async def test_execute_concurrency_timeout(self):
        """Semaphore timeout → TaskResult status='failed'."""
        runner = _build_runner(concurrency_acquired=False)

        result = await runner.execute("test_stream")

        assert result.status == "failed"
        assert "Concurrency timeout" in result.error

    def test_adapter_resolution(self):
        """All 10 adapter types resolve to correct classes."""
        from hydra.scheduler.task_runner import _ADAPTER_TYPE_MAP

        expected_types = {
            "rest_json", "fdsn", "ckan", "odata", "sdmx",
            "tap_vo", "s3_bulk", "scrape_rss", "ais_adsb", "stix_taxii",
        }
        assert set(_ADAPTER_TYPE_MAP.keys()) == expected_types

    def test_unknown_adapter_type(self):
        """Unknown type raises AdapterResolutionError."""
        runner = _build_runner()
        # Reset the mock to use real resolution
        runner._resolve_adapter_class = TaskRunner._resolve_adapter_class.__get__(runner)
        runner._adapter_cache = {}

        with pytest.raises(AdapterResolutionError, match="Unknown adapter type"):
            runner._resolve_adapter_class("nonexistent_adapter")

    @pytest.mark.asyncio
    async def test_metrics_in_task_result(self):
        """records_fetched, records_routed, duration_ms populated."""
        records = [_make_record() for _ in range(3)]
        runner = _build_runner(
            adapter_records=records,
            route_result=_make_route_result(routed=3),
        )

        result = await runner.execute("test_stream")

        assert result.records_fetched == 3
        assert result.records_routed == 3
        assert result.duration_ms > 0
        assert result.timestamp != ""

    @pytest.mark.asyncio
    async def test_dead_stream_counter_increment(self):
        """Failed execution increments Redis failure counter."""
        runner = _build_runner(adapter_error=FetchError("down"))
        mock_redis = MagicMock()
        mock_redis._redis = AsyncMock()
        mock_redis._redis.hincrby = AsyncMock()
        mock_redis._redis.hset = AsyncMock()
        runner._redis = mock_redis

        result = await runner.execute("test_stream")

        assert result.status == "failed"
        mock_redis._redis.hincrby.assert_called_once()

    @pytest.mark.asyncio
    async def test_dead_stream_counter_reset(self):
        """Successful execution resets counter to 0."""
        runner = _build_runner()
        mock_redis = MagicMock()
        mock_redis._redis = AsyncMock()
        mock_redis._redis.hset = AsyncMock()
        runner._redis = mock_redis

        result = await runner.execute("test_stream")

        assert result.status == "success"
        mock_redis._redis.hset.assert_called_once()
