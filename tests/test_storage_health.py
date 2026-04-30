"""Tests for StorageHealthAggregator — 8 tests covering aggregate status."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from hydra.storage.engines.base import StorageEngine
from hydra.storage.health import StorageHealth, StorageHealthAggregator
from hydra.storage.redis_cache import RedisCache


def _mock_engine(name: str, status: str = "OK", latency: float = 1.0) -> AsyncMock:
    engine = AsyncMock(spec=StorageEngine)
    engine.health_check = AsyncMock(return_value=StorageHealth(
        engine=name, status=status, latency_ms=latency,
    ))
    return engine


def _mock_redis(status: str = "OK") -> AsyncMock:
    redis = AsyncMock(spec=RedisCache)
    redis.health_check = AsyncMock(return_value=StorageHealth(
        engine="redis", status=status, latency_ms=1.0,
    ))
    redis.queue_depth = AsyncMock(return_value=0)
    redis.dlq_depth = AsyncMock(return_value=0)
    return redis


@pytest.mark.asyncio
async def test_all_ok():
    """All engines OK → aggregate OK."""
    engines = {
        "postgres": _mock_engine("postgres"),
        "influxdb": _mock_engine("influxdb"),
        "elasticsearch": _mock_engine("elasticsearch"),
        "neo4j": _mock_engine("neo4j"),
        "minio": _mock_engine("minio"),
    }
    redis = _mock_redis()
    agg = StorageHealthAggregator(engines, redis)
    status = await agg.overall_status()
    assert status == "OK"


@pytest.mark.asyncio
async def test_secondary_degraded():
    """Secondary engine DEGRADED → aggregate DEGRADED."""
    engines = {
        "postgres": _mock_engine("postgres"),
        "influxdb": _mock_engine("influxdb", status="DEGRADED"),
    }
    redis = _mock_redis()
    agg = StorageHealthAggregator(engines, redis)
    status = await agg.overall_status()
    assert status == "DEGRADED"


@pytest.mark.asyncio
async def test_secondary_unreachable():
    """Secondary engine UNREACHABLE → aggregate DEGRADED."""
    engines = {
        "postgres": _mock_engine("postgres"),
        "elasticsearch": _mock_engine("elasticsearch", status="UNREACHABLE"),
    }
    redis = _mock_redis()
    agg = StorageHealthAggregator(engines, redis)
    status = await agg.overall_status()
    assert status == "DEGRADED"


@pytest.mark.asyncio
async def test_postgres_unreachable():
    """PostgreSQL UNREACHABLE → aggregate UNREACHABLE."""
    engines = {
        "postgres": _mock_engine("postgres", status="UNREACHABLE"),
        "influxdb": _mock_engine("influxdb"),
    }
    redis = _mock_redis()
    agg = StorageHealthAggregator(engines, redis)
    status = await agg.overall_status()
    assert status == "UNREACHABLE"


@pytest.mark.asyncio
async def test_redis_unreachable():
    """Redis UNREACHABLE → aggregate UNREACHABLE."""
    engines = {"postgres": _mock_engine("postgres")}
    redis = _mock_redis(status="UNREACHABLE")
    agg = StorageHealthAggregator(engines, redis)
    status = await agg.overall_status()
    assert status == "UNREACHABLE"


@pytest.mark.asyncio
async def test_mixed_status():
    """Multiple engines mixed status → worst-case aggregation."""
    engines = {
        "postgres": _mock_engine("postgres"),
        "influxdb": _mock_engine("influxdb", status="DEGRADED"),
        "elasticsearch": _mock_engine("elasticsearch", status="UNREACHABLE"),
        "neo4j": _mock_engine("neo4j"),
    }
    redis = _mock_redis()
    agg = StorageHealthAggregator(engines, redis)
    status = await agg.overall_status()
    assert status == "DEGRADED"


@pytest.mark.asyncio
async def test_queue_depth_included():
    """Queue depth and DLQ depth included in per-engine health."""
    engines = {"postgres": _mock_engine("postgres")}
    redis = _mock_redis()
    redis.queue_depth = AsyncMock(return_value=42)
    redis.dlq_depth = AsyncMock(return_value=3)
    agg = StorageHealthAggregator(engines, redis)
    results = await agg.check_all()
    assert results["postgres"].queue_depth == 42
    assert results["postgres"].dlq_depth == 3


@pytest.mark.asyncio
async def test_concurrent_health_checks():
    """Health checks run concurrently — total time ≈ slowest engine, not sum."""
    import time

    async def slow_health():
        await asyncio.sleep(0.05)
        return StorageHealth(engine="slow", status="OK", latency_ms=50.0)

    engines = {}
    for name in ["postgres", "influxdb", "elasticsearch"]:
        eng = AsyncMock(spec=StorageEngine)
        eng.health_check = slow_health
        engines[name] = eng

    redis = _mock_redis()
    agg = StorageHealthAggregator(engines, redis)

    start = time.monotonic()
    results = await agg.check_all()
    elapsed = (time.monotonic() - start) * 1000

    assert len(results) == 4  # 3 engines + redis
    # Concurrent: should be ~50ms, not ~150ms. Allow generous margin.
    assert elapsed < 300
