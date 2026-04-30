"""Tests for PostgresEngine — 14 tests covering insert, dedup, health."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest

from hydra.config import HydraSettings
from hydra.models.normalized import GeoGeometry, NormalizedRecord, SourceMeta, Tier
from hydra.storage.engines.postgres import PostgresEngine
from hydra.utils.hashing import compute_raw_hash


def _make_record(**overrides) -> NormalizedRecord:
    defaults = dict(
        stream_id="test_stream_1",
        tier=Tier.GEOPHYSICAL_SEISMIC,
        timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
        geo=None,
        payload={"magnitude": 5.2, "depth_km": 10.0},
        source_meta=SourceMeta(source_name="USGS", adapter_type="rest_json"),
        raw_hash=compute_raw_hash(b"test_data"),
        confidence=0.95,
        tags=["earthquake"],
    )
    defaults.update(overrides)
    return NormalizedRecord(**defaults)


def _mock_pool():
    pool = AsyncMock()
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=None)
    # Make pool.acquire() return an async context manager that yields conn
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire = MagicMock(return_value=ctx)
    return pool, conn


@pytest.mark.asyncio
async def test_single_record_insert():
    """Single record insert succeeds."""
    engine = PostgresEngine(HydraSettings())
    pool, conn = _mock_pool()
    engine._pool = pool
    record = _make_record()
    result = await engine.store([record])
    assert result.stored == 1
    assert result.failed == 0
    conn.execute.assert_called_once()


@pytest.mark.asyncio
async def test_batch_insert():
    """Batch insert succeeds."""
    engine = PostgresEngine(HydraSettings())
    pool, conn = _mock_pool()
    engine._pool = pool
    records = [_make_record(raw_hash=compute_raw_hash(f"r{i}".encode())) for i in range(5)]
    result = await engine.store(records)
    assert result.stored == 5
    assert conn.execute.call_count == 5


@pytest.mark.asyncio
async def test_geo_record_converted():
    """Record with GeoJSON geo field is converted to PostGIS geometry."""
    engine = PostgresEngine(HydraSettings())
    pool, conn = _mock_pool()
    engine._pool = pool
    record = _make_record(geo=GeoGeometry(type="Point", coordinates=[-117.5, 35.8, 10.0]))
    result = await engine.store([record])
    assert result.stored == 1
    # Verify the geo argument was passed as JSON string
    call_args = conn.execute.call_args
    geo_arg = call_args[0][4]  # 4th positional arg is geo
    assert geo_arg is not None
    parsed = json.loads(geo_arg)
    assert parsed["type"] == "Point"


@pytest.mark.asyncio
async def test_null_geo_inserts_null():
    """Record with null geo inserts with NULL geometry."""
    engine = PostgresEngine(HydraSettings())
    pool, conn = _mock_pool()
    engine._pool = pool
    record = _make_record(geo=None)
    result = await engine.store([record])
    assert result.stored == 1
    call_args = conn.execute.call_args
    geo_arg = call_args[0][4]
    assert geo_arg is None


@pytest.mark.asyncio
async def test_unique_violation_counted_as_dedup():
    """Duplicate raw_hash triggers UniqueViolationError → counted as deduplicated."""
    engine = PostgresEngine(HydraSettings())
    pool, conn = _mock_pool()
    engine._pool = pool
    conn.execute.side_effect = Exception("duplicate key value violates unique constraint on raw_hash")
    record = _make_record()
    result = await engine.store([record])
    assert result.deduplicated == 1
    assert result.failed == 0


@pytest.mark.asyncio
async def test_storage_status_pending():
    """storage_status set to pending on insert."""
    engine = PostgresEngine(HydraSettings())
    record = _make_record()
    row = engine._serialize_record(record)
    assert row["storage_status"] == "pending"


@pytest.mark.asyncio
async def test_storage_engines_populated():
    """storage_engines array populated correctly."""
    engine = PostgresEngine(HydraSettings())
    record = _make_record()
    row = engine._serialize_record(record, storage_engines=["postgres", "influxdb"])
    assert row["storage_engines"] == ["postgres", "influxdb"]


@pytest.mark.asyncio
async def test_source_meta_extracted():
    """source_meta fields extracted to flat columns."""
    engine = PostgresEngine(HydraSettings())
    record = _make_record()
    row = engine._serialize_record(record)
    assert row["source_name"] == "USGS"
    assert row["adapter_type"] == "rest_json"


@pytest.mark.asyncio
async def test_payload_stored_as_json():
    """payload stored as JSONB."""
    engine = PostgresEngine(HydraSettings())
    record = _make_record()
    row = engine._serialize_record(record)
    parsed = json.loads(row["payload"])
    assert parsed["magnitude"] == 5.2


@pytest.mark.asyncio
async def test_tags_stored_as_array():
    """tags stored as TEXT array."""
    engine = PostgresEngine(HydraSettings())
    record = _make_record()
    row = engine._serialize_record(record)
    assert row["tags"] == ["earthquake"]


@pytest.mark.asyncio
async def test_confidence_in_range():
    """confidence constraint enforced (0.0–1.0)."""
    engine = PostgresEngine(HydraSettings())
    record = _make_record(confidence=0.5)
    row = engine._serialize_record(record)
    assert 0.0 <= row["confidence"] <= 1.0


@pytest.mark.asyncio
async def test_tier_in_range():
    """tier constraint enforced (1–28)."""
    engine = PostgresEngine(HydraSettings())
    record = _make_record(tier=Tier.GEOPHYSICAL_SEISMIC)
    row = engine._serialize_record(record)
    assert 1 <= row["tier"] <= 28


@pytest.mark.asyncio
async def test_health_check_ok():
    """Health check returns OK when PG and PostGIS respond."""
    engine = PostgresEngine(HydraSettings())
    pool = AsyncMock()
    conn = AsyncMock()
    conn.fetchval = AsyncMock(return_value=1)
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire = MagicMock(return_value=ctx)
    engine._pool = pool
    health = await engine.health_check()
    assert health.status == "OK"
    assert health.details["postgis"] is True


@pytest.mark.asyncio
async def test_health_check_degraded_no_postgis():
    """Health check returns DEGRADED when PostGIS unavailable."""
    engine = PostgresEngine(HydraSettings())
    pool = AsyncMock()
    conn = AsyncMock()
    call_count = 0

    async def mock_fetchval(query):
        nonlocal call_count
        call_count += 1
        if "PostGIS" in query:
            raise Exception("PostGIS not available")
        return 1

    conn.fetchval = mock_fetchval
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire = MagicMock(return_value=ctx)
    engine._pool = pool
    health = await engine.health_check()
    assert health.status == "DEGRADED"
    assert health.details["postgis"] is False
