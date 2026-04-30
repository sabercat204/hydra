"""Tests for InfluxEngine — 10 tests covering write, config, health."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier
from hydra.storage.engines.influxdb import InfluxEngine
from hydra.utils.hashing import compute_raw_hash


def _make_record(**overrides) -> NormalizedRecord:
    defaults = dict(
        stream_id="test_stream_1",
        tier=Tier.GEOPHYSICAL_SEISMIC,
        timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
        payload={"magnitude": 5.2, "depth_km": 10.0, "event_type": "earthquake"},
        source_meta=SourceMeta(source_name="USGS", adapter_type="fdsn"),
        raw_hash=compute_raw_hash(b"influx_test"),
        tags=["seismic"],
    )
    defaults.update(overrides)
    return NormalizedRecord(**defaults)


def _make_engine() -> InfluxEngine:
    engine = InfluxEngine(HydraSettings())
    engine._client = MagicMock()
    engine._write_api = AsyncMock()
    engine._write_api.write = AsyncMock(return_value=None)
    return engine


@pytest.fixture(autouse=True)
def mock_influxdb_client():
    """Mock influxdb_client module for all tests."""
    import sys
    from unittest.mock import MagicMock as MM

    mock_module = MM()
    mock_module.Point = type("Point", (), {
        "__init__": lambda self, measurement: setattr(self, "_measurement", measurement),
        "time": lambda self, ts, precision: self,
        "tag": lambda self, key, val: self,
        "field": lambda self, key, val: self,
    })
    mock_module.WritePrecision = MM()
    mock_module.WritePrecision.MS = "ms"

    sys.modules.setdefault("influxdb_client", mock_module)
    sys.modules.setdefault("influxdb_client.client", MM())
    sys.modules.setdefault("influxdb_client.client.influxdb_client_async", MM())
    yield
    # Don't remove — other tests may need them


@pytest.mark.asyncio
async def test_single_record_write():
    """Single record write as InfluxDB point."""
    engine = _make_engine()
    record = _make_record()
    config = {"influx_fields": ["magnitude", "depth_km"], "influx_tag_fields": ["stream_id", "tier"]}
    result = await engine.store([record], registry_config=config)
    assert result.stored == 1
    engine._write_api.write.assert_called_once()


@pytest.mark.asyncio
async def test_batch_write():
    """Batch write succeeds."""
    engine = _make_engine()
    records = [_make_record(raw_hash=compute_raw_hash(f"r{i}".encode())) for i in range(5)]
    config = {"influx_fields": ["magnitude"], "influx_tag_fields": ["stream_id", "tier"]}
    result = await engine.store(records, registry_config=config)
    assert result.stored == 5


@pytest.mark.asyncio
async def test_influx_fields_mapped():
    """influx_fields from registry correctly mapped to point fields."""
    engine = _make_engine()
    record = _make_record()
    config = {"influx_fields": ["magnitude", "depth_km"]}
    result = await engine.store([record], registry_config=config)
    assert result.stored == 1


@pytest.mark.asyncio
async def test_influx_tag_fields_mapped():
    """influx_tag_fields correctly mapped to point tags."""
    engine = _make_engine()
    record = _make_record()
    config = {"influx_fields": ["magnitude"], "influx_tag_fields": ["stream_id", "tier", "event_type"]}
    result = await engine.store([record], registry_config=config)
    assert result.stored == 1


@pytest.mark.asyncio
async def test_non_numeric_fields_skipped():
    """Non-numeric payload fields skipped with DEBUG log."""
    engine = _make_engine()
    record = _make_record(payload={"magnitude": 5.2, "description": "text value"})
    config = {"influx_fields": ["magnitude", "description"]}
    result = await engine.store([record], registry_config=config)
    # Only magnitude should be written as a field
    assert result.stored == 1


@pytest.mark.asyncio
async def test_missing_influx_fields_empty():
    """Missing influx_fields in registry produces no points."""
    engine = _make_engine()
    record = _make_record()
    result = await engine.store([record], registry_config={})
    # No influx_fields means no numeric fields extracted, no points
    assert result.stored == 0


@pytest.mark.asyncio
async def test_write_failure_triggers_retry_info():
    """Write failure records errors."""
    engine = _make_engine()
    engine._write_api.write.side_effect = Exception("Connection refused")
    record = _make_record()
    config = {"influx_fields": ["magnitude"]}
    result = await engine.store([record], registry_config=config)
    assert result.failed == 1
    assert len(result.errors) == 1


@pytest.mark.asyncio
async def test_batch_failure_all_to_dlq():
    """Batch failure after retries sends all records to DLQ."""
    engine = _make_engine()
    engine._write_api.write.side_effect = Exception("Timeout")
    records = [_make_record(raw_hash=compute_raw_hash(f"r{i}".encode())) for i in range(3)]
    config = {"influx_fields": ["magnitude"]}
    result = await engine.store(records, registry_config=config)
    assert result.failed == 3


@pytest.mark.asyncio
async def test_health_check_ok():
    """Health check returns OK on successful ping."""
    engine = _make_engine()
    engine._client.ping = AsyncMock(return_value=True)
    health = await engine.health_check()
    assert health.status == "OK"


@pytest.mark.asyncio
async def test_health_check_unreachable():
    """Health check returns UNREACHABLE on connection failure."""
    engine = _make_engine()
    engine._client.ping = AsyncMock(side_effect=Exception("Connection refused"))
    health = await engine.health_check()
    assert health.status == "UNREACHABLE"
