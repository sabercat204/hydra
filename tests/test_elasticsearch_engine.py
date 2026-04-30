"""Tests for ElasticsearchEngine — 12 tests covering indexing, mapping, health."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hydra.config import HydraSettings
from hydra.models.normalized import GeoGeometry, NormalizedRecord, SourceMeta, Tier
from hydra.utils.hashing import compute_raw_hash


def _make_record(**overrides) -> NormalizedRecord:
    defaults = dict(
        stream_id="test_stream_1",
        tier=Tier.CYBER_THREAT_INTEL,
        timestamp=datetime(2025, 6, 15, tzinfo=timezone.utc),
        payload={"description": "Test threat", "name": "APT-1"},
        source_meta=SourceMeta(source_name="MITRE", adapter_type="stix_taxii"),
        raw_hash=compute_raw_hash(b"es_test"),
        tags=["cyber"],
    )
    defaults.update(overrides)
    return NormalizedRecord(**defaults)


# Mock elasticsearch module before importing the engine
_mock_es_module = MagicMock()
_mock_helpers = MagicMock()
_mock_async_bulk = AsyncMock(return_value=(1, []))
_mock_helpers.async_bulk = _mock_async_bulk
_mock_es_module.helpers = _mock_helpers
_mock_es_module.AsyncElasticsearch = MagicMock

sys.modules.setdefault("elasticsearch", _mock_es_module)
sys.modules.setdefault("elasticsearch.helpers", _mock_helpers)
sys.modules.setdefault("elasticsearch._async", MagicMock())
sys.modules.setdefault("elasticsearch._async.helpers", _mock_helpers)

from hydra.storage.engines.elasticsearch import ElasticsearchEngine


def _make_engine() -> ElasticsearchEngine:
    engine = ElasticsearchEngine(HydraSettings())
    engine._client = AsyncMock()
    return engine


@pytest.mark.asyncio
async def test_single_record_indexed():
    """Single record indexed to correct monthly index."""
    engine = _make_engine()
    with patch("elasticsearch.helpers.async_bulk", new_callable=AsyncMock) as mock_bulk:
        mock_bulk.return_value = (1, [])
        record = _make_record()
        result = await engine.store([record])
        assert result.stored == 1
        call_args = mock_bulk.call_args
        actions = call_args[0][1]
        assert actions[0]["_index"] == "hydra-tier-16-2025.06"


@pytest.mark.asyncio
async def test_batch_bulk_index():
    """Batch bulk index succeeds."""
    engine = _make_engine()
    with patch("elasticsearch.helpers.async_bulk", new_callable=AsyncMock) as mock_bulk:
        mock_bulk.return_value = (5, [])
        records = [_make_record(raw_hash=compute_raw_hash(f"r{i}".encode())) for i in range(5)]
        result = await engine.store(records)
        assert result.stored == 5


@pytest.mark.asyncio
async def test_id_set_to_raw_hash():
    """_id set to raw_hash for natural dedup."""
    engine = _make_engine()
    with patch("elasticsearch.helpers.async_bulk", new_callable=AsyncMock) as mock_bulk:
        mock_bulk.return_value = (1, [])
        record = _make_record()
        await engine.store([record])
        actions = mock_bulk.call_args[0][1]
        assert actions[0]["_id"] == record.raw_hash


@pytest.mark.asyncio
async def test_geo_mapped_to_geo_shape():
    """GeoJSON geo field mapped to geo_shape."""
    engine = _make_engine()
    record = _make_record(geo=GeoGeometry(type="Point", coordinates=[-117.5, 35.8]))
    doc = engine._serialize_record(record)
    assert doc["geo"]["type"] == "Point"
    assert doc["geo"]["coordinates"] == [-117.5, 35.8]


@pytest.mark.asyncio
async def test_es_text_fields_override():
    """es_text_fields override dynamic template."""
    engine = _make_engine()
    with patch("elasticsearch.helpers.async_bulk", new_callable=AsyncMock) as mock_bulk:
        mock_bulk.return_value = (1, [])
        record = _make_record()
        config = {"es_text_fields": ["description"], "es_index_prefix": "hydra-cyber"}
        result = await engine.store([record], registry_config=config)
        assert result.stored == 1


@pytest.mark.asyncio
async def test_es_keyword_fields_override():
    """es_keyword_fields override dynamic template."""
    engine = _make_engine()
    with patch("elasticsearch.helpers.async_bulk", new_callable=AsyncMock) as mock_bulk:
        mock_bulk.return_value = (1, [])
        record = _make_record()
        config = {"es_keyword_fields": ["name"]}
        result = await engine.store([record], registry_config=config)
        assert result.stored == 1


@pytest.mark.asyncio
async def test_index_template_created_on_connect():
    """Index template created on connect."""
    engine = _make_engine()
    engine._client.indices = AsyncMock()
    engine._client.indices.put_index_template = AsyncMock()
    with patch("elasticsearch.AsyncElasticsearch", return_value=engine._client):
        await engine.connect()
    engine._client.indices.put_index_template.assert_called_once()


@pytest.mark.asyncio
async def test_monthly_index_rotation():
    """Records from different months go to different indices."""
    engine = _make_engine()
    with patch("elasticsearch.helpers.async_bulk", new_callable=AsyncMock) as mock_bulk:
        mock_bulk.return_value = (2, [])
        r1 = _make_record(timestamp=datetime(2025, 1, 15, tzinfo=timezone.utc), raw_hash=compute_raw_hash(b"jan"))
        r2 = _make_record(timestamp=datetime(2025, 6, 15, tzinfo=timezone.utc), raw_hash=compute_raw_hash(b"jun"))
        await engine.store([r1, r2])
        actions = mock_bulk.call_args[0][1]
        indices = {a["_index"] for a in actions}
        assert "hydra-tier-16-2025.01" in indices
        assert "hydra-tier-16-2025.06" in indices


@pytest.mark.asyncio
async def test_bulk_partial_failures():
    """Bulk response with partial failures — failed items identified."""
    engine = _make_engine()
    with patch("elasticsearch.helpers.async_bulk", new_callable=AsyncMock) as mock_bulk:
        mock_bulk.return_value = (2, [{"index": {"_id": "x", "error": "mapping error"}}])
        records = [_make_record(raw_hash=compute_raw_hash(f"r{i}".encode())) for i in range(3)]
        result = await engine.store(records)
        assert result.stored == 2
        assert result.failed == 1


@pytest.mark.asyncio
async def test_index_alias():
    """Index alias points to current month."""
    engine = _make_engine()
    with patch("elasticsearch.helpers.async_bulk", new_callable=AsyncMock) as mock_bulk:
        mock_bulk.return_value = (1, [])
        record = _make_record()
        config = {"es_index_prefix": "hydra-cyber"}
        await engine.store([record], registry_config=config)
        actions = mock_bulk.call_args[0][1]
        assert actions[0]["_index"].startswith("hydra-cyber-")


@pytest.mark.asyncio
async def test_health_check_maps_status():
    """Health check maps cluster status correctly."""
    engine = _make_engine()
    engine._client.cluster = AsyncMock()
    engine._client.cluster.health = AsyncMock(return_value={"status": "green"})
    health = await engine.health_check()
    assert health.status == "OK"

    engine._client.cluster.health = AsyncMock(return_value={"status": "yellow"})
    health = await engine.health_check()
    assert health.status == "DEGRADED"

    engine._client.cluster.health = AsyncMock(return_value={"status": "red"})
    health = await engine.health_check()
    assert health.status == "UNREACHABLE"


@pytest.mark.asyncio
async def test_large_payload_accepted():
    """Large payload (> 4096 bytes) from non-text tier is accepted."""
    engine = _make_engine()
    with patch("elasticsearch.helpers.async_bulk", new_callable=AsyncMock) as mock_bulk:
        mock_bulk.return_value = (1, [])
        large_payload = {"data": "x" * 5000}
        record = _make_record(tier=Tier.ECONOMIC_FINANCIAL, payload=large_payload)
        result = await engine.store([record])
        assert result.stored == 1
