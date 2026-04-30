"""Tests for StorageRouter — 22 tests covering dedup, classification, and dispatch."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hydra.config import HydraSettings
from hydra.models.normalized import GeoGeometry, NormalizedRecord, SourceMeta, Tier
from hydra.registry.stream_registry import StreamRegistry, StreamTier
from hydra.storage.redis_cache import RedisCache
from hydra.storage.router import RouteResult, StorageRouter
from hydra.utils.hashing import compute_raw_hash


def _make_record(
    tier: Tier = Tier.ECONOMIC_FINANCIAL,
    geo: GeoGeometry | None = None,
    payload: dict | None = None,
    raw_hash: str | None = None,
    stream_id: str = "test_stream_1",
) -> NormalizedRecord:
    raw = (raw_hash or compute_raw_hash(stream_id.encode()))
    return NormalizedRecord(
        stream_id=stream_id,
        tier=tier,
        timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
        geo=geo,
        payload=payload or {"key": "value"},
        source_meta=SourceMeta(source_name="Test", adapter_type="rest_json"),
        raw_hash=raw,
        tags=["test"],
    )


def _make_registry(cadence: str = "daily", storage_config: dict | None = None) -> StreamRegistry:
    tier = StreamTier(
        id=5, name="Economic", streams=1, access="5G",
        formats=["json"], cadence=cadence, adapter="rest_json",
        fallback=None, sources=[], storage=storage_config,
    )
    return StreamRegistry(tiers={5: tier})


def _make_redis() -> AsyncMock:
    redis = AsyncMock(spec=RedisCache)
    redis.is_duplicate_batch.return_value = [False]
    redis.mark_seen_batch.return_value = None
    redis.enqueue.return_value = None
    return redis


@pytest.mark.asyncio
async def test_no_special_traits_routes_to_postgres_only():
    """Record with no special traits routes to PostgreSQL only."""
    redis = _make_redis()
    registry = _make_registry()
    router = StorageRouter(redis, registry, HydraSettings())
    record = _make_record()
    result = await router.route([record])
    assert result.routed == 1
    assert "postgres" in result.engine_counts
    assert result.engine_counts.get("influxdb", 0) == 0


@pytest.mark.asyncio
async def test_geo_record_routes_to_postgres():
    """Record with geo populated routes to PostgreSQL with PostGIS flag."""
    redis = _make_redis()
    registry = _make_registry()
    router = StorageRouter(redis, registry, HydraSettings())
    record = _make_record(geo=GeoGeometry(type="Point", coordinates=[-117.5, 35.8]))
    result = await router.route([record])
    assert result.routed == 1
    assert "postgres" in result.engine_counts


@pytest.mark.asyncio
async def test_sub_minute_tier_routes_to_influxdb():
    """Record from sub_minute tier routes to PostgreSQL + InfluxDB."""
    redis = _make_redis()
    tier = StreamTier(id=1, name="Geo", streams=1, access="5G", formats=["json"],
                      cadence="sub_minute", adapter="fdsn", fallback=None, sources=[])
    registry = StreamRegistry(tiers={1: tier})
    router = StorageRouter(redis, registry, HydraSettings())
    record = _make_record(tier=Tier.GEOPHYSICAL_SEISMIC)
    result = await router.route([record])
    assert "postgres" in result.engine_counts
    assert "influxdb" in result.engine_counts


@pytest.mark.asyncio
async def test_text_heavy_tier_routes_to_elasticsearch():
    """Record from text-heavy tier routes to PostgreSQL + Elasticsearch."""
    redis = _make_redis()
    tier = StreamTier(id=16, name="Cyber", streams=1, access="5G", formats=["json"],
                      cadence="hourly", adapter="stix_taxii", fallback=None, sources=[])
    registry = StreamRegistry(tiers={16: tier})
    router = StorageRouter(redis, registry, HydraSettings())
    record = _make_record(tier=Tier.CYBER_THREAT_INTEL)
    result = await router.route([record])
    assert "elasticsearch" in result.engine_counts


@pytest.mark.asyncio
async def test_large_payload_routes_to_elasticsearch():
    """Record with large payload (> 4096 bytes) routes to Elasticsearch regardless of tier."""
    redis = _make_redis()
    registry = _make_registry()
    router = StorageRouter(redis, registry, HydraSettings())
    large_payload = {"data": "x" * 5000}
    record = _make_record(payload=large_payload)
    result = await router.route([record])
    assert "elasticsearch" in result.engine_counts


@pytest.mark.asyncio
async def test_graph_schema_tier_routes_to_neo4j():
    """Record from graph-schema tier routes to PostgreSQL + Neo4j."""
    redis = _make_redis()
    tier = StreamTier(id=16, name="Cyber", streams=1, access="5G", formats=["json"],
                      cadence="hourly", adapter="stix_taxii", fallback=None, sources=[],
                      storage={"storage_engines": ["postgres", "neo4j"], "graph_schema": {"node_label_field": "type"}})
    registry = StreamRegistry(tiers={16: tier})
    router = StorageRouter(redis, registry, HydraSettings())
    record = _make_record(tier=Tier.CYBER_THREAT_INTEL)
    result = await router.route([record])
    assert "neo4j" in result.engine_counts


@pytest.mark.asyncio
async def test_binary_artifact_routes_to_minio():
    """Record with _binary_artifact routes to PostgreSQL + MinIO."""
    redis = _make_redis()
    tier = StreamTier(id=1, name="Geo", streams=1, access="5G", formats=["json"],
                      cadence="sub_minute", adapter="fdsn", fallback=None, sources=[])
    registry = StreamRegistry(tiers={1: tier})
    router = StorageRouter(redis, registry, HydraSettings())
    record = _make_record(
        tier=Tier.GEOPHYSICAL_SEISMIC,
        payload={"_binary_artifact": {"content": b"data", "content_type": "application/octet-stream", "original_key": "test.bin"}},
    )
    result = await router.route([record])
    assert "minio" in result.engine_counts


@pytest.mark.asyncio
async def test_multiple_traits_routes_to_all_qualifying():
    """Record matching multiple traits routes to all qualifying engines."""
    redis = _make_redis()
    tier = StreamTier(id=16, name="Cyber", streams=1, access="5G", formats=["json"],
                      cadence="hourly", adapter="stix_taxii", fallback=None, sources=[],
                      storage={"storage_engines": ["postgres", "elasticsearch", "neo4j"], "graph_schema": {"node_label_field": "type"}})
    registry = StreamRegistry(tiers={16: tier})
    router = StorageRouter(redis, registry, HydraSettings())
    record = _make_record(tier=Tier.CYBER_THREAT_INTEL)
    result = await router.route([record])
    assert "postgres" in result.engine_counts
    assert "elasticsearch" in result.engine_counts
    assert "neo4j" in result.engine_counts


@pytest.mark.asyncio
async def test_registry_override_overrides_trait_inference():
    """Registry override storage_engines overrides trait inference."""
    redis = _make_redis()
    tier = StreamTier(id=5, name="Economic", streams=1, access="5G", formats=["json"],
                      cadence="daily", adapter="rest_json", fallback=None, sources=[],
                      storage={"storage_engines": ["postgres", "influxdb"]})
    registry = StreamRegistry(tiers={5: tier})
    router = StorageRouter(redis, registry, HydraSettings())
    record = _make_record()
    result = await router.route([record])
    assert "influxdb" in result.engine_counts


@pytest.mark.asyncio
async def test_registry_override_always_includes_postgres():
    """Registry override always includes PostgreSQL even if omitted."""
    redis = _make_redis()
    tier = StreamTier(id=5, name="Economic", streams=1, access="5G", formats=["json"],
                      cadence="daily", adapter="rest_json", fallback=None, sources=[],
                      storage={"storage_engines": ["influxdb"]})
    registry = StreamRegistry(tiers={5: tier})
    router = StorageRouter(redis, registry, HydraSettings())
    record = _make_record()
    result = await router.route([record])
    assert "postgres" in result.engine_counts


@pytest.mark.asyncio
async def test_duplicate_record_dropped():
    """Duplicate record (same raw_hash) is dropped by dedup."""
    redis = _make_redis()
    redis.is_duplicate_batch.return_value = [True]
    registry = _make_registry()
    router = StorageRouter(redis, registry, HydraSettings())
    record = _make_record()
    result = await router.route([record])
    assert result.deduplicated == 1
    assert result.routed == 0


@pytest.mark.asyncio
async def test_batch_mix_duplicates_and_new():
    """Batch with mix of duplicates and new records — correct counts."""
    redis = _make_redis()
    redis.is_duplicate_batch.return_value = [False, True, False]
    registry = _make_registry()
    router = StorageRouter(redis, registry, HydraSettings())
    records = [
        _make_record(raw_hash=compute_raw_hash(b"a")),
        _make_record(raw_hash=compute_raw_hash(b"b")),
        _make_record(raw_hash=compute_raw_hash(b"c")),
    ]
    result = await router.route(records)
    assert result.deduplicated == 1
    assert result.routed == 2


@pytest.mark.asyncio
async def test_dedup_ttl_varies_by_cadence():
    """Dedup TTL varies by tier cadence."""
    redis = _make_redis()
    registry_sub = StreamRegistry(tiers={1: StreamTier(
        id=1, name="Geo", streams=1, access="5G", formats=["json"],
        cadence="sub_minute", adapter="fdsn", fallback=None, sources=[])})
    router = StorageRouter(redis, registry_sub, HydraSettings())
    assert router._get_dedup_ttl(1) == 86_400

    registry_weekly = StreamRegistry(tiers={8: StreamTier(
        id=8, name="Intl", streams=1, access="5G", formats=["json"],
        cadence="weekly", adapter="rest_json", fallback=None, sources=[])})
    router2 = StorageRouter(redis, registry_weekly, HydraSettings())
    assert router2._get_dedup_ttl(8) == 2_592_000


@pytest.mark.asyncio
async def test_redis_dedup_failure_falls_through():
    """Redis dedup failure falls through gracefully."""
    redis = _make_redis()
    redis.is_duplicate_batch.side_effect = Exception("Redis down")
    registry = _make_registry()
    router = StorageRouter(redis, registry, HydraSettings())
    record = _make_record()
    result = await router.route([record])
    assert result.routed == 1


@pytest.mark.asyncio
async def test_empty_batch_returns_zero():
    """Empty record batch returns zero counts."""
    redis = _make_redis()
    registry = _make_registry()
    router = StorageRouter(redis, registry, HydraSettings())
    result = await router.route([])
    assert result.total == 0
    assert result.routed == 0


@pytest.mark.asyncio
async def test_invalid_tier_does_not_crash():
    """Record with tier not in registry does not crash router."""
    redis = _make_redis()
    registry = _make_registry()
    router = StorageRouter(redis, registry, HydraSettings())
    record = _make_record(tier=Tier.NATIONAL_PORTAL_INDEX)
    result = await router.route([record])
    # Should still route to postgres at minimum
    assert result.routed == 1


def test_classify_returns_correct_engines():
    """_classify() returns correct engine set for each tier."""
    redis = AsyncMock(spec=RedisCache)
    tier = StreamTier(id=16, name="Cyber", streams=1, access="5G", formats=["json"],
                      cadence="hourly", adapter="stix_taxii", fallback=None, sources=[])
    registry = StreamRegistry(tiers={16: tier})
    router = StorageRouter(redis, registry, HydraSettings())
    record = _make_record(tier=Tier.CYBER_THREAT_INTEL)
    engines = router._classify(record)
    assert "postgres" in engines
    assert "elasticsearch" in engines


def test_has_binary_artifact():
    """_has_binary_artifact() detects presence/absence correctly."""
    redis = AsyncMock(spec=RedisCache)
    registry = _make_registry()
    router = StorageRouter(redis, registry, HydraSettings())
    record_with = _make_record(payload={"_binary_artifact": {"content": b"x", "content_type": "bin", "original_key": "f"}})
    record_without = _make_record()
    assert router._has_binary_artifact(record_with) is True
    assert router._has_binary_artifact(record_without) is False


def test_is_text_heavy():
    """_is_text_heavy() checks tier and payload size."""
    redis = AsyncMock(spec=RedisCache)
    tier16 = StreamTier(id=16, name="Cyber", streams=1, access="5G", formats=["json"],
                        cadence="hourly", adapter="stix_taxii", fallback=None, sources=[])
    registry = StreamRegistry(tiers={16: tier16})
    router = StorageRouter(redis, registry, HydraSettings())
    record_text_tier = _make_record(tier=Tier.CYBER_THREAT_INTEL)
    assert router._is_text_heavy(record_text_tier) is True
    record_small = _make_record(tier=Tier.ECONOMIC_FINANCIAL, payload={"x": "y"})
    assert router._is_text_heavy(record_small) is False


def test_has_graph_schema():
    """_has_graph_schema() checks registry."""
    redis = AsyncMock(spec=RedisCache)
    tier = StreamTier(id=16, name="Cyber", streams=1, access="5G", formats=["json"],
                      cadence="hourly", adapter="stix_taxii", fallback=None, sources=[],
                      storage={"graph_schema": {"node_label_field": "type"}})
    registry = StreamRegistry(tiers={16: tier})
    router = StorageRouter(redis, registry, HydraSettings())
    assert router._has_graph_schema(16) is True
    assert router._has_graph_schema(5) is False


def test_route_result_aggregates():
    """RouteResult aggregates engine counts correctly."""
    r = RouteResult(total=5, routed=3, deduplicated=2, engine_counts={"postgres": 3, "influxdb": 1})
    assert r.total == 5
    assert r.engine_counts["postgres"] == 3


@pytest.mark.asyncio
async def test_concurrent_route_calls():
    """Concurrent route calls do not corrupt shared state."""
    import asyncio
    redis = _make_redis()
    redis.is_duplicate_batch.return_value = [False]
    registry = _make_registry()
    router = StorageRouter(redis, registry, HydraSettings())

    async def route_one():
        record = _make_record(raw_hash=compute_raw_hash(b"concurrent"))
        return await router.route([record])

    results = await asyncio.gather(*[route_one() for _ in range(10)])
    for r in results:
        assert r.routed == 1
