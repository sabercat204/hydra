"""Tests for QueryLayer — unified analytical read interface."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, PropertyMock

import pytest

from hydra.analysis.exceptions import QueryLayerError
from hydra.analysis.queries import QueryLayer
from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier
from hydra.storage.engines.elasticsearch import ElasticsearchEngine
from hydra.storage.engines.influxdb import InfluxEngine
from hydra.storage.engines.postgres import PostgresEngine
from hydra.utils.hashing import compute_raw_hash


def _settings() -> HydraSettings:
    return HydraSettings()


def _mock_pg_row(
    tier: int = 1,
    raw_hash: str = "a" * 16,
    payload: dict | None = None,
) -> dict:
    """Create a mock asyncpg row."""
    return {
        "stream_id": f"stream_{tier}",
        "tier": tier,
        "timestamp": datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        "geo_json": None,
        "payload": json.dumps(payload or {"test": True}),
        "raw_hash": raw_hash,
        "ingested_at": datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        "confidence": 0.9,
        "tags": ["test"],
    }


def _mock_corr_row(hash_a: str = "a" * 16, hash_b: str = "b" * 16) -> dict:
    return {
        "correlation_id": "corr-001",
        "pipeline_id": "test",
        "record_a_hash": hash_a,
        "record_b_hash": hash_b,
        "tier_a": 1,
        "tier_b": 15,
        "confidence": 0.85,
        "match_dimensions": json.dumps({"spatial": 0.9}),
        "evidence": json.dumps({"test": True}),
        "correlation_hash": "corrhash001",
        "tags": ["test"],
        "created_at": datetime(2026, 1, 15, tzinfo=timezone.utc),
    }


class AsyncContextMock:
    def __init__(self, return_value: object) -> None:
        self._value = return_value

    async def __aenter__(self) -> object:
        return self._value

    async def __aexit__(self, *args: object) -> None:
        pass


def _make_query_layer(
    pg_rows: list[dict] | None = None,
    es_available: bool = False,
    influx_available: bool = False,
) -> QueryLayer:
    pg = MagicMock(spec=PostgresEngine)
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=pg_rows or [])
    pg._pool = MagicMock()
    pg._pool.acquire = MagicMock(return_value=AsyncContextMock(conn))

    es = None
    if es_available:
        es = MagicMock(spec=ElasticsearchEngine)
        es._client = AsyncMock()
        es._client.search = AsyncMock(return_value={"hits": {"hits": []}})

    influx = None
    if influx_available:
        influx = MagicMock(spec=InfluxEngine)
        influx._client = AsyncMock()

    return QueryLayer(pg, es, influx, _settings())


@pytest.mark.asyncio
async def test_query_records_by_tier_and_time() -> None:
    row = _mock_pg_row(tier=1)
    ql = _make_query_layer(pg_rows=[row])

    result = await ql.query_records(
        tiers=[1],
        time_start="2026-01-01T00:00:00Z",
        time_end="2026-01-31T00:00:00Z",
    )

    assert 1 in result
    assert len(result[1]) == 1
    assert result[1][0].tier == Tier.GEOPHYSICAL_SEISMIC


@pytest.mark.asyncio
async def test_query_records_with_region() -> None:
    row = _mock_pg_row(tier=1, payload={"country_code": "US"})
    ql = _make_query_layer(pg_rows=[row])

    result = await ql.query_records(
        tiers=[1],
        time_start="2026-01-01T00:00:00Z",
        time_end="2026-01-31T00:00:00Z",
        region="US",
    )

    assert 1 in result


@pytest.mark.asyncio
async def test_query_records_with_keywords_es() -> None:
    """ES used for keyword discovery when available."""
    ql = _make_query_layer(es_available=True)

    result = await ql.query_records(
        tiers=[1],
        time_start="2026-01-01T00:00:00Z",
        time_end="2026-01-31T00:00:00Z",
        keywords=["earthquake"],
    )

    # ES was called (even if empty results)
    ql._es._client.search.assert_called_once()


@pytest.mark.asyncio
async def test_query_records_with_keywords_no_es() -> None:
    """PG JSONB fallback when ES unavailable."""
    row = _mock_pg_row(tier=1, payload={"type": "earthquake"})
    ql = _make_query_layer(pg_rows=[row], es_available=False)

    result = await ql.query_records(
        tiers=[1],
        time_start="2026-01-01T00:00:00Z",
        time_end="2026-01-31T00:00:00Z",
        keywords=["earthquake"],
    )

    # Should fall back to PG
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_query_correlations() -> None:
    row = _mock_corr_row()
    pg = MagicMock(spec=PostgresEngine)
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[row])
    pg._pool = MagicMock()
    pg._pool.acquire = MagicMock(return_value=AsyncContextMock(conn))

    ql = QueryLayer(pg, None, None, _settings())
    result = await ql.query_correlations({"a" * 16, "b" * 16})

    assert len(result) == 1
    assert result[0].confidence == 0.85


@pytest.mark.asyncio
async def test_query_entity_by_id() -> None:
    row = _mock_pg_row(tier=19, payload={"ofac_id": "OFAC-123", "entity_name": "Test"})
    pg = MagicMock(spec=PostgresEngine)
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[row])
    pg._pool = MagicMock()
    pg._pool.acquire = MagicMock(return_value=AsyncContextMock(conn))

    ql = QueryLayer(pg, None, None, _settings())
    result = await ql.query_entity_by_id("OFAC-123", tiers=[19])

    assert len(result) >= 1


@pytest.mark.asyncio
async def test_query_entity_by_name_es() -> None:
    """ES more_like_this for name search."""
    ql = _make_query_layer(es_available=True)
    result = await ql.query_entity_by_name("Test Entity", tiers=[19])

    ql._es._client.search.assert_called_once()


@pytest.mark.asyncio
async def test_query_timeseries_influx() -> None:
    """InfluxDB query with aggregation."""
    ql = _make_query_layer(influx_available=True)
    query_api = AsyncMock()
    query_api.query = AsyncMock(return_value=[])
    ql._influx._client.query_api = MagicMock(return_value=query_api)

    result = await ql.query_timeseries(
        stream_ids=["stream_1"],
        time_start="2026-01-01T00:00:00Z",
        time_end="2026-01-31T00:00:00Z",
        aggregation="1h",
    )

    query_api.query.assert_called_once()
    assert "stream_1" in result


@pytest.mark.asyncio
async def test_query_timeseries_pg_fallback() -> None:
    """PG fallback when InfluxDB unavailable."""
    row = {
        "timestamp": datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        "payload": json.dumps({"value": 42}),
        "raw_hash": "a" * 16,
    }
    pg = MagicMock(spec=PostgresEngine)
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[row])
    pg._pool = MagicMock()
    pg._pool.acquire = MagicMock(return_value=AsyncContextMock(conn))

    ql = QueryLayer(pg, None, None, _settings())
    result = await ql.query_timeseries(
        stream_ids=["stream_1"],
        time_start="2026-01-01T00:00:00Z",
        time_end="2026-01-31T00:00:00Z",
    )

    assert "stream_1" in result
    assert len(result["stream_1"]) == 1


@pytest.mark.asyncio
async def test_search_text() -> None:
    """ES full-text search with tier/time filters."""
    ql = _make_query_layer(es_available=True)
    result = await ql.search_text("earthquake", tiers=[1])

    ql._es._client.search.assert_called_once()
