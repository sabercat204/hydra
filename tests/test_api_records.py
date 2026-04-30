"""Tests for records router — /api/v1/records."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from hydra.models.normalized import GeoGeometry, NormalizedRecord, SourceMeta, Tier
from tests.conftest_api import *  # noqa: F401, F403

API = "/api/v1/records"
HEADERS = {"X-API-Key": "test-api-key-12345"}


def _make_record(tier: int = 1, confidence: float = 0.9) -> NormalizedRecord:
    return NormalizedRecord(
        stream_id="test_stream",
        tier=Tier(tier),
        timestamp=datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        geo=GeoGeometry(type="Point", coordinates=[-117.5, 35.8]),
        payload={"value": 42},
        source_meta=SourceMeta(
            source_name="Test", adapter_type="rest_json",
        ),
        raw_hash="a" * 16,
        confidence=confidence,
        tags=["test"],
    )


@pytest.mark.asyncio
async def test_query_records_no_filters(client, mock_query_layer):
    mock_query_layer.query_records.return_value = [_make_record()]
    resp = await client.get(API, headers=HEADERS)
    assert resp.status_code == 200
    body = resp.json()["data"]
    assert body["total"] == 1


@pytest.mark.asyncio
async def test_query_records_by_tier(client, mock_query_layer):
    mock_query_layer.query_records.return_value = [_make_record(tier=1)]
    resp = await client.get(f"{API}?tiers=1&tiers=5", headers=HEADERS)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_query_records_time_window(client, mock_query_layer):
    mock_query_layer.query_records.return_value = []
    resp = await client.get(
        f"{API}?time_start=2026-01-01T00:00:00Z&time_end=2026-01-02T00:00:00Z",
        headers=HEADERS,
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_query_records_by_region(client, mock_query_layer):
    mock_query_layer.query_records.return_value = []
    resp = await client.get(f"{API}?region=UA", headers=HEADERS)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_query_records_min_confidence(client, mock_query_layer):
    mock_query_layer.query_records.return_value = [_make_record(confidence=0.9)]
    resp = await client.get(f"{API}?min_confidence=0.8", headers=HEADERS)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_query_records_pagination(client, mock_query_layer):
    mock_query_layer.query_records.return_value = [_make_record()]
    resp = await client.get(f"{API}?limit=1", headers=HEADERS)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_query_timeseries_valid(client, mock_query_layer):
    mock_query_layer.query_timeseries.return_value = {"stream1": [{"time": "2026-01-01", "value": 1}]}
    resp = await client.get(
        f"{API}/timeseries?stream_ids=stream1&time_start=2026-01-01T00:00:00Z&time_end=2026-01-02T00:00:00Z",
        headers=HEADERS,
    )
    assert resp.status_code == 200
    assert "stream1" in resp.json()["data"]


@pytest.mark.asyncio
async def test_query_timeseries_invalid_time_window(client):
    resp = await client.get(
        f"{API}/timeseries?stream_ids=s1&time_start=2026-02-01T00:00:00Z&time_end=2026-01-01T00:00:00Z",
        headers=HEADERS,
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_search_text_basic(client, mock_query_layer):
    mock_query_layer.search_text.return_value = [_make_record()]
    resp = await client.get(f"{API}/search?query=earthquake", headers=HEADERS)
    assert resp.status_code == 200
    assert len(resp.json()["data"]) == 1


@pytest.mark.asyncio
async def test_search_text_with_tier_filter(client, mock_query_layer):
    mock_query_layer.search_text.return_value = [_make_record()]
    resp = await client.get(f"{API}/search?query=earthquake&tiers=1", headers=HEADERS)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_text_empty_query(client):
    resp = await client.get(f"{API}/search?query=", headers=HEADERS)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_search_text_pagination(client, mock_query_layer):
    mock_query_layer.search_text.return_value = [_make_record()]
    resp = await client.get(f"{API}/search?query=test&limit=1", headers=HEADERS)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_query_timeseries_invalid_stream(client, mock_query_layer):
    mock_query_layer.query_timeseries.side_effect = Exception("Stream not found")
    resp = await client.get(
        f"{API}/timeseries?stream_ids=nonexistent&time_start=2026-01-01T00:00:00Z&time_end=2026-01-02T00:00:00Z",
        headers=HEADERS,
    )
    assert resp.status_code == 500


@pytest.mark.asyncio
async def test_query_timeseries_max_window(client, mock_query_layer):
    # 31 days for raw should still work (validation is in query_layer)
    mock_query_layer.query_timeseries.return_value = {}
    resp = await client.get(
        f"{API}/timeseries?stream_ids=s1&time_start=2026-01-01T00:00:00Z&time_end=2026-02-01T00:00:00Z",
        headers=HEADERS,
    )
    assert resp.status_code == 200
