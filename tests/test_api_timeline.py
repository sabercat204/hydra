"""Tests for timeline router — /api/v1/timeline."""

from __future__ import annotations

import pytest

from hydra.analysis.models import EventCluster, TimelineEvent, TimelineResult
from tests.conftest_api import *  # noqa: F401, F403

API = "/api/v1/timeline"
HEADERS = {"X-API-Key": "test-api-key-12345"}


@pytest.mark.asyncio
async def test_build_timeline_basic(client, mock_timeline_builder):
    mock_timeline_builder.build.return_value = TimelineResult(
        events=[
            TimelineEvent(
                timestamp="2026-01-01T12:00:00Z", record_hash="a" * 16,
                tier=1, stream_id="s1", title="Event 1", description="Desc",
                significance=0.8,
            ),
        ],
        time_window_start="2026-01-01T00:00:00Z",
        time_window_end="2026-01-02T00:00:00Z",
        total_events=1,
        tiers_represented=[1],
    )
    resp = await client.post(
        API,
        json={"time_start": "2026-01-01T00:00:00Z", "time_end": "2026-01-02T00:00:00Z"},
        headers=HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()["data"]
    assert body["total_events"] == 1
    assert len(body["events"]) == 1


@pytest.mark.asyncio
async def test_build_timeline_with_filters(client, mock_timeline_builder):
    mock_timeline_builder.build.return_value = TimelineResult(
        time_window_start="2026-01-01T00:00:00Z",
        time_window_end="2026-01-02T00:00:00Z",
    )
    resp = await client.post(
        API,
        json={
            "time_start": "2026-01-01T00:00:00Z",
            "time_end": "2026-01-02T00:00:00Z",
            "tiers": [1, 2],
            "region": "US",
            "keywords": ["earthquake"],
        },
        headers=HEADERS,
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_build_timeline_invalid_window(client):
    resp = await client.post(
        API,
        json={"time_start": "2026-02-01T00:00:00Z", "time_end": "2026-01-01T00:00:00Z"},
        headers=HEADERS,
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_build_timeline_max_window(client):
    resp = await client.post(
        API,
        json={"time_start": "2026-01-01T00:00:00Z", "time_end": "2026-07-01T00:00:00Z"},
        headers=HEADERS,
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_build_timeline_clusters(client, mock_timeline_builder):
    mock_timeline_builder.build.return_value = TimelineResult(
        events=[],
        time_window_start="2026-01-01T00:00:00Z",
        time_window_end="2026-01-02T00:00:00Z",
        clusters=[
            EventCluster(
                cluster_id="c1", events=["a" * 16], centroid_time="2026-01-01T12:00:00Z",
                tier_count=2, significance=0.7,
            ),
        ],
    )
    resp = await client.post(
        API,
        json={"time_start": "2026-01-01T00:00:00Z", "time_end": "2026-01-02T00:00:00Z"},
        headers=HEADERS,
    )
    assert resp.status_code == 200
    clusters = resp.json()["data"]["clusters"]
    assert len(clusters) == 1
    assert clusters[0]["significance"] == 0.7


@pytest.mark.asyncio
async def test_build_timeline_min_significance(client, mock_timeline_builder):
    mock_timeline_builder.build.return_value = TimelineResult(
        time_window_start="2026-01-01T00:00:00Z",
        time_window_end="2026-01-02T00:00:00Z",
    )
    resp = await client.post(
        API,
        json={
            "time_start": "2026-01-01T00:00:00Z",
            "time_end": "2026-01-02T00:00:00Z",
            "min_significance": 0.5,
        },
        headers=HEADERS,
    )
    assert resp.status_code == 200
