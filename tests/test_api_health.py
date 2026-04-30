"""Tests for health router — /api/v1/health."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from hydra.scheduler.backpressure import BackpressureState, EngineBackpressure
from hydra.scheduler.health import SchedulerHealth
from hydra.storage.health import StorageHealth
from tests.conftest_api import *  # noqa: F401, F403

API = "/api/v1/health"
HEADERS = {"X-API-Key": "test-api-key-12345"}


@pytest.mark.asyncio
async def test_health_ok(client, mock_scheduler_health):
    resp = await client.get(API, headers=HEADERS)
    assert resp.status_code == 200
    body = resp.json()["data"]
    assert body["status"] == "OK"
    assert "backpressure" in body


@pytest.mark.asyncio
async def test_health_degraded(client, mock_scheduler_health):
    bp = BackpressureState(
        overall="THROTTLED",
        engines={
            "postgres": EngineBackpressure(
                engine="postgres", queue_depth=2000, soft_limit=1000, hard_limit=5000, state="THROTTLED"
            ),
        },
        checked_at=datetime.now(timezone.utc).isoformat(),
    )
    mock_scheduler_health.check.return_value = SchedulerHealth(
        status="DEGRADED",
        active_adapters=3,
        active_by_cadence={"hourly": 1},
        backpressure=bp,
        storage_health={
            "postgres": StorageHealth(engine="postgres", status="DEGRADED", latency_ms=500.0),
        },
    )
    resp = await client.get(API, headers=HEADERS)
    assert resp.status_code == 200
    assert resp.json()["data"]["status"] == "DEGRADED"


@pytest.mark.asyncio
async def test_backpressure_clear(client, mock_backpressure_monitor):
    resp = await client.get(f"{API}/backpressure", headers=HEADERS)
    assert resp.status_code == 200
    body = resp.json()["data"]
    assert body["overall"] == "CLEAR"


@pytest.mark.asyncio
async def test_backpressure_throttled(client, mock_backpressure_monitor):
    mock_backpressure_monitor.check.return_value = BackpressureState(
        overall="THROTTLED",
        engines={
            "elasticsearch": EngineBackpressure(
                engine="elasticsearch", queue_depth=1500, soft_limit=1000, hard_limit=5000, state="THROTTLED"
            ),
        },
        checked_at=datetime.now(timezone.utc).isoformat(),
    )
    resp = await client.get(f"{API}/backpressure", headers=HEADERS)
    assert resp.status_code == 200
    assert resp.json()["data"]["overall"] == "THROTTLED"


@pytest.mark.asyncio
async def test_dead_streams(client, mock_scheduler_health):
    bp = BackpressureState(
        overall="CLEAR",
        engines={},
        checked_at=datetime.now(timezone.utc).isoformat(),
    )
    mock_scheduler_health.check.return_value = SchedulerHealth(
        status="OK",
        active_adapters=5,
        active_by_cadence={},
        backpressure=bp,
        storage_health={},
        dead_streams=["stream_a", "stream_b"],
    )
    resp = await client.get(f"{API}/streams/dead", headers=HEADERS)
    assert resp.status_code == 200
    assert resp.json()["data"] == ["stream_a", "stream_b"]


@pytest.mark.asyncio
async def test_ping_unauthenticated(client):
    resp = await client.get(f"{API}/ping")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_ping_response(client):
    resp = await client.get(f"{API}/ping")
    assert resp.json() == {"status": "ok"}
