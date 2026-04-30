"""Tests for correlations router — /api/v1/correlations."""

from __future__ import annotations

import uuid

import pytest

from hydra.correlation.models import CorrelationRunResult
from tests.conftest_api import *  # noqa: F401, F403

API = "/api/v1/correlations"
HEADERS = {"X-API-Key": "test-api-key-12345"}


@pytest.mark.asyncio
async def test_run_correlation_returns_202(client, mock_job_manager):
    resp = await client.post(
        f"{API}/run",
        json={"pipeline_id": "geospatial_temporal"},
        headers=HEADERS,
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["data"]["job_id"]
    assert body["data"]["status"] in ("pending", "running")


@pytest.mark.asyncio
async def test_run_correlation_invalid_pipeline(client):
    resp = await client.post(
        f"{API}/run",
        json={"pipeline_id": "nonexistent"},
        headers=HEADERS,
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_get_correlation_job_completed(client, mock_job_manager):
    from hydra.api.schemas.common import JobStatus
    from datetime import datetime, timezone

    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    mock_job_manager._jobs[job_id] = JobStatus(
        job_id=job_id, status="completed", result_id="run-123",
        created_at=now, updated_at=now,
    )
    resp = await client.get(f"{API}/jobs/{job_id}", headers=HEADERS)
    assert resp.status_code == 200
    assert resp.json()["data"]["status"] == "completed"


@pytest.mark.asyncio
async def test_get_correlation_run_detail(client, mock_correlation_engine):
    run_result = CorrelationRunResult(
        pipeline_id="geospatial_temporal",
        candidates_queried=100,
        pairs_evaluated=50,
        correlations_found=10,
        correlations_new=8,
        correlations_updated=2,
        correlations_deduplicated=0,
        persisted_pg=10,
        persisted_neo4j=10,
        duration_ms=500.0,
        time_window_start="2026-01-01T00:00:00Z",
        time_window_end="2026-01-02T00:00:00Z",
    )
    mock_correlation_engine.get_run.return_value = run_result
    resp = await client.get(f"{API}/runs/run-123", headers=HEADERS)
    assert resp.status_code == 200
    body = resp.json()["data"]
    assert body["pipeline_id"] == "geospatial_temporal"
    assert body["correlations_found"] == 10


@pytest.mark.asyncio
async def test_list_correlations_no_filters(client, mock_correlation_engine):
    mock_correlation_engine.list_correlations.return_value = []
    resp = await client.get(API, headers=HEADERS)
    assert resp.status_code == 200
    assert resp.json()["data"] == []


@pytest.mark.asyncio
async def test_list_correlations_by_pipeline(client, mock_correlation_engine, sample_correlation):
    mock_correlation_engine.list_correlations.return_value = [sample_correlation]
    resp = await client.get(f"{API}?pipeline_id=geospatial_temporal", headers=HEADERS)
    assert resp.status_code == 200
    assert len(resp.json()["data"]) == 1


@pytest.mark.asyncio
async def test_list_correlations_by_confidence(client, mock_correlation_engine, sample_correlation):
    mock_correlation_engine.list_correlations.return_value = [sample_correlation]
    resp = await client.get(f"{API}?min_confidence=0.7", headers=HEADERS)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_list_correlations_by_tiers(client, mock_correlation_engine, sample_correlation):
    mock_correlation_engine.list_correlations.return_value = [sample_correlation]
    resp = await client.get(f"{API}?tier_a=1&tier_b=2", headers=HEADERS)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_get_single_correlation(client, mock_correlation_engine, sample_correlation):
    mock_correlation_engine.get_correlation.return_value = sample_correlation
    resp = await client.get(f"{API}/{sample_correlation.correlation_id}", headers=HEADERS)
    assert resp.status_code == 200
    assert resp.json()["data"]["correlation_id"] == sample_correlation.correlation_id


@pytest.mark.asyncio
async def test_get_correlation_not_found(client, mock_correlation_engine):
    mock_correlation_engine.get_correlation.return_value = None
    resp = await client.get(f"{API}/{str(uuid.uuid4())}", headers=HEADERS)
    assert resp.status_code == 404
