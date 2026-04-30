"""Tests for products router — /api/v1/products."""

from __future__ import annotations

import uuid

import pytest

from tests.conftest_api import *  # noqa: F401, F403

API = "/api/v1/products"
HEADERS = {"X-API-Key": "test-api-key-12345"}


@pytest.mark.asyncio
async def test_generate_product_returns_202_with_job_id(client, mock_job_manager):
    resp = await client.post(
        f"{API}/generate",
        json={"product_type": "situation_report"},
        headers=HEADERS,
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["data"]["status"] in ("pending", "running")
    assert body["data"]["job_id"]


@pytest.mark.asyncio
async def test_generate_dossier_requires_entity(client):
    resp = await client.post(
        f"{API}/generate",
        json={"product_type": "entity_dossier"},
        headers=HEADERS,
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["errors"][0]["code"] == "ENTITY_REQUIRED"


@pytest.mark.asyncio
async def test_generate_invalid_time_window(client):
    resp = await client.post(
        f"{API}/generate",
        json={
            "product_type": "situation_report",
            "time_window_start": "2026-02-01T00:00:00Z",
            "time_window_end": "2026-01-01T00:00:00Z",
        },
        headers=HEADERS,
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["errors"][0]["code"] == "INVALID_TIME_WINDOW"


@pytest.mark.asyncio
async def test_get_job_status_pending(client, mock_job_manager):
    # Create a job first
    resp = await client.post(
        f"{API}/generate",
        json={"product_type": "situation_report"},
        headers=HEADERS,
    )
    job_id = resp.json()["data"]["job_id"]

    resp = await client.get(f"{API}/jobs/{job_id}", headers=HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["data"]["job_id"] == job_id
    assert body["data"]["status"] in ("pending", "running")


@pytest.mark.asyncio
async def test_get_job_status_completed(client, mock_job_manager):
    from hydra.api.schemas.common import JobStatus
    from datetime import datetime, timezone

    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    mock_job_manager._jobs[job_id] = JobStatus(
        job_id=job_id, status="completed", result_id="prod-123",
        created_at=now, updated_at=now,
    )
    resp = await client.get(f"{API}/jobs/{job_id}", headers=HEADERS)
    assert resp.status_code == 200
    assert resp.json()["data"]["status"] == "completed"
    assert resp.json()["data"]["result_id"] == "prod-123"


@pytest.mark.asyncio
async def test_get_job_status_failed(client, mock_job_manager):
    from hydra.api.schemas.common import JobStatus
    from datetime import datetime, timezone

    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    mock_job_manager._jobs[job_id] = JobStatus(
        job_id=job_id, status="failed", error="Something went wrong",
        created_at=now, updated_at=now,
    )
    resp = await client.get(f"{API}/jobs/{job_id}", headers=HEADERS)
    assert resp.status_code == 200
    assert resp.json()["data"]["status"] == "failed"
    assert resp.json()["data"]["error"] == "Something went wrong"


@pytest.mark.asyncio
async def test_get_job_status_expired(client, mock_job_manager):
    resp = await client.get(f"{API}/jobs/{str(uuid.uuid4())}", headers=HEADERS)
    assert resp.status_code == 404
    assert resp.json()["errors"][0]["code"] == "JOB_NOT_FOUND"


@pytest.mark.asyncio
async def test_list_products_no_filters(client, mock_analysis_engine):
    mock_analysis_engine.list_products.return_value = []
    resp = await client.get(API, headers=HEADERS)
    assert resp.status_code == 200
    assert resp.json()["data"]["products"] == []


@pytest.mark.asyncio
async def test_list_products_by_type(client, mock_analysis_engine, sample_product):
    mock_analysis_engine.list_products.return_value = [sample_product]
    resp = await client.get(f"{API}?product_type=situation_report", headers=HEADERS)
    assert resp.status_code == 200
    products = resp.json()["data"]["products"]
    assert len(products) == 1
    assert products[0]["product_type"] == "situation_report"


@pytest.mark.asyncio
async def test_list_products_since(client, mock_analysis_engine, sample_product):
    mock_analysis_engine.list_products.return_value = [sample_product]
    resp = await client.get(f"{API}?since=2026-04-01T00:00:00Z", headers=HEADERS)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_list_products_pagination(client, mock_analysis_engine, sample_product):
    mock_analysis_engine.list_products.return_value = [sample_product]
    resp = await client.get(f"{API}?limit=1", headers=HEADERS)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_get_product_found(client, mock_analysis_engine, sample_product):
    mock_analysis_engine.get_product.return_value = sample_product
    resp = await client.get(f"{API}/{sample_product.product_id}", headers=HEADERS)
    assert resp.status_code == 200
    body = resp.json()["data"]
    assert body["product_id"] == sample_product.product_id
    assert len(body["sections"]) == 1


@pytest.mark.asyncio
async def test_get_product_not_found(client, mock_analysis_engine):
    mock_analysis_engine.get_product.return_value = None
    resp = await client.get(f"{API}/{str(uuid.uuid4())}", headers=HEADERS)
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_generate_product_unauthenticated(client):
    resp = await client.post(
        f"{API}/generate",
        json={"product_type": "situation_report"},
    )
    assert resp.status_code == 422  # Missing required header


@pytest.mark.asyncio
async def test_generate_product_invalid_tiers(client):
    resp = await client.post(
        f"{API}/generate",
        json={"product_type": "situation_report", "tiers": [99]},
        headers=HEADERS,
    )
    assert resp.status_code == 422
