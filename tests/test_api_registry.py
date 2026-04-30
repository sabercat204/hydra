"""Tests for registry router — /api/v1/registry."""

from __future__ import annotations

import pytest

from tests.conftest_api import *  # noqa: F401, F403

API = "/api/v1/registry"
HEADERS = {"X-API-Key": "test-api-key-12345"}


@pytest.mark.asyncio
async def test_list_tiers(client, mock_registry):
    resp = await client.get(f"{API}/tiers", headers=HEADERS)
    assert resp.status_code == 200
    body = resp.json()["data"]
    assert len(body["tiers"]) == 3  # mock has 3 tiers


@pytest.mark.asyncio
async def test_list_tiers_total(client, mock_registry):
    resp = await client.get(f"{API}/tiers", headers=HEADERS)
    assert resp.json()["data"]["total"] == 3


@pytest.mark.asyncio
async def test_get_tier_valid(client, mock_registry):
    resp = await client.get(f"{API}/tiers/1", headers=HEADERS)
    assert resp.status_code == 200
    body = resp.json()["data"]
    assert body["id"] == 1
    assert body["name"] == "Geophysical / Seismic"
    assert len(body["sources"]) >= 1


@pytest.mark.asyncio
async def test_get_tier_invalid(client, mock_registry):
    resp = await client.get(f"{API}/tiers/28", headers=HEADERS)
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_tier_zero(client):
    resp = await client.get(f"{API}/tiers/0", headers=HEADERS)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_get_analysis_config(client):
    resp = await client.get(f"{API}/config/analysis", headers=HEADERS)
    assert resp.status_code == 200
    body = resp.json()["data"]
    assert "sitrep_max_events_per_tier" in body
    assert "sitrep_domain_groups" in body
    assert "dossier_network_depth" in body
    assert "timeline_max_events" in body
    assert "default_max_records" in body
