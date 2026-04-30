"""Tests for watchlists router — /api/v1/watchlists."""

from __future__ import annotations

import pytest

from tests.conftest_api import *  # noqa: F401, F403

API = "/api/v1/watchlists"
HEADERS = {"X-API-Key": "test-api-key-12345"}


@pytest.mark.asyncio
async def test_list_entity_watchlist_empty(client):
    resp = await client.get(f"{API}/entities", headers=HEADERS)
    assert resp.status_code == 200
    assert resp.json()["data"] == []


@pytest.mark.asyncio
async def test_add_entity_watchlist(client):
    resp = await client.post(
        f"{API}/entities",
        json={"entity_id": "e1", "name": "Test Entity", "entity_type": "person"},
        headers=HEADERS,
    )
    assert resp.status_code == 201
    body = resp.json()["data"]
    assert body["entity_id"] == "e1"
    assert body["name"] == "Test Entity"
    assert body["added_at"] is not None


@pytest.mark.asyncio
async def test_add_entity_watchlist_duplicate(client):
    await client.post(
        f"{API}/entities",
        json={"entity_id": "dup1", "name": "Entity"},
        headers=HEADERS,
    )
    resp = await client.post(
        f"{API}/entities",
        json={"entity_id": "dup1", "name": "Entity"},
        headers=HEADERS,
    )
    assert resp.status_code == 409
    assert resp.json()["errors"][0]["code"] == "WATCHLIST_CONFLICT"


@pytest.mark.asyncio
async def test_remove_entity_watchlist(client):
    await client.post(
        f"{API}/entities",
        json={"entity_id": "rm1", "name": "To Remove"},
        headers=HEADERS,
    )
    resp = await client.delete(f"{API}/entities/rm1", headers=HEADERS)
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_remove_entity_not_found(client):
    resp = await client.delete(f"{API}/entities/nonexistent", headers=HEADERS)
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_entity_by_type(client):
    await client.post(
        f"{API}/entities",
        json={"entity_id": "p1", "name": "Person 1", "entity_type": "person"},
        headers=HEADERS,
    )
    await client.post(
        f"{API}/entities",
        json={"entity_id": "o1", "name": "Org 1", "entity_type": "organization"},
        headers=HEADERS,
    )
    resp = await client.get(f"{API}/entities?entity_type=person", headers=HEADERS)
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert all(e["entity_type"] == "person" for e in data)


@pytest.mark.asyncio
async def test_list_region_watchlist(client):
    resp = await client.get(f"{API}/regions", headers=HEADERS)
    assert resp.status_code == 200
    assert resp.json()["data"] == []


@pytest.mark.asyncio
async def test_add_region_watchlist(client):
    resp = await client.post(
        f"{API}/regions",
        json={"region_code": "UA", "name": "Ukraine"},
        headers=HEADERS,
    )
    assert resp.status_code == 201
    assert resp.json()["data"]["region_code"] == "UA"


@pytest.mark.asyncio
async def test_add_region_invalid_code(client):
    resp = await client.post(
        f"{API}/regions",
        json={"region_code": "XXXX"},
        headers=HEADERS,
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_add_region_duplicate(client):
    await client.post(
        f"{API}/regions",
        json={"region_code": "US"},
        headers=HEADERS,
    )
    resp = await client.post(
        f"{API}/regions",
        json={"region_code": "US"},
        headers=HEADERS,
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_remove_region_watchlist(client):
    await client.post(
        f"{API}/regions",
        json={"region_code": "GB"},
        headers=HEADERS,
    )
    resp = await client.delete(f"{API}/regions/GB", headers=HEADERS)
    assert resp.status_code == 204
