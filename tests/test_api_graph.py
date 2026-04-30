"""Tests for graph router — /api/v1/graph."""

from __future__ import annotations

import pytest

from hydra.analysis.models import CentralityScore, GraphPath, GraphResult
from tests.conftest_api import *  # noqa: F401, F403

API = "/api/v1/graph"
HEADERS = {"X-API-Key": "test-api-key-12345"}


@pytest.mark.asyncio
async def test_analyze_network_basic(client, mock_graph_analyzer):
    mock_graph_analyzer.analyze_entity_network.return_value = GraphResult(query_duration_ms=15.0)
    resp = await client.post(
        f"{API}/network",
        json={"entity_hashes": ["abc123"], "max_depth": 2, "max_nodes": 50},
        headers=HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()["data"]
    assert "nodes" in body
    assert "edges" in body
    assert body["query_duration_ms"] == 15.0


@pytest.mark.asyncio
async def test_analyze_network_empty_hashes(client):
    resp = await client.post(
        f"{API}/network",
        json={"entity_hashes": [], "max_depth": 2, "max_nodes": 50},
        headers=HEADERS,
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_analyze_network_max_depth(client, mock_graph_analyzer):
    mock_graph_analyzer.analyze_entity_network.return_value = GraphResult(query_duration_ms=5.0)
    # max_depth=5 should be accepted
    resp = await client.post(
        f"{API}/network",
        json={"entity_hashes": ["abc"], "max_depth": 5},
        headers=HEADERS,
    )
    assert resp.status_code == 200

    # max_depth=6 should fail
    resp = await client.post(
        f"{API}/network",
        json={"entity_hashes": ["abc"], "max_depth": 6},
        headers=HEADERS,
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_shortest_paths_found(client, mock_graph_analyzer):
    mock_graph_analyzer.shortest_paths.return_value = [
        GraphPath(start_id="a", end_id="b", path_nodes=["a", "c", "b"], path_edges=["e1", "e2"], length=2),
    ]
    resp = await client.post(
        f"{API}/paths",
        json={"source_hash": "a", "target_hashes": ["b"]},
        headers=HEADERS,
    )
    assert resp.status_code == 200
    paths = resp.json()["data"]
    assert len(paths) == 1
    assert paths[0]["length"] == 2


@pytest.mark.asyncio
async def test_shortest_paths_unreachable(client, mock_graph_analyzer):
    mock_graph_analyzer.shortest_paths.return_value = []
    resp = await client.post(
        f"{API}/paths",
        json={"source_hash": "a", "target_hashes": ["z"]},
        headers=HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["data"] == []


@pytest.mark.asyncio
async def test_centrality_default(client, mock_graph_analyzer):
    mock_graph_analyzer.centrality_ranking.return_value = [
        CentralityScore(node_id="n1", label="Entity A", metric="degree", score=10.0),
    ]
    resp = await client.get(f"{API}/centrality", headers=HEADERS)
    assert resp.status_code == 200
    scores = resp.json()["data"]
    assert len(scores) == 1
    assert scores[0]["metric"] == "degree"


@pytest.mark.asyncio
async def test_centrality_by_metric(client, mock_graph_analyzer):
    mock_graph_analyzer.centrality_ranking.return_value = [
        CentralityScore(node_id="n1", label="Entity A", metric="pagerank", score=0.5),
    ]
    resp = await client.get(f"{API}/centrality?metric=pagerank", headers=HEADERS)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_centrality_by_tier(client, mock_graph_analyzer):
    mock_graph_analyzer.centrality_ranking.return_value = []
    resp = await client.get(f"{API}/centrality?tier=5", headers=HEADERS)
    assert resp.status_code == 200
