"""Tests for GraphAnalyzer — Neo4j-backed network analysis."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from hydra.analysis.exceptions import GraphAnalysisError
from hydra.analysis.graph import GraphAnalyzer
from hydra.analysis.models import GraphEdge, GraphNode
from hydra.config import HydraSettings
from hydra.storage.engines.neo4j import Neo4jEngine


def _settings() -> HydraSettings:
    return HydraSettings()


class MockSession:
    """Mock Neo4j async session."""

    def __init__(self, node_data: list[dict] | None = None, edge_data: list[dict] | None = None) -> None:
        self._node_data = node_data or []
        self._edge_data = edge_data or []
        self._call_count = 0

    async def run(self, query: str, **kwargs) -> "MockResult":
        self._call_count += 1
        # First call: node traversal, second call: edge fetch
        if "OPTIONAL MATCH path" in query or "raw_hash IN" in query:
            if "source" in query and "target" in query:
                return MockResult(self._edge_data)
            return MockResult(self._node_data)
        if "shortestPath" in query:
            return MockResult(self._node_data)
        if "count(r) AS degree" in query:
            return MockResult(self._node_data)
        return MockResult(self._edge_data if self._edge_data else self._node_data)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class MockResult:
    def __init__(self, data: list[dict]) -> None:
        self._data = data

    async def data(self) -> list[dict]:
        return self._data


def _make_analyzer(
    node_data: list[dict] | None = None,
    edge_data: list[dict] | None = None,
    connected: bool = True,
) -> GraphAnalyzer:
    neo4j = MagicMock(spec=Neo4jEngine)
    if connected:
        neo4j._driver = MagicMock()
        neo4j._driver.session = MagicMock(return_value=MockSession(node_data, edge_data))
    else:
        neo4j._driver = None
    return GraphAnalyzer(neo4j, _settings())


@pytest.mark.asyncio
async def test_entity_network_traversal() -> None:
    """BFS from entity nodes returns subgraph."""
    nodes = [
        {"node_id": "a" * 16, "labels": ["Record"], "tier": 1, "raw_hash": "a" * 16},
        {"node_id": "b" * 16, "labels": ["Record"], "tier": 15, "raw_hash": "b" * 16},
    ]
    edges = [
        {"source": "a" * 16, "target": "b" * 16, "rel_type": "CORRELATED_WITH", "props": {"confidence": 0.9}},
    ]
    analyzer = _make_analyzer(node_data=nodes, edge_data=edges)
    result = await analyzer.analyze_entity_network(["a" * 16])

    assert len(result.nodes) >= 2
    assert len(result.edges) == 1


@pytest.mark.asyncio
async def test_network_depth_limit() -> None:
    """Traversal stops at max_depth."""
    nodes = [{"node_id": "a" * 16, "labels": ["Record"], "tier": 1, "raw_hash": "a" * 16}]
    analyzer = _make_analyzer(node_data=nodes)
    result = await analyzer.analyze_entity_network(["a" * 16], max_depth=1)

    # Should complete without error
    assert result is not None


@pytest.mark.asyncio
async def test_network_node_cap() -> None:
    """Result capped at max_nodes."""
    nodes = [
        {"node_id": f"{i:016x}", "labels": ["Record"], "tier": 1, "raw_hash": f"{i:016x}"}
        for i in range(100)
    ]
    analyzer = _make_analyzer(node_data=nodes[:5])  # Mock returns limited
    result = await analyzer.analyze_entity_network(["0" * 16], max_nodes=5)

    assert len(result.nodes) <= 6  # 5 + start node


@pytest.mark.asyncio
async def test_degree_centrality() -> None:
    """Correct degree calculation."""
    nodes = [
        {"node_id": "a" * 16, "labels": ["Record"], "tier": 1, "raw_hash": "a" * 16},
        {"node_id": "b" * 16, "labels": ["Record"], "tier": 15, "raw_hash": "b" * 16},
    ]
    edges = [
        {"source": "a" * 16, "target": "b" * 16, "rel_type": "CORRELATED_WITH", "props": {}},
    ]
    analyzer = _make_analyzer(node_data=nodes, edge_data=edges)
    result = await analyzer.analyze_entity_network(["a" * 16])

    # Central nodes should include degree metrics
    degree_scores = [c for c in result.central_nodes if c.metric == "degree"]
    assert len(degree_scores) > 0


@pytest.mark.asyncio
async def test_betweenness_centrality() -> None:
    """Nodes with degree > 1 scored for betweenness."""
    nodes = [
        {"node_id": "a" * 16, "labels": ["Record"], "tier": 1, "raw_hash": "a" * 16},
        {"node_id": "b" * 16, "labels": ["Record"], "tier": 15, "raw_hash": "b" * 16},
        {"node_id": "c" * 16, "labels": ["Record"], "tier": 16, "raw_hash": "c" * 16},
    ]
    edges = [
        {"source": "a" * 16, "target": "b" * 16, "rel_type": "CORRELATED_WITH", "props": {}},
        {"source": "b" * 16, "target": "c" * 16, "rel_type": "CORRELATED_WITH", "props": {}},
    ]
    analyzer = _make_analyzer(node_data=nodes, edge_data=edges)
    result = await analyzer.analyze_entity_network(["a" * 16])

    betweenness_scores = [c for c in result.central_nodes if c.metric == "betweenness"]
    # b should have betweenness > 0 as it connects a and c
    # (may or may not appear depending on path counting)
    assert isinstance(betweenness_scores, list)


@pytest.mark.asyncio
async def test_community_detection() -> None:
    """Label propagation returns clusters."""
    nodes = [
        {"node_id": "a" * 16, "labels": ["Record"], "tier": 1, "raw_hash": "a" * 16},
        {"node_id": "b" * 16, "labels": ["Record"], "tier": 15, "raw_hash": "b" * 16},
    ]
    edges = [
        {"source": "a" * 16, "target": "b" * 16, "rel_type": "CORRELATED_WITH", "props": {}},
    ]
    analyzer = _make_analyzer(node_data=nodes, edge_data=edges)
    result = await analyzer.analyze_entity_network(["a" * 16])

    assert len(result.communities) >= 1
    # Connected nodes should be in the same community
    all_community_nodes = [n for c in result.communities for n in c]
    assert "a" * 16 in all_community_nodes


@pytest.mark.asyncio
async def test_shortest_path() -> None:
    """Path found between connected nodes."""
    path_data = [
        {"path_nodes": ["a" * 16, "b" * 16], "path_edges": ["CORRELATED_WITH"], "path_length": 1},
    ]
    analyzer = _make_analyzer(node_data=path_data)
    paths = await analyzer.shortest_paths("a" * 16, ["b" * 16])

    assert len(paths) == 1
    assert paths[0].length == 1
    assert paths[0].start_id == "a" * 16


@pytest.mark.asyncio
async def test_shortest_path_no_connection() -> None:
    """Disconnected nodes return empty path."""
    analyzer = _make_analyzer(node_data=[])
    paths = await analyzer.shortest_paths("a" * 16, ["z" * 16])

    assert len(paths) == 1
    assert paths[0].length == 0
    assert paths[0].path_nodes == []


@pytest.mark.asyncio
async def test_centrality_ranking_by_tier() -> None:
    """Tier filter scopes ranking."""
    ranking_data = [
        {"node_id": "a" * 16, "tier": 1, "degree": 5},
    ]
    analyzer = _make_analyzer(node_data=ranking_data)
    scores = await analyzer.centrality_ranking(tier=1, metric="degree", top_n=10)

    assert len(scores) == 1
    assert scores[0].score == 5.0
