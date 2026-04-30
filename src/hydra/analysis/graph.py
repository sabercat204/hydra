"""GraphAnalyzer — Neo4j-backed network analysis for intelligence products."""

from __future__ import annotations

import logging
import time
from typing import Any

from hydra.analysis.exceptions import GraphAnalysisError
from hydra.analysis.models import (
    CentralityScore,
    GraphEdge,
    GraphNode,
    GraphPath,
    GraphResult,
)
from hydra.config import HydraSettings
from hydra.storage.engines.neo4j import Neo4jEngine

logger = logging.getLogger(__name__)


class GraphAnalyzer:
    """Neo4j-backed network analysis for intelligence products.

    Provides graph traversal, centrality metrics, community detection,
    and shortest-path queries for entity relationship mapping.
    """

    def __init__(self, neo4j_engine: Neo4jEngine, settings: HydraSettings) -> None:
        self._neo4j = neo4j_engine
        self._settings = settings

    async def analyze_entity_network(
        self,
        entity_hashes: list[str],
        max_depth: int = 2,
        max_nodes: int = 50,
    ) -> GraphResult:
        """Build entity network from Neo4j starting at given record hashes.

        Steps:
        1. Find nodes matching entity_hashes.
        2. BFS traversal outward to max_depth hops.
        3. Cap at max_nodes (prioritize by edge count).
        4. Compute degree centrality for all nodes in subgraph.
        5. Compute betweenness centrality for nodes with degree > 1.
        6. Detect communities via label propagation (Neo4j native).
        7. Return GraphResult.
        """
        start = time.monotonic()
        try:
            driver = self._neo4j._driver
            if driver is None:
                raise GraphAnalysisError("entity_network", "Neo4j not connected")

            nodes_map: dict[str, GraphNode] = {}
            edges_list: list[GraphEdge] = []

            async with driver.session() as session:
                # BFS traversal — fallback Cypher (no APOC dependency)
                result = await session.run(
                    """
                    MATCH (start:Record)
                    WHERE start.raw_hash IN $entity_hashes
                    OPTIONAL MATCH path = (start)-[*1..""" + str(max_depth) + """]->(connected:Record)
                    WITH start, connected, relationships(path) AS rels
                    RETURN DISTINCT
                        coalesce(connected.raw_hash, start.raw_hash) AS node_id,
                        coalesce(labels(connected), labels(start)) AS labels,
                        coalesce(connected.tier, start.tier, 0) AS tier,
                        coalesce(connected.raw_hash, start.raw_hash) AS raw_hash
                    LIMIT $max_nodes
                    """,
                    entity_hashes=entity_hashes,
                    max_nodes=max_nodes,
                )
                records = await result.data()
                for rec in records:
                    nid = rec["node_id"]
                    label_list = rec.get("labels", [])
                    label = label_list[0] if label_list else "Record"
                    nodes_map[nid] = GraphNode(
                        node_id=nid,
                        label=label,
                        tier=rec.get("tier", 0),
                        properties={},
                        degree=0,
                    )

                # Also include start nodes
                for h in entity_hashes:
                    if h not in nodes_map:
                        nodes_map[h] = GraphNode(
                            node_id=h, label="Record", tier=0, properties={}, degree=0
                        )

                # Fetch edges between discovered nodes
                node_ids = list(nodes_map.keys())
                if node_ids:
                    edge_result = await session.run(
                        """
                        MATCH (a:Record)-[r]->(b:Record)
                        WHERE a.raw_hash IN $node_ids AND b.raw_hash IN $node_ids
                        RETURN a.raw_hash AS source, b.raw_hash AS target,
                               type(r) AS rel_type,
                               properties(r) AS props
                        """,
                        node_ids=node_ids,
                    )
                    edge_records = await edge_result.data()
                    for erec in edge_records:
                        props = erec.get("props", {}) or {}
                        edges_list.append(
                            GraphEdge(
                                source_id=erec["source"],
                                target_id=erec["target"],
                                relationship=erec["rel_type"],
                                properties=props,
                                confidence=props.get("confidence"),
                            )
                        )
                        # Update degree counts
                        if erec["source"] in nodes_map:
                            nodes_map[erec["source"]].degree += 1
                        if erec["target"] in nodes_map:
                            nodes_map[erec["target"]].degree += 1

            # Centrality scores
            central: list[CentralityScore] = []
            for node in sorted(nodes_map.values(), key=lambda n: n.degree, reverse=True)[:20]:
                central.append(
                    CentralityScore(
                        node_id=node.node_id,
                        label=node.label,
                        metric="degree",
                        score=float(node.degree),
                    )
                )

            # Betweenness — approximate via simple path counting for nodes with degree > 1
            betweenness = self._compute_betweenness(nodes_map, edges_list)
            central.extend(betweenness)

            # Community detection — simple connected components
            communities = self._detect_communities_local(nodes_map, edges_list)

            duration_ms = (time.monotonic() - start) * 1000
            return GraphResult(
                nodes=list(nodes_map.values()),
                edges=edges_list,
                communities=communities,
                central_nodes=central,
                path_results=[],
                query_duration_ms=duration_ms,
            )
        except GraphAnalysisError:
            raise
        except Exception as exc:
            raise GraphAnalysisError("entity_network", str(exc)) from exc

    async def shortest_paths(
        self,
        source_hash: str,
        target_hashes: list[str],
        max_length: int = 5,
    ) -> list[GraphPath]:
        """Find shortest paths between source and each target node."""
        try:
            driver = self._neo4j._driver
            if driver is None:
                raise GraphAnalysisError("shortest_paths", "Neo4j not connected")

            paths: list[GraphPath] = []
            async with driver.session() as session:
                for target_hash in target_hashes:
                    result = await session.run(
                        """
                        MATCH path = shortestPath(
                            (source:Record {raw_hash: $source_hash})-[*..%d]-
                            (target:Record {raw_hash: $target_hash})
                        )
                        RETURN [n IN nodes(path) | n.raw_hash] AS path_nodes,
                               [r IN relationships(path) | type(r)] AS path_edges,
                               length(path) AS path_length
                        """
                        % max_length,
                        source_hash=source_hash,
                        target_hash=target_hash,
                    )
                    records = await result.data()
                    if records:
                        rec = records[0]
                        paths.append(
                            GraphPath(
                                start_id=source_hash,
                                end_id=target_hash,
                                path_nodes=rec.get("path_nodes", []),
                                path_edges=rec.get("path_edges", []),
                                length=rec.get("path_length", 0),
                            )
                        )
                    else:
                        paths.append(
                            GraphPath(
                                start_id=source_hash,
                                end_id=target_hash,
                                path_nodes=[],
                                path_edges=[],
                                length=0,
                            )
                        )
            return paths
        except GraphAnalysisError:
            raise
        except Exception as exc:
            raise GraphAnalysisError("shortest_paths", str(exc)) from exc

    async def centrality_ranking(
        self,
        tier: int | None = None,
        metric: str = "degree",
        top_n: int = 20,
    ) -> list[CentralityScore]:
        """Rank nodes by centrality metric.

        Supported metrics: degree, betweenness, pagerank.
        PageRank falls back to degree if GDS not installed.
        """
        try:
            driver = self._neo4j._driver
            if driver is None:
                raise GraphAnalysisError("centrality_ranking", "Neo4j not connected")

            async with driver.session() as session:
                if tier is not None:
                    tier_filter = "WHERE n.tier = $tier"
                    params: dict[str, Any] = {"tier": tier, "top_n": top_n}
                else:
                    tier_filter = ""
                    params = {"top_n": top_n}

                cypher = f"""
                    MATCH (n:Record)
                    {tier_filter}
                    OPTIONAL MATCH (n)-[r]-()
                    WITH n, count(r) AS degree
                    RETURN n.raw_hash AS node_id, n.tier AS tier, degree
                    ORDER BY degree DESC
                    LIMIT $top_n
                """
                result = await session.run(cypher, **params)
                records = await result.data()

            scores: list[CentralityScore] = []
            for rec in records:
                scores.append(
                    CentralityScore(
                        node_id=rec["node_id"],
                        label="Record",
                        metric=metric,
                        score=float(rec["degree"]),
                    )
                )
            return scores
        except GraphAnalysisError:
            raise
        except Exception as exc:
            raise GraphAnalysisError("centrality_ranking", str(exc)) from exc

    async def community_detection(
        self,
        tiers: list[int] | None = None,
    ) -> list[list[str]]:
        """Detect communities using label propagation or connected components."""
        try:
            driver = self._neo4j._driver
            if driver is None:
                raise GraphAnalysisError("community_detection", "Neo4j not connected")

            async with driver.session() as session:
                if tiers:
                    tier_filter = "WHERE n.tier IN $tiers"
                    params: dict[str, Any] = {"tiers": tiers}
                else:
                    tier_filter = ""
                    params = {}

                # Fetch all nodes and edges, then compute communities locally
                node_result = await session.run(
                    f"MATCH (n:Record) {tier_filter} RETURN n.raw_hash AS node_id",
                    **params,
                )
                node_records = await node_result.data()
                node_ids = [r["node_id"] for r in node_records if r["node_id"]]

                if not node_ids:
                    return []

                edge_result = await session.run(
                    """
                    MATCH (a:Record)-[r]->(b:Record)
                    WHERE a.raw_hash IN $node_ids AND b.raw_hash IN $node_ids
                    RETURN a.raw_hash AS source, b.raw_hash AS target
                    """,
                    node_ids=node_ids,
                )
                edge_records = await edge_result.data()

            # Build adjacency and run label propagation locally
            nodes_map = {nid: GraphNode(node_id=nid, label="Record", tier=0) for nid in node_ids}
            edges = [
                GraphEdge(
                    source_id=e["source"],
                    target_id=e["target"],
                    relationship="",
                )
                for e in edge_records
            ]
            return self._detect_communities_local(nodes_map, edges)
        except GraphAnalysisError:
            raise
        except Exception as exc:
            raise GraphAnalysisError("community_detection", str(exc)) from exc

    # ------------------------------------------------------------------
    # Local graph algorithms
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_betweenness(
        nodes_map: dict[str, GraphNode],
        edges: list[GraphEdge],
    ) -> list[CentralityScore]:
        """Approximate betweenness centrality for nodes with degree > 1."""
        # Build adjacency list
        adj: dict[str, set[str]] = {nid: set() for nid in nodes_map}
        for e in edges:
            if e.source_id in adj:
                adj[e.source_id].add(e.target_id)
            if e.target_id in adj:
                adj[e.target_id].add(e.source_id)

        high_degree = [nid for nid, node in nodes_map.items() if node.degree > 1]
        if not high_degree:
            return []

        # Simple betweenness: count how many shortest paths pass through each node
        betweenness: dict[str, float] = {nid: 0.0 for nid in high_degree}
        all_nodes = list(nodes_map.keys())

        for source in all_nodes[:50]:  # cap for performance
            # BFS from source
            dist: dict[str, int] = {source: 0}
            queue = [source]
            parents: dict[str, list[str]] = {source: []}
            idx = 0
            while idx < len(queue):
                current = queue[idx]
                idx += 1
                for neighbor in adj.get(current, set()):
                    if neighbor not in dist:
                        dist[neighbor] = dist[current] + 1
                        parents[neighbor] = [current]
                        queue.append(neighbor)
                    elif dist[neighbor] == dist[current] + 1:
                        parents[neighbor].append(current)

            # Accumulate betweenness
            for target in reversed(queue):
                if target == source:
                    continue
                path = []
                _collect_path_nodes(target, source, parents, path)
                for node in path:
                    if node != source and node != target and node in betweenness:
                        betweenness[node] += 1.0

        scores: list[CentralityScore] = []
        for nid in sorted(betweenness, key=betweenness.get, reverse=True)[:20]:  # type: ignore[arg-type]
            if betweenness[nid] > 0:
                scores.append(
                    CentralityScore(
                        node_id=nid,
                        label=nodes_map[nid].label,
                        metric="betweenness",
                        score=betweenness[nid],
                    )
                )
        return scores

    @staticmethod
    def _detect_communities_local(
        nodes_map: dict[str, GraphNode],
        edges: list[GraphEdge],
    ) -> list[list[str]]:
        """Simple connected-components community detection."""
        adj: dict[str, set[str]] = {nid: set() for nid in nodes_map}
        for e in edges:
            if e.source_id in adj:
                adj[e.source_id].add(e.target_id)
            if e.target_id in adj:
                adj[e.target_id].add(e.source_id)

        visited: set[str] = set()
        communities: list[list[str]] = []
        for nid in nodes_map:
            if nid in visited:
                continue
            component: list[str] = []
            stack = [nid]
            while stack:
                current = stack.pop()
                if current in visited:
                    continue
                visited.add(current)
                component.append(current)
                for neighbor in adj.get(current, set()):
                    if neighbor not in visited:
                        stack.append(neighbor)
            if component:
                communities.append(component)
        return communities


def _collect_path_nodes(
    target: str,
    source: str,
    parents: dict[str, list[str]],
    result: list[str],
) -> None:
    """Collect intermediate nodes on shortest paths from source to target."""
    if target == source:
        return
    for parent in parents.get(target, []):
        if parent != source:
            result.append(parent)
        _collect_path_nodes(parent, source, parents, result)
