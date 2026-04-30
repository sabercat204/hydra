"""Graph router — /api/v1/graph."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from hydra.api.dependencies import (
    APIKeyRecord,
    get_current_api_key,
    get_graph_analyzer,
)
from hydra.api.schemas.common import APIResponse
from hydra.api.schemas.graph import (
    CentralityScoreResponse,
    EntityNetworkRequest,
    GraphEdgeResponse,
    GraphNodeResponse,
    GraphPathResponse,
    GraphResultResponse,
    ShortestPathRequest,
)

router = APIRouter(prefix="/graph", tags=["graph"])


def _graph_result_to_response(r: Any) -> GraphResultResponse:
    nodes = [
        GraphNodeResponse(
            node_id=n.node_id, label=n.label, tier=n.tier,
            properties=n.properties, degree=n.degree,
        )
        for n in r.nodes
    ]
    edges = [
        GraphEdgeResponse(
            source_id=e.source_id, target_id=e.target_id,
            relationship=e.relationship, properties=e.properties,
            confidence=e.confidence,
        )
        for e in r.edges
    ]
    central = [
        CentralityScoreResponse(
            node_id=c.node_id, label=c.label, metric=c.metric, score=c.score,
        )
        for c in r.central_nodes
    ]
    paths = [
        GraphPathResponse(
            start_id=p.start_id, end_id=p.end_id,
            path_nodes=p.path_nodes, path_edges=p.path_edges,
            length=p.length,
        )
        for p in r.path_results
    ]
    return GraphResultResponse(
        nodes=nodes,
        edges=edges,
        communities=r.communities,
        central_nodes=central,
        path_results=paths,
        query_duration_ms=r.query_duration_ms,
    )


@router.post(
    "/network",
    response_model=APIResponse[GraphResultResponse],
    summary="Analyze entity network",
)
async def analyze_entity_network(
    request: EntityNetworkRequest,
    graph: Any = Depends(get_graph_analyzer),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[GraphResultResponse]:
    result = await graph.analyze_entity_network(
        entity_hashes=request.entity_hashes,
        max_depth=request.max_depth,
        max_nodes=request.max_nodes,
    )
    return APIResponse(data=_graph_result_to_response(result))


@router.post(
    "/paths",
    response_model=APIResponse[list[GraphPathResponse]],
    summary="Find shortest paths between entities",
)
async def shortest_paths(
    request: ShortestPathRequest,
    graph: Any = Depends(get_graph_analyzer),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[list[GraphPathResponse]]:
    results = await graph.shortest_paths(
        source_hash=request.source_hash,
        target_hashes=request.target_hashes,
        max_length=request.max_length,
    )
    paths = [
        GraphPathResponse(
            start_id=p.start_id, end_id=p.end_id,
            path_nodes=p.path_nodes, path_edges=p.path_edges,
            length=p.length,
        )
        for p in results
    ]
    return APIResponse(data=paths)


@router.get(
    "/centrality",
    response_model=APIResponse[list[CentralityScoreResponse]],
    summary="Rank entities by centrality metric",
)
async def centrality_ranking(
    tier: int | None = Query(None),
    metric: str = Query("degree"),
    top_n: int = Query(20, ge=1, le=100),
    graph: Any = Depends(get_graph_analyzer),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[list[CentralityScoreResponse]]:
    results = await graph.centrality_ranking(
        tier=tier,
        metric=metric,
        top_n=top_n,
    )
    scores = [
        CentralityScoreResponse(
            node_id=c.node_id, label=c.label, metric=c.metric, score=c.score,
        )
        for c in results
    ]
    return APIResponse(data=scores)
