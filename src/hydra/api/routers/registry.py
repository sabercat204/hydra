"""Registry router — /api/v1/registry."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Path

from hydra.api.dependencies import (
    APIKeyRecord,
    get_current_api_key,
    get_pagination_params,
    get_registry,
)
from hydra.api.errors import ErrorCode, HydraAPIException, NotFoundException
from hydra.api.schemas.common import APIResponse, PaginationParams
from hydra.api.schemas.registry import (
    AnalysisConfigResponse,
    StreamSourceResponse,
    TierListResponse,
    TierResponse,
)
from hydra.config import settings

router = APIRouter(prefix="/registry", tags=["registry"])


def _tier_to_response(t: Any) -> TierResponse:
    sources = [
        StreamSourceResponse(
            name=s.name, url=s.url, format=s.format, auth=s.auth, notes=s.notes,
        )
        for s in t.sources
    ]
    return TierResponse(
        id=t.id,
        name=t.name,
        streams=t.streams,
        access=t.access,
        formats=t.formats,
        cadence=t.cadence,
        adapter=t.adapter,
        fallback=t.fallback,
        sources=sources,
    )


@router.get(
    "/tiers",
    response_model=APIResponse[TierListResponse],
    summary="List all tiers",
)
async def list_tiers(
    pagination: PaginationParams = Depends(get_pagination_params),
    registry: Any = Depends(get_registry),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[TierListResponse]:
    tiers = [_tier_to_response(t) for t in sorted(registry.tiers.values(), key=lambda x: x.id)]
    return APIResponse(data=TierListResponse(tiers=tiers, total=len(tiers)))


@router.get(
    "/tiers/{tier_id}",
    response_model=APIResponse[TierResponse],
    summary="Get tier detail",
)
async def get_tier(
    tier_id: int = Path(..., ge=1, le=28),
    registry: Any = Depends(get_registry),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[TierResponse]:
    tier = registry.get_tier(tier_id)
    if tier is None:
        raise HydraAPIException(
            code=ErrorCode.TIER_NOT_FOUND,
            message=f"Tier {tier_id} not found",
            status_code=404,
        )
    return APIResponse(data=_tier_to_response(tier))


@router.get(
    "/config/analysis",
    response_model=APIResponse[AnalysisConfigResponse],
    summary="Get analysis configuration (read-only)",
)
async def get_analysis_config(
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[AnalysisConfigResponse]:
    a = settings.analysis
    return APIResponse(
        data=AnalysisConfigResponse(
            sitrep_max_events_per_tier=a.sitrep_max_events_per_tier,
            sitrep_significance_threshold=a.sitrep_significance_threshold,
            sitrep_domain_groups=a.sitrep_domain_groups,
            dossier_network_depth=a.dossier_network_depth,
            dossier_max_network_nodes=a.dossier_max_network_nodes,
            threat_min_convergence_tiers=a.threat_min_convergence_tiers,
            timeline_cluster_window_s=a.timeline_cluster_window_s,
            timeline_max_events=a.timeline_max_events,
            default_max_records=a.default_max_records,
        )
    )
