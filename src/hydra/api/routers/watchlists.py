"""Watchlists router — /api/v1/watchlists."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query

from hydra.api.dependencies import (
    APIKeyRecord,
    get_current_api_key,
    get_pagination_params,
)
from hydra.api.errors import ConflictError, NotFoundException
from hydra.api.schemas.common import APIResponse, PaginationParams
from hydra.api.schemas.watchlists import (
    CreateEntityWatchlistRequest,
    CreateRegionWatchlistRequest,
    EntityWatchlistEntry,
    RegionWatchlistEntry,
)

router = APIRouter(prefix="/watchlists", tags=["watchlists"])

# In-memory stores for MVP / testing. Production uses PG tables.
_entity_watchlist: dict[str, EntityWatchlistEntry] = {}
_region_watchlist: dict[str, RegionWatchlistEntry] = {}


def reset_watchlists() -> None:
    """Reset in-memory watchlists (for testing)."""
    _entity_watchlist.clear()
    _region_watchlist.clear()


# --- Entity Watchlist ---

@router.get(
    "/entities",
    response_model=APIResponse[list[EntityWatchlistEntry]],
    summary="List entity watchlist",
)
async def list_entity_watchlist(
    entity_type: str | None = Query(None),
    pagination: PaginationParams = Depends(get_pagination_params),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[list[EntityWatchlistEntry]]:
    entries = list(_entity_watchlist.values())
    if entity_type:
        entries = [e for e in entries if e.entity_type == entity_type]
    return APIResponse(data=entries)


@router.post(
    "/entities",
    status_code=201,
    response_model=APIResponse[EntityWatchlistEntry],
    summary="Add entity to watchlist",
)
async def add_entity_watchlist(
    request: CreateEntityWatchlistRequest,
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[EntityWatchlistEntry]:
    if request.entity_id in _entity_watchlist:
        raise ConflictError(f"Entity {request.entity_id} already in watchlist")
    entry = EntityWatchlistEntry(
        entity_id=request.entity_id,
        name=request.name,
        entity_type=request.entity_type,
        notes=request.notes,
        added_at=datetime.now(timezone.utc).isoformat(),
    )
    _entity_watchlist[request.entity_id] = entry
    return APIResponse(data=entry)


@router.delete(
    "/entities/{entity_id}",
    status_code=204,
    summary="Remove entity from watchlist",
)
async def remove_entity_watchlist(
    entity_id: str,
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> None:
    if entity_id not in _entity_watchlist:
        raise NotFoundException(f"Entity {entity_id} not found in watchlist")
    del _entity_watchlist[entity_id]


# --- Region Watchlist ---

@router.get(
    "/regions",
    response_model=APIResponse[list[RegionWatchlistEntry]],
    summary="List region watchlist",
)
async def list_region_watchlist(
    pagination: PaginationParams = Depends(get_pagination_params),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[list[RegionWatchlistEntry]]:
    return APIResponse(data=list(_region_watchlist.values()))


@router.post(
    "/regions",
    status_code=201,
    response_model=APIResponse[RegionWatchlistEntry],
    summary="Add region to watchlist",
)
async def add_region_watchlist(
    request: CreateRegionWatchlistRequest,
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[RegionWatchlistEntry]:
    if request.region_code in _region_watchlist:
        raise ConflictError(f"Region {request.region_code} already in watchlist")
    entry = RegionWatchlistEntry(
        region_code=request.region_code,
        name=request.name,
        notes=request.notes,
        added_at=datetime.now(timezone.utc).isoformat(),
    )
    _region_watchlist[request.region_code] = entry
    return APIResponse(data=entry)


@router.delete(
    "/regions/{region_code}",
    status_code=204,
    summary="Remove region from watchlist",
)
async def remove_region_watchlist(
    region_code: str,
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> None:
    if region_code not in _region_watchlist:
        raise NotFoundException(f"Region {region_code} not found in watchlist")
    del _region_watchlist[region_code]
