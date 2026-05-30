"""Maps router — bounding-box feature queries and tile aggregation (Design §7.1, R12, R13).

Single endpoint:

* ``GET /api/v1/maps/features?bbox={min_lon},{min_lat},{max_lon},{max_lat}``
  with optional ``zoom``, ``tier``, ``time_start``, ``time_end``,
  ``min_confidence``, and ``tag`` query parameters.

Response shape follows GeoJSON: one ``Feature`` per raw record (no
``zoom``) or one ``Feature`` per aggregated cell (``zoom`` supplied).
The full body is wrapped in :class:`APIResponse` with an unpopulated
pagination slot — maps are not paged; bbox + zoom together bound the
response size.

**Error paths**

* Malformed ``bbox`` or out-of-order coordinates → 422
  ``VALIDATION_ERROR`` without a DB query (R12.2).
* Intersecting record count > ``EASSettings.maps_feature_limit`` with
  no ``zoom`` → 413 ``BBOX_TOO_BROAD`` with the hint text required by
  R12.4.
* Invalid ``zoom`` (outside ``[0, 18]``) → 422 via the Query(ge=, le=)
  constraint.

**Tenant scoping**

Maps reads are tenant-agnostic per R20.5, but the endpoint still
requires an authenticated key — we depend on
:func:`get_current_tenant_id` for that gate; the returned UUID is
ignored in the query (PostGIS doesn't carry tenant metadata here).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from hydra.api.dependencies import get_db_pool
from hydra.api.errors import ErrorCode, HydraAPIException
from hydra.api.pagination import PaginationMeta
from hydra.api.schemas.common import APIResponse, ResponseMeta
from hydra.eas.dependencies import get_current_tenant_id, get_eas_settings
from hydra.eas.maps.repository import MapsFilters, MapsRepository
from hydra.eas.maps.tile_aggregator import TileAggregator
from hydra.eas.schemas.maps import (
    FeatureCollectionResponse,
    FeatureResponse,
)

logger = logging.getLogger(__name__)


router = APIRouter(tags=["maps"])

__all__ = ["router"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _empty_meta() -> ResponseMeta:
    """Minimal ``ResponseMeta`` with no pagination — the bbox/zoom
    combination bounds the response size, so maps don't use cursors.
    """

    return ResponseMeta(
        request_id="",
        timestamp="",
        duration_ms=0.0,
        pagination=PaginationMeta(
            next_cursor=None,
            has_more=False,
            total_estimate=None,
        ),
    )


def _parse_bbox(raw: str) -> tuple[float, float, float, float]:
    """Parse and validate the ``bbox`` query parameter.

    Splits on ``,`` — exactly four floats are required. Ordering
    invariants per R12.1:

    * ``-180.0 <= min_lon <= max_lon <= 180.0``
    * ``-90.0 <= min_lat <= max_lat <= 90.0``

    A 422 ``VALIDATION_ERROR`` is raised on any failure; the router
    catches this **before** issuing a DB query so R12.2 ("SHALL NOT
    execute any PostGIS query") is satisfied structurally — we don't
    rely on the DB layer to reject bad bboxes.
    """

    parts = [p.strip() for p in raw.split(",")]
    if len(parts) != 4:
        raise HydraAPIException(
            code=ErrorCode.VALIDATION_ERROR,
            message="bbox must have exactly four comma-separated floats",
            detail={"bbox": raw},
            status_code=422,
        )

    try:
        min_lon, min_lat, max_lon, max_lat = (float(p) for p in parts)
    except ValueError as exc:
        raise HydraAPIException(
            code=ErrorCode.VALIDATION_ERROR,
            message="bbox values must be parseable as floats",
            detail={"bbox": raw},
            status_code=422,
        ) from exc

    # Longitude ordering check (R12.1).
    if not (-180.0 <= min_lon <= max_lon <= 180.0):
        raise HydraAPIException(
            code=ErrorCode.VALIDATION_ERROR,
            message=(
                "bbox longitude must satisfy "
                "-180.0 <= min_lon <= max_lon <= 180.0"
            ),
            detail={
                "min_lon": min_lon,
                "max_lon": max_lon,
            },
            status_code=422,
        )

    # Latitude ordering check (R12.1).
    if not (-90.0 <= min_lat <= max_lat <= 90.0):
        raise HydraAPIException(
            code=ErrorCode.VALIDATION_ERROR,
            message=(
                "bbox latitude must satisfy "
                "-90.0 <= min_lat <= max_lat <= 90.0"
            ),
            detail={
                "min_lat": min_lat,
                "max_lat": max_lat,
            },
            status_code=422,
        )

    return min_lon, min_lat, max_lon, max_lat


def _raw_feature(record: dict[str, Any]) -> FeatureResponse:
    """Map a raw record row to a GeoJSON ``Feature``.

    ``geometry`` carries ``(lon, lat)`` per the GeoJSON spec. Record
    properties include the pass-through fields the caller needs to
    drive map rendering (``raw_hash``, ``tier``, ``tags``,
    ``confidence``).
    """

    lon = record.get("lon")
    lat = record.get("lat")
    # ``geo IS NOT NULL`` in the SQL already filters these out, but we
    # guard anyway so a future schema change can't land a None here
    # and crash the serializer.
    coordinates = [float(lon) if lon is not None else 0.0,
                   float(lat) if lat is not None else 0.0]
    return FeatureResponse(
        geometry={"type": "Point", "coordinates": coordinates},
        properties={
            "raw_hash": record.get("raw_hash"),
            "tier": record.get("tier"),
            "tags": list(record.get("tags") or []),
            "confidence": record.get("confidence"),
        },
    )


def _cell_feature(cell: Any) -> FeatureResponse:
    """Map a :class:`TileCellResponse` to a GeoJSON ``Feature``.

    The geometry is the centroid ``Point`` (GeoJSON coord order).
    Properties carry the cell summary so clients can render density
    without another round-trip.
    """

    centroid_lon, centroid_lat = cell.centroid
    return FeatureResponse(
        geometry={
            "type": "Point",
            "coordinates": [float(centroid_lon), float(centroid_lat)],
        },
        properties={
            "cell_id": cell.cell_id,
            "strategy": cell.strategy,
            "resolution": cell.resolution,
            "count": cell.count,
            "tier_breakdown": dict(cell.tier_breakdown),
            "dominant_tag": cell.dominant_tag,
        },
    )


# ---------------------------------------------------------------------------
# GET /api/v1/maps/features (R12.1, R13.1)
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/maps/features",
    response_model=APIResponse[FeatureCollectionResponse],
    summary=(
        "Query normalized_records by bounding box; optional zoom triggers "
        "server-side H3/geohash aggregation"
    ),
)
async def get_map_features(
    bbox: Annotated[str, Query(description="min_lon,min_lat,max_lon,max_lat")],
    zoom: Annotated[int | None, Query(ge=0, le=18)] = None,
    tier: Annotated[int | None, Query(ge=1, le=29)] = None,
    time_start: Annotated[datetime | None, Query()] = None,
    time_end: Annotated[datetime | None, Query()] = None,
    min_confidence: Annotated[float | None, Query(ge=0.0, le=1.0)] = None,
    tag: Annotated[str | None, Query()] = None,
    tenant_id: UUID = Depends(get_current_tenant_id),
    pg_pool: Any = Depends(get_db_pool),
    settings: Any = Depends(get_eas_settings),
) -> APIResponse[FeatureCollectionResponse]:
    """Return a GeoJSON ``FeatureCollection`` for records in ``bbox``.

    See module docstring for error paths; the happy path branches are:

    * ``zoom`` supplied → fetch records within the bbox, aggregate
      into H3 / geohash cells via :class:`TileAggregator`, and return
      one feature per non-empty cell (R13.1, R13.4).
    * ``zoom`` not supplied, record count ≤
      ``maps_feature_limit`` → return raw point features (R12.1).
    * ``zoom`` not supplied, record count >
      ``maps_feature_limit`` → 413 ``BBOX_TOO_BROAD`` (R12.4).
    """

    # Auth-only — the tenant id is consumed as the authentication gate
    # (R20.5 says Maps is tenant-agnostic for reads, but R21.3 requires
    # an authenticated caller). Silences unused-variable warnings.
    del tenant_id

    if pg_pool is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Database is not available",
            status_code=503,
        )

    parsed_bbox = _parse_bbox(bbox)

    eas = settings.eas
    filters = MapsFilters(
        tier=tier,
        time_start=time_start,
        time_end=time_end,
        min_confidence=min_confidence,
        tag=tag,
    )

    # We fetch up to ``maps_feature_limit + 1`` records so we can
    # distinguish "exactly at the limit" from "over the limit" without
    # a separate COUNT(*) — matches the pattern used by the cursor
    # pagination helpers in ``hydra.api.pagination``. The repository
    # additionally caps at ``100 * maps_tile_max_cells`` (Design §6.4)
    # when ``zoom`` is supplied.
    if zoom is None:
        fetch_limit = int(eas.maps_feature_limit) + 1
    else:
        # With aggregation we fetch more aggressively — the result
        # collapses into at most ``maps_tile_max_cells`` cells, so the
        # §6.4 cap (100 * max_cells) is the sensible ceiling.
        fetch_limit = _LIMIT_CAP_MULTIPLIER * int(eas.maps_tile_max_cells)

    records = await MapsRepository.query_bbox(
        pg_pool,
        parsed_bbox,
        filters,
        fetch_limit,
        maps_tile_max_cells=int(eas.maps_tile_max_cells),
    )

    # R12.4 — raw mode over the limit = 413 BBOX_TOO_BROAD.
    if zoom is None and len(records) > int(eas.maps_feature_limit):
        raise HydraAPIException(
            code=ErrorCode.BBOX_TOO_BROAD,
            message=(
                "bbox returned too many features; supply a zoom parameter "
                "to enable server-side aggregation"
            ),
            detail={
                "hint": "supply a zoom parameter to enable server-side aggregation",
                "feature_limit": int(eas.maps_feature_limit),
            },
            status_code=413,
        )

    if zoom is not None:
        aggregator = TileAggregator(eas.maps_aggregation_strategy)
        cells, truncated, total_cells = aggregator.aggregate(
            records,
            zoom=int(zoom),
            max_cells=int(eas.maps_tile_max_cells),
        )
        collection = FeatureCollectionResponse(
            features=[_cell_feature(c) for c in cells],
            bbox=parsed_bbox,
            aggregation=eas.maps_aggregation_strategy,
            truncated=truncated,
            total_cells=total_cells,
        )
    else:
        collection = FeatureCollectionResponse(
            features=[_raw_feature(r) for r in records],
            bbox=parsed_bbox,
            aggregation="raw",
            truncated=False,
            total_cells=None,
        )

    return APIResponse[FeatureCollectionResponse](
        data=collection,
        meta=_empty_meta(),
    )


# Keep this in sync with :mod:`hydra.eas.maps.repository`. Duplicated as
# a module-level constant so the router can compute its own fetch
# ceiling without importing the private symbol from the repository.
_LIMIT_CAP_MULTIPLIER = 100
