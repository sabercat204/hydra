"""Assets router — six endpoints under ``/api/v1`` (Design §7.1, R1/R2/R4).

Endpoints:

+---------+------------------------------------------+--------------------+
| Method  | Path                                     | Returns            |
+=========+==========================================+====================+
| POST    | ``/api/v1/assets``                       | 201 (or 200 idem)  |
+---------+------------------------------------------+--------------------+
| GET     | ``/api/v1/assets``                       | paged              |
+---------+------------------------------------------+--------------------+
| GET     | ``/api/v1/assets/{asset_id}``            | single             |
+---------+------------------------------------------+--------------------+
| DELETE  | ``/api/v1/assets/{asset_id}``            | 204                |
+---------+------------------------------------------+--------------------+
| GET     | ``/api/v1/assets/{asset_id}/exposures`` | paged              |
+---------+------------------------------------------+--------------------+
| GET     | ``/api/v1/exposures``                    | paged              |
+---------+------------------------------------------+--------------------+

All endpoints depend on :func:`get_current_tenant_id` — repository
queries are tenant-scoped (R20.3). Cross-tenant access returns a 404
NOT_FOUND rather than 403 so that the existence of foreign rows is not
disclosed (R2.4 / R20.4).

The 201-vs-200 distinction for idempotent POSTs uses the ``xmax = 0``
flag returned by :meth:`AssetRepository.upsert` — the canonical PG
pattern. A genuinely new row gets 201; an idempotent re-upsert of an
existing row gets 200.

Pagination uses :class:`APIResponse` with the ``meta.pagination`` slot
populated by :func:`hydra.api.pagination.build_paged_response`. The
``data`` shape for list endpoints is ``list[AssetResponse]`` /
``list[ExposureResponse]`` — the ``PagedResponse[T]`` wrapper mentioned
in the design lives in ``meta``, consistent with other HYDRA routers.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response
from fastapi.responses import JSONResponse

from hydra.api.errors import ErrorCode, HydraAPIException, NotFoundException
from hydra.api.pagination import PaginationMeta
from hydra.api.schemas.common import APIResponse, PaginationParams, ResponseMeta
from hydra.eas.assets.normalizer import normalize_asset_value
from hydra.eas.assets.repository import AssetRepository, ExposureRepository
from hydra.eas.dependencies import (
    get_asset_repository,
    get_current_tenant_id,
    get_exposure_repository,
)
from hydra.eas.schemas.assets import (
    AssetCreate,
    AssetResponse,
    AssetType,
    ExposureResponse,
    ExposureSeverity,
)

logger = logging.getLogger(__name__)


router = APIRouter(tags=["assets"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _asset_to_response(asset: Any) -> AssetResponse:
    """Adapt the internal :class:`Asset` dataclass to the public schema.

    The internal model stores ``asset_type`` as a plain string to keep
    repository rows cheap; the response model demands the
    :class:`AssetType` enum. ``AssetType(asset.asset_type)`` is the
    guaranteed-stable round-trip because the DDL CHECK constraint on
    ``assets.asset_type`` is identical to the enum values.
    """

    return AssetResponse(
        asset_id=asset.asset_id,
        tenant_id=asset.tenant_id,
        asset_type=AssetType(asset.asset_type),
        value=asset.raw_value,
        normalized_value=asset.normalized_value,
        is_active=asset.is_active,
        capture_screenshots=asset.capture_screenshots,
        notes=asset.notes,
        created_at=asset.created_at,
        deactivated_at=asset.deactivated_at,
    )


def _exposure_to_response(ev: Any) -> ExposureResponse:
    return ExposureResponse(
        exposure_id=ev.exposure_id,
        asset_id=ev.asset_id,
        record_hash=ev.record_hash,
        tier=int(ev.tier),
        matched_indicator=ev.matched_indicator,
        severity=ExposureSeverity(ev.severity),
        created_at=ev.created_at,
    )


def _paged_meta(next_cursor: str | None) -> ResponseMeta:
    """Build a minimal ``ResponseMeta`` carrying pagination info.

    The rest of the ``ResponseMeta`` fields (``request_id``, ``duration_ms``)
    are populated by the app-wide middleware; we only fill the pagination
    slot here. Because the middleware assigns a fresh ``ResponseMeta`` at
    the app boundary, the empty strings used here are overwritten before
    the response hits the wire.
    """

    return ResponseMeta(
        request_id="",
        timestamp="",
        duration_ms=0.0,
        pagination=PaginationMeta(
            next_cursor=next_cursor,
            has_more=next_cursor is not None,
            total_estimate=None,
        ),
    )


# ---------------------------------------------------------------------------
# POST /api/v1/assets — create / idempotent upsert (R1)
# ---------------------------------------------------------------------------


@router.post(
    "/api/v1/assets",
    response_model=APIResponse[AssetResponse],
    summary="Register or idempotently re-fetch an asset for the caller's tenant",
)
async def create_asset(
    body: AssetCreate,
    tenant_id: UUID = Depends(get_current_tenant_id),
    repo: AssetRepository = Depends(get_asset_repository),
) -> Response:
    """Create (or idempotently re-fetch) an asset.

    The ``AssetCreate`` Pydantic validator already rejects malformed values
    with a 422 ``VALIDATION_ERROR`` (R1.2). Here we:

    1. Enforce the per-tenant quota from
       ``EASSettings.asset_quota_per_tenant`` (R1.4). We check **before**
       the upsert so that a duplicate submission doesn't waste the check —
       if the upsert ends up being an UPDATE, the count didn't go up.
    2. Compute the canonical normalized value and hand it to the
       repository alongside the raw body.
    3. Map the ``was_new`` flag to 201 vs 200 (R1.3).
    """

    # Quota check (R1.4). A tenant at their cap is still allowed to
    # re-submit an already-registered asset (idempotent update path):
    # only **new** rows consume quota.
    from hydra.eas.dependencies import get_eas_settings

    settings = await get_eas_settings()
    quota = settings.eas.asset_quota_per_tenant

    try:
        normalized_value = normalize_asset_value(body.asset_type, body.value)
    except ValueError as exc:
        # The AssetCreate validator already guards bad values, so
        # reaching here is either a library bug or a slipped edge case.
        raise HydraAPIException(
            code=ErrorCode.VALIDATION_ERROR,
            message=str(exc),
            status_code=422,
        )

    active_count = await repo.count_active(tenant_id)
    if active_count >= quota:
        existing = await repo.get_active_by_key(
            tenant_id, body.asset_type.value, normalized_value
        )
        if existing is None:
            raise HydraAPIException(
                code=ErrorCode.ASSET_QUOTA_EXCEEDED,
                message="Per-tenant asset quota exceeded",
                status_code=409,
            )

    result = await repo.upsert(tenant_id, body, normalized_value)

    envelope = APIResponse[AssetResponse](data=_asset_to_response(result.asset))
    status_code = 201 if result.was_new else 200
    return JSONResponse(
        status_code=status_code,
        content=envelope.model_dump(mode="json"),
    )


# ---------------------------------------------------------------------------
# GET /api/v1/assets — paged listing (R2.1, R2.2)
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/assets",
    response_model=APIResponse[list[AssetResponse]],
    summary="List the caller's active assets",
)
async def list_assets(
    asset_type: Annotated[AssetType | None, Query()] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    tenant_id: UUID = Depends(get_current_tenant_id),
    repo: AssetRepository = Depends(get_asset_repository),
) -> APIResponse[list[AssetResponse]]:
    # ``PaginationParams`` would also work but the repository method
    # takes kwargs for cursor / limit directly, so we unpack manually.
    _ = PaginationParams(cursor=cursor, limit=limit)  # validates shape
    rows, next_cursor = await repo.list_active(
        tenant_id,
        asset_type=asset_type.value if asset_type is not None else None,
        cursor=cursor,
        limit=limit,
    )
    return APIResponse[list[AssetResponse]](
        data=[_asset_to_response(r) for r in rows],
        meta=_paged_meta(next_cursor),
    )


# ---------------------------------------------------------------------------
# GET /api/v1/assets/{asset_id} — single (R2.1, R2.4)
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/assets/{asset_id}",
    response_model=APIResponse[AssetResponse],
    summary="Fetch a single asset by id (tenant-scoped)",
)
async def get_asset(
    asset_id: UUID,
    tenant_id: UUID = Depends(get_current_tenant_id),
    repo: AssetRepository = Depends(get_asset_repository),
) -> APIResponse[AssetResponse]:
    asset = await repo.get(tenant_id, asset_id)
    if asset is None:
        # R2.4 / R20.4 — never disclose the existence of another
        # tenant's row; 404 NOT_FOUND is the policy.
        raise NotFoundException(f"Asset {asset_id} not found")
    return APIResponse[AssetResponse](data=_asset_to_response(asset))


# ---------------------------------------------------------------------------
# DELETE /api/v1/assets/{asset_id} — soft-delete (R2.3)
# ---------------------------------------------------------------------------


@router.delete(
    "/api/v1/assets/{asset_id}",
    status_code=204,
    summary="Soft-delete an asset (sets is_active = FALSE)",
)
async def delete_asset(
    asset_id: UUID,
    tenant_id: UUID = Depends(get_current_tenant_id),
    repo: AssetRepository = Depends(get_asset_repository),
) -> Response:
    updated = await repo.deactivate(tenant_id, asset_id)
    if not updated:
        # Treat "already deactivated" as NOT_FOUND too — the asset is
        # not currently active, so from the tenant's perspective it
        # does not exist for listing / exposure purposes.
        raise NotFoundException(f"Asset {asset_id} not found")
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# GET /api/v1/assets/{asset_id}/exposures (R4.1, R4.2, R4.3)
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/assets/{asset_id}/exposures",
    response_model=APIResponse[list[ExposureResponse]],
    summary="List exposures for a single asset (tenant-scoped)",
)
async def list_asset_exposures(
    asset_id: UUID,
    severity: Annotated[list[ExposureSeverity] | None, Query()] = None,
    since: Annotated[datetime | None, Query()] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    tenant_id: UUID = Depends(get_current_tenant_id),
    asset_repo: AssetRepository = Depends(get_asset_repository),
    exposure_repo: ExposureRepository = Depends(get_exposure_repository),
) -> APIResponse[list[ExposureResponse]]:
    # Verify the asset belongs to the caller before hitting the exposure
    # table. This double-check gives a clean 404 for cross-tenant
    # asset_ids (R2.4) rather than "you see nothing" which could be
    # confused with "no exposures yet".
    asset = await asset_repo.get(tenant_id, asset_id)
    if asset is None:
        raise NotFoundException(f"Asset {asset_id} not found")

    severity_values = (
        [s.value for s in severity] if severity else None
    )
    rows, next_cursor = await exposure_repo.list_for_asset(
        asset_id,
        tenant_id,
        severity=severity_values,
        since=since,
        cursor=cursor,
        limit=limit,
    )
    return APIResponse[list[ExposureResponse]](
        data=[_exposure_to_response(r) for r in rows],
        meta=_paged_meta(next_cursor),
    )


# ---------------------------------------------------------------------------
# GET /api/v1/exposures — tenant-wide (R4.4)
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/exposures",
    response_model=APIResponse[list[ExposureResponse]],
    summary="List exposures across all assets owned by the caller",
)
async def list_tenant_exposures(
    severity: Annotated[list[ExposureSeverity] | None, Query()] = None,
    since: Annotated[datetime | None, Query()] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    tenant_id: UUID = Depends(get_current_tenant_id),
    repo: ExposureRepository = Depends(get_exposure_repository),
) -> APIResponse[list[ExposureResponse]]:
    severity_values = (
        [s.value for s in severity] if severity else None
    )
    rows, next_cursor = await repo.list_for_tenant(
        tenant_id,
        severity=severity_values,
        since=since,
        cursor=cursor,
        limit=limit,
    )
    return APIResponse[list[ExposureResponse]](
        data=[_exposure_to_response(r) for r in rows],
        meta=_paged_meta(next_cursor),
    )


__all__ = ["router"]
