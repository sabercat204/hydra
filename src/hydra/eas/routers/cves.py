"""CVEs router — detail / search / affected-assets / correlate (Design §7.1).

Four endpoints under ``/api/v1``:

* ``GET /api/v1/cves/{cve_id}`` — 422 on malformed id, 404 on miss,
  ``APIResponse[CVEDetailResponse]`` otherwise (R11.1, R11.2).
* ``GET /api/v1/cves/search`` — paged :class:`CVESearchResult` across
  the ``hydra-cves`` ES index with ``vendor``, ``product``,
  ``min_cvss``, ``kev_only``, ``published_after``, and
  ``published_before`` filters (R11.3).
* ``GET /api/v1/cves/{cve_id}/affected-assets`` — tenant-scoped list of
  :class:`AssetResponse`. Depends on :func:`get_current_tenant_id`
  (R11.4, R20.3).
* ``POST /api/v1/cves/correlate`` — 202 ``JobStatus``; schedules a
  one-shot CVE correlation run on the correlation engine (R11.6,
  R21.2).

The expensive-tier rate limit and the ``cve_correlations_per_day`` cost
quota are wired by task 15 — this module intentionally stops short of
depending on them so the router is already import-able under task 9.
"""

from __future__ import annotations

import logging
import re
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query
from fastapi.responses import JSONResponse

from hydra.api.dependencies import (
    get_correlation_engine,
    get_db_pool,
    get_job_manager,
)
from hydra.api.errors import ErrorCode, HydraAPIException, NotFoundException
from hydra.api.pagination import PaginationMeta
from hydra.api.schemas.common import APIResponse, JobStatus, ResponseMeta
from hydra.eas.cves.repository import CVERepository
from hydra.eas.dependencies import get_current_tenant_id, get_es_client
from hydra.eas.schemas.assets import AssetResponse, AssetType
from hydra.eas.schemas.cves import (
    CVEDetailResponse,
    CVESearchParams,
    CVESearchResult,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["cves"])

__all__ = ["router"]


# Same regex used by :class:`CVEDetailResponse` — we run it at the router
# boundary so malformed ids produce 422 ``VALIDATION_ERROR`` without
# hitting ES (R11.2).
_CVE_ID_RE = re.compile(r"^CVE-\d{4}-\d{4,7}$")


def _empty_meta(next_cursor: str | None = None) -> ResponseMeta:
    """Minimal ``ResponseMeta`` with pagination populated."""

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


def _asset_to_response(asset: Any) -> AssetResponse:
    """Adapt a :class:`hydra.eas.assets.models.Asset` to the public schema.

    Duplicated from ``routers/assets.py`` to avoid a cross-router import;
    the mapping is narrow enough that keeping it local reads better than
    coupling the two routers through a shared helper module.
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


def _validate_cve_id(cve_id: str) -> None:
    """R11.2 — reject malformed CVE ids at the router boundary."""

    if not _CVE_ID_RE.match(cve_id):
        raise HydraAPIException(
            code=ErrorCode.VALIDATION_ERROR,
            message="cve_id must match CVE-YYYY-NNNN[...]",
            status_code=422,
        )


# ---------------------------------------------------------------------------
# GET /api/v1/cves/{cve_id} — detail (R11.1, R11.2)
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/cves/{cve_id}",
    response_model=APIResponse[CVEDetailResponse],
    summary="Fetch a CVE's merged NVD+EPSS+KEV view",
)
async def get_cve(
    cve_id: Annotated[str, Path()],
    es: Any = Depends(get_es_client),
) -> APIResponse[CVEDetailResponse]:
    _validate_cve_id(cve_id)

    if es is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="CVE index is not available",
            status_code=503,
        )

    detail = await CVERepository.get_cve_detail(es, cve_id)
    if detail is None:
        raise NotFoundException(f"CVE {cve_id} not found")

    return APIResponse[CVEDetailResponse](data=detail, meta=_empty_meta())


# ---------------------------------------------------------------------------
# GET /api/v1/cves/search — paged search (R11.3)
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/cves/search",
    response_model=APIResponse[list[CVESearchResult]],
    summary="Paged CVE search across vendor/product/CVSS/KEV filters",
)
async def search_cves(
    vendor: Annotated[str | None, Query()] = None,
    product: Annotated[str | None, Query()] = None,
    min_cvss: Annotated[float | None, Query(ge=0.0, le=10.0)] = None,
    kev_only: Annotated[bool, Query()] = False,
    published_after: Annotated[Any | None, Query()] = None,
    published_before: Annotated[Any | None, Query()] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    es: Any = Depends(get_es_client),
) -> APIResponse[list[CVESearchResult]]:
    if es is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="CVE index is not available",
            status_code=503,
        )

    # ``CVESearchParams`` re-validates the filter set (vendors & products
    # survive as raw strings; min_cvss / published_* enforce types).
    params = CVESearchParams(
        vendor=vendor,
        product=product,
        min_cvss=min_cvss,
        kev_only=kev_only,
        published_after=published_after,
        published_before=published_before,
    )

    rows, next_cursor = await CVERepository.search_cves(
        es, params, cursor=cursor, limit=int(limit)
    )
    return APIResponse[list[CVESearchResult]](
        data=rows, meta=_empty_meta(next_cursor)
    )


# ---------------------------------------------------------------------------
# GET /api/v1/cves/{cve_id}/affected-assets — tenant-scoped (R11.4, R20.3)
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/cves/{cve_id}/affected-assets",
    response_model=APIResponse[list[AssetResponse]],
    summary="Tenant-owned assets affected by a given CVE",
)
async def list_affected_assets(
    cve_id: Annotated[str, Path()],
    tenant_id: UUID = Depends(get_current_tenant_id),
    pg_pool: Any = Depends(get_db_pool),
) -> APIResponse[list[AssetResponse]]:
    _validate_cve_id(cve_id)

    if pg_pool is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Database is not available",
            status_code=503,
        )

    assets = await CVERepository.list_affected_assets(pg_pool, cve_id, tenant_id)
    return APIResponse[list[AssetResponse]](
        data=[_asset_to_response(a) for a in assets],
        meta=_empty_meta(),
    )


# ---------------------------------------------------------------------------
# POST /api/v1/cves/correlate — 202 JobStatus (R11.6, R21.2)
# ---------------------------------------------------------------------------


@router.post(
    "/api/v1/cves/correlate",
    response_model=APIResponse[JobStatus],
    status_code=202,
    summary="Schedule an on-demand CVE correlation run (expensive tier)",
)
async def schedule_cve_correlation(
    engine: Any = Depends(get_correlation_engine),
    jobs: Any = Depends(get_job_manager),
    tenant_id: UUID = Depends(get_current_tenant_id),
) -> JSONResponse:
    # The tenant_id dependency is kept even though the CVE pipeline is
    # not tenant-scoped (R20.5) — it enforces authentication, which is
    # required before enqueueing an expensive-tier job.
    del tenant_id

    if jobs is None or engine is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Correlation engine is not available",
            status_code=503,
        )

    job_id = await jobs.create_job()

    async def _run() -> str:
        result = await engine.run(pipeline_id="cve_correlation")
        # ``JobManager.run_in_background`` stores this string as the
        # ``result_id`` field; we surface the pipeline_id rather than an
        # opaque run_id because the correlation engine doesn't persist
        # run ids for on-demand invocations.
        return result.pipeline_id

    await jobs.run_in_background(job_id, _run())
    job = await jobs.get_job(job_id)
    if job is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Job disappeared after creation",
            status_code=503,
        )
    envelope = APIResponse[JobStatus](data=job, meta=_empty_meta())
    return JSONResponse(
        status_code=202, content=envelope.model_dump(mode="json")
    )
