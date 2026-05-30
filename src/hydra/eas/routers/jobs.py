"""Jobs progress router — ``GET /api/v1/jobs/{job_id}/progress`` (Design §7.1, R15).

Adds a progress-focused view to P11's :class:`JobManager` without
modifying the pre-existing ``/api/v1/products/jobs/{job_id}`` or
``/api/v1/correlations/jobs/{job_id}`` endpoints (R15.5). All three
paths share a single :class:`JobManager` instance wired via
``Depends(get_job_manager)``, so a job created by any router is
readable from any other.

The response body is :class:`JobProgressResponse`:

* ``progress_ratio = progress_current / progress_total`` when
  ``progress_total > 0``; ``None`` otherwise (R15.3).
* 404 ``JOB_NOT_FOUND`` when the Redis record is missing or expired
  (R15.4).

This endpoint is authn-only — ``Depends(get_current_tenant_id)`` is
consumed to validate the API key; tenant scoping of jobs is deferred
to a later iteration because P11 jobs are shared across tenants.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Path

from hydra.api.dependencies import get_job_manager
from hydra.api.errors import ErrorCode, HydraAPIException
from hydra.api.pagination import PaginationMeta
from hydra.api.schemas.common import APIResponse, JobStatus, ResponseMeta
from hydra.eas.dependencies import get_current_tenant_id
from hydra.eas.schemas.trends import JobProgressResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["jobs"])

__all__ = ["router"]


def _jobs_meta() -> ResponseMeta:
    """Minimal, pagination-free :class:`ResponseMeta` for a single-object body."""

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


def _parse_iso(value: str) -> datetime:
    """Parse the ISO-8601 timestamps stored on :class:`JobStatus`.

    :class:`JobManager` stores ``created_at`` / ``updated_at`` as
    ``datetime.now(timezone.utc).isoformat()``. We re-parse them here
    so the response schema can emit them as ``datetime`` objects
    (``JobProgressResponse`` declares them as such).
    """

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _compute_ratio(current: int | None, total: int | None) -> float | None:
    """Return ``current / total`` when ``total > 0``, else ``None``.

    R15.3 — the ratio is only well-defined when ``progress_total`` is
    strictly positive. We also clip the result to ``[0.0, 1.0]`` so a
    bogus ``current > total`` doesn't leak an out-of-range ratio into
    the response (Property 19).
    """

    if current is None or total is None or total <= 0:
        return None
    raw = float(current) / float(total)
    if raw < 0.0:
        return 0.0
    if raw > 1.0:
        return 1.0
    return raw


def _to_progress_response(job: JobStatus) -> JobProgressResponse:
    """Build :class:`JobProgressResponse` from a raw :class:`JobStatus`."""

    return JobProgressResponse(
        job_id=job.job_id,
        status=job.status,
        progress_current=job.progress_current,
        progress_total=job.progress_total,
        progress_ratio=_compute_ratio(job.progress_current, job.progress_total),
        eta_seconds=job.eta_seconds,
        created_at=_parse_iso(job.created_at),
        updated_at=_parse_iso(job.updated_at),
    )


@router.get(
    "/api/v1/jobs/{job_id}/progress",
    response_model=APIResponse[JobProgressResponse],
    summary="Extended progress view for a long-running job",
)
async def get_job_progress(
    job_id: Annotated[str, Path()],
    tenant_id: UUID = Depends(get_current_tenant_id),
    jobs: Any = Depends(get_job_manager),
) -> APIResponse[JobProgressResponse]:
    """Return the current :class:`JobProgressResponse` for ``job_id``.

    Raises 404 ``JOB_NOT_FOUND`` when the Redis TTL has expired or the
    job was never created (R15.4). The ``JobManager`` instance is
    shared with the pre-existing product/correlation job endpoints so
    a job created via ``POST /api/v1/products/generate`` (or
    ``POST /api/v1/cves/correlate``) is readable here (R15.5).
    """

    # Auth-only — the job manager doesn't enforce tenant scoping yet
    # (P11 jobs are tenant-agnostic), so the dependency is consumed
    # purely to validate the ``X-API-Key`` header. Keeping the gate
    # here means switching to tenant-scoped jobs in a later phase
    # is a drop-in change that doesn't require a new dependency.
    del tenant_id

    if jobs is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Job manager is not available",
            status_code=503,
        )

    job = await jobs.get_job(job_id)
    if job is None:
        raise HydraAPIException(
            code=ErrorCode.JOB_NOT_FOUND,
            message="Job not found or expired",
            status_code=404,
        )

    return APIResponse[JobProgressResponse](
        data=_to_progress_response(job),
        meta=_jobs_meta(),
    )
