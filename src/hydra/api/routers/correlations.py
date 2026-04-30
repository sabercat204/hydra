"""Correlations router — /api/v1/correlations."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from hydra.api.dependencies import (
    APIKeyRecord,
    get_correlation_engine,
    get_current_api_key,
    get_job_manager,
    get_pagination_params,
)
from hydra.api.errors import (
    ErrorCode,
    HydraAPIException,
    InvalidTimeWindowError,
    NotFoundException,
)
from hydra.api.schemas.common import APIResponse, JobStatus, PaginationParams
from hydra.api.schemas.correlations import (
    CorrelationResponse,
    CorrelationRunResponse,
    RunCorrelationRequest,
)

router = APIRouter(prefix="/correlations", tags=["correlations"])


def _run_result_to_response(r: Any) -> CorrelationRunResponse:
    return CorrelationRunResponse(
        pipeline_id=r.pipeline_id,
        candidates_queried=r.candidates_queried,
        pairs_evaluated=r.pairs_evaluated,
        correlations_found=r.correlations_found,
        correlations_new=r.correlations_new,
        correlations_updated=r.correlations_updated,
        correlations_deduplicated=r.correlations_deduplicated,
        persisted_pg=r.persisted_pg,
        persisted_neo4j=r.persisted_neo4j,
        duration_ms=r.duration_ms,
        time_window_start=r.time_window_start,
        time_window_end=r.time_window_end,
        trigger_tiers=r.trigger_tiers,
    )


def _correlation_to_response(c: Any) -> CorrelationResponse:
    return CorrelationResponse(
        correlation_id=c.correlation_id,
        pipeline_id=c.pipeline_id,
        record_a_hash=c.record_a_hash,
        record_b_hash=c.record_b_hash,
        tier_a=c.tier_a,
        tier_b=c.tier_b,
        confidence=c.confidence,
        match_dimensions=c.match_dimensions,
        evidence=c.evidence,
        created_at=c.created_at,
        tags=c.tags,
    )


@router.post(
    "/run",
    status_code=202,
    response_model=APIResponse[JobStatus],
    summary="Run a correlation pipeline (async)",
)
async def run_correlation(
    request: RunCorrelationRequest,
    engine: Any = Depends(get_correlation_engine),
    jobs: Any = Depends(get_job_manager),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[JobStatus]:
    if request.time_window_start and request.time_window_end:
        if request.time_window_start >= request.time_window_end:
            raise InvalidTimeWindowError("time_window_start must precede time_window_end")

    if request.trigger_tiers:
        for t in request.trigger_tiers:
            if t < 1 or t > 28:
                raise HydraAPIException(
                    code=ErrorCode.VALIDATION_ERROR,
                    message=f"Invalid tier: {t}",
                    status_code=422,
                )

    job_id = await jobs.create_job()

    async def _run() -> str:
        result = await engine.run(
            pipeline_id=request.pipeline_id,
            time_window_start=request.time_window_start,
            time_window_end=request.time_window_end,
            trigger_tiers=request.trigger_tiers,
        )
        return result.pipeline_id

    await jobs.run_in_background(job_id, _run())
    job = await jobs.get_job(job_id)
    return APIResponse(data=job)


@router.get(
    "/jobs/{job_id}",
    response_model=APIResponse[JobStatus],
    summary="Check correlation job status",
)
async def get_correlation_job_status(
    job_id: str,
    jobs: Any = Depends(get_job_manager),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[JobStatus]:
    job = await jobs.get_job(job_id)
    if job is None:
        raise HydraAPIException(
            code=ErrorCode.JOB_NOT_FOUND,
            message="Job not found or expired",
            status_code=404,
        )
    return APIResponse(data=job)


@router.get(
    "/runs/{run_id}",
    response_model=APIResponse[CorrelationRunResponse],
    summary="Get correlation run results",
)
async def get_correlation_run(
    run_id: str,
    engine: Any = Depends(get_correlation_engine),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[CorrelationRunResponse]:
    result = await engine.get_run(run_id)
    if result is None:
        raise NotFoundException(f"Correlation run {run_id} not found")
    return APIResponse(data=_run_result_to_response(result))


@router.get(
    "",
    response_model=APIResponse[list[CorrelationResponse]],
    summary="List correlation results",
)
async def list_correlations(
    pipeline_id: str | None = Query(None),
    tier_a: int | None = Query(None),
    tier_b: int | None = Query(None),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0),
    time_start: str | None = Query(None),
    time_end: str | None = Query(None),
    pagination: PaginationParams = Depends(get_pagination_params),
    engine: Any = Depends(get_correlation_engine),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[list[CorrelationResponse]]:
    results = await engine.list_correlations(
        pipeline_id=pipeline_id,
        tier_a=tier_a,
        tier_b=tier_b,
        min_confidence=min_confidence,
        time_start=time_start,
        time_end=time_end,
        limit=pagination.limit,
        cursor=pagination.cursor,
    )
    return APIResponse(data=[_correlation_to_response(c) for c in results])


@router.get(
    "/{correlation_id}",
    response_model=APIResponse[CorrelationResponse],
    summary="Get a single correlation result",
)
async def get_correlation(
    correlation_id: str,
    engine: Any = Depends(get_correlation_engine),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[CorrelationResponse]:
    result = await engine.get_correlation(correlation_id)
    if result is None:
        raise NotFoundException(f"Correlation {correlation_id} not found")
    return APIResponse(data=_correlation_to_response(result))
