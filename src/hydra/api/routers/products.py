"""Products router — /api/v1/products."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query

from hydra.api.dependencies import (
    APIKeyRecord,
    get_analysis_engine,
    get_current_api_key,
    get_job_manager,
    get_pagination_params,
)
from hydra.api.errors import (
    EntityRequiredError,
    ErrorCode,
    HydraAPIException,
    InvalidTimeWindowError,
    NotFoundException,
)
from hydra.api.schemas.common import APIResponse, JobStatus, PaginationParams
from hydra.api.schemas.products import (
    GenerateProductRequest,
    ProductListResponse,
    ProductResponse,
    ProductSectionResponse,
)
from hydra.analysis.models import IntelligenceProduct, ProductParams

router = APIRouter(prefix="/products", tags=["products"])


def _product_to_response(p: Any) -> ProductResponse:
    sections = [
        ProductSectionResponse(
            section_id=s.section_id,
            title=s.title,
            section_type=s.section_type,
            content=s.content,
            records=s.records,
            correlations=s.correlations,
            confidence=s.confidence,
            order=s.order,
        )
        for s in (p.sections or [])
    ]
    return ProductResponse(
        product_id=p.product_id,
        product_type=p.product_type,
        title=p.title,
        classification=p.classification,
        generated_at=p.generated_at if isinstance(p.generated_at, str) else p.generated_at.isoformat(),
        time_window_start=p.time_window_start if isinstance(p.time_window_start, str) else p.time_window_start.isoformat(),
        time_window_end=p.time_window_end if isinstance(p.time_window_end, str) else p.time_window_end.isoformat(),
        sections=sections,
        summary=p.summary,
        key_findings=p.key_findings,
        confidence_score=p.confidence_score,
        completeness_score=p.completeness_score,
        source_tiers=p.source_tiers,
        record_count=p.record_count,
        correlation_count=p.correlation_count,
        parameters=p.parameters,
        tags=p.tags,
    )


@router.post(
    "/generate",
    status_code=202,
    response_model=APIResponse[JobStatus],
    summary="Generate an intelligence product (async)",
)
async def generate_product(
    request: GenerateProductRequest,
    engine: Any = Depends(get_analysis_engine),
    jobs: Any = Depends(get_job_manager),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[JobStatus]:
    # Validate entity_dossier requires entity
    if request.product_type == "entity_dossier" and not request.entity_id and not request.entity_name:
        raise EntityRequiredError()

    # Validate tiers
    if request.tiers:
        for t in request.tiers:
            if t < 1 or t > 28:
                raise HydraAPIException(
                    code=ErrorCode.VALIDATION_ERROR,
                    message=f"Invalid tier: {t}. Must be 1-28.",
                    status_code=422,
                )

    # Validate time window
    if request.time_window_start and request.time_window_end:
        if request.time_window_start >= request.time_window_end:
            raise InvalidTimeWindowError("time_window_start must precede time_window_end")

    job_id = await jobs.create_job()

    params = ProductParams(
        time_window_start=request.time_window_start,
        time_window_end=request.time_window_end,
        tiers=request.tiers,
        region=request.region,
        entity_id=request.entity_id,
        entity_name=request.entity_name,
        keywords=request.keywords,
        min_confidence=request.min_confidence,
        max_records=request.max_records,
        include_graph=request.include_graph,
        include_timeline=request.include_timeline,
    )

    async def _run() -> str:
        product = await engine.generate(request.product_type, params)
        return product.product_id

    await jobs.run_in_background(job_id, _run())

    job = await jobs.get_job(job_id)
    return APIResponse(data=job)


@router.get(
    "/jobs/{job_id}",
    response_model=APIResponse[JobStatus],
    summary="Check product generation job status",
)
async def get_job_status(
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
    "",
    response_model=APIResponse[ProductListResponse],
    summary="List intelligence products",
)
async def list_products(
    product_type: str | None = Query(None),
    since: str | None = Query(None, description="ISO 8601 — return products generated after this time"),
    classification: str | None = Query(None),
    tags: list[str] | None = Query(None),
    pagination: PaginationParams = Depends(get_pagination_params),
    engine: Any = Depends(get_analysis_engine),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[ProductListResponse]:
    products = await engine.list_products(
        product_type=product_type,
        since=since,
        classification=classification,
        tags=tags,
        limit=pagination.limit,
        cursor=pagination.cursor,
    )
    responses = [_product_to_response(p) for p in products]
    return APIResponse(data=ProductListResponse(products=responses))


@router.get(
    "/{product_id}",
    response_model=APIResponse[ProductResponse],
    summary="Get a single intelligence product",
)
async def get_product(
    product_id: str,
    engine: Any = Depends(get_analysis_engine),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[ProductResponse]:
    product = await engine.get_product(product_id)
    if product is None:
        raise NotFoundException(f"Product {product_id} not found")
    return APIResponse(data=_product_to_response(product))
