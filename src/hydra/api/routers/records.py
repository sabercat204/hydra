"""Records router — /api/v1/records."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Query

from hydra.api.dependencies import (
    APIKeyRecord,
    get_current_api_key,
    get_pagination_params,
    get_query_layer,
    get_registry,
)
from hydra.api.errors import (
    ErrorCode,
    HydraAPIException,
    InvalidTimeWindowError,
)
from hydra.api.schemas.common import APIResponse, PaginationParams
from hydra.api.schemas.records import RecordResponse, RecordsByTierResponse

router = APIRouter(prefix="/records", tags=["records"])


def _record_to_response(r: Any) -> RecordResponse:
    geo = None
    if r.geo is not None:
        geo = r.geo if isinstance(r.geo, dict) else r.geo.model_dump() if hasattr(r.geo, "model_dump") else {"type": r.geo.type, "coordinates": r.geo.coordinates}
    ts = r.timestamp.isoformat() if isinstance(r.timestamp, datetime) else str(r.timestamp)
    ingested = r.ingested_at.isoformat() if isinstance(r.ingested_at, datetime) else str(r.ingested_at)
    sm = r.source_meta if isinstance(r.source_meta, dict) else r.source_meta.model_dump() if hasattr(r.source_meta, "model_dump") else {}
    return RecordResponse(
        stream_id=r.stream_id,
        tier=int(r.tier),
        timestamp=ts,
        geo=geo,
        payload=r.payload,
        source_meta=sm,
        raw_hash=r.raw_hash,
        ingested_at=ingested,
        confidence=r.confidence,
        tags=r.tags,
    )


@router.get(
    "",
    response_model=APIResponse[RecordsByTierResponse],
    summary="Query records by tier, time, region, confidence",
)
async def query_records(
    tiers: list[int] | None = Query(None),
    time_start: str | None = Query(None),
    time_end: str | None = Query(None),
    region: str | None = Query(None),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0),
    limit: int = Query(1000, ge=1, le=10_000),
    pagination: PaginationParams = Depends(get_pagination_params),
    query_layer: Any = Depends(get_query_layer),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[RecordsByTierResponse]:
    if time_start and time_end and time_start >= time_end:
        raise InvalidTimeWindowError("time_start must precede time_end")

    records = await query_layer.query_records(
        tiers=tiers,
        time_start=time_start,
        time_end=time_end,
        region=region,
        min_confidence=min_confidence,
        limit=limit,
    )
    # Group by tier
    by_tier: dict[int, list[RecordResponse]] = {}
    for r in records:
        resp = _record_to_response(r)
        by_tier.setdefault(resp.tier, []).append(resp)
    total = sum(len(v) for v in by_tier.values())
    return APIResponse(data=RecordsByTierResponse(records=by_tier, total=total))


@router.get(
    "/timeseries",
    response_model=APIResponse[dict[str, list[dict]]],
    summary="Query time-series data",
)
async def query_timeseries(
    stream_ids: list[str] = Query(...),
    time_start: str = Query(...),
    time_end: str = Query(...),
    aggregation: str = Query("raw"),
    fields: list[str] | None = Query(None),
    query_layer: Any = Depends(get_query_layer),
    registry: Any = Depends(get_registry),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[dict[str, list[dict]]]:
    if time_start >= time_end:
        raise InvalidTimeWindowError("time_start must precede time_end")

    valid_aggs = {"raw", "1m", "5m", "1h", "1d"}
    if aggregation not in valid_aggs:
        raise HydraAPIException(
            code=ErrorCode.VALIDATION_ERROR,
            message=f"Invalid aggregation: {aggregation}. Must be one of {valid_aggs}",
            status_code=422,
        )

    result = await query_layer.query_timeseries(
        stream_ids=stream_ids,
        time_start=time_start,
        time_end=time_end,
        aggregation=aggregation,
        fields=fields,
    )
    return APIResponse(data=result)


@router.get(
    "/search",
    response_model=APIResponse[list[RecordResponse]],
    summary="Full-text search across records",
)
async def search_text(
    query: str = Query(..., min_length=1, max_length=1000),
    tiers: list[int] | None = Query(None),
    time_start: str | None = Query(None),
    time_end: str | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    pagination: PaginationParams = Depends(get_pagination_params),
    query_layer: Any = Depends(get_query_layer),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[list[RecordResponse]]:
    records = await query_layer.search_text(
        query=query,
        tiers=tiers,
        time_start=time_start,
        time_end=time_end,
        limit=limit,
    )
    responses = [_record_to_response(r) for r in records]
    return APIResponse(data=responses)
