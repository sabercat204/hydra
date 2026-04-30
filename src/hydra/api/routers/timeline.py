"""Timeline router — /api/v1/timeline."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends

from hydra.api.dependencies import (
    APIKeyRecord,
    get_current_api_key,
    get_query_layer,
    get_timeline_builder,
)
from hydra.api.errors import InvalidTimeWindowError
from hydra.api.schemas.common import APIResponse
from hydra.api.schemas.timeline import (
    EventClusterResponse,
    TimelineEventResponse,
    TimelineRequest,
    TimelineResultResponse,
)
from hydra.config import settings

router = APIRouter(prefix="/timeline", tags=["timeline"])


@router.post(
    "",
    response_model=APIResponse[TimelineResultResponse],
    summary="Build cross-tier event timeline",
)
async def build_timeline(
    request: TimelineRequest,
    query_layer: Any = Depends(get_query_layer),
    timeline: Any = Depends(get_timeline_builder),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[TimelineResultResponse]:
    if request.time_start >= request.time_end:
        raise InvalidTimeWindowError("time_start must precede time_end")

    # Check max window
    try:
        ts = datetime.fromisoformat(request.time_start)
        te = datetime.fromisoformat(request.time_end)
        max_days = settings.api.timeline_max_days
        if (te - ts) > timedelta(days=max_days):
            raise InvalidTimeWindowError(f"Time window exceeds maximum of {max_days} days")
    except InvalidTimeWindowError:
        raise
    except Exception:
        pass

    # Query records
    records = await query_layer.query_records(
        tiers=request.tiers,
        time_start=request.time_start,
        time_end=request.time_end,
        region=request.region,
        limit=request.max_events,
    )

    # Query correlations
    correlations = await query_layer.query_correlations(
        time_start=request.time_start,
        time_end=request.time_end,
    )

    # Build timeline
    result = await timeline.build(
        records=records,
        correlations=correlations,
        time_start=request.time_start,
        time_end=request.time_end,
        min_significance=request.min_significance,
        max_events=request.max_events,
    )

    events = [
        TimelineEventResponse(
            timestamp=e.timestamp,
            record_hash=e.record_hash,
            tier=e.tier,
            stream_id=e.stream_id,
            title=e.title,
            description=e.description,
            geo=e.geo,
            significance=e.significance,
            correlated_events=e.correlated_events,
        )
        for e in result.events
    ]
    clusters = [
        EventClusterResponse(
            cluster_id=c.cluster_id,
            events=c.events,
            centroid_time=c.centroid_time,
            centroid_geo=c.centroid_geo,
            tier_count=c.tier_count,
            significance=c.significance,
        )
        for c in result.clusters
    ]
    return APIResponse(
        data=TimelineResultResponse(
            events=events,
            time_window_start=result.time_window_start,
            time_window_end=result.time_window_end,
            total_events=result.total_events,
            tiers_represented=result.tiers_represented,
            clusters=clusters,
        )
    )
