"""Health router — /api/v1/health."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from hydra.api.dependencies import (
    APIKeyRecord,
    get_backpressure_monitor,
    get_current_api_key,
    get_scheduler_health,
)
from hydra.api.schemas.common import APIResponse
from hydra.api.schemas.health import (
    BackpressureResponse,
    EngineBackpressureResponse,
    SchedulerHealthResponse,
)

router = APIRouter(prefix="/health", tags=["health"])


def _bp_to_response(bp: Any) -> BackpressureResponse:
    engines = {}
    for name, ebp in bp.engines.items():
        engines[name] = EngineBackpressureResponse(
            engine=ebp.engine,
            queue_depth=ebp.queue_depth,
            soft_limit=ebp.soft_limit,
            hard_limit=ebp.hard_limit,
            state=ebp.state,
        )
    return BackpressureResponse(
        overall=bp.overall,
        engines=engines,
        checked_at=bp.checked_at,
    )


def _health_to_response(h: Any) -> SchedulerHealthResponse:
    bp = _bp_to_response(h.backpressure)
    storage = {}
    for name, sh in h.storage_health.items():
        storage[name] = {"engine": sh.engine, "status": sh.status, "latency_ms": sh.latency_ms} if hasattr(sh, "engine") else sh
    adapter = {}
    for name, ah in h.adapter_health.items():
        adapter[name] = ah if isinstance(ah, dict) else {"status": str(ah)}
    return SchedulerHealthResponse(
        status=h.status,
        active_adapters=h.active_adapters,
        active_by_cadence=h.active_by_cadence,
        backpressure=bp,
        storage_health=storage,
        adapter_health=adapter,
        dead_streams=h.dead_streams,
        checked_at=h.checked_at,
    )


@router.get(
    "",
    response_model=APIResponse[SchedulerHealthResponse],
    summary="Get overall system health",
)
async def get_health(
    health: Any = Depends(get_scheduler_health),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[SchedulerHealthResponse]:
    result = await health.check()
    return APIResponse(data=_health_to_response(result))


@router.get(
    "/backpressure",
    response_model=APIResponse[BackpressureResponse],
    summary="Get backpressure state per storage engine",
)
async def get_backpressure(
    bp: Any = Depends(get_backpressure_monitor),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[BackpressureResponse]:
    result = await bp.check()
    return APIResponse(data=_bp_to_response(result))


@router.get(
    "/streams/dead",
    response_model=APIResponse[list[str]],
    summary="List dead streams",
)
async def get_dead_streams(
    health: Any = Depends(get_scheduler_health),
    api_key: APIKeyRecord = Depends(get_current_api_key),
) -> APIResponse[list[str]]:
    result = await health.check()
    return APIResponse(data=result.dead_streams)


@router.get(
    "/ping",
    tags=["health"],
    summary="Liveness probe",
    include_in_schema=False,
)
async def ping() -> dict:
    """Unauthenticated liveness probe."""
    return {"status": "ok"}
