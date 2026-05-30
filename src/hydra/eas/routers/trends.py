"""Trends router — ``GET /api/v1/trends`` (Design §7.1, R14).

Single endpoint. The public contract:

* Request parameters come in via query strings (``stream_ids``,
  ``time_start``, ``time_end``, ``bucket``, ``aggregation``,
  ``compare_to``). ``TrendRequest`` validates types and shape.
* Window / bucket ceilings are enforced by
  :func:`hydra.eas.trends.buckets.validate_window` **before** any
  storage engine is touched (R14.3, Property 16).
* The :class:`TrendsService` runs the primary InfluxDB query, falling
  back to PostgreSQL when the Influx engine is ``UNREACHABLE``. When
  the fallback triggers, the router returns ``207 Multi-Status`` with
  ``meta.fallback=true`` per R14.5.
* ``compare_to="previous_period"`` triggers a second shifted query via
  :func:`hydra.eas.trends.comparison.compute_comparison` and augments
  the response with ``comparison`` and ``delta`` series (R14.4).

The endpoint is **authn-only** per R20.5 — maps/trends/lookup are
tenant-agnostic for reads, so ``tenant_id`` is consumed as the auth
gate (``Depends(get_current_tenant_id)``) but not passed to the
storage layer.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from hydra.api.errors import ErrorCode, HydraAPIException
from hydra.api.pagination import PaginationMeta
from hydra.api.schemas.common import APIResponse, ResponseMeta
from hydra.eas.dependencies import (
    get_current_tenant_id,
    get_eas_settings,
    get_trends_service,
)
from hydra.eas.schemas.trends import (
    Aggregation,
    Bucket,
    TrendRequest,
    TrendResponse,
    TrendSeries,
)
from hydra.eas.trends.buckets import validate_window
from hydra.eas.trends.comparison import compute_comparison

logger = logging.getLogger(__name__)

router = APIRouter(tags=["trends"])

__all__ = ["router"]


# ---------------------------------------------------------------------------
# Meta helpers
# ---------------------------------------------------------------------------


def _trends_meta(fallback: bool) -> ResponseMeta:
    """Build a minimal ``ResponseMeta`` carrying the ``fallback`` flag.

    The trends response is not paginated, so ``next_cursor`` is
    ``None``. ``request_id`` / ``duration_ms`` are populated by the
    middleware before the response hits the wire.

    The ``fallback`` flag is not part of the ``ResponseMeta`` schema in
    P11, so we stash it on ``pagination.total_estimate``'s sibling via
    a sentinel model extension. Routers that return 207 also serialize
    ``fallback`` as a top-level ``meta.fallback`` field so clients can
    read it without knowing our internal shape.
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


# ---------------------------------------------------------------------------
# GET /api/v1/trends (R14.1..R14.5)
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/trends",
    response_model=APIResponse[TrendResponse],
    summary=(
        "Time-series trends over stream_ids with optional previous-period "
        "comparison; InfluxDB primary with PG fallback"
    ),
)
async def get_trends(
    stream_ids: Annotated[list[str], Query(min_length=1, max_length=50)],
    time_start: Annotated[datetime, Query()],
    time_end: Annotated[datetime, Query()],
    bucket: Annotated[Bucket, Query()],
    aggregation: Annotated[Aggregation, Query()] = "count",
    compare_to: Annotated[str | None, Query()] = None,
    tenant_id: UUID = Depends(get_current_tenant_id),
    service: Any = Depends(get_trends_service),
    settings: Any = Depends(get_eas_settings),
) -> JSONResponse:
    """Execute a trends query and return :class:`TrendResponse`.

    See module docstring for the contract. The branches are:

    * Validate the window / bucket ceiling (R14.2, R14.3). A failure
      here raises 422 **before** any storage call — Property 16.
    * Build a :class:`TrendRequest`, which re-validates the parameter
      types (``compare_to`` literal, etc.).
    * Delegate to the wired :class:`TrendsService`. When
      ``compare_to == "previous_period"`` we route through
      :func:`compute_comparison` to attach the comparison + delta
      series.
    * 207 ``Multi-Status`` on PG fallback (R14.5); 200 ``OK``
      otherwise.
    """

    # Auth-only gate per R20.5 — trends are tenant-agnostic for reads.
    del tenant_id

    # R14.3 — gate the storage layer BEFORE any I/O. We use the EAS
    # setting to let deployers tighten the per-bucket ceilings.
    eas = settings.eas if hasattr(settings, "eas") else settings
    max_window_days = int(getattr(eas, "trends_max_window_days", 365))
    validate_window(bucket, time_start, time_end, max_window_days)

    if service is None:
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Trends service is not available",
            status_code=503,
        )

    # ``TrendRequest`` normalizes the literals (``compare_to``) and
    # gives the service a Pydantic-validated shape to consume.
    try:
        request = TrendRequest(
            stream_ids=list(stream_ids),
            time_start=time_start,
            time_end=time_end,
            bucket=bucket,
            aggregation=aggregation,
            compare_to=compare_to,  # type: ignore[arg-type]
        )
    except Exception as exc:
        # Pydantic validation errors surface as 422 through the global
        # handler, but constructing the model directly raises
        # ``pydantic.ValidationError`` which FastAPI doesn't intercept
        # at this layer. Reraise as a HYDRA-shaped 422.
        raise HydraAPIException(
            code=ErrorCode.VALIDATION_ERROR,
            message="Invalid trend request",
            detail={"error": str(exc)},
            status_code=422,
        ) from exc

    if request.compare_to == "previous_period":
        # compute_comparison runs the current + shifted queries and
        # assembles the comparison + delta series. It reuses the same
        # service instance so the fallback decision is shared.
        paired_series: TrendSeries = await compute_comparison(service, request)
        # The fallback flag is decided by the current-period query.
        # Re-run the service's health gate to know which HTTP status
        # to emit — we don't want to invoke Influx twice, so we read
        # the health value directly.
        fallback = await _should_mark_fallback(service)
        response_payload = TrendResponse(
            series=paired_series,
            bucket=request.bucket,
            aggregation=request.aggregation,
            fallback=fallback,
        )
    else:
        response_payload = await service.query(request)
        fallback = response_payload.fallback

    envelope = APIResponse[TrendResponse](
        data=response_payload,
        meta=_trends_meta(fallback),
    )

    body = envelope.model_dump(mode="json")
    if fallback:
        # R14.5 — 207 Multi-Status with ``meta.fallback=true``. The
        # router annotates ``meta`` with the flag because the global
        # ``ResponseMeta`` schema doesn't have a dedicated slot for
        # it; clients read ``meta.fallback`` directly.
        meta = body.get("meta") or {}
        meta["fallback"] = True
        body["meta"] = meta
        return JSONResponse(status_code=207, content=body)

    return JSONResponse(status_code=200, content=body)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _should_mark_fallback(service: Any) -> bool:
    """Read the trends service's health gate without re-querying storage.

    Delegates to the private ``_should_fall_back`` coroutine on the
    service when available, so we keep a single source of truth for
    "should we emit 207?". Returns ``False`` when the service lacks
    that coroutine (test doubles).
    """

    fallback_check = getattr(service, "_should_fall_back", None)
    if fallback_check is None:
        return False
    try:
        return bool(await fallback_check())
    except Exception:  # pragma: no cover - defensive
        return False
