"""Previous-period comparison for trends (Design §6.5, R14.4).

When ``TrendRequest.compare_to == "previous_period"``, the Trends_Router
wants two aligned series per stream:

* ``comparison`` — same query shape, shifted back by one full window
  length. If the current window is ``[T0, T1)`` then the comparison
  window is ``[T0 - (T1 - T0), T0)``.
* ``delta`` — per-bucket ``current - comparison`` (R14.4).

:func:`compute_comparison` executes the second query against the same
:class:`TrendsService` instance and returns a :class:`TrendSeries` with
``series`` (current), ``comparison``, and ``delta`` all populated.

Alignment strategy: we pair points **by position in the sorted list**
(not by timestamp). The two queries use identical bucket widths over
identical durations, so ``series[i]`` corresponds to ``comparison[i]``.
When a series has fewer points (e.g. a gap), we pad with ``0.0`` so the
delta list is always ``len(current)`` long — matches Property 15's
delta-per-bucket requirement without losing information about missing
comparison buckets.
"""

from __future__ import annotations

from datetime import datetime

from hydra.eas.schemas.trends import (
    TrendPoint,
    TrendRequest,
    TrendSeries,
)

__all__ = ["compute_comparison"]


async def compute_comparison(
    service: "TrendsService",  # type: ignore[name-defined]  # forward ref
    request: TrendRequest,
) -> TrendSeries:
    """Execute the current + comparison queries and return a paired series.

    Parameters
    ----------
    service:
        A :class:`hydra.eas.trends.service.TrendsService` instance. The
        forward-referenced type keeps this module import-cycle-free —
        ``TrendsService`` imports from ``schemas.trends`` but not from
        this module.
    request:
        The original :class:`TrendRequest` with
        ``compare_to == "previous_period"``. The current-window query
        runs with ``request`` as-is; the comparison query runs with the
        window shifted back by one full length.

    Returns
    -------
    TrendSeries
        With:

        * ``series`` — the current-window series, keyed by stream_id.
        * ``comparison`` — the previous-period series, keyed by stream_id.
        * ``delta`` — per-bucket ``current - comparison``, keyed by
          stream_id. ``delta[i].bucket_start`` is the **current**
          bucket's start (the absolute time the client most likely
          wants to render).
    """

    # Current window
    current_response = await service.query(request)
    current_series = current_response.series.series

    # Shift the window back by its own length. If the client asked for
    # ``[T0, T1)`` we execute ``[T0 - dt, T0)`` — the immediately
    # preceding period, per R14.4.
    window = request.time_end - request.time_start
    comparison_request = request.model_copy(
        update={
            "time_start": request.time_start - window,
            "time_end": request.time_start,
            # Prevent infinite recursion — the nested query is a
            # plain point query, not another comparison.
            "compare_to": None,
        }
    )
    comparison_response = await service.query(comparison_request)
    comparison_series = comparison_response.series.series

    # Compute delta per stream. We pair by position (see module
    # docstring) and stamp each delta with the **current** bucket_start
    # so the client can render a single time axis.
    delta_series: dict[str, list[TrendPoint]] = {}
    for stream_id, current_points in current_series.items():
        comp_points = comparison_series.get(stream_id, [])
        delta_points: list[TrendPoint] = []
        max_len = max(len(current_points), len(comp_points))
        for i in range(max_len):
            current_value = current_points[i].value if i < len(current_points) else 0.0
            comp_value = comp_points[i].value if i < len(comp_points) else 0.0
            bucket_start: datetime
            if i < len(current_points):
                bucket_start = current_points[i].bucket_start
            else:
                # No current point at this position. Fall back to
                # projecting the comparison bucket forward by the
                # window length so the bucket_start is still on the
                # current-period axis.
                bucket_start = comp_points[i].bucket_start + window
            delta_points.append(
                TrendPoint(
                    bucket_start=bucket_start,
                    value=current_value - comp_value,
                )
            )
        delta_series[stream_id] = delta_points

    return TrendSeries(
        series=current_series,
        comparison=comparison_series,
        delta=delta_series,
    )
