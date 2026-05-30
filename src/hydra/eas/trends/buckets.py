"""Trends bucket / window validation (Design §3.6, R14.2, R14.3).

Every ``/api/v1/trends`` request must declare a bucket in
``{"1m","5m","15m","1h","6h","1d","7d"}`` and a ``(time_start, time_end)``
window. Two separate invariants guard the storage layer:

* **Monotonic window.** ``time_start < time_end`` — R14.2.
* **Per-bucket ceiling.** The window length must not exceed the
  per-bucket ceiling from Design §3.6 *or* the global
  ``trends_max_window_days`` cap (R14.3, Property 16). Whichever is
  tighter wins; both are applied.

The ceilings live in :data:`BUCKET_MAX_WINDOW_DAYS`. They are fixed at
spec time — tenants cannot raise them via config — so the safety
ceiling from the §3.6 table is always enforced. The global
``trends_max_window_days`` setting can only **lower** the effective cap
(per-bucket ``min(ceiling, global_cap)``).

:func:`validate_window` is a pure function — no I/O, no clock reads —
so routers can call it before touching any storage engine. That is
mandatory for R14.3 ("SHALL NOT execute any storage query") and for
Property 16. The router in ``src/hydra/eas/routers/trends.py`` takes
this as a precondition.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Final, Mapping

from hydra.api.errors import ErrorCode, HydraAPIException

__all__ = [
    "BUCKET_MAX_WINDOW_DAYS",
    "validate_window",
]


# Per-bucket window ceilings from Design §3.6. The per-bucket point cap
# is derived from ``ceiling_days * bucket_seconds`` and kept conservative
# so /trends responses never blow past a few-dozen-kilobyte payload.
#
# These values are the hard ceiling. The ``trends_max_window_days``
# setting can only *lower* the effective cap per bucket at runtime.
BUCKET_MAX_WINDOW_DAYS: Final[Mapping[str, int]] = {
    "1m": 14,
    "5m": 60,
    "15m": 120,
    "1h": 365,
    "6h": 730,
    "1d": 1825,
    "7d": 3650,
}


def validate_window(
    bucket: str,
    time_start: datetime,
    time_end: datetime,
    trends_max_window_days: int,
) -> None:
    """Validate a ``(bucket, time_start, time_end)`` trend request.

    Raises :class:`HydraAPIException` on failure; returns ``None`` on
    success so callers can use it as an assertion-style gate.

    Parameters
    ----------
    bucket:
        One of the literals in :data:`BUCKET_MAX_WINDOW_DAYS`. Unknown
        buckets raise ``422 VALIDATION_ERROR`` — the Pydantic
        ``TrendRequest`` model should have caught this earlier, but
        defensive validation keeps this function safe to call on raw
        strings.
    time_start, time_end:
        The requested window. Must satisfy ``time_start < time_end``
        per R14.2.
    trends_max_window_days:
        The global cap from ``EASSettings.trends_max_window_days``.
        The effective ceiling is ``min(bucket_ceiling, global_cap)``
        so tenants can only tighten the bucket-specific limit, never
        loosen it.

    Raises
    ------
    HydraAPIException
        * ``INVALID_TIME_WINDOW`` (422) when ``time_start >= time_end``
          (R14.2).
        * ``VALIDATION_ERROR`` (422) when ``bucket`` is not a known
          literal.
        * ``WINDOW_TOO_LARGE`` (422) when the window exceeds the
          per-bucket ceiling or the global cap (R14.3, Property 16).
    """

    # R14.2 — monotonic window. This check comes first because the
    # cap check below depends on a positive timedelta.
    if time_start >= time_end:
        raise HydraAPIException(
            code=ErrorCode.INVALID_TIME_WINDOW,
            message="time_start must precede time_end",
            detail={
                "time_start": time_start.isoformat(),
                "time_end": time_end.isoformat(),
            },
            status_code=422,
        )

    if bucket not in BUCKET_MAX_WINDOW_DAYS:
        # Defense in depth — ``TrendRequest.bucket`` is a Literal, so
        # FastAPI/Pydantic should already have rejected bad values.
        # Raising 422 here means a direct call path (e.g. from the
        # TrendsService) still gets a useful error.
        raise HydraAPIException(
            code=ErrorCode.VALIDATION_ERROR,
            message=f"unsupported bucket: {bucket!r}",
            detail={
                "bucket": bucket,
                "allowed": sorted(BUCKET_MAX_WINDOW_DAYS),
            },
            status_code=422,
        )

    bucket_ceiling_days = BUCKET_MAX_WINDOW_DAYS[bucket]
    # The global cap can only *tighten* the per-bucket ceiling — a
    # higher value is clamped. ``max(1, ...)`` guards against config
    # mishaps that would otherwise reject every request.
    effective_ceiling_days = min(bucket_ceiling_days, max(1, trends_max_window_days))

    window = time_end - time_start
    ceiling = timedelta(days=effective_ceiling_days)
    if window > ceiling:
        raise HydraAPIException(
            code=ErrorCode.WINDOW_TOO_LARGE,
            message=(
                f"window of {window.total_seconds() / 86400:.2f} days exceeds "
                f"the maximum of {effective_ceiling_days} days for bucket {bucket!r}"
            ),
            detail={
                "bucket": bucket,
                "window_days": window.total_seconds() / 86400,
                "max_window_days": effective_ceiling_days,
                "bucket_ceiling_days": bucket_ceiling_days,
                "trends_max_window_days": trends_max_window_days,
            },
            status_code=422,
        )
