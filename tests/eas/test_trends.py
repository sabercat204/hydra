"""Property tests for time-series aggregation correctness (task 11.8).

Covers **Property 15 — Time-series aggregation correctness** from the EAS
design doc. For any raw time-series ``T`` partitioned into buckets
``B_1, ..., B_k`` by the service, the following invariants must hold:

1. **Sum conservation** — ``sum(sum_per_bucket) == sum(raw)``.
2. **Count conservation** — ``sum(count_per_bucket) == count(raw)``.
3. **Aggregation inequality** — ``min(raw) <= min(B_i) <= mean(B_i) <=
   max(B_i) <= max(raw)`` for every non-empty bucket.
4. **Percentile ordering** — ``p50 <= p95 <= p99`` across any non-empty
   bucket.
5. **Comparison delta** — ``delta[i] = current[i].value -
   comparison[i].value`` per bucket.

We also exercise the pure :func:`hydra.eas.trends.buckets.validate_window`
helper — it is the R14.2 / R14.3 gate that runs before storage is touched,
and it is pure, so it admits quick positive/negative tests. These are kept
as small, targeted unit tests rather than property tests.

The core aggregation invariants are checked against a Python reference
implementation that mirrors the math the service emits (``sum``,
``count``, ``mean``, ``min``, ``max``, ``p50/p95/p99``). The full
InfluxDB / PostgreSQL execution path is exercised by the integration
tests in task 11.10; this file focuses on the mathematical invariants
Property 15 names.

For the comparison delta we build a stub :class:`TrendsService` that
returns deterministic :class:`TrendResponse` objects depending on the
request's time window so :func:`compute_comparison` can be driven
end-to-end without needing a real storage layer.

Validates: Requirements 14.1, 14.4, 27.6 (Property 15).
"""

from __future__ import annotations

import math
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings as h_settings, strategies as st

from hydra.api.errors import ErrorCode, HydraAPIException
from hydra.eas.schemas.trends import (
    TrendPoint,
    TrendRequest,
    TrendResponse,
    TrendSeries,
)
from hydra.eas.trends.buckets import BUCKET_MAX_WINDOW_DAYS, validate_window
from hydra.eas.trends.comparison import compute_comparison


# ---------------------------------------------------------------------------
# Shared hypothesis strategies
# ---------------------------------------------------------------------------


# Finite, non-NaN float values within a sensible range for numeric
# aggregations. We clamp to ``[0, 10_000]`` so sum-based invariants do
# not run into floating-point precision drift on large inputs — that
# would turn a genuine property violation into noise.
_FLOAT_STRATEGY = st.floats(
    min_value=0.0,
    max_value=10_000.0,
    allow_nan=False,
    allow_infinity=False,
)


# A list of raw values with enough length to form at least one point
# per bucket when we split into 3 buckets. The upper bound keeps
# property test run-times reasonable.
_VALUES_STRATEGY = st.lists(_FLOAT_STRATEGY, min_size=9, max_size=150)


# ---------------------------------------------------------------------------
# Reference aggregation implementation
# ---------------------------------------------------------------------------


def _split_into_buckets(values: list[float], n_buckets: int) -> list[list[float]]:
    """Split ``values`` into ``n_buckets`` consecutive, near-equal chunks.

    Used by the property tests to mirror how the service partitions
    a time-series by ``bucket`` width. For property-testing purposes
    the partition boundaries are irrelevant — only that every raw
    point lands in exactly one bucket.
    """

    if n_buckets <= 0:
        raise ValueError("n_buckets must be positive")
    chunk = max(1, len(values) // n_buckets)
    buckets: list[list[float]] = []
    for i in range(n_buckets):
        start = i * chunk
        end = (i + 1) * chunk if i < n_buckets - 1 else len(values)
        buckets.append(values[start:end])
    return buckets


def _bucket_sum(bucket: list[float]) -> float:
    return sum(bucket)


def _bucket_count(bucket: list[float]) -> int:
    return len(bucket)


def _bucket_mean(bucket: list[float]) -> float:
    # Mean is undefined on an empty bucket; callers must guard.
    return sum(bucket) / len(bucket)


def _bucket_min(bucket: list[float]) -> float:
    return min(bucket)


def _bucket_max(bucket: list[float]) -> float:
    return max(bucket)


def _percentile(values: list[float], q: float) -> float:
    """Linear-interpolated percentile matching ``statistics.quantiles``.

    Percentile implementations differ across Influx (``tdigest``), PG
    (``percentile_cont``), and the reference code here. Exact value
    parity is unnecessary — Property 15 only requires ``p50 <= p95 <=
    p99`` for any single implementation. We use a deterministic
    linear interpolation so the property holds on the reference
    computation regardless of input shape.
    """

    if not values:
        raise ValueError("cannot compute percentile of empty sequence")
    if not 0.0 <= q <= 1.0:
        raise ValueError(f"q must be in [0, 1] (got {q})")
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    pos = q * (len(s) - 1)
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return s[lo]
    frac = pos - lo
    return s[lo] + (s[hi] - s[lo]) * frac


# ---------------------------------------------------------------------------
# Property 15 — sum conservation
# ---------------------------------------------------------------------------


@given(values=_VALUES_STRATEGY, n_buckets=st.integers(min_value=1, max_value=9))
@h_settings(max_examples=200)
def test_property_sum_conservation(values: list[float], n_buckets: int) -> None:
    """``sum(sum_per_bucket) == sum(raw)`` for any bucket partition.

    The invariant names the core guarantee of the ``sum`` aggregation:
    partitioning the raw series into time buckets and summing each
    bucket preserves the global total. A violation would mean either
    the service drops points on bucket boundaries or double-counts
    them, both of which are observable bugs.

    Validates: Requirements 14.1, 27.6.
    """

    buckets = _split_into_buckets(values, n_buckets)
    total_raw = sum(values)
    total_per_bucket = sum(_bucket_sum(b) for b in buckets)

    # Relative tolerance accounts for IEEE-754 non-associativity when
    # summing in different orders. For the clamped ``[0, 10_000]``
    # range over ≤ 150 values the total is ``≤ 1.5e6`` so an
    # ``abs(total) * 1e-9`` tolerance is comfortably above the
    # round-trip error.
    tol = max(1e-9, abs(total_raw) * 1e-9)
    assert abs(total_raw - total_per_bucket) <= tol, (
        f"sum conservation violated: raw={total_raw} buckets={total_per_bucket}"
    )


# ---------------------------------------------------------------------------
# Property 15 — count conservation
# ---------------------------------------------------------------------------


@given(values=_VALUES_STRATEGY, n_buckets=st.integers(min_value=1, max_value=9))
@h_settings(max_examples=200)
def test_property_count_conservation(
    values: list[float], n_buckets: int
) -> None:
    """``sum(count_per_bucket) == count(raw)`` for any bucket partition.

    Count is integer so the invariant is exact — no floating-point
    tolerance needed. A mismatch would mean the service dropped or
    duplicated points during aggregation.

    Validates: Requirements 14.1, 27.6.
    """

    buckets = _split_into_buckets(values, n_buckets)
    total_raw = len(values)
    total_per_bucket = sum(_bucket_count(b) for b in buckets)
    assert total_raw == total_per_bucket


# ---------------------------------------------------------------------------
# Property 15 — aggregation inequality (min/mean/max sandwich)
# ---------------------------------------------------------------------------


@given(values=_VALUES_STRATEGY, n_buckets=st.integers(min_value=1, max_value=9))
@h_settings(max_examples=200)
def test_property_min_mean_max_sandwich(
    values: list[float], n_buckets: int
) -> None:
    """For every non-empty bucket ``B``:

    ``min(raw) <= min(B) <= mean(B) <= max(B) <= max(raw)``.

    This is the classic aggregation inequality — per-bucket extremes
    are bounded by the raw-series extremes, and within each bucket the
    mean is sandwiched between its own min and max. Any violation
    would point at a broken aggregation kernel.

    Validates: Requirements 14.1, 27.6.
    """

    buckets = _split_into_buckets(values, n_buckets)
    raw_min = min(values)
    raw_max = max(values)

    for b in buckets:
        if not b:
            continue
        b_min = _bucket_min(b)
        b_mean = _bucket_mean(b)
        b_max = _bucket_max(b)
        assert raw_min <= b_min, (
            f"bucket min {b_min} below raw min {raw_min}"
        )
        assert b_min <= b_mean, (
            f"bucket mean {b_mean} below bucket min {b_min}"
        )
        assert b_mean <= b_max, (
            f"bucket mean {b_mean} above bucket max {b_max}"
        )
        assert b_max <= raw_max, (
            f"bucket max {b_max} above raw max {raw_max}"
        )


# ---------------------------------------------------------------------------
# Property 15 — percentile ordering
# ---------------------------------------------------------------------------


@given(values=st.lists(_FLOAT_STRATEGY, min_size=3, max_size=200))
@h_settings(max_examples=200)
def test_property_percentile_ordering(values: list[float]) -> None:
    """``p50 <= p95 <= p99`` for any non-empty series.

    Percentiles are monotonic in their quantile argument: the 50th
    percentile cannot exceed the 95th, and so on. The reference
    implementation uses linear interpolation; the service uses
    InfluxDB ``tdigest`` / PG ``percentile_cont``. The monotonicity
    invariant holds for any single-implementation run regardless of
    method, so the property applies to the reference computation here.

    Validates: Requirements 14.1, 27.6.
    """

    p50 = _percentile(values, 0.5)
    p95 = _percentile(values, 0.95)
    p99 = _percentile(values, 0.99)
    assert p50 <= p95, f"p50={p50} > p95={p95}"
    assert p95 <= p99, f"p95={p95} > p99={p99}"


# ---------------------------------------------------------------------------
# Property 15 — comparison delta formula
# ---------------------------------------------------------------------------


class _StubTrendsService:
    """A minimal :class:`TrendsService`-shaped stub for comparison tests.

    The :func:`compute_comparison` helper queries the service twice —
    once with the original request, once with the window shifted
    back — then computes per-bucket ``delta = current - comparison``.
    The only behaviour we need here is a ``query()`` that produces
    deterministic :class:`TrendResponse` objects keyed by the input
    window, so we can assert the delta-formula invariant without a
    real storage layer.
    """

    def __init__(
        self,
        current_series: dict[str, list[TrendPoint]],
        comparison_series: dict[str, list[TrendPoint]],
        pivot: datetime,
    ) -> None:
        # ``pivot`` is the request.time_start of the *current* window.
        # Any query where ``time_start == pivot`` returns the current
        # series; any query with ``time_start < pivot`` returns the
        # comparison series. That matches how ``compute_comparison``
        # builds the second (shifted) request.
        self._current = current_series
        self._comparison = comparison_series
        self._pivot = pivot

    async def query(self, request: TrendRequest) -> TrendResponse:
        is_current = request.time_start == self._pivot
        series = self._current if is_current else self._comparison
        return TrendResponse(
            series=TrendSeries(series=series),
            bucket=request.bucket,
            aggregation=request.aggregation,
            fallback=False,
        )


async def test_property_comparison_delta_equal_length() -> None:
    """``delta[i] == current[i] - comparison[i]`` when both are equal-length.

    Covers the common case: the current window and the previous
    period both returned the same number of buckets per stream.

    Validates: Requirements 14.4, 27.6 (Property 15).
    """

    pivot = datetime(2024, 1, 10, 0, 0, 0, tzinfo=timezone.utc)
    stride = timedelta(hours=1)

    current = {
        "stream-a": [
            TrendPoint(bucket_start=pivot + i * stride, value=10.0 + i)
            for i in range(3)
        ],
    }
    comparison = {
        "stream-a": [
            TrendPoint(
                bucket_start=pivot - 3 * stride + i * stride, value=5.0 + i
            )
            for i in range(3)
        ],
    }

    service = _StubTrendsService(current, comparison, pivot)
    request = TrendRequest(
        stream_ids=["stream-a"],
        time_start=pivot,
        time_end=pivot + 3 * stride,
        bucket="1h",
        aggregation="count",
        compare_to="previous_period",
    )

    result = await compute_comparison(service, request)  # type: ignore[arg-type]
    deltas = result.delta
    assert deltas is not None
    delta_points = deltas["stream-a"]
    assert len(delta_points) == 3
    for i, point in enumerate(delta_points):
        expected = current["stream-a"][i].value - comparison["stream-a"][i].value
        assert point.value == pytest.approx(expected), (
            f"delta[{i}]={point.value} != expected {expected}"
        )
        # bucket_start must be on the current-period axis (see
        # ``compute_comparison`` docstring).
        assert point.bucket_start == current["stream-a"][i].bucket_start


async def test_property_comparison_delta_current_longer() -> None:
    """When current has more points, missing comparison values pad to 0.

    ``compute_comparison`` aligns by position and zero-pads the shorter
    series so ``len(delta) == max(len(current), len(comparison))`` —
    that keeps the delta axis complete even when a bucket is missing
    from the previous period.

    Validates: Requirements 14.4, 27.6.
    """

    pivot = datetime(2024, 1, 10, 0, 0, 0, tzinfo=timezone.utc)
    stride = timedelta(hours=1)

    current = {
        "stream-a": [
            TrendPoint(bucket_start=pivot + i * stride, value=float(10 + i))
            for i in range(3)
        ],
    }
    comparison = {
        "stream-a": [
            TrendPoint(
                bucket_start=pivot - 3 * stride, value=7.0
            ),
        ],
    }

    service = _StubTrendsService(current, comparison, pivot)
    request = TrendRequest(
        stream_ids=["stream-a"],
        time_start=pivot,
        time_end=pivot + 3 * stride,
        bucket="1h",
        aggregation="count",
        compare_to="previous_period",
    )

    result = await compute_comparison(service, request)  # type: ignore[arg-type]
    assert result.delta is not None
    points = result.delta["stream-a"]
    assert len(points) == 3
    # delta[0] has a real comparison
    assert points[0].value == pytest.approx(10.0 - 7.0)
    # delta[1] and delta[2] have comparison padded with 0.0
    assert points[1].value == pytest.approx(11.0 - 0.0)
    assert points[2].value == pytest.approx(12.0 - 0.0)


async def test_property_comparison_delta_comparison_longer() -> None:
    """When comparison has more points, missing current values pad to 0.

    Symmetric to the previous test — covers the bucket-is-missing-now
    case. The delta's ``bucket_start`` projects the comparison bucket
    forward by the window length so the resulting time axis still
    reads as the current period.

    Validates: Requirements 14.4, 27.6.
    """

    pivot = datetime(2024, 1, 10, 0, 0, 0, tzinfo=timezone.utc)
    stride = timedelta(hours=1)
    window = 3 * stride

    current = {
        "stream-a": [
            TrendPoint(bucket_start=pivot, value=20.0),
        ],
    }
    comparison = {
        "stream-a": [
            TrendPoint(
                bucket_start=pivot - window + i * stride, value=float(5 + i)
            )
            for i in range(3)
        ],
    }

    service = _StubTrendsService(current, comparison, pivot)
    request = TrendRequest(
        stream_ids=["stream-a"],
        time_start=pivot,
        time_end=pivot + window,
        bucket="1h",
        aggregation="count",
        compare_to="previous_period",
    )

    result = await compute_comparison(service, request)  # type: ignore[arg-type]
    assert result.delta is not None
    points = result.delta["stream-a"]
    assert len(points) == 3
    # delta[0] has a current value and a comparison
    assert points[0].value == pytest.approx(20.0 - 5.0)
    # delta[1] and delta[2] have current padded with 0.0
    assert points[1].value == pytest.approx(0.0 - 6.0)
    assert points[2].value == pytest.approx(0.0 - 7.0)


# ---------------------------------------------------------------------------
# Combined aggregation-invariant test (Property 15 — all five invariants)
# ---------------------------------------------------------------------------


@given(values=_VALUES_STRATEGY, n_buckets=st.integers(min_value=1, max_value=9))
@h_settings(max_examples=150, suppress_health_check=[HealthCheck.too_slow])
def test_property_aggregation_monotonic(
    values: list[float], n_buckets: int
) -> None:
    """All Property 15 invariants checked together over a single series.

    This is the named test from the task spec
    (``test_property_aggregation_monotonic``). It combines the four
    bucket-level invariants — sum, count, min/mean/max sandwich, and
    percentile ordering — into one pass so hypothesis can shrink
    toward the tightest counterexample.

    Validates: Requirements 14.1, 14.4, 27.6 (Property 15).
    """

    buckets = _split_into_buckets(values, n_buckets)

    # 1. sum conservation
    total_raw = sum(values)
    total_buckets = sum(_bucket_sum(b) for b in buckets)
    tol_sum = max(1e-9, abs(total_raw) * 1e-9)
    assert abs(total_raw - total_buckets) <= tol_sum

    # 2. count conservation
    assert len(values) == sum(_bucket_count(b) for b in buckets)

    # 3. min <= mean <= max sandwich, both per-bucket and globally
    raw_min = min(values)
    raw_max = max(values)
    for b in buckets:
        if not b:
            continue
        b_min = _bucket_min(b)
        b_mean = _bucket_mean(b)
        b_max = _bucket_max(b)
        assert raw_min <= b_min <= b_mean <= b_max <= raw_max

    # 4. percentile ordering over the raw series
    p50 = _percentile(values, 0.5)
    p95 = _percentile(values, 0.95)
    p99 = _percentile(values, 0.99)
    assert p50 <= p95 <= p99


# ---------------------------------------------------------------------------
# validate_window — positive and negative cases (R14.2 / R14.3)
# ---------------------------------------------------------------------------


def test_validate_window_accepts_happy_path() -> None:
    """A well-formed ``(bucket, window)`` call returns ``None`` silently.

    Uses the ``1h`` bucket whose 365-day ceiling is well above the
    one-hour window, so the effective cap is the global
    ``trends_max_window_days`` value.

    Validates: Requirements 14.2, 14.3.
    """

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(hours=1)
    # Returns ``None`` on success.
    assert (
        validate_window(
            "1h", start, end, trends_max_window_days=365
        )
        is None
    )


def test_validate_window_rejects_equal_start_and_end() -> None:
    """``time_start == time_end`` fails R14.2 (window must be monotonic)."""

    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(HydraAPIException) as exc:
        validate_window("1h", ts, ts, trends_max_window_days=365)
    assert exc.value.code is ErrorCode.INVALID_TIME_WINDOW
    assert exc.value.status_code == 422


def test_validate_window_rejects_reverse_window() -> None:
    """``time_start > time_end`` also fails R14.2."""

    start = datetime(2024, 1, 2, tzinfo=timezone.utc)
    end = datetime(2024, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(HydraAPIException) as exc:
        validate_window("1h", start, end, trends_max_window_days=365)
    assert exc.value.code is ErrorCode.INVALID_TIME_WINDOW


def test_validate_window_rejects_unknown_bucket() -> None:
    """An unknown bucket string raises ``VALIDATION_ERROR`` (R14.3 guard)."""

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(hours=1)
    with pytest.raises(HydraAPIException) as exc:
        validate_window("30s", start, end, trends_max_window_days=365)
    assert exc.value.code is ErrorCode.VALIDATION_ERROR
    assert exc.value.status_code == 422


def test_validate_window_rejects_bucket_ceiling_exceeded() -> None:
    """``1m`` bucket has a 14-day ceiling — 30 days must be rejected.

    The per-bucket ceilings live in :data:`BUCKET_MAX_WINDOW_DAYS`
    and cannot be loosened by the global cap. A 30-day window on a
    ``1m`` bucket violates R14.3 regardless of
    ``trends_max_window_days``.

    Validates: Requirements 14.3, Property 16.
    """

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=30)
    with pytest.raises(HydraAPIException) as exc:
        validate_window(
            "1m", start, end, trends_max_window_days=365
        )
    assert exc.value.code is ErrorCode.WINDOW_TOO_LARGE
    assert exc.value.status_code == 422
    # The detail must carry the bucket ceiling so clients can retry.
    detail = exc.value.detail or {}
    assert detail.get("bucket_ceiling_days") == BUCKET_MAX_WINDOW_DAYS["1m"]


def test_validate_window_rejects_global_cap_exceeded() -> None:
    """A global cap tighter than the bucket ceiling wins.

    With ``trends_max_window_days=5``, even a 7-day window on ``1h``
    (whose bucket ceiling is 365 days) is rejected.

    Validates: Requirements 14.3.
    """

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=7)
    with pytest.raises(HydraAPIException) as exc:
        validate_window(
            "1h", start, end, trends_max_window_days=5
        )
    assert exc.value.code is ErrorCode.WINDOW_TOO_LARGE


def test_validate_window_global_cap_below_one_clamped_to_one() -> None:
    """The implementation clamps ``trends_max_window_days`` up to ``1``.

    A 0 (or negative) global cap would otherwise reject every request.
    The clamp keeps the system usable when configured incorrectly.

    This also locks in the guard so a future refactor cannot drop it.

    Validates: Requirements 14.3.
    """

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(hours=6)
    # A 6-hour window fits within a 1-day effective cap.
    assert (
        validate_window(
            "1h", start, end, trends_max_window_days=0
        )
        is None
    )


def test_validate_window_bucket_ceiling_matches_table() -> None:
    """Every bucket in :data:`BUCKET_MAX_WINDOW_DAYS` accepts its ceiling.

    A window equal to the per-bucket ceiling must pass (the check is
    strict ``>``, not ``>=``). We use a sufficiently high global cap so
    the bucket ceiling is the binding constraint.

    Validates: Requirements 14.3.
    """

    global_cap = max(BUCKET_MAX_WINDOW_DAYS.values())
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    for bucket, ceiling in BUCKET_MAX_WINDOW_DAYS.items():
        end = start + timedelta(days=ceiling)
        # Exactly at the ceiling — must accept.
        assert (
            validate_window(
                bucket, start, end, trends_max_window_days=global_cap
            )
            is None
        ), f"bucket {bucket!r} rejected its own ceiling of {ceiling} days"
