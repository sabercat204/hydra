"""Tests for ``hydra.monitoring.capacity`` — storage growth projection.

Covers:

* Task 9.3 — **Property 9: Growth Projection Correctness**
  (Requirements 12.2, 12.3, 12.4, 12.5). The pure
  :py:meth:`CapacityPlanner._project_growth` static method is driven
  directly with Hypothesis-generated snapshot histories across each
  branch of its decision tree: insufficient data, zero X-variance,
  over-threshold, non-positive slope, and the nominal
  ``(threshold - current) / rate`` path.
* Task 9.4 — **Property 10: Linear Regression Slope Direction**
  (Requirement 12.1). Strictly increasing snapshot sequences must
  produce a strictly positive growth rate, and strictly decreasing
  sequences a strictly negative one.
* Task 9.5 — Unit tests for the full collection cycle: per-engine size
  gathering (PG/ES/Influx/MinIO), snapshot persistence, retention
  cleanup, boundary behavior of days-to-threshold, per-engine failure
  isolation, and metric publication
  (Requirements 12.1–12.5, 13.1–13.7, 22.3).

All tests mock upstream dependencies — no real DB / ES / MinIO client
is instantiated. The asyncpg connection mock mirrors the
``_AcquireCtx``/``MagicMock`` pattern already used in
``tests/test_collectors.py``.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from hydra.config import MonitoringSettings
from hydra.monitoring.capacity import (
    ENGINE_ELASTICSEARCH,
    ENGINE_INFLUXDB,
    ENGINE_MINIO,
    ENGINE_POSTGRES,
    CapacityPlanner,
    ESSizeBackend,
    InfluxSizeBackend,
    MinIOSizeBackend,
)
from hydra.monitoring.capacity import (
    _ALREADY_OVER_THRESHOLD,
    _CLEANUP_SQL,
    _ES_METRIC_PREFIX,
    _INFLUX_METRIC,
    _INSERT_SNAPSHOT_SQL,
    _MINIO_METRIC_PREFIX,
    _NO_PROJECTION,
    _PG_DATABASE_METRIC,
    _PG_TABLE_METRIC_PREFIX,
)
from hydra.monitoring.metrics import (
    hydra_capacity_days_to_threshold,
    hydra_capacity_es_index_size_bytes,
    hydra_capacity_influx_bucket_size_bytes,
    hydra_capacity_minio_bucket_size_bytes,
    hydra_capacity_pg_growth_rate_bytes_per_day,
    hydra_capacity_pg_size_bytes,
    hydra_capacity_pg_table_size_bytes,
)


# ---------------------------------------------------------------------------
# Shared helpers — asyncpg mocking
# ---------------------------------------------------------------------------


def _make_pool(conn: MagicMock) -> MagicMock:
    """Wrap a mocked asyncpg connection in a pool with the ``acquire()`` CM.

    Mirrors the contract expected by :meth:`CapacityPlanner.collect`:
    ``async with pool.acquire() as conn:``. A fresh context manager is
    returned on every ``acquire()`` call so code paths that open
    multiple times within one cycle still get a valid CM each time.
    """

    class _AcquireCtx:
        async def __aenter__(self_inner):  # noqa: N805 — ctx protocol
            return conn

        async def __aexit__(self_inner, *exc):  # noqa: N805
            return False

    pool = MagicMock()
    pool.acquire = MagicMock(side_effect=lambda: _AcquireCtx())
    return pool


def _base_t0() -> datetime:
    """Deterministic anchor timestamp used by the regression tests."""
    return datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Stub backends for ES / Influx / MinIO
# ---------------------------------------------------------------------------


class _StubESBackend:
    """In-memory :class:`ESSizeBackend` returning a fixed mapping."""

    def __init__(self, sizes: dict[str, int]) -> None:
        self._sizes = sizes
        self.calls = 0

    async def fetch_index_sizes(self) -> dict[str, int]:
        self.calls += 1
        return dict(self._sizes)


class _FailingESBackend:
    """:class:`ESSizeBackend` that always raises — used for failure isolation."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc
        self.calls = 0

    async def fetch_index_sizes(self) -> dict[str, int]:
        self.calls += 1
        raise self._exc


class _StubInfluxBackend:
    """In-memory :class:`InfluxSizeBackend` returning a fixed size."""

    def __init__(self, size: int) -> None:
        self._size = size
        self.calls = 0

    async def fetch_bucket_size(self) -> int:
        self.calls += 1
        return self._size


class _StubMinIOBackend:
    """In-memory :class:`MinIOSizeBackend` returning a fixed mapping."""

    def __init__(self, sizes: dict[str, int]) -> None:
        self._sizes = sizes
        self.calls = 0

    async def fetch_bucket_sizes(self) -> dict[str, int]:
        self.calls += 1
        return dict(self._sizes)


# ---------------------------------------------------------------------------
# Property 9 — Growth Projection Correctness
# ---------------------------------------------------------------------------
#
# The ``_project_growth`` static method has five branches; the property
# test drives each of them explicitly rather than relying on one
# free-form generator. Each branch maps to one Requirement acceptance
# criterion:
#
#   * Requirement 12.2  — fewer than 3 snapshots → (0.0, -1.0)
#   * (implicit 12.2)   — zero X-variance (all same timestamp)
#                         → (0.0, -1.0)  [regression undefined]
#   * Requirement 12.3  — non-positive growth rate → days = -1.0
#   * Requirement 12.4  — current > threshold     → days = 0.0
#   * Requirement 12.5  — nominal case            → days = (thr-cur)/rate
#
# Each branch uses a unique ``@given`` decorator so a counter-example
# for one branch is reported against the branch that generated it.


@settings(
    deadline=None,
    max_examples=50,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    num_points=st.integers(min_value=0, max_value=2),
    base_size=st.integers(min_value=0, max_value=10**12),
    threshold=st.integers(min_value=1, max_value=10**12),
    current=st.integers(min_value=0, max_value=10**12),
)
def test_property9_insufficient_points_returns_no_projection(
    num_points: int,
    base_size: int,
    threshold: int,
    current: int,
) -> None:
    """**Validates: Requirement 12.2**.

    For any ``len(snapshots) < 3``, regardless of threshold/current
    values, the function returns ``(0.0, -1.0)``.
    """
    t0 = _base_t0()
    snapshots = [
        (t0 + timedelta(hours=i), base_size + i * 100) for i in range(num_points)
    ]
    rate, days = CapacityPlanner._project_growth(snapshots, threshold, current)
    assert rate == 0.0
    assert days == _NO_PROJECTION  # -1.0


@settings(
    deadline=None,
    max_examples=30,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    num_points=st.integers(min_value=3, max_value=20),
    threshold=st.integers(min_value=1, max_value=10**12),
    current=st.integers(min_value=0, max_value=10**12),
    value=st.integers(min_value=0, max_value=10**12),
)
def test_property9_zero_x_variance_returns_no_projection(
    num_points: int,
    threshold: int,
    current: int,
    value: int,
) -> None:
    """**Validates: Requirement 12.2**.

    When every snapshot shares the same timestamp, the denominator of
    the least-squares slope is zero and regression is undefined. The
    implementation must short-circuit with ``(0.0, -1.0)`` rather than
    dividing by zero.
    """
    t0 = _base_t0()
    # Same timestamp for every point, varying sizes. The Y values have
    # variance, but X does not — the branch we're testing.
    snapshots = [(t0, value + i) for i in range(num_points)]
    rate, days = CapacityPlanner._project_growth(snapshots, threshold, current)
    assert rate == 0.0
    assert days == _NO_PROJECTION


@settings(
    deadline=None,
    max_examples=40,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    num_points=st.integers(min_value=3, max_value=20),
    threshold=st.integers(min_value=1, max_value=10**9),
    # ``current`` MUST exceed ``threshold`` — we generate a positive
    # offset and add it to the threshold.
    excess=st.integers(min_value=1, max_value=10**9),
    # Slope is irrelevant per Requirement 12.4 — test with mixed signs.
    slope_sign=st.sampled_from([-1, 0, 1]),
)
def test_property9_over_threshold_returns_zero_days(
    num_points: int,
    threshold: int,
    excess: int,
    slope_sign: int,
) -> None:
    """**Validates: Requirement 12.4**.

    Whenever ``current > threshold``, days-to-threshold is ``0.0``
    regardless of whether growth is positive, zero, or negative. The
    over-threshold state always dominates the slope direction.
    """
    t0 = _base_t0()
    current = threshold + excess
    # Non-zero variance in X. Y values have a deterministic slope of
    # ``slope_sign`` bytes per hour so the rate is computable.
    snapshots = [
        (t0 + timedelta(hours=i), 1000 + slope_sign * i * 10)
        for i in range(num_points)
    ]
    rate, days = CapacityPlanner._project_growth(snapshots, threshold, current)
    assert days == _ALREADY_OVER_THRESHOLD  # 0.0
    # The returned rate is whatever regression produced — we only
    # constrain its sign against what we fed in.
    if slope_sign > 0:
        assert rate > 0
    elif slope_sign < 0:
        assert rate < 0
    else:
        assert rate == pytest.approx(0.0, abs=1e-9)


@settings(
    deadline=None,
    max_examples=40,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    num_points=st.integers(min_value=3, max_value=20),
    # Flat or decreasing series; rate will be ≤ 0.
    y_offset=st.integers(min_value=0, max_value=10**9),
    decrement=st.integers(min_value=0, max_value=10**6),
    threshold_gap=st.integers(min_value=1, max_value=10**9),
)
def test_property9_non_positive_rate_returns_no_projection(
    num_points: int,
    y_offset: int,
    decrement: int,
    threshold_gap: int,
) -> None:
    """**Validates: Requirement 12.3**.

    When the regression slope is zero or negative (storage not
    growing or shrinking), days-to-threshold is ``-1.0`` — "no
    exhaustion projected" — provided ``current <= threshold``.
    """
    t0 = _base_t0()
    # Strictly non-increasing Y with non-zero X variance.
    snapshots = [
        (t0 + timedelta(hours=i), y_offset - decrement * i)
        for i in range(num_points)
    ]
    # Ensure ``current <= threshold`` so we don't trip the
    # over-threshold short-circuit (Requirement 12.4). ``current`` is
    # the last observed value.
    current = snapshots[-1][1]
    threshold = max(current, 0) + threshold_gap  # guaranteed current <= threshold

    rate, days = CapacityPlanner._project_growth(snapshots, threshold, current)
    # Rate may be negative or zero.
    assert rate <= 0.0 + 1e-9
    assert days == _NO_PROJECTION


@settings(
    deadline=None,
    max_examples=40,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    num_points=st.integers(min_value=3, max_value=20),
    start_size=st.integers(min_value=0, max_value=10**9),
    # Strictly positive per-hour growth so the slope is guaranteed > 0.
    increment=st.integers(min_value=1, max_value=10**6),
    # Threshold is set *above* the latest observed size so
    # ``current < threshold`` holds and we hit the nominal branch.
    headroom=st.integers(min_value=1, max_value=10**9),
)
def test_property9_nominal_case_matches_closed_form(
    num_points: int,
    start_size: int,
    increment: int,
    headroom: int,
) -> None:
    """**Validates: Requirement 12.5**.

    On the nominal branch (≥3 points, non-zero X variance, positive
    rate, current ≤ threshold), days-to-threshold equals the
    closed-form ``(threshold - current) / rate``.
    """
    t0 = _base_t0()
    snapshots = [
        (t0 + timedelta(hours=i), start_size + increment * i)
        for i in range(num_points)
    ]
    current = snapshots[-1][1]
    threshold = current + headroom

    rate, days = CapacityPlanner._project_growth(snapshots, threshold, current)
    assert rate > 0
    # Closed-form sanity check: days * rate == threshold - current.
    expected = (threshold - current) / rate
    assert days == pytest.approx(expected, rel=1e-9, abs=1e-9)


# ---------------------------------------------------------------------------
# Property 10 — Linear Regression Slope Direction
# ---------------------------------------------------------------------------


# Strategy: generate a strictly monotonic list of distinct integers. We
# sort a set of unique values so the output is naturally ordered.
def _monotonic_increasing_values(min_n: int = 3, max_n: int = 20):
    return st.lists(
        st.integers(min_value=0, max_value=10**9),
        min_size=min_n,
        max_size=max_n,
        unique=True,
    ).map(sorted)


def _monotonic_decreasing_values(min_n: int = 3, max_n: int = 20):
    return _monotonic_increasing_values(min_n, max_n).map(
        lambda xs: list(reversed(xs))
    )


@settings(
    deadline=None,
    max_examples=50,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(values=_monotonic_increasing_values())
def test_property10_strictly_increasing_produces_positive_rate(
    values: list[int],
) -> None:
    """**Validates: Requirement 12.1**.

    Monotonically increasing snapshots (distinct values) must produce
    a strictly positive least-squares slope. The threshold and current
    are set such that neither the over-threshold (12.4) nor
    non-positive-rate (12.3) branch is taken, so the returned rate is
    the raw regression slope.
    """
    t0 = _base_t0()
    snapshots = [(t0 + timedelta(hours=i), v) for i, v in enumerate(values)]
    current = snapshots[-1][1]
    threshold = current + 1  # ensures current < threshold

    rate, _days = CapacityPlanner._project_growth(snapshots, threshold, current)
    assert rate > 0, f"expected positive rate for strictly increasing {values}, got {rate}"


@settings(
    deadline=None,
    max_examples=50,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(values=_monotonic_decreasing_values())
def test_property10_strictly_decreasing_produces_negative_rate(
    values: list[int],
) -> None:
    """**Validates: Requirement 12.1**.

    Monotonically decreasing snapshots (distinct values) must produce
    a strictly negative least-squares slope. We verify the raw slope
    sign rather than the days-to-threshold sentinel, which is already
    covered by Property 9's non-positive-rate test.
    """
    t0 = _base_t0()
    snapshots = [(t0 + timedelta(hours=i), v) for i, v in enumerate(values)]
    current = snapshots[-1][1]
    threshold = current + 1  # avoid over-threshold branch

    rate, _days = CapacityPlanner._project_growth(snapshots, threshold, current)
    assert rate < 0, f"expected negative rate for strictly decreasing {values}, got {rate}"


# ===========================================================================
# Task 9.5 — Unit tests
# ===========================================================================


# ---------------------------------------------------------------------------
# Pure math — boundary & sanity cases for ``_project_growth``
# ---------------------------------------------------------------------------


def test_project_growth_exact_boundary_current_equals_threshold() -> None:
    """When ``current == threshold`` exactly, we are NOT over-threshold.

    Requirement 12.4 specifies ``current > threshold``, so the boundary
    case falls through to the normal rate-based calculation. With a
    positive slope and zero headroom, days-to-threshold is exactly 0.
    """
    t0 = _base_t0()
    snapshots = [(t0 + timedelta(hours=i), 1000 + i * 100) for i in range(5)]
    current = snapshots[-1][1]
    # threshold == current → headroom is 0, not negative.
    threshold = current

    rate, days = CapacityPlanner._project_growth(snapshots, threshold, current)
    assert rate > 0
    # Division is (threshold - current) / rate == 0 / rate == 0.0.
    # This matches the literal value of _ALREADY_OVER_THRESHOLD but is
    # arrived at via the nominal branch, not the short-circuit.
    assert days == pytest.approx(0.0, abs=1e-9)


def test_project_growth_known_slope_and_intercept() -> None:
    """Regression on a perfectly linear series recovers the expected slope.

    Given y = 1000 + 200*x_days (i.e. 200 bytes/day), the recovered
    growth rate must be exactly 200 (within float tolerance). Using
    per-day x coordinates keeps the arithmetic exact.
    """
    t0 = _base_t0()
    # Sample once per day for 7 days; y grows by 200 bytes per day.
    snapshots = [(t0 + timedelta(days=i), 1000 + 200 * i) for i in range(7)]
    current = snapshots[-1][1]  # 1000 + 200*6 = 2200
    threshold = current + 1000  # plenty of headroom

    rate, days = CapacityPlanner._project_growth(snapshots, threshold, current)
    assert rate == pytest.approx(200.0, rel=1e-9)
    # (threshold - current) / rate == 1000 / 200 == 5.0 days.
    assert days == pytest.approx(5.0, rel=1e-9)


def test_project_growth_min_data_points_exactly_three() -> None:
    """Exactly three snapshots is the minimum for a projection.

    With two points we get ``(0.0, -1.0)``; with three the regression
    runs. This boundary lines up with Requirement 12.2 which specifies
    "fewer than 3 data points" as the short-circuit condition.
    """
    t0 = _base_t0()
    # 2 points → no projection.
    two_points = [(t0, 100), (t0 + timedelta(hours=1), 200)]
    rate2, days2 = CapacityPlanner._project_growth(two_points, 10_000, 200)
    assert rate2 == 0.0 and days2 == _NO_PROJECTION

    # 3 points → regression runs.
    three_points = two_points + [(t0 + timedelta(hours=2), 300)]
    rate3, days3 = CapacityPlanner._project_growth(three_points, 10_000, 300)
    assert rate3 > 0
    assert days3 > 0


# ---------------------------------------------------------------------------
# PG / ES / MinIO / Influx size collection
# ---------------------------------------------------------------------------


async def test_pg_sizes_collection_via_mocked_asyncpg() -> None:
    """PG size collection calls ``pg_database_size`` then per-table sizes.

    Validates Requirement 13.1.
    """
    # Mock fetchrow to return different rows depending on the SQL.
    db_size = 50 * 1024**3  # 50 GB
    table_sizes = {
        "normalized_records": 20 * 1024**3,
        "correlation_results": 5 * 1024**3,
        "intelligence_products": 1 * 1024**3,
    }

    async def _fetchrow(sql: str, *args: Any) -> Any:
        stripped = sql.strip().upper()
        if "PG_DATABASE_SIZE" in stripped:
            return {"size_bytes": db_size}
        if "PG_TOTAL_RELATION_SIZE" in stripped:
            table = args[0]
            size = table_sizes.get(table)
            if size is None:
                return None  # ``to_regclass`` returned NULL → skip
            return {"size_bytes": size}
        return None

    conn = MagicMock()
    conn.fetchrow = AsyncMock(side_effect=_fetchrow)
    conn.fetch = AsyncMock(return_value=[])
    conn.execute = AsyncMock(return_value="DELETE 0")
    conn.executemany = AsyncMock(return_value=None)

    pool = _make_pool(conn)
    settings_obj = MonitoringSettings()
    planner = CapacityPlanner(pg_pool=pool, settings=settings_obj, interval=3600.0)

    sizes = await planner._collect_pg_sizes()

    assert sizes[_PG_DATABASE_METRIC] == db_size
    for table, value in table_sizes.items():
        assert sizes[f"{_PG_TABLE_METRIC_PREFIX}{table}"] == value


async def test_pg_sizes_collection_skips_missing_tables() -> None:
    """Tables that return NULL from ``to_regclass`` are silently omitted.

    Validates Requirement 13.1 and the hardening note in
    ``_collect_pg_sizes`` docstring.
    """
    async def _fetchrow(sql: str, *args: Any) -> Any:
        if "PG_DATABASE_SIZE" in sql.upper():
            return {"size_bytes": 1000}
        # Every per-table query returns None (missing table).
        return None

    conn = MagicMock()
    conn.fetchrow = AsyncMock(side_effect=_fetchrow)

    pool = _make_pool(conn)
    planner = CapacityPlanner(
        pg_pool=pool, settings=MonitoringSettings(), interval=3600.0
    )
    sizes = await planner._collect_pg_sizes()

    # Only the database size is populated; no per-table entries.
    assert sizes == {_PG_DATABASE_METRIC: 1000}


async def test_es_sizes_collection_via_stub_backend() -> None:
    """ES size collection delegates to the :class:`ESSizeBackend` protocol.

    Validates Requirement 13.2.
    """
    raw_sizes = {"hydra-records": 10_000_000, "hydra-correlations": 2_500_000}
    backend = _StubESBackend(raw_sizes)
    planner = CapacityPlanner(
        pg_pool=_make_pool(MagicMock()),
        settings=MonitoringSettings(),
        es_backend=backend,
        interval=3600.0,
    )

    result = await planner._collect_es_sizes(backend)

    assert result == {
        f"{_ES_METRIC_PREFIX}hydra-records": 10_000_000,
        f"{_ES_METRIC_PREFIX}hydra-correlations": 2_500_000,
    }
    assert backend.calls == 1


async def test_influx_sizes_collection_via_stub_backend() -> None:
    """InfluxDB size collection returns a single-key dict under ``_INFLUX_METRIC``.

    Validates Requirement 13.3.
    """
    backend = _StubInfluxBackend(size=7_500_000_000)
    planner = CapacityPlanner(
        pg_pool=_make_pool(MagicMock()),
        settings=MonitoringSettings(),
        influx_backend=backend,
        interval=3600.0,
    )

    result = await planner._collect_influx_sizes(backend)

    assert result == {_INFLUX_METRIC: 7_500_000_000}
    assert backend.calls == 1


async def test_minio_sizes_collection_via_stub_backend() -> None:
    """MinIO size collection delegates to the :class:`MinIOSizeBackend` protocol.

    Validates Requirement 13.4.
    """
    raw_sizes = {"hydra-raw": 100 * 1024**3, "hydra-products": 25 * 1024**3}
    backend = _StubMinIOBackend(raw_sizes)
    planner = CapacityPlanner(
        pg_pool=_make_pool(MagicMock()),
        settings=MonitoringSettings(),
        minio_backend=backend,
        interval=3600.0,
    )

    result = await planner._collect_minio_sizes(backend)

    assert result == {
        f"{_MINIO_METRIC_PREFIX}hydra-raw": 100 * 1024**3,
        f"{_MINIO_METRIC_PREFIX}hydra-products": 25 * 1024**3,
    }
    assert backend.calls == 1


# ---------------------------------------------------------------------------
# End-to-end ``collect()`` behavior
# ---------------------------------------------------------------------------


def _make_cycle_conn(
    *,
    db_size: int = 50 * 1024**3,
    history_pg: list[tuple[datetime, int]] | None = None,
) -> MagicMock:
    """Build a mocked asyncpg connection sufficient for a full collect() cycle.

    The connection exposes:
    * ``fetchrow`` — returns ``pg_database_size`` and per-table sizes
    * ``fetch``    — returns empty history by default (or ``history_pg``
                     when the SQL targets the PG database metric)
    * ``executemany`` — records the persistence call
    * ``execute``    — records the retention cleanup call
    """
    history_pg = history_pg or []

    async def _fetchrow(sql: str, *args: Any) -> Any:
        stripped = sql.strip().upper()
        if "PG_DATABASE_SIZE" in stripped:
            return {"size_bytes": db_size}
        if "PG_TOTAL_RELATION_SIZE" in stripped:
            # All per-table lookups miss in this fixture — keeps the test
            # focused on the database-level gauge.
            return None
        return None

    async def _fetch(sql: str, *args: Any) -> Any:
        # ``_fetch_history`` → filters by engine + metric_name.
        # ``_fetch_history_aggregated`` → no metric_name arg (only 2).
        if _PG_DATABASE_METRIC in args:
            return [
                {"collected_at": ts, "value_bytes": value}
                for ts, value in history_pg
            ]
        return []

    conn = MagicMock()
    conn.fetchrow = AsyncMock(side_effect=_fetchrow)
    conn.fetch = AsyncMock(side_effect=_fetch)
    conn.execute = AsyncMock(return_value="DELETE 0")
    conn.executemany = AsyncMock(return_value=None)
    return conn


async def test_collect_publishes_metrics_per_engine() -> None:
    """One full cycle updates gauges for every configured engine.

    Validates Requirements 13.7 and (indirectly) 13.1–13.4.
    """
    db_size = 40 * 1024**3
    es_sizes = {"idx_a": 1_000_000, "idx_b": 2_000_000}
    influx_size = 500_000_000
    minio_sizes = {"bucket_x": 10_000_000}

    conn = _make_cycle_conn(db_size=db_size)
    pool = _make_pool(conn)
    planner = CapacityPlanner(
        pg_pool=pool,
        settings=MonitoringSettings(),
        es_backend=_StubESBackend(es_sizes),
        influx_backend=_StubInfluxBackend(influx_size),
        minio_backend=_StubMinIOBackend(minio_sizes),
        interval=3600.0,
    )

    await planner.collect()

    # PG size gauge
    assert hydra_capacity_pg_size_bytes._value.get() == float(db_size)
    # ES per-index gauges
    assert (
        hydra_capacity_es_index_size_bytes.labels(index="idx_a")._value.get()
        == 1_000_000.0
    )
    assert (
        hydra_capacity_es_index_size_bytes.labels(index="idx_b")._value.get()
        == 2_000_000.0
    )
    # Influx aggregate gauge
    assert hydra_capacity_influx_bucket_size_bytes._value.get() == float(
        influx_size
    )
    # MinIO per-bucket gauge
    assert (
        hydra_capacity_minio_bucket_size_bytes.labels(bucket="bucket_x")._value.get()
        == 10_000_000.0
    )


async def test_snapshot_persistence_insert_sql_called() -> None:
    """``_persist_snapshots`` batches all rows into one ``executemany`` call.

    Validates Requirement 13.5.
    """
    conn = _make_cycle_conn(db_size=1000)
    pool = _make_pool(conn)

    planner = CapacityPlanner(
        pg_pool=pool,
        settings=MonitoringSettings(),
        es_backend=_StubESBackend({"idx_a": 500}),
        influx_backend=_StubInfluxBackend(700),
        minio_backend=_StubMinIOBackend({"bucket_x": 900}),
        interval=3600.0,
    )

    await planner.collect()

    # Persistence happened exactly once, using the canonical INSERT SQL.
    conn.executemany.assert_awaited_once()
    insert_sql, rows = conn.executemany.await_args.args
    assert insert_sql == _INSERT_SNAPSHOT_SQL

    # All four engines contributed rows. Check presence of each.
    engines_seen = {row[0] for row in rows}
    assert engines_seen == {
        ENGINE_POSTGRES,
        ENGINE_ELASTICSEARCH,
        ENGINE_INFLUXDB,
        ENGINE_MINIO,
    }

    # Row tuples are (engine, metric_name, value_bytes).
    # Spot-check one: PG database row.
    pg_rows = [r for r in rows if r[0] == ENGINE_POSTGRES]
    assert (ENGINE_POSTGRES, _PG_DATABASE_METRIC, 1000) in pg_rows


async def test_retention_cleanup_uses_retention_setting() -> None:
    """``_cleanup_old_snapshots`` calls ``execute(_CLEANUP_SQL, days)``.

    Validates Requirement 13.6. The parameter must be the configured
    ``capacity_history_retention_days`` setting — not a hard-coded 90.
    """
    conn = _make_cycle_conn()
    pool = _make_pool(conn)

    # Use a non-default retention to ensure the planner actually reads
    # it from settings rather than defaulting.
    custom_retention = 45
    settings_obj = MonitoringSettings(capacity_history_retention_days=custom_retention)
    planner = CapacityPlanner(pg_pool=pool, settings=settings_obj, interval=3600.0)

    await planner.collect()

    conn.execute.assert_awaited_once_with(_CLEANUP_SQL, custom_retention)


async def test_growth_projection_published_when_history_present() -> None:
    """When history is present, the PG growth-rate gauge reflects the slope.

    Combines Requirements 12.1 and 13.7 — the regression output ends up
    in ``hydra_capacity_pg_growth_rate_bytes_per_day``.
    """
    # 7 days of perfectly linear growth at 1 MB/day.
    t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    history = [
        (t0 + timedelta(days=i), 100_000_000 + 1_000_000 * i) for i in range(7)
    ]
    # The "current" size the planner reads from PG must match the
    # latest history value so the days-to-threshold branch is nominal.
    current = history[-1][1]
    conn = _make_cycle_conn(db_size=current, history_pg=history)
    pool = _make_pool(conn)

    settings_obj = MonitoringSettings()
    planner = CapacityPlanner(pg_pool=pool, settings=settings_obj, interval=3600.0)

    await planner.collect()

    # 1 MB/day == 1_000_000 bytes per day (within float tolerance).
    rate = hydra_capacity_pg_growth_rate_bytes_per_day._value.get()
    assert rate == pytest.approx(1_000_000.0, rel=1e-6)

    # days_to_threshold should be (threshold - current) / rate. With
    # default threshold 100 GB >> current ~106 MB, the result is > 0.
    days = hydra_capacity_days_to_threshold.labels(
        engine=ENGINE_POSTGRES
    )._value.get()
    assert days > 0


async def test_per_engine_failure_isolation_pg_still_publishes() -> None:
    """A failing ES backend does not prevent PG gauges from publishing.

    Validates Requirement 22.3.
    """
    db_size = 7 * 1024**3
    conn = _make_cycle_conn(db_size=db_size)
    pool = _make_pool(conn)

    # ES raises; InfluxDB and MinIO succeed; PG succeeds.
    failing_es = _FailingESBackend(ConnectionError("ES down"))

    planner = CapacityPlanner(
        pg_pool=pool,
        settings=MonitoringSettings(),
        es_backend=failing_es,
        influx_backend=_StubInfluxBackend(123_000),
        minio_backend=_StubMinIOBackend({"b1": 456_000}),
        interval=3600.0,
    )

    # Must not raise — the per-engine try/except inside ``collect()``
    # absorbs the failure.
    await planner.collect()

    # PG gauge published despite ES failure.
    assert hydra_capacity_pg_size_bytes._value.get() == float(db_size)
    # Influx still reported.
    assert hydra_capacity_influx_bucket_size_bytes._value.get() == 123_000.0
    # MinIO still reported.
    assert (
        hydra_capacity_minio_bucket_size_bytes.labels(bucket="b1")._value.get()
        == 456_000.0
    )
    # ES backend was invoked exactly once (the failing call).
    assert failing_es.calls == 1


async def test_collect_without_pg_skips_persistence_gracefully() -> None:
    """When PG itself is unavailable, no snapshot rows are persisted.

    The cycle still returns cleanly and ES/Influx/MinIO metrics are
    published for dashboards (Requirement 22.3).
    """
    # Pool whose acquire() raises on entry to simulate total PG outage
    # for both the size-collection path and the persistence path.
    class _FailingPool:
        def acquire(self_inner):  # noqa: N805
            raise ConnectionError("pg down")

    pool = _FailingPool()

    planner = CapacityPlanner(
        pg_pool=pool,  # type: ignore[arg-type]
        settings=MonitoringSettings(),
        es_backend=_StubESBackend({"idx": 777}),
        influx_backend=_StubInfluxBackend(888),
        minio_backend=_StubMinIOBackend({"b": 999}),
        interval=3600.0,
    )

    # Must not raise.
    await planner.collect()

    # ES / Influx / MinIO gauges still updated even though PG failed.
    assert (
        hydra_capacity_es_index_size_bytes.labels(index="idx")._value.get() == 777.0
    )
    assert hydra_capacity_influx_bucket_size_bytes._value.get() == 888.0
    assert (
        hydra_capacity_minio_bucket_size_bytes.labels(bucket="b")._value.get()
        == 999.0
    )


async def test_collect_with_all_backends_none_publishes_only_pg() -> None:
    """Without optional backends, only PG metrics are published.

    This is the "PG-only development environment" path described in the
    capacity module docstring. Per Requirement 13.7 the PG gauge is
    still updated.
    """
    db_size = 123_000_000
    conn = _make_cycle_conn(db_size=db_size)
    pool = _make_pool(conn)

    planner = CapacityPlanner(
        pg_pool=pool,
        settings=MonitoringSettings(),
        # No ES, Influx, or MinIO backends.
        interval=3600.0,
    )

    await planner.collect()

    assert hydra_capacity_pg_size_bytes._value.get() == float(db_size)
    # executemany was called with only PG rows.
    insert_sql, rows = conn.executemany.await_args.args
    engines_seen = {row[0] for row in rows}
    assert engines_seen == {ENGINE_POSTGRES}
