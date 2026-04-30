"""Tests for ``hydra.monitoring.collectors.BaseCollector`` error isolation.

Covers Task 3.3 — **Property 2: Collector Error Isolation** (Requirements
4.2 and 22.1):

    A collector whose ``collect()`` method raises arbitrary exceptions
    must not crash the background loop. Every raised exception must
    increment :data:`~hydra.monitoring.metrics.COLLECTOR_ERRORS` for that
    collector exactly once, and subsequent successful cycles must still
    run.

An additional invariant is verified alongside the property: the loop
must **not** swallow :class:`asyncio.CancelledError` — cooperative
cancellation must propagate so that
``MonitoringContext.shutdown()`` can tear down tasks deterministically.

Later tasks (4.5) will add concrete-collector tests to this file; the
current file intentionally covers only the base class.
"""

from __future__ import annotations

import asyncio

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from hydra.monitoring.collectors import BaseCollector
from hydra.monitoring.metrics import COLLECTOR_ERRORS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _current_error_count(collector_name: str) -> float:
    """Return the current ``COLLECTOR_ERRORS`` counter value for a collector.

    Reading ``_value.get()`` directly on the child metric is the canonical
    way to inspect a counter without mutating it; the counter is a
    process-wide singleton so we measure deltas rather than attempting to
    reset it between test cases.
    """
    return COLLECTOR_ERRORS.labels(collector=collector_name)._value.get()


class _ScriptedCollector(BaseCollector):
    """Collector whose ``collect()`` replays a scripted sequence of actions.

    Each entry in ``script`` is either ``None`` (success, return cleanly)
    or an exception **instance** to raise. The collector records how many
    ``collect()`` calls have occurred in ``calls`` and stops itself after
    the script is exhausted.
    """

    def __init__(self, script: list[BaseException | None], interval: float = 0.0) -> None:
        super().__init__(interval=interval)
        self._script = script
        self.calls = 0

    async def collect(self) -> None:
        idx = self.calls
        self.calls += 1
        action = self._script[idx]
        # Last scripted step: signal the loop to stop after this iteration
        # completes (whether this step raises or returns cleanly). This
        # keeps ``calls == len(script)`` deterministically.
        if idx == len(self._script) - 1:
            self._running = False
        if action is None:
            return
        raise action


async def _run_collector(collector: _ScriptedCollector, timeout: float = 5.0) -> None:
    """Start the collector and await its orderly completion.

    The scripted collector sets ``self._running = False`` on its last
    scripted step, so after the loop finishes its final
    ``asyncio.sleep(0)`` the task completes naturally. We simply await
    that, with a safety-net timeout.
    """
    task = await collector.start()
    try:
        await asyncio.wait_for(task, timeout=timeout)
    except asyncio.TimeoutError:  # pragma: no cover — safety net
        task.cancel()
        raise


# ---------------------------------------------------------------------------
# Property 2 — Collector Error Isolation
# ---------------------------------------------------------------------------


# The set of exception classes the property test samples from. We keep
# this small and well-known so Hypothesis generates diverse counter-
# examples quickly.
_EXCEPTION_CLASSES: tuple[type[Exception], ...] = (
    ValueError,
    RuntimeError,
    KeyError,
    ZeroDivisionError,
    ConnectionError,
    TimeoutError,
)


@settings(
    deadline=None,
    max_examples=20,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    exception_class=st.sampled_from(_EXCEPTION_CLASSES),
    raise_count=st.integers(min_value=1, max_value=5),
    trailing_success=st.booleans(),
)
def test_collector_error_isolation(
    exception_class: type[Exception],
    raise_count: int,
    trailing_success: bool,
) -> None:
    """**Validates: Requirements 4.2, 22.1**.

    For any exception class and any number of raises in ``[1, 5]``, the
    loop must:

    1. Complete every scripted iteration without propagating the
       exception to the task runner.
    2. Increment ``COLLECTOR_ERRORS{collector=<class>}`` by exactly
       ``raise_count`` — never more, never fewer.
    3. Continue to run a trailing successful iteration if one is
       scheduled after the failures.
    """
    # Use a fresh subclass per Hypothesis example so each example gets a
    # distinct ``collector`` label value and cannot interfere with
    # another example's counter reading.
    collector_cls = type(
        f"PropCollector_{exception_class.__name__}_{raise_count}_{int(trailing_success)}",
        (_ScriptedCollector,),
        {},
    )
    collector_name = collector_cls.__name__

    script: list[BaseException | None] = [exception_class("boom") for _ in range(raise_count)]
    if trailing_success:
        script.append(None)

    before = _current_error_count(collector_name)
    collector = collector_cls(script=script, interval=0.0)

    asyncio.run(_run_collector(collector))

    after = _current_error_count(collector_name)

    # (2) Counter delta equals the number of raised exceptions.
    assert after - before == float(raise_count), (
        f"expected {raise_count} error-counter increment(s) for "
        f"{exception_class.__name__}, got {after - before}"
    )
    # (1) + (3) Every scripted iteration ran — i.e., raises did not
    # terminate the loop, and a trailing success executed if scheduled.
    assert collector.calls == len(script), (
        f"expected {len(script)} collect() call(s), got {collector.calls}"
    )


# ---------------------------------------------------------------------------
# Companion invariant — CancelledError must propagate
# ---------------------------------------------------------------------------


class _CancellingCollector(BaseCollector):
    """Collector that raises :class:`asyncio.CancelledError` on first call."""

    def __init__(self) -> None:
        super().__init__(interval=0.0)
        self.calls = 0

    async def collect(self) -> None:
        self.calls += 1
        raise asyncio.CancelledError


async def test_collector_loop_does_not_swallow_cancelled_error() -> None:
    """``asyncio.CancelledError`` from ``collect()`` must propagate.

    If this invariant breaks, ``MonitoringContext.shutdown()`` would no
    longer be able to stop collectors via task cancellation. The error
    counter must **not** be incremented for cancellation — cancellation
    is not a collector failure.
    """
    collector = _CancellingCollector()
    collector_name = collector.__class__.__name__
    before = _current_error_count(collector_name)

    task = await collector.start()

    with pytest.raises(asyncio.CancelledError):
        await task

    after = _current_error_count(collector_name)
    assert after == before, "CancelledError must not increment COLLECTOR_ERRORS"
    assert collector.calls == 1, "collect() should have been invoked exactly once"


# ===========================================================================
# Task 4.5 — Concrete collector tests
# ===========================================================================
#
# These tests exercise each concrete collector
# (SchedulerCollector, StorageCollector, APICollector, PipelineCollector)
# with fully mocked upstream dependencies. For each collector we verify:
#
#   1. ``collect()`` updates the correct Prometheus metrics with the
#      values derived from the mocked upstream responses.
#   2. A smoke lifecycle test — ``start()`` schedules the background
#      loop, ``stop()`` flags it to exit, and task cancellation tears
#      the loop down without leaking exceptions into the test runner.
#
# Error-isolation (Requirement 4.2, 22.1) is already covered
# exhaustively by Property 2 above, so concrete collectors don't
# duplicate that here — we only assert that the *wiring* between each
# collector and the base class works.
#
# Requirements covered by this section: 5.1–5.7, 6.1–6.5, 7.1–7.2,
# 8.1–8.4, 4.1–4.4, 22.1.

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from hydra.config import HydraSettings
from hydra.monitoring.collectors.api import APICollector
from hydra.monitoring.collectors.pipeline import PipelineCollector
from hydra.monitoring.collectors.scheduler import SchedulerCollector
from hydra.monitoring.collectors.storage import StorageCollector
from hydra.monitoring.metrics import (
    hydra_adapter_consecutive_failures,
    hydra_adapter_dead_streams,
    hydra_adapter_health_status,
    hydra_api_active_keys,
    hydra_api_job_status,
    hydra_backpressure_hard_limit,
    hydra_backpressure_soft_limit,
    hydra_backpressure_state,
    hydra_correlation_total,
    hydra_product_completeness_score,
    hydra_product_confidence_score,
    hydra_product_generated_total,
    hydra_scheduler_active_adapters,
    hydra_scheduler_active_by_cadence,
    hydra_scheduler_health_status,
    hydra_scheduler_sla_misses_total,
    hydra_storage_dlq_depth,
    hydra_storage_health_latency_seconds,
    hydra_storage_health_status,
    hydra_storage_records_total,
    hydra_storage_waq_depth,
)
from hydra.scheduler.backpressure import BackpressureState, EngineBackpressure
from hydra.scheduler.health import SchedulerHealth
from hydra.storage.health import StorageHealth


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


async def _async_iter(items):
    """Wrap an iterable so it can be consumed by ``async for``.

    ``redis.asyncio.Redis.scan_iter`` returns an async iterator; mocks
    must mirror that contract for the collectors' scan loops to work.
    """
    for item in items:
        yield item


def _make_scan_iter(items):
    """Return a callable suitable for ``Mock.scan_iter=...``.

    The callable ignores its kwargs (``match=...``) and returns a fresh
    async iterator over ``items`` every call, so multiple cycles don't
    exhaust a single generator.
    """

    def _factory(*_args, **_kwargs):
        return _async_iter(list(items))

    return _factory


# ---------------------------------------------------------------------------
# SchedulerCollector
# ---------------------------------------------------------------------------


async def test_scheduler_collector_updates_all_metrics(registry) -> None:
    """``SchedulerCollector.collect()`` writes health, concurrency,
    per-stream failure, and SLA miss metrics derived from mocked
    upstream state.

    Validates Requirements 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7.
    """
    # --- Build a SchedulerHealth with a known adapter_health entry. The
    #     stream_id must exist in the real registry so the collector can
    #     resolve its tier label. We pick the first registered source.
    tier_id, source = next(iter(registry.get_all_sources()))
    stream_id = source.name

    adapter_health = SimpleNamespace(status="DEGRADED")
    scheduler_health = SchedulerHealth(
        status="DEGRADED",
        active_adapters=7,
        active_by_cadence={"hourly": 3, "daily": 4},
        backpressure=BackpressureState(
            overall="CLEAR", engines={}, checked_at="2024-01-01T00:00:00+00:00"
        ),
        storage_health={},
        adapter_health={stream_id: adapter_health},
    )
    health_aggregator = SimpleNamespace(check=AsyncMock(return_value=scheduler_health))

    # --- ConcurrencyManager: sync attribute + sync method (wrapped in
    #     MagicMock so the collector can call it directly).
    concurrency = MagicMock()
    concurrency.active_count = 7
    # active_by_cadence returns different values depending on arg.
    concurrency.active_by_cadence = MagicMock(
        side_effect=lambda cadence: {
            "sub_minute": 1,
            "realtime": 0,
            "15min": 2,
            "hourly": 3,
            "daily": 4,
            "weekly": 0,
            "monthly_plus": 0,
        }.get(cadence, 0)
    )

    # --- Redis: scan_iter over two patterns + get() returning failure
    #     counts. Use a unique stream id so label values don't collide
    #     with other tests in the module.
    failing_stream = "prop_failing_stream_abc"
    dead_stream = "prop_dead_stream_xyz"
    failure_keys = [
        f"hydra:stream_failures:{failing_stream}".encode(),
        f"hydra:stream_failures:{dead_stream}".encode(),
    ]
    sla_keys = [b"hydra:sla_miss:dag_hourly:hourly:2024-01-01T00:00:00Z"]

    def _scan_iter_factory(*, match: str, **_kwargs):
        if match.startswith("hydra:stream_failures"):
            return _async_iter(failure_keys)
        if match.startswith("hydra:sla_miss"):
            return _async_iter(sla_keys)
        return _async_iter([])

    get_values = {
        f"hydra:stream_failures:{failing_stream}": b"2",
        f"hydra:stream_failures:{dead_stream}": b"9",
    }
    redis = MagicMock()
    redis.scan_iter = _scan_iter_factory
    redis.get = AsyncMock(side_effect=lambda key: get_values.get(key))

    collector = SchedulerCollector(
        health_aggregator=health_aggregator,
        concurrency_manager=concurrency,
        redis=redis,
        registry=registry,
        interval=60.0,
        dead_stream_threshold=5,
    )

    # Record the SLA counter baseline before running.
    sla_before = hydra_scheduler_sla_misses_total.labels(
        dag_id="dag_hourly", cadence="hourly"
    )._value.get()

    await collector.collect()

    # 1. Scheduler health gauge — DEGRADED → 1.
    assert hydra_scheduler_health_status._value.get() == 1.0

    # 2. Active adapters (global) — 7.
    assert hydra_scheduler_active_adapters._value.get() == 7.0

    # 3. Active by cadence — values match side_effect.
    assert (
        hydra_scheduler_active_by_cadence.labels(cadence="hourly")._value.get() == 3.0
    )
    assert (
        hydra_scheduler_active_by_cadence.labels(cadence="daily")._value.get() == 4.0
    )

    # 4. Per-stream adapter health — DEGRADED → 1.
    assert (
        hydra_adapter_health_status.labels(
            stream_id=stream_id, tier=str(tier_id)
        )._value.get()
        == 1.0
    )

    # 5. Consecutive failures published per stream.
    assert (
        hydra_adapter_consecutive_failures.labels(
            stream_id=failing_stream
        )._value.get()
        == 2.0
    )
    assert (
        hydra_adapter_consecutive_failures.labels(
            stream_id=dead_stream
        )._value.get()
        == 9.0
    )

    # 6. Dead streams aggregate — only dead_stream (9 >= threshold 5).
    assert hydra_adapter_dead_streams._value.get() == 1.0

    # 7. SLA misses — one new key → counter incremented exactly once.
    sla_after = hydra_scheduler_sla_misses_total.labels(
        dag_id="dag_hourly", cadence="hourly"
    )._value.get()
    assert sla_after - sla_before == 1.0

    # 8. Re-running the cycle must NOT double-count the same SLA key
    #    (the collector tracks ``_seen_sla_keys``).
    await collector.collect()
    sla_after_second = hydra_scheduler_sla_misses_total.labels(
        dag_id="dag_hourly", cadence="hourly"
    )._value.get()
    assert sla_after_second == sla_after


# ---------------------------------------------------------------------------
# StorageCollector
# ---------------------------------------------------------------------------


async def test_storage_collector_updates_all_metrics() -> None:
    """``StorageCollector.collect()`` writes per-engine health, queue
    depth, and backpressure metrics.

    Validates Requirements 6.1, 6.2, 6.3, 6.4, 6.5.
    """
    engine = "postgres"

    # --- StorageHealthAggregator: returns a dict keyed by engine with
    #     StorageHealth values (status + latency_ms).
    storage_health = SimpleNamespace(
        check_all=AsyncMock(
            return_value={
                engine: StorageHealth(
                    engine=engine,
                    status="OK",
                    latency_ms=25.0,
                )
            }
        )
    )

    # --- RedisCache: queue_depth + dlq_depth keyed on engine name.
    queue_depths = {"hydra:waq:postgres": 42}
    dlq_depths = {"hydra:dlq:postgres": 3}
    redis_cache = MagicMock()
    redis_cache.queue_depth = AsyncMock(side_effect=lambda key: queue_depths.get(key, 0))
    redis_cache.dlq_depth = AsyncMock(side_effect=lambda key: dlq_depths.get(key, 0))

    # --- BackpressureMonitor: returns a BackpressureState with one
    #     throttled engine. Other engines have no report so the
    #     collector falls back to static settings for their limits.
    engine_state = EngineBackpressure(
        engine=engine,
        queue_depth=42,
        soft_limit=1000,
        hard_limit=5000,
        state="THROTTLED",
    )
    bp_state = BackpressureState(
        overall="THROTTLED",
        engines={engine: engine_state},
        checked_at="2024-01-01T00:00:00+00:00",
    )
    backpressure = SimpleNamespace(check=AsyncMock(return_value=bp_state))

    # Use the real HydraSettings so default soft/hard limits are
    # exercised for the engines not in ``bp_state.engines``.
    settings = HydraSettings()

    collector = StorageCollector(
        storage_health=storage_health,
        redis_cache=redis_cache,
        backpressure_monitor=backpressure,
        settings=settings,
        interval=60.0,
    )

    await collector.collect()

    # 1. Health status — OK → 2, latency_ms 25.0 → 0.025s.
    assert hydra_storage_health_status.labels(engine=engine)._value.get() == 2.0
    assert hydra_storage_health_latency_seconds.labels(engine=engine)._value.get() == 0.025

    # 2. WAQ + DLQ depths.
    assert hydra_storage_waq_depth.labels(engine=engine)._value.get() == 42.0
    assert hydra_storage_dlq_depth.labels(engine=engine)._value.get() == 3.0

    # 3. Backpressure state — THROTTLED → 1.
    assert hydra_backpressure_state.labels(engine=engine)._value.get() == 1.0

    # 4. Soft/hard limits for the reported engine come from engine_state.
    assert hydra_backpressure_soft_limit.labels(engine=engine)._value.get() == 1000.0
    assert hydra_backpressure_hard_limit.labels(engine=engine)._value.get() == 5000.0

    # 5. Engines not in bp_state.engines still get static limits
    #    published from settings — check one to confirm the fallback
    #    branch executed.
    default_soft = float(settings.scheduler.backpressure_soft_limit)
    default_hard = float(settings.scheduler.backpressure_hard_limit)
    assert (
        hydra_backpressure_soft_limit.labels(engine="elasticsearch")._value.get()
        == default_soft
    )
    assert (
        hydra_backpressure_hard_limit.labels(engine="elasticsearch")._value.get()
        == default_hard
    )


# ---------------------------------------------------------------------------
# APICollector
# ---------------------------------------------------------------------------


async def test_api_collector_updates_job_and_key_metrics() -> None:
    """``APICollector.collect()`` counts jobs by status and publishes
    the active API key count from PostgreSQL.

    Validates Requirements 7.1 and 7.2.
    """
    # --- Redis: three job keys with different statuses; one malformed
    #     record is skipped gracefully. scan_iter returns bytes keys
    #     (matching the real redis-py contract); the collector decodes
    #     them to str before calling get(), so the payload dict is
    #     keyed on str.
    job_keys = [
        b"hydra:job:j1",
        b"hydra:job:j2",
        b"hydra:job:j3",
        b"hydra:job:bad",
    ]
    job_payloads = {
        "hydra:job:j1": json.dumps({"status": "running"}).encode(),
        "hydra:job:j2": json.dumps({"status": "running"}).encode(),
        "hydra:job:j3": json.dumps({"status": "completed"}).encode(),
        "hydra:job:bad": b"not-valid-json",
    }
    redis = MagicMock()
    redis.scan_iter = _make_scan_iter(job_keys)
    redis.get = AsyncMock(side_effect=lambda key: job_payloads.get(key))

    # --- asyncpg.Pool: acquire() returns an async context manager whose
    #     connection exposes fetchval.
    conn = MagicMock()
    conn.fetchval = AsyncMock(return_value=17)

    class _AcquireCtx:
        async def __aenter__(self_inner):  # noqa: N805 — ctx protocol
            return conn

        async def __aexit__(self_inner, *exc):  # noqa: N805
            return False

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=_AcquireCtx())

    collector = APICollector(redis=redis, pg_pool=pool, interval=60.0)

    await collector.collect()

    # 1. Running jobs = 2, completed = 1, others = 0.
    assert hydra_api_job_status.labels(status="running")._value.get() == 2.0
    assert hydra_api_job_status.labels(status="completed")._value.get() == 1.0
    assert hydra_api_job_status.labels(status="pending")._value.get() == 0.0
    assert hydra_api_job_status.labels(status="failed")._value.get() == 0.0

    # 2. Active API keys — the fetchval result.
    assert hydra_api_active_keys._value.get() == 17.0
    conn.fetchval.assert_awaited_once()


# ---------------------------------------------------------------------------
# PipelineCollector
# ---------------------------------------------------------------------------


async def test_pipeline_collector_updates_product_and_correlation_metrics() -> None:
    """``PipelineCollector.collect()`` increments product / correlation
    counters, observes score histograms, and snapshots record counts.

    Validates Requirements 8.1, 8.2, 8.3, 8.4.
    """
    # Use unique label values so counter deltas can be asserted without
    # interference from other tests in the session.
    product_type = "prop_test_product_type_a"
    classification = "UNCLASSIFIED_TEST"
    pipeline_id = "prop_test_pipeline_abc"
    tier = "prop_test_tier_1"
    storage_status = "prop_test_status_active"

    products_rows = [
        {
            "product_type": product_type,
            "classification": classification,
            "confidence_score": 0.87,
            "completeness_score": 0.62,
        },
        {
            "product_type": product_type,
            "classification": classification,
            "confidence_score": 0.55,
            "completeness_score": 0.40,
        },
    ]
    correlation_rows = [
        {"pipeline_id": pipeline_id, "cnt": 4},
    ]
    records_rows = [
        {"tier": tier, "storage_status": storage_status, "cnt": 250},
    ]

    async def _fetch(sql, *args):
        stripped = sql.strip().upper()
        if "INTELLIGENCE_PRODUCTS" in stripped:
            return products_rows
        if "CORRELATION_RESULTS" in stripped:
            return correlation_rows
        if "NORMALIZED_RECORDS" in stripped:
            return records_rows
        return []

    conn = MagicMock()
    conn.fetch = AsyncMock(side_effect=_fetch)

    class _AcquireCtx:
        async def __aenter__(self_inner):  # noqa: N805
            return conn

        async def __aexit__(self_inner, *exc):  # noqa: N805
            return False

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=_AcquireCtx())

    collector = PipelineCollector(pg_pool=pool, interval=60.0)

    # Capture baselines on counters/histograms.
    product_counter = hydra_product_generated_total.labels(
        product_type=product_type, classification=classification
    )
    correlation_counter = hydra_correlation_total.labels(pipeline_id=pipeline_id)
    confidence_hist = hydra_product_confidence_score.labels(product_type=product_type)
    completeness_hist = hydra_product_completeness_score.labels(product_type=product_type)

    prod_before = product_counter._value.get()
    corr_before = correlation_counter._value.get()
    conf_sum_before = confidence_hist._sum.get()
    compl_sum_before = completeness_hist._sum.get()

    watermark_before = collector.last_collection_ts

    await collector.collect()

    # 1. Product counter incremented once per row (2 rows).
    assert product_counter._value.get() - prod_before == 2.0

    # 2. Confidence histogram observed both scores.
    assert confidence_hist._sum.get() - conf_sum_before == pytest.approx(0.87 + 0.55)

    # 3. Completeness histogram observed both scores.
    assert completeness_hist._sum.get() - compl_sum_before == pytest.approx(0.62 + 0.40)

    # 4. Correlation counter incremented by the aggregated count (4).
    assert correlation_counter._value.get() - corr_before == 4.0

    # 5. Records gauge set to the rollup value.
    assert (
        hydra_storage_records_total.labels(
            tier=tier, storage_status=storage_status
        )._value.get()
        == 250.0
    )

    # 6. Watermark advanced.
    assert collector.last_collection_ts > watermark_before


# ---------------------------------------------------------------------------
# Smoke lifecycle tests — one per concrete collector
# ---------------------------------------------------------------------------


async def _run_one_cycle_then_stop(collector, collect_event: asyncio.Event) -> None:
    """Start ``collector``, await one ``collect()`` cycle, then stop and cancel.

    ``collect_event`` is set inside the mocked ``collect()`` on the first
    invocation. We use it to wait deterministically for the loop to make
    forward progress instead of sleeping.
    """
    task = await collector.start()
    try:
        await asyncio.wait_for(collect_event.wait(), timeout=2.0)
    finally:
        await collector.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def test_scheduler_collector_lifecycle(registry) -> None:
    """SchedulerCollector ``start()``/``stop()`` schedules and tears down
    the loop — smoke test for Requirements 4.1 and 4.3."""
    event = asyncio.Event()

    scheduler_health = SchedulerHealth(
        status="OK",
        active_adapters=0,
        active_by_cadence={},
        backpressure=BackpressureState(
            overall="CLEAR", engines={}, checked_at="2024-01-01T00:00:00+00:00"
        ),
        storage_health={},
        adapter_health={},
    )

    async def _check():
        event.set()
        return scheduler_health

    health_aggregator = SimpleNamespace(check=_check)

    concurrency = MagicMock()
    concurrency.active_count = 0
    concurrency.active_by_cadence = MagicMock(return_value=0)

    redis = MagicMock()
    redis.scan_iter = _make_scan_iter([])
    redis.get = AsyncMock(return_value=None)

    collector = SchedulerCollector(
        health_aggregator=health_aggregator,
        concurrency_manager=concurrency,
        redis=redis,
        registry=registry,
        interval=0.01,
    )

    await _run_one_cycle_then_stop(collector, event)
    assert event.is_set()


async def test_storage_collector_lifecycle() -> None:
    """StorageCollector ``start()``/``stop()`` smoke test — 4.1, 4.3."""
    event = asyncio.Event()

    async def _check_all():
        event.set()
        return {}

    storage_health = SimpleNamespace(check_all=_check_all)
    redis_cache = MagicMock()
    redis_cache.queue_depth = AsyncMock(return_value=0)
    redis_cache.dlq_depth = AsyncMock(return_value=0)
    backpressure = SimpleNamespace(
        check=AsyncMock(
            return_value=BackpressureState(
                overall="CLEAR",
                engines={},
                checked_at="2024-01-01T00:00:00+00:00",
            )
        )
    )
    settings = HydraSettings()

    collector = StorageCollector(
        storage_health=storage_health,
        redis_cache=redis_cache,
        backpressure_monitor=backpressure,
        settings=settings,
        interval=0.01,
    )

    await _run_one_cycle_then_stop(collector, event)
    assert event.is_set()


async def test_api_collector_lifecycle() -> None:
    """APICollector ``start()``/``stop()`` smoke test — 4.1, 4.3."""
    event = asyncio.Event()

    redis = MagicMock()
    redis.scan_iter = _make_scan_iter([])
    redis.get = AsyncMock(return_value=None)

    async def _fetchval(sql):
        event.set()
        return 0

    conn = MagicMock()
    conn.fetchval = AsyncMock(side_effect=_fetchval)

    class _AcquireCtx:
        async def __aenter__(self_inner):  # noqa: N805
            return conn

        async def __aexit__(self_inner, *exc):  # noqa: N805
            return False

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=_AcquireCtx())

    collector = APICollector(redis=redis, pg_pool=pool, interval=0.01)
    await _run_one_cycle_then_stop(collector, event)
    assert event.is_set()


async def test_pipeline_collector_lifecycle() -> None:
    """PipelineCollector ``start()``/``stop()`` smoke test — 4.1, 4.3."""
    event = asyncio.Event()

    async def _fetch(sql, *args):
        event.set()
        return []

    conn = MagicMock()
    conn.fetch = AsyncMock(side_effect=_fetch)

    class _AcquireCtx:
        async def __aenter__(self_inner):  # noqa: N805
            return conn

        async def __aexit__(self_inner, *exc):  # noqa: N805
            return False

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=_AcquireCtx())

    collector = PipelineCollector(pg_pool=pool, interval=0.01)
    await _run_one_cycle_then_stop(collector, event)
    assert event.is_set()
