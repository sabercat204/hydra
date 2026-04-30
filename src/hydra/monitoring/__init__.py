"""HYDRA monitoring & alerting subsystem (P12).

This package provides the observability layer: Prometheus-compatible metrics,
background metric collectors, statistical anomaly detection, capacity
planning, and SLO/error-budget computation.

The public entry points are :func:`setup_monitoring` — wires monitoring into
the FastAPI application lifecycle — and :class:`MonitoringContext` — the
handle returned by :func:`setup_monitoring` used for graceful shutdown.

Configuration is provided via :class:`hydra.config.MonitoringSettings`,
nested under ``HydraSettings.monitoring``.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI

from hydra.config import HydraSettings
from hydra.monitoring.anomaly import AnomalyDetector
from hydra.monitoring.capacity import (
    CapacityPlanner,
    ESSizeBackend,
    InfluxSizeBackend,
    MinIOSizeBackend,
)
from hydra.monitoring.collectors import (
    APICollector,
    BaseCollector,
    PipelineCollector,
    SchedulerCollector,
    StorageCollector,
)
from hydra.monitoring.instrumentator import instrument_app
from hydra.monitoring.log_config import (
    JSONLogFormatter,
    configure_monitoring_logging,
)
from hydra.monitoring.slo import SLIQueryFn, SLOComputer

if TYPE_CHECKING:
    import asyncpg
    from redis.asyncio import Redis

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# MonitoringContext
# ---------------------------------------------------------------------------


@dataclass
class MonitoringContext:
    """Handle to every background component created by :func:`setup_monitoring`.

    Populated once at application startup and passed around for graceful
    shutdown. Every field is optional-ish — components that fail to construct
    simply remain ``None`` / absent from ``collectors`` / ``tasks`` so a
    single broken dependency cannot bring down the whole app.

    Attributes:
        collectors: All BaseCollector instances that were successfully
            constructed (scheduler, storage, api, pipeline, plus
            AnomalyDetector and CapacityPlanner which also inherit from
            :class:`BaseCollector`). The latter two are additionally stored
            as named attributes for type-specific access.
        anomaly_detector: The :class:`AnomalyDetector` instance, or ``None``
            if construction failed or the pg_pool was unavailable.
        capacity_planner: The :class:`CapacityPlanner` instance, or ``None``
            if construction failed or the pg_pool was unavailable.
        slo_computer: The :class:`SLOComputer` instance. Unlike the other
            components this has no background task — it is invoked on
            demand, so only the object reference is tracked here.
        tasks: Background task handles returned by ``start()`` on each
            collector. Cancelled during :meth:`shutdown`.
    """

    collectors: list[BaseCollector] = field(default_factory=list)
    anomaly_detector: AnomalyDetector | None = None
    capacity_planner: CapacityPlanner | None = None
    slo_computer: SLOComputer | None = None
    tasks: list[asyncio.Task[None]] = field(default_factory=list)

    async def shutdown(self) -> None:
        """Stop every collector loop and cancel all background tasks.

        The shutdown is best-effort: exceptions from individual components
        are logged but do not prevent the remaining components from being
        stopped. This matches the robustness contract used throughout the
        monitoring subsystem — teardown must never propagate a failure back
        into the FastAPI lifespan machinery.
        """
        # First pass: signal every collector loop to exit after its current
        # sleep. This is cooperative — the loop checks ``_running`` at the
        # top of each iteration.
        for collector in self.collectors:
            try:
                await collector.stop()
            except Exception as exc:  # noqa: BLE001 — teardown must not propagate
                logger.warning(
                    "MonitoringContext.shutdown: collector %s.stop() raised: %s",
                    type(collector).__name__,
                    exc,
                )

        # Second pass: cancel the background tasks. Cancellation unblocks
        # any outstanding ``asyncio.sleep`` in the loops so they terminate
        # promptly rather than waiting out their full interval.
        for task in self.tasks:
            if not task.done():
                task.cancel()

        # Third pass: await each task so cancellation actually completes.
        # ``gather(..., return_exceptions=True)`` swallows the expected
        # ``CancelledError`` plus any stray exception that escaped the
        # collector loop's own error isolation.
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)


# ---------------------------------------------------------------------------
# setup_monitoring
# ---------------------------------------------------------------------------


async def setup_monitoring(
    app: FastAPI,
    *,
    settings: HydraSettings,
    pg_pool: "asyncpg.Pool | None" = None,
    redis: "Redis | None" = None,
    storage_health: Any = None,
    redis_cache: Any = None,
    backpressure_monitor: Any = None,
    scheduler_health: Any = None,
    concurrency_manager: Any = None,
    stream_registry: Any = None,
    slo_query_fn: SLIQueryFn | None = None,
    es_backend: ESSizeBackend | None = None,
    influx_backend: InfluxSizeBackend | None = None,
    minio_backend: MinIOSizeBackend | None = None,
) -> MonitoringContext:
    """Wire the monitoring subsystem into a FastAPI application.

    Responsibilities (Requirements 1.1–1.6, 23.1):

    1. Instrument ``app`` via :func:`hydra.monitoring.instrumentator.instrument_app`
       — mounts ``/metrics`` and registers the FastAPI HTTP metrics.
    2. Construct the four background collectors (scheduler, storage, api,
       pipeline) using the per-collector intervals from ``settings.monitoring``.
       Any collector whose required dependencies are ``None`` is silently
       skipped — this is useful in tests / ad-hoc ``create_app`` calls that
       do not wire a full infrastructure stack.
    3. Construct :class:`AnomalyDetector` and :class:`CapacityPlanner` — both
       require a ``pg_pool`` and are skipped when one is not available.
    4. Construct :class:`SLOComputer`. It has no background task (SLOs are
       computed on demand via ``compute_slo`` / ``compute_all``) — only the
       object reference is stored on the returned context.
    5. Start every successfully-constructed collector as an ``asyncio.Task``
       and return the populated :class:`MonitoringContext`.

    Robustness: every component construction is individually guarded. If one
    of them raises during construction or startup, the error is logged and
    the remaining components continue to be wired. The only strictly
    essential step is the instrumentator — everything else is additive.

    Args:
        app: FastAPI application being instrumented.
        settings: Root HYDRA settings (``settings.monitoring`` is consumed
            by the per-component constructors).
        pg_pool: Async PostgreSQL pool shared by APICollector,
            PipelineCollector, AnomalyDetector, and CapacityPlanner. When
            ``None``, these components are skipped.
        redis: Async Redis client shared by SchedulerCollector and
            APICollector. When ``None``, these collectors are skipped.
        storage_health: ``StorageHealthAggregator`` for StorageCollector.
        redis_cache: ``RedisCache`` wrapper for StorageCollector.
        backpressure_monitor: ``BackpressureMonitor`` for StorageCollector.
        scheduler_health: ``SchedulerHealthAggregator`` for SchedulerCollector.
        concurrency_manager: ``ConcurrencyManager`` for SchedulerCollector.
        stream_registry: ``StreamRegistry`` for SchedulerCollector.
        slo_query_fn: Optional SLI query backend passed to
            :class:`SLOComputer`. Defaults to the built-in failing stub.
        es_backend: Optional :class:`ESSizeBackend` for CapacityPlanner.
        influx_backend: Optional :class:`InfluxSizeBackend` for CapacityPlanner.
        minio_backend: Optional :class:`MinIOSizeBackend` for CapacityPlanner.

    Returns:
        Populated :class:`MonitoringContext` with references to every
        successfully-constructed component and task.
    """
    ctx = MonitoringContext()

    # --- 0. Structured logging -------------------------------------------
    # Configure JSON logging for every ``hydra.monitoring.*`` logger
    # before any of the downstream components emit their first message.
    # This is a no-op when ``settings.monitoring.log_format`` is "text".
    # Requirements 26.1, 26.2.
    try:
        configure_monitoring_logging(settings.monitoring)
    except Exception as exc:  # noqa: BLE001 — logging setup must not crash startup
        logger.warning("setup_monitoring: logging configuration failed: %s", exc)

    # --- 1. Instrumentator (essential) -----------------------------------
    # If this fails we let the exception propagate: the ``/metrics``
    # endpoint is the core deliverable of setup_monitoring, and a FastAPI
    # instrumentation failure usually indicates a programming error (e.g.,
    # double-instrumenting the same app) that warrants a loud failure.
    instrument_app(app)

    monitoring = settings.monitoring

    # --- 2. SchedulerCollector --------------------------------------------
    if (
        scheduler_health is not None
        and concurrency_manager is not None
        and redis is not None
        and stream_registry is not None
    ):
        try:
            collector: BaseCollector = SchedulerCollector(
                health_aggregator=scheduler_health,
                concurrency_manager=concurrency_manager,
                redis=redis,
                registry=stream_registry,
                interval=monitoring.scheduler_collector_interval,
                dead_stream_threshold=settings.scheduler.dead_stream_threshold,
            )
            ctx.collectors.append(collector)
        except Exception as exc:  # noqa: BLE001 — monitoring must be resilient
            logger.warning("setup_monitoring: SchedulerCollector init failed: %s", exc)
    else:
        logger.debug(
            "setup_monitoring: skipping SchedulerCollector "
            "(missing scheduler_health / concurrency_manager / redis / stream_registry)"
        )

    # --- 3. StorageCollector ----------------------------------------------
    if (
        storage_health is not None
        and redis_cache is not None
        and backpressure_monitor is not None
    ):
        try:
            ctx.collectors.append(
                StorageCollector(
                    storage_health=storage_health,
                    redis_cache=redis_cache,
                    backpressure_monitor=backpressure_monitor,
                    settings=settings,
                    interval=monitoring.storage_collector_interval,
                )
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("setup_monitoring: StorageCollector init failed: %s", exc)
    else:
        logger.debug(
            "setup_monitoring: skipping StorageCollector "
            "(missing storage_health / redis_cache / backpressure_monitor)"
        )

    # --- 4. APICollector --------------------------------------------------
    if redis is not None and pg_pool is not None:
        try:
            ctx.collectors.append(
                APICollector(
                    redis=redis,
                    pg_pool=pg_pool,
                    interval=monitoring.api_collector_interval,
                )
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("setup_monitoring: APICollector init failed: %s", exc)
    else:
        logger.debug(
            "setup_monitoring: skipping APICollector (missing redis / pg_pool)"
        )

    # --- 5. PipelineCollector ---------------------------------------------
    if pg_pool is not None:
        try:
            ctx.collectors.append(
                PipelineCollector(
                    pg_pool=pg_pool,
                    interval=monitoring.pipeline_collector_interval,
                )
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("setup_monitoring: PipelineCollector init failed: %s", exc)
    else:
        logger.debug("setup_monitoring: skipping PipelineCollector (missing pg_pool)")

    # --- 6. AnomalyDetector ----------------------------------------------
    # AnomalyDetector also inherits from BaseCollector — tracked in both
    # ``collectors`` (for shutdown) and ``anomaly_detector`` (for typed
    # access by callers).
    if pg_pool is not None:
        try:
            detector = AnomalyDetector(pg_pool=pg_pool, settings=monitoring)
            ctx.anomaly_detector = detector
            ctx.collectors.append(detector)
        except Exception as exc:  # noqa: BLE001
            logger.warning("setup_monitoring: AnomalyDetector init failed: %s", exc)
    else:
        logger.debug("setup_monitoring: skipping AnomalyDetector (missing pg_pool)")

    # --- 7. CapacityPlanner ----------------------------------------------
    if pg_pool is not None:
        try:
            planner = CapacityPlanner(
                pg_pool=pg_pool,
                settings=monitoring,
                es_backend=es_backend,
                influx_backend=influx_backend,
                minio_backend=minio_backend,
            )
            ctx.capacity_planner = planner
            ctx.collectors.append(planner)
        except Exception as exc:  # noqa: BLE001
            logger.warning("setup_monitoring: CapacityPlanner init failed: %s", exc)
    else:
        logger.debug("setup_monitoring: skipping CapacityPlanner (missing pg_pool)")

    # --- 8. SLOComputer (no background task) ------------------------------
    # The design calls for SLOs to be computed on demand (e.g., from a
    # handler or via a Prometheus recording rule scrape). No asyncio.Task
    # is started here.
    try:
        ctx.slo_computer = SLOComputer(settings=monitoring, sli_query=slo_query_fn)
    except Exception as exc:  # noqa: BLE001
        logger.warning("setup_monitoring: SLOComputer init failed: %s", exc)

    # --- 9. Start every collector task -----------------------------------
    for collector in ctx.collectors:
        try:
            task = await collector.start()
            ctx.tasks.append(task)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "setup_monitoring: failed to start %s: %s",
                type(collector).__name__,
                exc,
            )

    logger.info(
        "setup_monitoring: started %d collector(s); anomaly=%s capacity=%s slo=%s",
        len(ctx.tasks),
        ctx.anomaly_detector is not None,
        ctx.capacity_planner is not None,
        ctx.slo_computer is not None,
    )
    return ctx


__all__ = [
    "JSONLogFormatter",
    "MonitoringContext",
    "configure_monitoring_logging",
    "setup_monitoring",
]
