"""FastAPI application factory.

The application factory wires middleware, routers, exception handlers, and
— when the matching infrastructure dependencies are supplied — the P12
monitoring subsystem via a lifespan context.

The monitoring hooks are optional: tests and ad-hoc callers frequently
invoke ``create_app()`` with only a subset of the real runtime
dependencies (typically just ``settings`` and ``redis``). In that case the
lifespan still runs, but ``setup_monitoring`` is skipped so the caller
gets a working FastAPI instance without requiring a live PostgreSQL pool.
A production startup path supplies ``pg_pool`` (and optionally the other
upstream handles) so monitoring is fully activated.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.middleware.cors import CORSMiddleware

from hydra.api.errors import (
    HydraAPIException,
    hydra_exception_handler,
    internal_handler,
    validation_handler,
)
from hydra.api.middleware import RateLimitMiddleware, RequestIDMiddleware, TimingMiddleware
from hydra.api.routers import all_routers
from hydra.config import HydraSettings, settings as default_settings
from hydra.monitoring import MonitoringContext, setup_monitoring

if TYPE_CHECKING:
    import asyncpg
    from redis.asyncio import Redis

    from hydra.monitoring.capacity import (
        ESSizeBackend,
        InfluxSizeBackend,
        MinIOSizeBackend,
    )
    from hydra.monitoring.slo import SLIQueryFn


def create_app(
    settings: HydraSettings | None = None,
    redis: Any = None,
    *,
    pg_pool: "asyncpg.Pool | None" = None,
    storage_health: Any = None,
    redis_cache: Any = None,
    backpressure_monitor: Any = None,
    scheduler_health: Any = None,
    concurrency_manager: Any = None,
    stream_registry: Any = None,
    slo_query_fn: "SLIQueryFn | None" = None,
    es_backend: "ESSizeBackend | None" = None,
    influx_backend: "InfluxSizeBackend | None" = None,
    minio_backend: "MinIOSizeBackend | None" = None,
) -> FastAPI:
    """FastAPI application factory.

    1. Initialize FastAPI with metadata and a monitoring-aware lifespan.
    2. Add middleware stack (outermost first).
    3. Include all routers under /api/v1 prefix.
    4. Register exception handlers.

    Monitoring integration: when ``pg_pool`` is supplied, the lifespan
    invokes :func:`hydra.monitoring.setup_monitoring` during startup and
    tears it down on shutdown. When ``pg_pool`` is ``None`` (the common
    case in tests), the lifespan no-ops for monitoring so the test doesn't
    need to provide a live database pool.

    Args:
        settings: Root HYDRA settings. Defaults to the module-level
            ``settings`` loaded at import time.
        redis: Low-level async Redis client used by rate-limit middleware
            and (when monitoring is enabled) by SchedulerCollector /
            APICollector.
        pg_pool: Async PostgreSQL pool. When ``None``, monitoring is
            skipped. When provided, it is passed through to
            ``setup_monitoring`` so the API / pipeline / anomaly / capacity
            components can start their background tasks.
        storage_health: ``StorageHealthAggregator`` — optional monitoring dep.
        redis_cache: ``RedisCache`` wrapper — optional monitoring dep.
        backpressure_monitor: ``BackpressureMonitor`` — optional monitoring dep.
        scheduler_health: ``SchedulerHealthAggregator`` — optional monitoring dep.
        concurrency_manager: ``ConcurrencyManager`` — optional monitoring dep.
        stream_registry: ``StreamRegistry`` — optional monitoring dep.
        slo_query_fn: SLI query backend — optional monitoring dep.
        es_backend: Capacity planner ES backend — optional monitoring dep.
        influx_backend: Capacity planner InfluxDB backend — optional monitoring dep.
        minio_backend: Capacity planner MinIO backend — optional monitoring dep.
    """
    if settings is None:
        settings = default_settings

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """App startup / shutdown — wires monitoring when pg_pool is available."""
        monitoring_ctx: MonitoringContext | None = None
        try:
            # Only wire monitoring when the core dependency (pg_pool) is
            # available. Tests frequently call create_app() without one,
            # and we must not force them to stand up a live database just
            # to build a FastAPI instance.
            if pg_pool is not None:
                monitoring_ctx = await setup_monitoring(
                    app,
                    settings=settings,
                    pg_pool=pg_pool,
                    redis=redis,
                    storage_health=storage_health,
                    redis_cache=redis_cache,
                    backpressure_monitor=backpressure_monitor,
                    scheduler_health=scheduler_health,
                    concurrency_manager=concurrency_manager,
                    stream_registry=stream_registry,
                    slo_query_fn=slo_query_fn,
                    es_backend=es_backend,
                    influx_backend=influx_backend,
                    minio_backend=minio_backend,
                )
            yield
        finally:
            if monitoring_ctx is not None:
                await monitoring_ctx.shutdown()

    app = FastAPI(
        title="HYDRA OSINT Platform",
        version="0.1.0",
        description="REST API for the HYDRA OSINT Aggregation & Correlation Platform",
        lifespan=lifespan,
    )

    # Middleware stack — order matters (outermost first)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.api.cors_origins,
        allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
        allow_headers=["*"],
        expose_headers=[
            "X-Request-ID",
            "X-RateLimit-Limit",
            "X-RateLimit-Remaining",
            "X-RateLimit-Reset",
        ],
    )
    app.add_middleware(RequestIDMiddleware)
    app.add_middleware(TimingMiddleware)
    app.add_middleware(RateLimitMiddleware, redis=redis, settings=settings)

    # Include routers
    prefix = settings.api.api_prefix
    for router in all_routers:
        app.include_router(router, prefix=prefix)

    # Mil-Int surface routers (their paths already include the /api/v1
    # prefix, so we mount them with an empty prefix — same pattern as EAS).
    from hydra.mil_int.setup import mount_mil_int_routers

    mount_mil_int_routers(app)

    # Exception handlers
    app.add_exception_handler(HydraAPIException, hydra_exception_handler)  # type: ignore[arg-type]
    app.add_exception_handler(RequestValidationError, validation_handler)  # type: ignore[arg-type]
    app.add_exception_handler(Exception, internal_handler)  # type: ignore[arg-type]

    return app
