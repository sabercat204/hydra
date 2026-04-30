"""Maintenance DAG — health checks, backpressure reports, dead stream detection, DLQ alerts."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from hydra.scheduler.dag_factory import DEFAULT_DAG_ARGS, sla_miss_callback

logger = logging.getLogger(__name__)

_maintenance_args = {
    **DEFAULT_DAG_ARGS,
    "retries": 1,
    "execution_timeout": timedelta(hours=1),
}


def _health_check_all_adapters(**context: Any) -> None:
    """Iterate all streams in registry, call adapter.health_check(), store results in Redis."""
    from hydra.config import settings
    from hydra.registry.stream_registry import get_registry
    from hydra.storage.redis_cache import RedisCache

    async def _run() -> dict[str, str]:
        registry = get_registry()
        redis = RedisCache(settings.database.redis_url, settings.database.redis_pool_max)
        await redis.connect()
        results: dict[str, str] = {}
        try:
            for tier in registry.tiers.values():
                for source in tier.sources:
                    stream_id = source.name.lower().replace(" ", "_").replace("/", "_")
                    try:
                        from hydra.scheduler.dag_factory import _get_task_runner
                        runner = _get_task_runner()
                        adapter_cls = runner._resolve_adapter_class(tier.adapter)
                        adapter = adapter_cls(stream_id=stream_id, settings=settings, registry=registry)
                        health = await adapter.health_check()
                        import json
                        await redis._redis.set(
                            f"hydra:adapter_health:{stream_id}",
                            json.dumps({
                                "stream_id": health.stream_id,
                                "status": health.status.value if hasattr(health.status, "value") else str(health.status),
                                "latency_ms": health.latency_ms,
                                "last_checked": health.last_checked.isoformat(),
                                "detail": health.detail,
                            }),
                        )
                        status_str = health.status.value if hasattr(health.status, "value") else str(health.status)
                        results[stream_id] = status_str
                        if status_str in ("DEGRADED", "UNREACHABLE"):
                            logger.warning(
                                "adapter_health_issue",
                                extra={"stream_id": stream_id, "status": status_str},
                            )
                    except Exception as exc:
                        logger.warning(
                            "adapter_health_check_failed",
                            extra={"stream_id": stream_id, "error": str(exc)},
                        )
                        results[stream_id] = "ERROR"
        finally:
            await redis.disconnect()
        return results

    result = asyncio.run(_run())
    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="adapter_health", value=result)


def _health_check_all_storage(**context: Any) -> None:
    """Call StorageHealthAggregator.check_all(), log per-engine status."""
    from hydra.config import settings
    from hydra.storage.redis_cache import RedisCache

    async def _run() -> dict[str, str]:
        redis = RedisCache(settings.database.redis_url, settings.database.redis_pool_max)
        await redis.connect()
        try:
            from hydra.storage.health import StorageHealthAggregator
            # In production, engines would be initialized; for maintenance we check Redis health
            aggregator = StorageHealthAggregator(engines={}, redis_cache=redis)
            checks = await aggregator.check_all()
            results = {}
            for name, health in checks.items():
                results[name] = health.status
                if health.status == "UNREACHABLE":
                    logger.critical("storage_unreachable", extra={"engine": name})
                elif health.status == "DEGRADED":
                    logger.warning("storage_degraded", extra={"engine": name})
            return results
        finally:
            await redis.disconnect()

    result = asyncio.run(_run())
    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="storage_health", value=result)


def _backpressure_report(**context: Any) -> None:
    """Call BackpressureMonitor.check(), log per-engine queue depths."""
    from hydra.config import settings
    from hydra.storage.redis_cache import RedisCache
    from hydra.scheduler.backpressure import BackpressureMonitor

    async def _run() -> dict[str, Any]:
        redis = RedisCache(settings.database.redis_url, settings.database.redis_pool_max)
        await redis.connect()
        try:
            monitor = BackpressureMonitor(redis, settings)
            state = await monitor.check()
            report: dict[str, Any] = {"overall": state.overall, "engines": {}}
            for engine, ebp in state.engines.items():
                report["engines"][engine] = {
                    "depth": ebp.queue_depth,
                    "state": ebp.state,
                }
                if ebp.state == "BLOCKED":
                    logger.error("backpressure_blocked", extra={"engine": engine, "depth": ebp.queue_depth})
                elif ebp.state == "THROTTLED":
                    logger.warning("backpressure_throttled", extra={"engine": engine, "depth": ebp.queue_depth})
            return report
        finally:
            await redis.disconnect()

    result = asyncio.run(_run())
    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="backpressure_report", value=result)


def _dead_stream_detection(**context: Any) -> None:
    """Scan all hydra:stream_failures:* keys, identify streams exceeding threshold."""
    from hydra.config import settings
    from hydra.storage.redis_cache import RedisCache

    async def _run() -> list[dict[str, Any]]:
        redis = RedisCache(settings.database.redis_url, settings.database.redis_pool_max)
        await redis.connect()
        try:
            threshold = settings.scheduler.dead_stream_threshold
            dead_streams: list[dict[str, Any]] = []
            # Scan for failure tracking keys
            cursor = 0
            while True:
                cursor, keys = await redis._redis.scan(cursor, match="hydra:stream_failures:*", count=100)
                for key in keys:
                    data = await redis._redis.hgetall(key)
                    failures = int(data.get("consecutive_failures", 0))
                    if failures >= threshold:
                        stream_id = key.split(":")[-1]
                        entry = {
                            "stream_id": stream_id,
                            "consecutive_failures": failures,
                            "last_failure_at": data.get("last_failure_at", ""),
                            "last_error": data.get("last_error", ""),
                        }
                        dead_streams.append(entry)
                        logger.error(
                            "dead_stream_detected",
                            extra=entry,
                        )
                if cursor == 0:
                    break
            return dead_streams
        finally:
            await redis.disconnect()

    result = asyncio.run(_run())
    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="dead_streams", value=result)


def _dlq_depth_alert(**context: Any) -> None:
    """Check DLQ depth for all engines, alert if exceeding threshold."""
    from hydra.config import settings
    from hydra.storage.redis_cache import RedisCache

    async def _run() -> dict[str, int]:
        redis = RedisCache(settings.database.redis_url, settings.database.redis_pool_max)
        await redis.connect()
        try:
            threshold = settings.database.reconciliation_alert_threshold
            dlq_keys = {
                "postgres": "hydra:dlq:postgres",
                "influxdb": "hydra:dlq:influxdb",
                "elasticsearch": "hydra:dlq:elasticsearch",
                "neo4j": "hydra:dlq:neo4j",
                "minio": "hydra:dlq:minio",
            }
            depths: dict[str, int] = {}
            for engine, dlq_key in dlq_keys.items():
                depth = await redis.dlq_depth(dlq_key)
                depths[engine] = depth
                if depth > threshold:
                    logger.error(
                        "dlq_depth_exceeded",
                        extra={"engine": engine, "depth": depth, "threshold": threshold},
                    )
            return depths
        finally:
            await redis.disconnect()

    result = asyncio.run(_run())
    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="dlq_depths", value=result)


# --- DAG definition ---

dag = DAG(
    dag_id="hydra_maintenance",
    schedule="@daily",
    default_args=_maintenance_args,
    max_active_runs=1,
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    tags=["hydra", "maintenance"],
)

health_check_adapters = PythonOperator(
    task_id="health_check_all_adapters",
    python_callable=_health_check_all_adapters,
    dag=dag,
)

health_check_storage = PythonOperator(
    task_id="health_check_all_storage",
    python_callable=_health_check_all_storage,
    dag=dag,
)

backpressure_report = PythonOperator(
    task_id="backpressure_report",
    python_callable=_backpressure_report,
    dag=dag,
)

dead_stream_detect = PythonOperator(
    task_id="dead_stream_detection",
    python_callable=_dead_stream_detection,
    dag=dag,
)

dlq_alert = PythonOperator(
    task_id="dlq_depth_alert",
    python_callable=_dlq_depth_alert,
    dag=dag,
)
