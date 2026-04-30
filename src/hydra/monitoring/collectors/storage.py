"""StorageCollector — scrapes storage engine health, queue depths, and backpressure (P12 §6.2).

Concrete :class:`BaseCollector` that polls the
:class:`~hydra.storage.health.StorageHealthAggregator`,
:class:`~hydra.scheduler.backpressure.BackpressureMonitor`, and the
:class:`~hydra.storage.redis_cache.RedisCache` queue-depth interface,
then publishes the results through the Prometheus custom metrics
registry.

Satisfies Requirements 6.1–6.5.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final

from hydra.monitoring.collectors import BaseCollector
from hydra.monitoring.metrics import (
    hydra_backpressure_hard_limit,
    hydra_backpressure_soft_limit,
    hydra_backpressure_state,
    hydra_storage_dlq_depth,
    hydra_storage_health_latency_seconds,
    hydra_storage_health_status,
    hydra_storage_waq_depth,
)
from hydra.scheduler.backpressure import ENGINE_QUEUE_KEYS

if TYPE_CHECKING:
    from hydra.config import HydraSettings
    from hydra.scheduler.backpressure import BackpressureMonitor
    from hydra.storage.health import StorageHealthAggregator
    from hydra.storage.redis_cache import RedisCache

logger = logging.getLogger(__name__)


# Gauge encodings per Requirements 6.1 (health) and 6.4 (backpressure).
_HEALTH_STATUS_MAP: Final[dict[str, int]] = {
    "UNREACHABLE": 0,
    "DEGRADED": 1,
    "OK": 2,
}
_BACKPRESSURE_MAP: Final[dict[str, int]] = {
    "CLEAR": 0,
    "THROTTLED": 1,
    "BLOCKED": 2,
}

# DLQ key template matches StorageRouter convention (see backpressure.py).
_DLQ_KEY_TEMPLATE: Final[str] = "hydra:dlq:{engine}"


class StorageCollector(BaseCollector):
    """Collect per-engine health, WAQ/DLQ depth, and backpressure metrics.

    Each ``collect()`` cycle:

    1. Calls :meth:`StorageHealthAggregator.check_all` and publishes
       :data:`hydra_storage_health_status` and
       :data:`hydra_storage_health_latency_seconds` per engine.
    2. Reads per-engine queue depth via
       :meth:`RedisCache.queue_depth` and publishes
       :data:`hydra_storage_waq_depth`.
    3. Reads per-engine DLQ depth via :meth:`RedisCache.dlq_depth` and
       publishes :data:`hydra_storage_dlq_depth`.
    4. Calls :meth:`BackpressureMonitor.check` and publishes
       :data:`hydra_backpressure_state` per engine plus the static
       :data:`hydra_backpressure_soft_limit` /
       :data:`hydra_backpressure_hard_limit` gauges.
    """

    def __init__(
        self,
        storage_health: "StorageHealthAggregator",
        redis_cache: "RedisCache",
        backpressure_monitor: "BackpressureMonitor",
        settings: "HydraSettings",
        interval: float = 30.0,
    ) -> None:
        super().__init__(interval=interval)
        self._storage_health = storage_health
        self._redis_cache = redis_cache
        self._backpressure = backpressure_monitor
        self._settings = settings
        # Engines managed by the backpressure/WAQ layer. Used to drive both
        # WAQ/DLQ depth scans and static limit gauges — limiting label
        # cardinality to this known set (Requirement 3.3).
        self._engines: tuple[str, ...] = tuple(ENGINE_QUEUE_KEYS.keys())

    async def collect(self) -> None:
        await self._update_health_metrics()
        await self._update_queue_depths()
        await self._update_backpressure_metrics()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _update_health_metrics(self) -> None:
        """Publish per-engine storage health status and latency."""
        checks = await self._storage_health.check_all()
        for engine, health in checks.items():
            hydra_storage_health_status.labels(engine=engine).set(
                _HEALTH_STATUS_MAP.get(str(health.status), 0)
            )
            # StorageHealth.latency_ms is milliseconds — convert to seconds
            # for consistency with the ``*_seconds`` gauge unit suffix.
            hydra_storage_health_latency_seconds.labels(engine=engine).set(
                health.latency_ms / 1000.0
            )

    async def _update_queue_depths(self) -> None:
        """Publish WAQ and DLQ depths per engine."""
        for engine in self._engines:
            waq_key = ENGINE_QUEUE_KEYS[engine]
            dlq_key = _DLQ_KEY_TEMPLATE.format(engine=engine)

            waq_depth = await self._redis_cache.queue_depth(waq_key)
            hydra_storage_waq_depth.labels(engine=engine).set(waq_depth)

            dlq_depth = await self._redis_cache.dlq_depth(dlq_key)
            hydra_storage_dlq_depth.labels(engine=engine).set(dlq_depth)

    async def _update_backpressure_metrics(self) -> None:
        """Publish backpressure state + static soft/hard limits per engine."""
        state = await self._backpressure.check()
        scheduler_settings = self._settings.scheduler
        overrides = scheduler_settings.engine_backpressure_overrides

        for engine, engine_state in state.engines.items():
            hydra_backpressure_state.labels(engine=engine).set(
                _BACKPRESSURE_MAP.get(str(engine_state.state), 0)
            )
            hydra_backpressure_soft_limit.labels(engine=engine).set(
                engine_state.soft_limit
            )
            hydra_backpressure_hard_limit.labels(engine=engine).set(
                engine_state.hard_limit
            )

        # Engines not reported by BackpressureMonitor.check() (e.g. newly
        # configured) still need their static limits published so dashboards
        # can show the configured ceilings before the first data point.
        for engine in self._engines:
            if engine in state.engines:
                continue
            engine_overrides = overrides.get(engine, {})
            soft_limit = engine_overrides.get(
                "soft_limit", scheduler_settings.backpressure_soft_limit
            )
            hard_limit = engine_overrides.get(
                "hard_limit", scheduler_settings.backpressure_hard_limit
            )
            hydra_backpressure_soft_limit.labels(engine=engine).set(soft_limit)
            hydra_backpressure_hard_limit.labels(engine=engine).set(hard_limit)


__all__ = ["StorageCollector"]
