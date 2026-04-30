"""SchedulerCollector — scrapes scheduler, concurrency, and adapter health (P12 §6.1).

Concrete :class:`BaseCollector` that periodically queries the scheduler
health aggregator, the concurrency manager, and Redis for stream-level
failure and SLA-miss state, then pushes the results into the Prometheus
custom metrics registry.

Upstream contracts (see design.md §Components 4):
- :class:`hydra.scheduler.health.SchedulerHealthAggregator` — ``check()``
  returns a :class:`SchedulerHealth` dataclass with ``status``,
  ``active_adapters``, ``active_by_cadence``, and ``adapter_health``.
- :class:`hydra.scheduler.concurrency.ConcurrencyManager` — exposes
  ``active_count`` (property) and ``active_by_cadence(cadence)`` (method).
- :class:`redis.asyncio.Redis` — raw async client used for ``scan_iter``
  over ``hydra:stream_failures:*`` and ``hydra:sla_miss:*`` keys.
- :class:`hydra.registry.stream_registry.StreamRegistry` — cap labels
  to streams actually defined in the registry.

Satisfies Requirements 5.1–5.7.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final

from hydra.monitoring.collectors import BaseCollector
from hydra.monitoring.metrics import (
    hydra_adapter_consecutive_failures,
    hydra_adapter_dead_streams,
    hydra_adapter_health_status,
    hydra_scheduler_active_adapters,
    hydra_scheduler_active_by_cadence,
    hydra_scheduler_health_status,
    hydra_scheduler_sla_misses_total,
)

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from hydra.registry.stream_registry import StreamRegistry
    from hydra.scheduler.concurrency import ConcurrencyManager
    from hydra.scheduler.health import SchedulerHealthAggregator

logger = logging.getLogger(__name__)


# Map Literal status strings produced by SchedulerHealthAggregator /
# StorageHealthAggregator / HealthStatus enum into the gauge encoding
# defined by Requirements 5.1 and 6.1.
_HEALTH_STATUS_MAP: Final[dict[str, int]] = {
    "UNREACHABLE": 0,
    "DEGRADED": 1,
    "OK": 2,
}

# Redis key patterns scanned by the scheduler collector.
_STREAM_FAILURES_PATTERN: Final[str] = "hydra:stream_failures:*"
_SLA_MISS_PATTERN: Final[str] = "hydra:sla_miss:*"

# Dead-stream consecutive-failure threshold (mirrors SchedulerSettings.dead_stream_threshold).
# Kept local so the collector doesn't depend on the full HydraSettings tree
# when SchedulerSettings already exposes the value.
_DEFAULT_DEAD_STREAM_THRESHOLD: Final[int] = 5


def _status_to_gauge(status: object) -> int:
    """Translate a status (Literal string or HealthStatus enum) to the gauge value."""
    if hasattr(status, "value"):
        status = status.value  # HealthStatus enum
    return _HEALTH_STATUS_MAP.get(str(status), 0)


class SchedulerCollector(BaseCollector):
    """Collect scheduler, adapter-health, and SLA-miss metrics from upstream state.

    Each ``collect()`` cycle:

    1. Calls :meth:`SchedulerHealthAggregator.check` and updates
       :data:`hydra_scheduler_health_status`.
    2. Reads :attr:`ConcurrencyManager.active_count` into
       :data:`hydra_scheduler_active_adapters`.
    3. Iterates the registry's cadences and calls
       :meth:`ConcurrencyManager.active_by_cadence` to populate
       :data:`hydra_scheduler_active_by_cadence`.
    4. Scans ``hydra:stream_failures:*`` and updates per-stream
       :data:`hydra_adapter_consecutive_failures` + the aggregate
       :data:`hydra_adapter_dead_streams`.
    5. Scans ``hydra:sla_miss:*`` since the last cycle and increments
       :data:`hydra_scheduler_sla_misses_total`.
    6. Updates per-stream :data:`hydra_adapter_health_status` from
       ``SchedulerHealth.adapter_health``.
    """

    def __init__(
        self,
        health_aggregator: "SchedulerHealthAggregator",
        concurrency_manager: "ConcurrencyManager",
        redis: "Redis",
        registry: "StreamRegistry",
        interval: float = 30.0,
        dead_stream_threshold: int = _DEFAULT_DEAD_STREAM_THRESHOLD,
    ) -> None:
        super().__init__(interval=interval)
        self._health = health_aggregator
        self._concurrency = concurrency_manager
        self._redis = redis
        self._registry = registry
        self._dead_stream_threshold = dead_stream_threshold
        # Track SLA miss keys already accounted for so each event is counted
        # exactly once across collection cycles.
        self._seen_sla_keys: set[str] = set()

    async def collect(self) -> None:
        await self._update_scheduler_health()
        await self._update_concurrency_metrics()
        await self._update_stream_failures()
        await self._update_sla_misses()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _update_scheduler_health(self) -> None:
        """Call SchedulerHealthAggregator.check() and publish status + adapter health."""
        health = await self._health.check()
        hydra_scheduler_health_status.set(_status_to_gauge(health.status))

        # Per-stream adapter health (Requirement 5.7).
        for stream_id, adapter_health in (health.adapter_health or {}).items():
            tier = _resolve_tier_label(self._registry, stream_id)
            hydra_adapter_health_status.labels(
                stream_id=stream_id, tier=tier
            ).set(_status_to_gauge(adapter_health.status))

    async def _update_concurrency_metrics(self) -> None:
        """Update active-adapter and per-cadence active counts."""
        hydra_scheduler_active_adapters.set(self._concurrency.active_count)

        # Iterate known cadences from the registry's scheduler_cadences.
        # Fall back to the cadences listed on tier definitions if empty.
        cadences = _extract_cadences(self._registry)
        for cadence in cadences:
            count = self._concurrency.active_by_cadence(cadence)
            hydra_scheduler_active_by_cadence.labels(cadence=cadence).set(count)

    async def _update_stream_failures(self) -> None:
        """Scan hydra:stream_failures:* and update failure/dead-stream gauges."""
        dead_count = 0
        async for raw_key in self._redis.scan_iter(match=_STREAM_FAILURES_PATTERN):
            key = raw_key.decode() if isinstance(raw_key, (bytes, bytearray)) else raw_key
            stream_id = key.rsplit(":", 1)[-1]
            value = await self._redis.get(key)
            if value is None:
                failures = 0
            else:
                try:
                    failures = int(value)
                except (TypeError, ValueError):
                    failures = 0
            hydra_adapter_consecutive_failures.labels(stream_id=stream_id).set(failures)
            if failures >= self._dead_stream_threshold:
                dead_count += 1

        hydra_adapter_dead_streams.set(dead_count)

    async def _update_sla_misses(self) -> None:
        """Scan hydra:sla_miss:* and increment SLA miss counter for new events."""
        current_keys: set[str] = set()
        async for raw_key in self._redis.scan_iter(match=_SLA_MISS_PATTERN):
            key = raw_key.decode() if isinstance(raw_key, (bytes, bytearray)) else raw_key
            current_keys.add(key)

        new_keys = current_keys - self._seen_sla_keys
        for key in new_keys:
            dag_id, cadence = _parse_sla_key(key)
            hydra_scheduler_sla_misses_total.labels(
                dag_id=dag_id, cadence=cadence
            ).inc()

        # Retain only keys still present so expired/deleted events can be
        # re-counted if Redis re-creates the same key path later.
        self._seen_sla_keys = current_keys


def _resolve_tier_label(registry: "StreamRegistry", stream_id: str) -> str:
    """Return the tier label for a stream_id, or ``"unknown"`` if not found.

    Stream IDs are not guaranteed to map 1:1 to a tier (a single tier owns
    many sources), so we look up sources by name and return the owning
    tier ID as a string. The label has cardinality bounded by the number
    of registered tiers (~28), which satisfies Requirement 3.3.
    """
    for tier_id, source in registry.get_all_sources():
        if source.name == stream_id:
            return str(tier_id)
    return "unknown"


def _extract_cadences(registry: "StreamRegistry") -> list[str]:
    """Return the cadences known to the registry (ordered, de-duplicated)."""
    cadences: list[str] = []
    seen: set[str] = set()
    for cadence in registry.scheduler_cadences:
        if cadence not in seen:
            cadences.append(cadence)
            seen.add(cadence)
    if cadences:
        return cadences
    # Fallback: derive from tier definitions if scheduler_cadences is empty.
    for tier in registry.tiers.values():
        if tier.cadence and tier.cadence not in seen:
            cadences.append(tier.cadence)
            seen.add(tier.cadence)
    return cadences


def _parse_sla_key(key: str) -> tuple[str, str]:
    """Extract (dag_id, cadence) labels from an SLA miss Redis key.

    Keys follow the convention ``hydra:sla_miss:<dag_id>:<cadence>[:...]``.
    Fallback to ``"unknown"`` for missing segments rather than dropping
    the event — keeps the counter monotonic and traceable.
    """
    segments = key.split(":")
    # segments[0:2] == ["hydra", "sla_miss"]
    dag_id = segments[2] if len(segments) > 2 else "unknown"
    cadence = segments[3] if len(segments) > 3 else "unknown"
    return dag_id, cadence


__all__ = ["SchedulerCollector"]
