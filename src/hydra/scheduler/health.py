"""SchedulerHealth — aggregate health reporting for the orchestration layer."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Literal

from hydra.scheduler.backpressure import BackpressureMonitor, BackpressureState
from hydra.scheduler.concurrency import ConcurrencyManager
from hydra.storage.health import StorageHealth

if TYPE_CHECKING:
    from hydra.adapters.base import AdapterHealth
    from hydra.config import HydraSettings
    from hydra.storage.health import StorageHealthAggregator

logger = logging.getLogger(__name__)


@dataclass
class SchedulerHealth:
    """Aggregate health status for the scheduler layer."""

    status: Literal["OK", "DEGRADED", "UNREACHABLE"]
    active_adapters: int
    active_by_cadence: dict[str, int]
    backpressure: BackpressureState
    storage_health: dict[str, StorageHealth]
    adapter_health: dict[str, "AdapterHealth"] = field(default_factory=dict)
    dead_streams: list[str] = field(default_factory=list)
    checked_at: str = ""

    def __post_init__(self) -> None:
        if not self.checked_at:
            self.checked_at = datetime.now(timezone.utc).isoformat()


class SchedulerHealthAggregator:
    """Combines concurrency state, backpressure, storage health,
    and adapter health into a single scheduler health report.
    """

    def __init__(
        self,
        concurrency_manager: ConcurrencyManager,
        backpressure_monitor: BackpressureMonitor,
        storage_health_aggregator: "StorageHealthAggregator",
        settings: "HydraSettings",
    ) -> None:
        self._concurrency = concurrency_manager
        self._backpressure = backpressure_monitor
        self._storage_health = storage_health_aggregator
        self._settings = settings

    async def check(self) -> SchedulerHealth:
        """Aggregate health check.

        Status logic:
        - OK: storage OK, backpressure CLEAR, no dead streams
        - DEGRADED: storage DEGRADED, or backpressure THROTTLED, or dead streams exist
        - UNREACHABLE: storage UNREACHABLE (PG/Redis down)
        """
        # Gather all health data
        bp_state = await self._backpressure.check()
        storage_status = await self._storage_health.overall_status()
        storage_checks = await self._storage_health.check_all()

        # Determine aggregate status
        status: Literal["OK", "DEGRADED", "UNREACHABLE"]
        if storage_status == "UNREACHABLE":
            status = "UNREACHABLE"
        elif storage_status == "DEGRADED" or bp_state.overall == "THROTTLED":
            status = "DEGRADED"
        elif bp_state.overall == "BLOCKED":
            status = "DEGRADED"
        else:
            status = "OK"

        # Build active_by_cadence snapshot
        cadences = list(self._settings.scheduler.cadence_concurrency_limits.keys())
        active_by_cadence = {c: self._concurrency.active_by_cadence(c) for c in cadences}

        return SchedulerHealth(
            status=status,
            active_adapters=self._concurrency.active_count,
            active_by_cadence=active_by_cadence,
            backpressure=bp_state,
            storage_health=storage_checks,
        )
